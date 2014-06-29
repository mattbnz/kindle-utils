#!/usr/bin/env python
#
# This file is released under the GPLv2 license.
#     Copyright (C) 2012 Matt Brown <matt@mattb.net.nz>

from datetime import datetime, timedelta, tzinfo
import code
import cPickle as pickle
import logging
import optparse
import os
import pytz
import re
import sys
import time

if sys.hexversion < 0x02070000:
    sys.exit("Python 2.7 or newer is required to run this program.")

TS_REGEXP = re.compile(r'^(\d{6}:\d{6})')

logger = logging.getLogger().getChild('log_parser')

def EqualWithFuzz(a, b, fuzz=300):
    """True if a and b are within fuzz seconds of each other."""
    if a == b: return True
    if abs(a-b) < fuzz: return True
    return False

def MatchWithFuzzByHour(ref, value):
    """Returns unfuzzy matched value if value is equal to ref +/- 1h.
    
    E.g.
    (3600, 3600) -> 3600
    (3600, 3601) -> 3600
    (3600, 3901) -> None
    (3600, 7200) -> 7200
    """
    if EqualWithFuzz(ref, value): return ref
    if ref > 3600 and EqualWithFuzz(ref - 3600, value): return ref - 3600
    if EqualWithFuzz(ref + 3600, value): return ref + 3600
    return None

def FormatTime(ts):
    return time.strftime('%Y-%m-%d-%H:%M:%S', time.localtime(ts))


class KindleLogState(object):

    DEFAULT_TZ = pytz.timezone('Europe/Dublin')
    
    def __init__(self, copy_from):
        if not copy_from:
            self._reset()
            return
        if not isinstance(copy_from, KindleLogState):
            raise TypeError
        self.last_filename = copy_from.last_filename
        self.last_ts = copy_from.last_ts
        self.next_tz_jump = copy_from.next_tz_jump
        self.next_tz = copy_from.next_tz
        self.old_tz = copy_from.old_tz
        self.old_tz_jump = copy_from.old_tz_jump
        self.timezone = copy_from.timezone
        self.power_state = copy_from.power_state
        self.base_realtime = copy_from.base_realtime
        self.base_badtime = copy_from.base_badtime
        self.book = copy_from.book

    def _reset(self):
        # name of the last file fully processed.
        self.last_filename = None
        # timestamp of last line processed, in 'realtime', i.e. with jump fixes
        # applied if they are active.
        self.last_ts = -1
        # These two variables track upcoming changes in timezone, they're set
        # when we see a log line indicating the timezone has changed. The first
        # tracks the expected jump in time we should see (in seconds), the
        # second variable describes the new timezone.
        self.next_tz_jump = None
        self.next_tz = None
        # After we swap timezones, we track the size of the jump so we can pick
        # up lines still logged in the old timezone.
        self.old_tz = None
        self.old_tz_jump = None
        # The current timezone.
        self.timezone = self.DEFAULT_TZ
        # The current state of the device, and the time it was entered.
        # (ts, state).
        self.power_state = (None, None)
        # The last 'good' time we saw
        self.base_realtime = None
        # The first 'bad' time we saw.
        self.base_badtime = None
        # Current book.
        self.book = None

    @property
    def _tz(self):
        tz_brackets = ''
        tz_was_str = None
        tz_next_str = None
        if self.old_tz_jump:
            tz_was_str = 'last tz jump of %s from %s' % (
                    self.old_tz_jump, self.old_tz)
        if self.next_tz_jump:
            tz_next_str = 'expecting tz jump of %s to %s' % (
                    self.next_tz_jump, self.next_tz)
        if tz_was_str or tz_next_str:
            tz_brackets = ' (%s)' % (
                    ', '.join(filter(None, [tz_was_str, tz_next_str])))
        return 'tz %s%s' % (self.timezone, tz_brackets)

    @property
    def _jump(self):
        if self.base_realtime:
            return 'jump bad=%s, real=%s' % (
                    FormatTime(self.base_badtime),
                    FormatTime(self.base_realtime))
        return None

    @property
    def _state(self):
        if self.power_state[0] == self.last_ts:
            state_ts = ''
        else:
            state_ts = '@%s' % FormatTime(self.power_state[0])
        return '%s%s in %s' % (self.power_state[1], state_ts, self.book)

    def __repr__(self):
        return '%s@%s: %s' % (
                self.last_filename, FormatTime(self.last_ts),
                '; '.join(filter(None, [self._state, self._tz, self._jump])))

    @classmethod
    def DefaultState(cls):
        d = cls()
        d._reset()
        return d


class KindleBook(object):

    # Events in a books life. The idea is that you have a single book off the
    # shelf which you open and close as you read. Once you're down with it you
    # put it back on the shelf, and pick up a new book.
    PICK_UP = 1    # Picked up off the shelf, implies OPEN.
    PUT_DOWN = 2   # Put back on shelf, implies CLOSE.
    OPEN = 3       # Opened to read.
    CLOSE = 4      # Closed for now.

    # Minimum amount of time a book must be picked up for it to be considered
    # that it was actually read, as opposed to picked up and discarded again.
    MIN_IN_HAND_SECS = 2 * 60
    
    # Minimum amount of time a book must be picked up for it to be consider
    # that it was actually read, if the reading appeared to progress backwards
    # through the book.
    MIN_IN_HAND_REVERSE_SECS = 10 * 60

    def __init__(self, asin, length):
        self.asin = asin
        if length:
            self.length = self._FixPosition(length)
        else:
            self.length = 0
        self.events = []

    def _CoalesceLast(self, ts, new_event, match_old=None, old_fuzz=1):
        """Coalesce consecutive events into one."""
        last = self.events and self.events[-1] or None
        position = None
        if last:
            if EqualWithFuzz(last[0], ts, 1):
                self.events[-1][0] = min(last[0], ts)
                self.events[-1][1] = new_event
                return                
            if last[1] == match_old and EqualWithFuzz(last[0], ts, old_fuzz):
                self.events[-1][0] = min(last[0], ts)
                self.events[-1][1] = new_event
                return
            position = last[2]
        self.events.append([ts, new_event, position])

    def _FixPosition(self, position):
        if ' ' in position:
            parts = position.split(' ')
            return int(float(parts[-1]))
        else:
            try:
                return int(float(position))
            except ValueError, e:
                logger.debug('Could not parse position \'%s\' in book %s: %s',
                              position, self.asin, e)
                return 0

    def PickUp(self, ts, position):
        self._CoalesceLast(ts, self.PICK_UP)
        if position:
            self.events[-1][2] = self._FixPosition(position)

    def PutDown(self, ts):
        self._CoalesceLast(ts, self.PUT_DOWN, self.CLOSE, 15)

    def Open(self, ts, position=None):
        self._CoalesceLast(ts, self.OPEN)
        if position:
            self.events[-1][2] = self._FixPosition(position)

    def Close(self, ts, position=None):
        self._CoalesceLast(ts, self.CLOSE)
        if position:
            self.events[-1][2] = self._FixPosition(position)

    def UpdateEvents(self, events):
        events.sort()
        if not events:
            return
        if self.events and events[0][0] < self.events[-1][0]:
            logger.fatal('%s: Going backwards in time from %s => %s!',
                         self.asin, FormatTime(self.events[-1][0]),
                         FormatTime(events[0][0]))
        self.events.extend(events)

    @classmethod
    def EventToString(cls, event_type):
        if event_type == cls.PICK_UP:
            return 'PICKED UP'
        elif event_type == cls.PUT_DOWN:
            return 'PUT DOWN'
        elif event_type == cls.OPEN:
            return 'OPENED'
        elif event_type == cls.CLOSE:
            return 'CLOSED'
        else:
            return 'UNKNOWN'

    @property
    def reads(self):
        """Return a list of reading events.

        Each entry in the list is a tuple of the form:
        (pick_up_ts, pick_up_loc, put_down_ts, put_down_loc, cum_reading_time)
        """
        rv = []
        first = None
        start = None
        firstpos = None
        latestpos = None
        read_time = 0
        last = None

        def _AppendRead(read):
            forwards = read[3] >= read[1]
            if read[4] < self.MIN_IN_HAND_SECS:
                # Not picked up long enough to be considered a read.
                return
            if not forwards and read[4] < self.MIN_IN_HAND_REVERSE_SECS:
                # Not picked up long enough to be considered a read, while
                # reading backwards...
                return
            if not rv:
                rv.append(read)
                return
            last = rv[-1]
            # Check if this read was a continuation of a previous read.
            continuing = False
            if last[3] == read[1]:
                # Starting where last read left off, must be a continuation.
                continuing = True
            elif (read[0] - last[2]) < self.MIN_IN_HAND_SECS:
                # Gap between reads is so short nothing else could have been
                # read in the interim... 
                continuing = True
            # But only continuing if we're still going fowards in the book...
            if continuing and forwards:
                readtime = last[4] + read[4]
                startpos = min(last[1], read[1])
                rv[-1] = (last[0], startpos, read[2], read[3], readtime)
                return
            # Not continuing, maybe a jump, or page mismatch.
            rv.append(read)

        for ts, etype, data in sorted(self.events):
            if etype == self.PICK_UP:
                start = first = ts
                firstpos = data
                latestpos = None
                read_time = 0
            elif etype == self.OPEN:
                if not first:
                    first = start
                if not firstpos and data:
                    firstpos = data
                    latestpos = data
                start = ts
            elif etype == self.CLOSE:
                if last in (self.PICK_UP, self.OPEN):
                    read_time += ts - start
                if data:
                    latestpos = data
            elif etype == self.PUT_DOWN:
                if last in (self.PICK_UP, self.OPEN):
                    read_time += ts - start
                if data:
                    latestpos = data
                _AppendRead((first, firstpos, ts, latestpos, read_time))
                first = None
                start = None
                read_time = 0
            last = etype
        if first:
            _AppendRead((first, firstpos, None, latestpos, read_time))
        return rv



class KindleLog(object):
    
    # Acceptable jumps in time (in seconds).
    MAX_BACKWARDS_JUMP = 3601  # Allow an hour for DST fuckups.
    MAX_FORWARDS_JUMP = 3600 * 24 * 120  # 4 months
    MAX_FILE_JUMP = 60 * 30  # 30 minutes

    # Regexp to extract state changes
    STATE_CHANGE_RE = re.compile(
            r'^.*?powerd.*?def:statech.*?:State change: (.*) -> (.*)$')
    
    # Regexp to extract timezone changes
    TZ_CHANGE_RE = re.compile(
            r'^.*TimezoneService:TimeZoneChange:offset=(.*),zone=(.*),.*$')

    # Regexps to match kernel reboot logs
    LINUX_REBOOT_RE = re.compile(
            r'^.*Linux #\d [A-Za-z]{3} [A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2} '
            '[A-Z]{3} \d{4}$')
    INIT_BOOT_RE = re.compile(r'^.*system: I S21init_time:initboot:time=.*$')
    SYSTEM_BOOTED_RE = re.compile(r'^.*system: I S96boot_finished:def:'
            'Boot finished script received framework booted event.*$')

    # Regexps to match booklet state changes
    BOOKLET_CHANGE_RE = re.compile(r'^.*: I BookletManager:SwitchingBooklets:'
                                   'from=(.*),to=(.*):.*$')
    BOOK_CHANGE_RE = re.compile(r'^.*: I Reader:BOOK INFO:book asin=(.*?),.*,'
                                'length=(.*?),.*,'
                                'last read position=(.*?),.*$')
    LPR_RE = re.compile(r'^.*: I Reader:SYNC LPR:position=(.*?):'
                        'Send LPR to server.*$')

    def __init__(self, filename, initial_state=None):
        self.filename = filename
        self._initial_state = initial_state
        self._reset(True)

    def _reset(self, force=False):
        if not force and not self.parsed:
            return

        self.parsed = False
        self._start = None
        self._end = None
        self._state = KindleLogState(self._initial_state)
        self._lineno = 0
        self._ts = 0
        self._ts_correction = 0
        self.states = []
        self.state_durations = {}
        self.books = {}

    def _ParseTimestamp(self, line):
        m = TS_REGEXP.match(line)
        if not m:
            return -1
        ts_str = m.groups()[0]
        d = datetime.strptime(ts_str, '%y%m%d:%H%M%S')
        ts = self._state.timezone.localize(d)
        ts = int(time.strftime('%s', ts.utctimetuple()))
        if ts < 0:
            # Epoch shit with timezones
            ts = 0
        return ts

    def _ParseFile(self):
        self._reset()

        fp = open(self.filename, 'r')
        for lineno, line in enumerate(fp, 1):
            self._lineno = lineno
            self._ts = self._ParseTimestamp(line)
            self._ts_correction = 0
            if self._ts < 0:
                self._debug('Invalid line. Skipping!')
                continue
            if self._state.last_ts > -1:
                self._CheckJump()
            if not self._start:
                self._start = self._ts
                if not self._state.power_state[0]:
                    self._state.power_state = (self._ts, 'NO_DATA')
            else:
                if self._ts < self._start:
                    self._debug('ts is less than file start %s. Ignoring line!',
                                FormatTime(self._start))
                    continue
            consumed = 0
            consumed += self._TrackPowerState(line)
            if consumed == 0:
                consumed += self._TrackReboot(line)
            if consumed == 0:
                consumed += self._TrackTimezone(line)
            if consumed == 0:
                consumed += self._TrackBook(line)
            self._state.last_ts = self._ts
        fp.close()

        if self._state.last_ts < 0:
            raise ValueError('No valid lines in file!')
        self._end = self._state.last_ts
        self._StateTransition(self._end)
        self._debug('Finished Processing! File covered %s -> %s',
                    FormatTime(self._start), FormatTime(self._end))
        self.parsed = True
        self._state.last_filename = os.path.basename(self.filename)

    def _CheckJump(self):
        """Check for large jumps in time between log lines
        
        If a large jump is detected this method fixes up the timestamp to
        remove it if possible, this stores state to keep subsequent log lines
        in sync.
        """
        # Adjust last_ts if we're already tracking a jump.
        last_ts = self._state.last_ts
        if self._state.base_badtime is not None:
            diff = last_ts - self._state.base_realtime
            if diff < 0:
                self._fatal('last_ts (%s) - base_realtime (%s) < 0',
                            self._state.last_ts,
                            self._state.base_realtime)
            last_ts = self._state.base_badtime + diff
        jump = self._ts - last_ts

        # Check for timezone change jumps.
        if (self._state.next_tz_jump and
                MatchWithFuzzByHour(self._state.next_tz_jump,
                                    jump) is not None):
            self._CalculateTime()
            self._SwitchTimezone()
            return
        if self._state.old_tz_jump:
            offset = MatchWithFuzzByHour(self._state.old_tz_jump, jump)
            if offset is not None:
                # Something still logging in the old timezone :(
                old = self._ts
                self._CalculateTime()
                self._ts = self._ts - offset
                self._ts_correction -= offset
                self._debug('Line had old timezone +/-1 hr '
                            '(would have been %s as %s (%s)).',
                            FormatTime(old), self._state.old_tz,
                            self._state.old_tz_jump)
                return

        if not self._start:
            if jump > 0 and jump > self.MAX_FILE_JUMP:
                # Giant jump between files. Assume missing data.
                self._warn('Missing data from %s till now. Resetting state.',
                           FormatTime(last_ts))
                self._StateTransition(last_ts, 'NO_DATA')
                self._StateTransition(self._ts, 'NO_DATA')
                return
            #if jump < 0:
            #    self._fatal('Time went backwards. Last file ended @ %s. '
            #                'Bailing out! Please fix manually.',
            #                FormatTime(last_ts))

        # Check for other time changing.
        if jump < 0 and abs(jump) > self.MAX_BACKWARDS_JUMP:
            self._debug('Large jump backwards from %s (%d)',
                        FormatTime(last_ts), jump)
            self._HandleJump(last_ts)
        elif jump > self.MAX_FORWARDS_JUMP:
            self._debug('Large jump forwards from %s (%d)',
                        FormatTime(last_ts), jump)
            self._HandleJump(last_ts)
        else:
            # No major changes. Just handle any existing offsets.
            self._CalculateTime()

    def _HandleJump(self, last_ts):
        """We've detected a jump not caused by a timezone change.

        Store the last good time, and the start time of the jump. _CalculateTime
        will use them  to calculate diffs for all future log lines.

        Return a boolean indicating if correct time needs to be calculated
        after the jump has been handled.
        """
        if self._state.base_realtime is None:
            # Simple case, no previous jumps being tracked.
            self._state.base_realtime = self._state.last_ts
            self._state.base_badtime = self._ts
            self._CalculateTime()
            self._debug('New jump offsets. real=%s, bad=%s',
                        FormatTime(self._state.base_realtime),
                        FormatTime(self._state.base_badtime))
            return

        # Already tracking a jump! Did we jump back to reality?
        jump = self._ts - self._state.base_realtime
        if jump >= 0 and jump <= self.MAX_FORWARDS_JUMP:
            # Back in reality.
            self._info('Jump from %s brings us back to reality',
                       FormatTime(last_ts))
            self._state.base_realtime = None
            self._state.base_badtime = None
            self._CalculateTime()
            return
        
        # Not back in reality. Update jump offsets, badtime becomes the new
        # value we just read from the line, realtime becomes the calculated
        # time we were up to after the previous line taking into account the
        # previous jump offsets.
        ts = self._ts
        self._CalculateTime(last_ts)
        self._debug('Second jump (%s -> %s). '
                    'Reset jump offsets (%s, %s) -> (%s, %s)',
                    FormatTime(last_ts), FormatTime(ts),
                    FormatTime(self._state.base_realtime),
                    FormatTime(self._state.base_badtime),
                    FormatTime(self._ts), FormatTime(ts))
        self._state.base_realtime = self._ts
        self._state.base_badtime = ts
        return

    def _CalculateTime(self, ts=None):
        """Calculate the 'realtime' based on the jump data we have stored.

        Calculate the time elapsed since the jump was detected, and add that to
        the last good timestamp we stored when we detected the jump. This works
        assuming that we had one good log line immediately before time jumped.
        """
        if self._state.base_realtime is None or self._state.base_badtime is None:
            # no jump being tracked, no calculation needed.
            return
        if ts is None:
            ts = self._ts

        diff = ts - self._state.base_badtime
        calculated_time = self._state.base_realtime + diff
        # Check this isn't jumping us *way* into the future itself.
        new_diff = calculated_time - self._state.base_realtime
        if new_diff > self.MAX_FORWARDS_JUMP:
            self._fatal('jump is too large (%d > %d)', new_diff,
                        self.MAX_FORWARDS_JUMP)
        self._ts_correction = (calculated_time - ts)
        self._ts = calculated_time
        self._debug('_CalculateTime: ts=%s, calc=%s, diff=%d, bad=%s, real=%s',
                    FormatTime(ts), FormatTime(calculated_time), diff,
                    FormatTime(self._state.base_badtime),
                    FormatTime(self._state.base_realtime))

    def _SwitchTimezone(self):
        """A jump in time which we expected for a TZ change has occured.

        Swaps the active timezone and updates tracking variables, so we can
        look out for lines still in the old timezone.
        """
        old = self._ts
        self._state.old_tz = self._state.timezone
        self._state.old_tz_jump = -1*self._state.next_tz_jump
        self._state.timezone = self._state.next_tz
        self._ts += (-1*self._state.next_tz_jump)
        self._state.next_tz = None
        self._state.next_tz_jump = None
        self._info('New timezone %s activated. (time would be %s as %s)',
                self._state.timezone, FormatTime(old), self._state.old_tz)

    def _TrackTimezone(self, line):
        """Watch for lines indicating that a timezone change is occurring.

        Don't swap timezones immediately, since the change seems to be logged a
        bit before things start logging in the new zone, so store the expected
        jump and wait until we see it before swapping the actual timezone in
        use.
        """
        m = self.TZ_CHANGE_RE.match(line)
        if not m:
            return 0
        
        offset, tzname = m.groups()
        try:
            new_tz = pytz.timezone(tzname)
        except pytz.UnknownTimeZoneError:
            self._debug('Timezone change to unknown zone %s detected', tzname)
            new_tz = pytz.FixedOffset(int(offset)/60)  # Convert to minutes.

        current_offset = self._state.timezone.localize(
                datetime.fromtimestamp(self._ts))
        new_offset = new_tz.localize(datetime.fromtimestamp(self._ts))
        delta = current_offset - new_offset
        self._state.next_tz_jump = delta.total_seconds()
        self._state.next_tz = new_tz
        self._debug('New timezone %s/%s, waiting for %d seconds jump from %s',
                   tzname, offset, self._state.next_tz_jump,
                   self._state.timezone)
        return 1

    def _TrackReboot(self, line):
        # Check for an unexpected reboot.
        m = self.LINUX_REBOOT_RE.match(line)
        if m:
            self._StateTransition(self._ts, 'KERNEL_BOOT')
            return 1
        # Skip further checks unless we know we're rebooting.
        last_ts, current_state = self._state.power_state
        if current_state not in ('KERNEL_BOOT', 'INITSCRIPTS'):
            return 0
        # Check for init scripts starting.
        m = self.INIT_BOOT_RE.match(line)
        if m:
            self._StateTransition(self._ts, 'INITSCRIPTS')
            return 1
        # Check for system started.
        m = self.SYSTEM_BOOTED_RE.match(line)
        if m:
            self._StateTransition(self._ts, 'ACTIVE')
            return 1
        return 0


    def _TrackPowerState(self, line):
        m = self.STATE_CHANGE_RE.match(line)
        if not m:
            return 0
        state_from, state_to = m.groups()
        last_ts, current_state = self._state.power_state
        if last_ts is None or current_state is None:
            self._debug('Found state transition (%s -> %s) before first '
                        'timestamp. Ignoring!', state_from, state_to)
            return 0
        if state_from != current_state:
            if last_ts == self._start:
                # We assume in _ParseFile that the file started in NO_DATA
                # for lack of better information. We just got some better
                # information, so update our guess.
                self._state.power_state = (last_ts, state_from)
            else:
                # Elsewhere in the file, something went wrong...
                self._debug('Unexpected state change from %s, expecting %s! '
                            'Durations will be inaccurate.',
                            state_from, current_state)
                # Fake a transition to the state the kindle tells us half way
                # through the duration from the state we were expecting to be in.
                duration = self._ts - last_ts
                fake_ts = last_ts + (duration / 2)
                self._StateTransition(fake_ts, state_from)
        # Move to the new state.
        self._StateTransition(self._ts, state_to)
        return 1

    def _StateTransition(self, ts, new_state=None):
        last_ts, current_state = self._state.power_state
        if not new_state:
            new_state = current_state
        self.states.append((ts, new_state))
        self.state_durations.setdefault(current_state, 0)
        self.state_durations[current_state] += (ts - last_ts)
        ts_str = ''
        if ts != self._ts:
            ts_str = ' @ %s' % FormatTime(ts)
        self._debug('Power State: %s -> %s%s', current_state, new_state, ts_str)
        self._state.power_state = (ts, new_state)
    
    def FormatStates(self):
        result = ['%s -> %s:' % (
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self._start)),
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self._end)))]
        t = sorted(self.state_durations.iteritems(), key=lambda x:x[1],
                   reverse=True)
        for state, duration in t:
            result.append('%s=%s' % (state, duration))
        return ' '.join(result)

    def _TrackBook(self, line):
        # Check for booklet change.
        m = self.BOOKLET_CHANGE_RE.match(line)
        if m:
            booklet_from, booklet_to = m.groups()
            self._BookletTransition(booklet_from, booklet_to)
            return 1
        # Check for book change.
        m = self.BOOK_CHANGE_RE.match(line)
        if m:
            asin, length, position = m.groups()
            self._BookTransition(asin, length, position)
            return 1
        # Check for position updates.
        m = self.LPR_RE.match(line)
        if m:
            position, = m.groups()
            self._BookTransition(None, '', position)
            return 1

        return 0

    def _EnsureBook(self, asin, length):
        if asin not in self.books:
            book = KindleBook(asin, length)
            self.books[asin] = book
        return self.books[asin]

    def _BookletTransition(self, b_from, b_to):
        self._debug('Booklet: %s -> %s', b_from, b_to)
        if not self._state.book:
            return
        book = self._EnsureBook(self._state.book, None)
        if b_to == 'Bookworm':
            book.Open(self._ts)
        elif b_from == 'Bookworm':
            book.Close(self._ts)
        
        if b_from == 'Home' and b_to == 'Bookworm':
            # New book being opened, handled by PickUp/PutDown in
            # _BookTransition.
            pass

    def _BookTransition(self, asin, length, position):
        if asin:
            self._debug('Book: %s -> %s. Length=%s, position=%s',
                        self._state.book, asin, length, position)
            # Get a book object for this new book.
            book = self._EnsureBook(asin, length)
            # Is this a brand new book.
            if not self._state.book:
                self._state.book = asin
                book.PickUp(self._ts, position)
                return
            # Has the book changed?
            if self._state.book != asin:
                old_book = self._EnsureBook(self._state.book, None)
                old_book.PutDown(self._ts)
                # Record new book.
                self._state.book = asin
                book.PickUp(self._ts, position)
            else:
                book.Open(self._ts, position)
        elif self._state.book:
            self._debug('Book: %s: position=%s',
                        self._state.book, position)
            # Position update.
            book = self._EnsureBook(self._state.book, None)
            book.Close(self._ts, position)
    
    @property
    def start(self):
        if not self.parsed:
            self._ParseFile()
        return self._start

    @property
    def end(self):
        if not self.parsed:
            self._ParseFile()
        return self._end

    @property
    def state(self):
        if not self.parsed:
            self._ParseFile()
        return self._state

    def __str__(self):
        return os.path.basename(self.filename)

    def __cmp__(self, other):
        return cmp(self.start, other.start)

    @property
    def logname(self):
        return str(self).lstrip('message_').lstrip('0')

    @property
    def _ts_correction_str(self):
        if self._ts_correction == 0:
            return ''
        return ' %s correction' % self._ts_correction

    @property
    def _log_prefix(self):
        return '%d@%s (%s%s):' % (self._lineno, self.logname,
                                  FormatTime(self._ts),
                                  self._ts_correction_str)

    def _debug(self, message, *args):
        logger.debug('%s %s' % (self._log_prefix, message), *args)

    def _info(self, message, *args):
        logger.info('%s %s' % (self._log_prefix, message), *args)

    def _warn(self, message, *args):
        logger.warn('%s %s' % (self._log_prefix, message), *args)

    def _fatal(self, message, *args):
        logger.fatal('%s %s' % (self._log_prefix, message), *args)
        sys.exit(1)


class KindleLogs(object):

    def __init__(self):
        self.files = []
        self.state = None

    def ProcessDirectory(self, directory):
        """Processes a directory of ordered Kindle logfiles.
        
        This method is aware of Kindle log file naming conventions and acts
        accordingly (skipping duplicates, ignoring partial logfiles).
        """
        logger.info('Processing logs from %s', directory)
        last_seq = ('', '')
        for logfile in sorted(os.listdir(directory)):
            if not logfile.startswith('messages_'):
                continue
            _, seq, datestr = logfile.split('_', 2)
            if self.state and logfile <= self.state.last_filename:
                logger.debug('Already processed %s', logfile)
                last_seq = (seq, datestr)
                continue
            if last_seq[0] == seq and datestr > last_seq[1]:
                # Newer version of the last logfile. Ignore it.
                old = self.files.pop(-1)
                logger.info('Ignoring %s in favour of %s',
                            old.state.last_filename, logfile)
                self.state = self.files[-1].state
            try:
                log = KindleLog(os.path.join(directory, logfile), self.state)
                self.files.append(log)
                self.state = log.state  # Triggers parsing.
                last_seq = (seq, datestr)
                logger.info('Parsed %s. %s -> %s.', log, FormatTime(log.start),
                            FormatTime(log.end))
                logger.debug('State: %s', self.state)
            except ValueError, e:
                logger.error('Could not parse %s! %s', logfile, e)
                continue
        self.files.sort()
        logger.info('Found %d logs. %s => %s', len(self.files),
                    FormatTime(self.files[0].start),
                    FormatTime(self.files[-1].end))

    def ProcessFiles(self, files):
        """Processes an ordered list of logfiles.
        
        This method simply parses the logfiles in the order given, with no
        attempt to interpret filenames and apply any special logic.
        """
        logger.info('Processing specified logfiles: %s', ', '.join(files))
        last_seq = ('', '')
        for logfile in files:
            try:
                log = KindleLog(logfile, self.state)
                self.files.append(log)
                self.state = log.state  # Triggers parsing.
                logger.info('Parsed %s. %s -> %s.', log, FormatTime(log.start),
                            FormatTime(log.end))
                logger.debug('State: %s', self.state)
            except ValueError, e:
                logger.error('Could not parse %s! %s', logfile, e)
                continue
        self.files.sort()
        logger.info('Processed %d logs. %s => %s', len(self.files),
                    FormatTime(self.files[0].start),
                    FormatTime(self.files[-1].end))

    def GetStates(self):
        states = {}
        for logfile in self.files:
            for state, duration in logfile.state_durations.iteritems():
                states.setdefault(state, 0)
                states[state] += duration
        return states

    def PrintStates(self):
        print ''
        states = self.GetStates()
        t = sorted(states.iteritems(), key=lambda x:x[1], reverse=True)
        for state, duration in t:
            print '%s: %d' % (state, duration)

    @property
    def books(self):
        books = {}
        for logfile in self.files:
            for book in logfile.books.values():
                if book.asin in books:
                    books[book.asin].UpdateEvents(book.events)
                else:
                    books[book.asin] = book
        # Remove any books with zero reads.
        for asin in books.keys():
            if len(books[asin].reads) <= 0:
                del books[asin]
        return books


def LoadHistory(filename):
    if not filename or not os.path.exists(filename):
        return None
    logger.info('Reading history from %s', filename)
    try:
        fp = open(filename, 'rb')
        logs = pickle.load(fp)
        fp.close()
        return logs
    except Exception, e:
        logger.fatal('Could not load history from %s: %s', filename, e)


def StoreHistory(logs, filename):
    if not filename:
        return
    tmp_filename = '%s.tmp' % filename
    logger.info('Storing state into %s', filename)
    try:
        fp = open(tmp_filename, 'wb')
        pickle.dump(logs, fp, pickle.HIGHEST_PROTOCOL)
        fp.close()
    except Exception, e:
        logger.error('Could not store history to %s: %s', filename, e)
        if os.path.exists(tmp_filename):
            os.unlink(tmp_filename)
        return
    os.rename(tmp_filename, filename)


def ParseOptions(args):
    parser = optparse.OptionParser()
    parser.add_option('-c', '--console', action='store_true', dest='console',
                      help='drop to an interactive console after parsing')
    parser.add_option('-s', '--state_file', action='store',
                      dest='state_file',
                      default=os.path.expanduser('~/.kindle-utils.state'),
                      help='Path to file to load/store state from')
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
                      help='enable verbose logging')

    return parser.parse_args(args)

def SetVerbosity(verbose):
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

def main():
    # everything in UTC please!
    os.environ['TZ'] = 'UTC'
    time.tzset()

    logging.basicConfig()
    options, args = ParseOptions(sys.argv)
    if len(args) < 2:
        logging.fatal('You must specify a dir/file or files to read from!')
        sys.exit(1)
    SetVerbosity(options.verbose)
    if len(args) > 2:
        # Multiple files, process as given.
        logs = KindleLogs()
        logs.ProcessFiles(args[1:])
        logs.PrintStates()
        books = logs.books
    elif os.path.isdir(args[1]):
        # Directory, process as if it contains ordered Kindle log files.
        logs = LoadHistory(options.state_file)
        if not logs:
            logs = KindleLogs()
        logs.ProcessDirectory(args[1])
        logs.PrintStates()
        StoreHistory(logs, options.state_file)
        books = logs.books
    elif os.path.isfile(args[1]):
        # Single file, process as given.
        log = KindleLog(args[1])
        logger.info('Parsed %s. %s -> %s.', log, FormatTime(log.start),
                    FormatTime(log.end))
        books = log.books
    else:
        logger.fatal('Invalid path: %s' % args[1])
        sys.exit(1)

    if options.console:
        t = globals()
        t.update(locals())
        code.interact(local=t)


if __name__ == '__main__':
    main()
