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

import apnx_parser
import log_parser
import mobibook

logger = logging.getLogger().getChild('kindle-stats')


def FormatHMS(hms_str):
    hour, mins, secs = map(int, hms_str.split(':', 2))
    if secs > 30:
        mins += 1
    rv = []
    if hour > 0:
        rv.append('%d hour' % hour)
        if hour > 1:
            rv.append('s')
        rv.append(', ')
    rv.append('%d min' % mins)
    if mins > 1:
        rv.append('s')
    return ''.join(rv)


def PrintHMS(seconds):
    d = timedelta(seconds=seconds)
    ds = str(d)
    if ', ' not in ds:
        return FormatHMS(ds)
    else:
        days, hms = ds.split(', ')
        return '%s, %s' % (days, FormatHMS(hms))

def GetBookMetadata(asin, book_dir):
    mobi = None
    sidecar = None
    for logfile in sorted(os.listdir(book_dir)):
        if mobi and sidecar:
            break
        if asin not in logfile:
            continue
        filename = os.path.join(book_dir, logfile)
        if logfile.endswith(('.azw', '.mobi')):
            try:
                mobi = mobibook.MobiBook(open(filename, 'r'))
            except mobibook.MobiException, e:
                logger.warn('Could not read MobiBook %s for %s: %s', logfile,
                            asin, e)
                mobi = None
        elif logfile.endswith('.apnx'):
            try:
                sidecar = apnx_parser.ApnxFile(filename)
            except apnx_parser.ApnxException, e:
                logger.warn('Could not read page number sidecar %s for %s: %s',
                            logfile, asin, e)
                sidecar = None
    return mobi, sidecar


def PrintBooks(books, book_dir, only_book=None, verbose=False):
    now = time.time()
    rv = []
    events = None
    for book in books.values():
        if only_book:
            if book.asin != only_book:
                continue
            events = book.events
        reads = book.reads
        if not reads:
            rv.append((0, book.asin, book))
        else:
            newest = max([t[2] is None and now or t[2] for t in reads])
            rv.append((newest, book.asin, book))

    total_duration = 0
    eventpos = 0
    for newest, asin, book in sorted(rv, reverse=True):
        metadata, sidecar = GetBookMetadata(asin, book_dir)
        if metadata:
            title = '%s: %s' % (asin, metadata.title)
        else:
            title = asin
        reads = book.reads
        print '%s: Read % 2d times. Last Finished: %s' % (
                title, len(reads),
                newest == now and 'In Progress!' or time.ctime(newest))
        if only_book and verbose:
            print ' Length: %d' % book.length
        for start, startpos, end, endpos, duration in reads:
            if sidecar:
                start_txt = 'p%s' % sidecar.GetPageLabelForPosition(startpos)
                end_txt = 'p%s' % sidecar.GetPageLabelForPosition(endpos)
            else:
                start_txt = '@%s' % startpos
                end_txt = '@%s' % endpos
            print ' - %s => %s. Reading time %s (%s => %s)' % (
                    time.ctime(start),
                    end is None and 'In Progress!' or time.ctime(end),
                    PrintHMS(duration), start_txt, end_txt)
            total_duration += duration
            if only_book and verbose:
                # Print all events.
                for idx, event in enumerate(events[eventpos:]):
                    ts, event_type, data = event
                    if end and ts > end:
                        eventpos += idx
                        break
                    print '   %s on page %s @ %s' % (
                            log_parser.KindleBook.EventToString(event_type),
                            sidecar.GetPageLabelForPosition(data),
                            time.ctime(ts))
        print ''

    if not only_book:
        print 'Read %d books in total. %s of reading time' % (
                len(rv), PrintHMS(total_duration))


def ParseOptions(args):
    parser = optparse.OptionParser()
    parser.add_option('-s', '--state_file', action='store',
                      dest='state_file',
                      default=os.path.expanduser('~/.kindle-utils.state'),
                      help='Path to file to load/store state from')
    parser.add_option('-b', '--book_dir', action='store',
                      dest='book_dir',
                      default=os.path.expanduser('~/kindle-books'),
                      help='Path to Kindle userstore documents directory')
    parser.add_option('-B', '--book', action='store',
                      dest='book',
                      default=None,
                      help='ASIN of specific book to view')
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
                      help='enable verbose logging')

    return parser.parse_args(args)

def main():
    # everything in UTC please!
    os.environ['TZ'] = 'UTC'
    time.tzset()

    logging.basicConfig()
    options, args = ParseOptions(sys.argv)
    if len(args) < 2:
        logging.fatal('You must specify a directory to read from!')
        sys.exit(1)

    logs = log_parser.LoadHistory(options.state_file)
    if not logs:
        logs = log_parser.KindleLogs()
    logs.ProcessDirectory(args[1])
    log_parser.StoreHistory(logs, options.state_file)
    books = logs.books

    PrintBooks(books, options.book_dir, options.book, options.verbose)


if __name__ == '__main__':
    main()
