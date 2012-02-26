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

import log_parser

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


def PrintBooks(books):
    now = time.time()
    rv = []
    for book in books.values():
        reads = book.reads
        if not reads:
            rv.append((0, book.asin, reads))
        else:
            newest = max([t[1] is None and now or t[1] for t in reads])
            rv.append((newest, book.asin, reads))

    total_duration = 0
    for newest, asin, reads in sorted(rv, reverse=True):
        print '%s: Read % 2d times. Last Finished: %s' % (
                asin, len(reads),
                newest == now and 'In Progress!' or time.ctime(newest))
        for start, end, duration in reads:
            print ' - %s => %s. Reading time %s' % (
                    time.ctime(start),
                    end is None and 'In Progress!' or time.ctime(end),
                    PrintHMS(duration))
            total_duration += duration
        print ''

    print 'Read %d books in total. %s of reading time' % (
            len(rv), PrintHMS(total_duration))


def ParseOptions(args):
    parser = optparse.OptionParser()
    parser.add_option('-s', '--state_file', action='store',
                      dest='state_file',
                      default=os.path.expanduser('~/.kindle-utils.state'),
                      help='Path to file to load/store state from')
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

    PrintBooks(books)


if __name__ == '__main__':
    main()
