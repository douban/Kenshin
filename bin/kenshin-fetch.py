#!/usr/bin/env python
# coding: utf-8
import sys
import time
import optparse
import signal
import kenshin
from kenshin.utils import get_metric

signal.signal(signal.SIGPIPE, signal.SIG_DFL)


def main():
    NOW = int(time.time())
    YESTERDAY = NOW - 24 * 60 * 60

    usage = "%prog [options] path"
    option_parser = optparse.OptionParser(usage=usage)
    option_parser.add_option('--from',
                             default=YESTERDAY,
                             type=int,
                             dest='_from',
                             help="begin timestamp(default: 24 hours ago)")
    option_parser.add_option('--until',
                             default=NOW,
                             type=int,
                             help="end timestamp")

    (options, args) = option_parser.parse_args()
    if len(args) != 1:
        option_parser.print_help()
        sys.exit(1)

    path = args[0]
    metric = get_metric(path)
    from_time = int(options._from)
    until_time = int(options.until)

    header, timeinfo, points = kenshin.fetch(path, from_time, until_time, NOW)
    start, end, step = timeinfo

    if metric:
        idx = header['tag_list'].index(metric)
        points = (p[idx] if p else None for p in points)

    t = start
    for p in points:
        print "%s\t%s" % (t, p)
        t += step


if __name__ == '__main__':
    main()
