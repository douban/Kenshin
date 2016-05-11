#!/usr/bin/env python
# coding: utf-8

import argparse
import struct
import kenshin
from datetime import datetime
from kenshin.utils import get_metric


def timestamp_to_datestr(ts):
    try:
        d = datetime.fromtimestamp(ts)
        return d.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return 'invalid timestamp'


def get_point(fh, offset, size, format):
    fh.seek(offset)
    data = fh.read(size)
    return struct.unpack(format, data)


def run(filepath, archive_idx, point_idx, error):
    with open(filepath) as f:
        header = kenshin.header(f)
        archive = header['archive_list'][archive_idx]
        point_size = header['point_size']
        point_format = header['point_format']

        start_offset = archive['offset'] + point_idx * point_size
        if point_idx < 0:
            start_offset += archive['size']

        point = get_point(f, start_offset, point_size, point_format)
        print 'count: %s' % archive['count']

        if not error:
            metric = get_metric(filepath)
            date_str = timestamp_to_datestr(point[0])
            if metric:
                idx = header['tag_list'].index(metric)
                return (point[0], point[idx + 1]), date_str

            else:
                return point, date_str
        else:
            sec_per_point = archive['sec_per_point']
            ts = point[0]
            start_offset += point_size
            point_idx += 1
            while start_offset < archive['size'] + archive['offset']:
                point = get_point(f, start_offset, point_size, point_format)
                if point[0] != ts + sec_per_point:
                    return point_idx
                start_offset += point_size
                point_idx += 1
                ts = point[0]
            return 'No error!'


def main():
    parser = argparse.ArgumentParser(description="debug kenshin file")
    parser.add_argument('filepath', help="metric file path")
    parser.add_argument('archive_idx', type=int, help="the archive index")
    parser.add_argument('point_idx', type=int, help="the point index")
    parser.add_argument('-e', '--error', action="store_true", help="run until meet an unexpected point (empty or error)")

    args = parser.parse_args()
    print run(args.filepath, args.archive_idx, args.point_idx, args.error)


if __name__ == '__main__':
    main()
