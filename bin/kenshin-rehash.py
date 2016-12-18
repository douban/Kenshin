#!/usr/bin/env python
# coding: utf-8
import os
import sys
import time
import urllib
import struct
import StringIO
from multiprocessing import Process, Queue

from kenshin import storage
from kenshin.agg import Agg
from kenshin.storage import Storage
from kenshin.consts import NULL_VALUE
from rurouni.utils import get_instance_of_metric
from kenshin.tools.whisper_tool import (
    read_header as whisper_read_header,
    pointFormat as whisperPointFormat,
)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t', '--src-type', required=True,
        choices=['whisper', 'kenshin'],
        help="src storage type"
        )
    parser.add_argument(
        '-d', '--src-data-dir', required=True,
        help="src data directory (local or http address)."
        )
    parser.add_argument(
        '-n', '--src-instance-num', type=int,
        help=('src rurouni cache instance number (required when src_type '
              'is kenshin)')
        )
    parser.add_argument(
        '-m', '--kenshin-file', required=True,
        help='kenshin data files that we want to add the history.'
        )
    parser.add_argument(
        '-p', '--processes', default=10, type=int,
        help="number of processes."
        )
    args = parser.parse_args()

    if args.src_type == 'kenshin' and args.src_instance_num is None:
        parser.error('src-instance-num is required')

    # start processes
    processes = []
    queue = Queue()
    for w in xrange(args.processes):
        p = Process(target=worker, args=(queue,))
        p.start()
        processes.append(p)

    # generate data
    with open(args.kenshin_file) as f:
        for line in f:
            kenshin_filepath = line.strip()
            if not kenshin_filepath:
                continue
            with open(kenshin_filepath) as f:
                header = Storage.header(f)
            metrics = header['tag_list']
            if args.src_type == 'kenshin':
                metric_paths = [
                    metric_to_filepath(args.src_data_dir, m, args.src_instance_num)
                    for m in metrics
                ]
            else:  # whisper
                metric_paths = [
                    metric_to_whisper_filepath(args.src_data_dir, m)
                    for m in metrics
                ]
            item = (args.src_type, header, metric_paths, metrics, kenshin_filepath)
            queue.put(item)

    # stop processes
    for _ in xrange(args.processes):
        queue.put("STOP")
    for p in processes:
        p.join()


def worker(queue):
    for (src_type, meta, metric_paths, metrics, dst_file) in iter(queue.get, 'STOP'):
        try:
            tmp_file = dst_file + '.tmp'
            merge_metrics(src_type, meta, metric_paths, metrics, tmp_file)
            os.rename(tmp_file, dst_file)
        except Exception as e:
            print >>sys.stderr, '[merge error] %s: %s' % (dst_file, e)
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
    return True


def merge_metrics(src_type, meta, metric_paths, metric_names, output_file):
    ''' Merge metrics to a kenshin file.
    '''
    # Get content(data points grouped by archive) of each metric.
    if src_type == 'kenshin':
        metrics_archives_points = [
            get_metric_content(path, metric)
            for (path, metric) in zip(metric_paths, metric_names)
        ]
    else:  # whipser
        metrics_archives_points = [
            get_whisper_metric_content(path)
            for path in metric_paths
        ]

    # Merge metrics to a kenshin file
    with open(output_file, 'wb') as f:
        archives = meta['archive_list']
        archive_info = [(archive['sec_per_point'], archive['count'])
                        for archive in archives]
        inter_tag_list = metric_names + ['']  # for reserved space

        # header
        packed_kenshin_header = Storage.pack_header(
            inter_tag_list,
            archive_info,
            meta['x_files_factor'],
            Agg.get_agg_name(meta['agg_id']),
            )[0]
        f.write(packed_kenshin_header)

        for i, archive in enumerate(archives):
            archive_points = [x[i] for x in metrics_archives_points]
            merged_points = merge_points(archive_points)
            points = fill_gap(merged_points, archive, len(meta['tag_list']))
            packed_str = packed_kenshin_points(points)
            f.write(packed_str)


def metric_to_filepath(data_dir, metric, instance_num):
    if metric.startswith('rurouni.'):
        instance = metric.split('.')[2]
    else:
        instance = str(get_instance_of_metric(metric, instance_num))
    return os.path.sep.join([data_dir, instance] + metric.split('.')) + '.hs'


def metric_to_whisper_filepath(data_dir, metric):
    return os.path.sep.join([data_dir] + metric.split('.')) + '.wsp'


def merge_points(metrics_archive_points):
    ''' Merge metrics' archive points to kenshin points.

    >>> whisper_points = [
    ...   [[1421830133, 0], [1421830134, 1], [1421830135, 2]],
    ...   [[1421830134, 4], [1421830135, 5]],
    ...   [[1421830133, 6], [1421830134, 7], [1421830135, 8]]
    ... ]
    >>> merge_points(whisper_points)
    [(1421830133, [0, -4294967296.0, 6]), (1421830134, [1, 4, 7]), (1421830135, [2, 5, 8])]
    '''
    length = len(metrics_archive_points)
    d = {}
    for i, points in enumerate(metrics_archive_points):
        for t, v in points:
            if not t:
                continue
            if t not in d:
                d[t] = [NULL_VALUE] * length
            d[t][i] = v
    return sorted(d.items())


def fill_gap(archive_points, archive, metric_num):
    EMPTY_POINT = (0, (0,) * metric_num)
    if not archive_points:
        return [EMPTY_POINT] * archive['count']
    step = archive['sec_per_point']
    rs = [archive_points[0]]
    prev_ts = archive_points[0][0]
    for ts, point in archive_points[1:]:
        if prev_ts + step == ts:
            rs.append((ts, point))
        else:
            rs.extend([EMPTY_POINT] * ((ts-prev_ts) / step))
        prev_ts = ts
    if len(rs) < archive['count']:
        rs.extend([EMPTY_POINT] * (archive['count'] - len(rs)))
    else:
        rs = rs[:archive['count']]
    return rs


def packed_kenshin_points(points):
    point_format = storage.POINT_FORMAT % len(points[0][1])
    str_format = point_format[0] + point_format[1:] * len(points)
    return struct.pack(str_format, *flatten(points))


def flatten(iterable):
    """ Recursively iterate lists and tuples.

    >>> list(flatten([1, (2, 3, [4]), 5]))
    [1, 2, 3, 4, 5]
    """
    for elm in iterable:
        if isinstance(elm, (list, tuple)):
            for relm in flatten(elm):
                yield relm
        else:
            yield elm


def get_metric_content(metric_path, metric_name):
    ''' Return data points of each archive of the metric.
    '''
    conn = urllib.urlopen(metric_path)
    if conn.code == 200:
        content = conn.read()
    else:
        raise Exception('HTTP Error Code %s for %s' % (conn.code, metric_path))

    header = Storage.header(StringIO.StringIO(content))
    metric_list = header['tag_list']
    metric_cnt = len(metric_list)
    metric_idx = metric_list.index(metric_name)
    step = metric_cnt + 1
    point_format = header['point_format']
    byte_order, point_type = point_format[0], point_format[1:]
    metric_content = []
    now = int(time.time())

    for archive in header['archive_list']:
        ts_min = now - archive['retention']
        archive_points = []
        series_format = byte_order + (point_type * archive['count'])
        packed_str = content[archive['offset']: archive['offset'] + archive['size']]
        unpacked_series = struct.unpack(series_format, packed_str)
        for i in xrange(0, len(unpacked_series), step):
            ts = unpacked_series[i]
            if ts > ts_min:
                # (timestamp, value)
                datapoint = (ts, unpacked_series[i+1+metric_idx])
                archive_points.append(datapoint)
        metric_content.append(archive_points)

    return metric_content


def get_whisper_metric_content(metric_path):
    conn = urllib.urlopen(metric_path)
    if conn.code == 200:
        content = conn.read()
    else:
        raise Exception('HTTP Error Code %s for %s' % (conn.code, metric_path))

    header = whisper_read_header(StringIO.StringIO(content))
    byte_order, point_type = whisperPointFormat[0], whisperPointFormat[1:]
    metric_content = []
    now = int(time.time())
    step = 2

    for archive in header['archives']:
        ts_min = now - archive['retention']
        archive_points = []
        series_format = byte_order + (point_type * archive['count'])
        packed_str = content[archive['offset']: archive['offset'] + archive['size']]
        unpacked_series = struct.unpack(series_format, packed_str)
        for i in xrange(0, len(unpacked_series), step):
            ts = unpacked_series[i]
            if ts > ts_min:
                datapoint = (ts, unpacked_series[i+1])
                archive_points.append(datapoint)
        metric_content.append(archive_points)

    return metric_content


if __name__ == '__main__':
    main()
