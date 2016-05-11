#!/usr/bin/env python
# coding: utf-8
import os
import time
import socket
import pickle
import urllib
import struct
import glob

from kenshin.tools.whisper_tool import (
    read_header, remote_url, pointFormat)


def send_metric(sock, metric, data_dir, default_val):
    now = int(time.time())

    val = default_val
    if val is None:
        m_val = get_metric_value(metric, data_dir, now)
        if m_val:
            val = m_val

    tuples = [(metric, (now, val))]
    package = pickle.dumps(tuples, 1)
    size = struct.pack('!L', len(package))
    sock.sendall(size)
    sock.sendall(package)


def get_metric_value(metric, data_dir, now):
    path = metric_to_filepath(metric, data_dir)
    header = read_header(path)
    content = get_whisper_file_content(data_dir, metric)
    for archive in header['archives']:
        points = read_whisper_points(content, archive, now)
        if points:
            return sorted(points)[-1][1]


def get_whisper_file_content(data_dir, m):
    filepath = metric_to_filepath(m, data_dir)
    if remote_url(filepath):
        c = urllib.urlopen(filepath)
        if c.code == 200:
            return c.read()
        else:
            raise Exception('HTTP Error Code %s for metric %s' % (c.code, m))
    else:
        with open(filename) as f:
            return f.read()


def read_whisper_points(content, archive, now):
    off, size, cnt = archive['offset'], archive['size'], archive['count']
    packed_bytes = content[off: off+size]
    point_format = pointFormat[0] + pointFormat[1:] * cnt
    points = struct.unpack(point_format, packed_bytes)
    ts_limit = now - archive['retention']
    return [(points[i], points[i+1]) for i in range(0, len(points), 2)
            if points[i] >= ts_limit]


def metric_to_filepath(metric, data_dir):
    return os.path.sep.join([data_dir] + metric.split('.')) + '.wsp'


def get_metrics(metrics_file):
    with open(metrics_file) as f:
        for line in f:
            line = line.strip()
            if line:
                yield line


def metric_exists(link_dir, metric):
    pat = os.path.join(link_dir, '*', metric.replace('.', '/') + '.hs')
    return glob.glob(pat)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--metrics', required=True, help="file contains metrics")
    parser.add_argument('--port', help="port of carbon-relay")
    parser.add_argument('--data_dir', help='whisper file data directory(local or http address)')
    parser.add_argument('--default', help='default value for metric')
    parser.add_argument('--link_dir', help='check if metric is already exists')
    args = parser.parse_args()

    port = int(args.port) if args.port else 2014
    sock = socket.socket()
    try:
        sock.connect(('127.0.0.1', port))
    except socket.error:
        raise SystemError("Couldn't connect to %s on port %s" %
                          (args.host, args.port))

    default_val = float(args.default) if args.default else None
    out_file = 'added_metrics.txt'
    f = open(out_file, 'w')
    for metric in get_metrics(args.metrics):
        if not metric_exists(args.link_dir, metric):
            print metric
            f.write(metric + '\n')
            send_metric(sock, metric, args.data_dir, default_val)
    f.close()


if __name__ == '__main__':
    main()
