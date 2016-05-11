#!/usr/bin/env python
# coding: utf-8
"""
为了在迁移数据时减少不必要的指标（metric），目前只迁移一周内发生过的指标（即至少有一点）。
但是一些类似 5XX 这样的 web error 相关的指标，可能会有一段时间没有发生（例如，一周），
所以新的节点上没有这些指标，但是 SA 那边希望保留这些指标。那么该脚本的作用就是给 Graphite
发送这些指标，每个指标发送一个 val（默认为0）.

# 指标的获取

$ kenshin-get-metrics.py -d /data/kenshin/storage/data/ -f error_code.re | awk '{print $5}'
$ cat error_code.re
.*\.code\.\d+\.rate$

# 使用方法

$ kenshin-send-zero-metric.py -a <relay-host>:<port> -m error_metric.src -b error_metric.dst
"""

import sys
import socket
import time
import argparse


def run(sock, interval, metrics):
    now = int(time.time())
    for m in metrics:
        line = '%s %s %d\n' % (m, 0, now)
        print line,
        sock.sendall(line)
        time.sleep(interval)


def get_metrics(filename):
    metrics = []
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if line:
                metrics.append(line)
    return set(metrics)


def parse_addr(addr):
    try:
        host, port = addr.split(":")
        return host, int(port)
    except ValueError:
        msg = "%r is not a valid addr" % addr
        raise argparse.ArgumentTypeError(msg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--addr', required=True, type=parse_addr,
                        help="addr of carbon relay, it is format host:port")
    parser.add_argument('-i', '--interval', default=0.01, type=float,
                        help="time interval between two send.")
    parser.add_argument('-m', '--metric-file', required=True,
                        help="file that contains metric name to send.")
    parser.add_argument('-b', '--black-list-file', default=None,
                        help="file that contains black list of metrics.")

    args = parser.parse_args()
    host, port = args.addr

    sock = socket.socket()
    try:
        sock.connect((host, port))
    except socket.error:
        raise SystemError("Couldn't connect to %s on port %d" %
                          (host, port))
    metrics = get_metrics(args.metric_file)
    if args.black_list_file:
        metrics -= get_metrics(args.black_list_file)
    try:
        run(sock, args.interval, metrics)
    except KeyboardInterrupt:
        sys.stderr.write("\nexiting on CTRL+c\n")
        sys.exit(0)


if __name__ == '__main__':
    main()
