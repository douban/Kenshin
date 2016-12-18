#!/usr/bin/env python
# coding: utf-8

import argparse
import struct
import socket
import cPickle as pickle
from ConfigParser import ConfigParser

from rurouni.utils import get_instance_of_metric


def gen_rurouni_query_port(conf_file):
    parser = ConfigParser()
    parser.read(conf_file)
    prefix = 'cache:'
    rs = {}
    for s in parser.sections():
        if s.startswith(prefix):
            instance = int(s[len(prefix):])
            for k, v in parser.items(s):
                k = k.upper()
                if k == 'CACHE_QUERY_PORT':
                    rs[instance] = int(v)
    return rs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='127.0.0.1',
                        help="server's host(or ip).")
    parser.add_argument('--num', type=int, default=3,
                        help='number of rurouni caches.')
    parser.add_argument('--conf',
                        help='rurouni-cache conf file path.')
    parser.add_argument('metric', help="metric name.")
    args = parser.parse_args()

    rurouni_query_ports = gen_rurouni_query_port(args.conf)
    server = args.server
    metric = args.metric
    num = args.num
    port_idx = get_instance_of_metric(metric, num)
    port = rurouni_query_ports[port_idx]

    conn = socket.socket()
    try:
        conn.connect((server, port))
    except socket.error:
        raise SystemError("Couldn't connect to %s on port %s" %
                          (server, port))

    request = {
        'type': 'cache-query',
        'metric': metric,
    }

    serialized_request = pickle.dumps(request, protocol=-1)
    length = struct.pack('!L', len(serialized_request))
    request_packet = length + serialized_request

    try:
        conn.sendall(request_packet)
        rs = recv_response(conn)
        print rs
    except Exception as e:
        raise e


def recv_response(conn):
    length = recv_exactly(conn, 4)
    body_size = struct.unpack('!L', length)[0]
    body = recv_exactly(conn, body_size)
    return pickle.loads(body)


def recv_exactly(conn, num_bytes):
    buf = ''
    while len(buf) < num_bytes:
        data = conn.recv(num_bytes - len(buf))
        if not data:
            raise Exception("Connection lost.")
        buf += data
    return buf


if __name__ == '__main__':
    main()
