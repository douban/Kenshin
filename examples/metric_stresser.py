# coding: utf-8
import time
import random
import socket
import struct
import cPickle as pickle
from multiprocessing import Process


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address", type=str, help="host:port pair.")
    parser.add_argument("-f", "--format", type=str, choices=["line", "pickle"], help="Format of data.")
    parser.add_argument("-p", "--process", type=int, default=1, help="Number of processes.")
    parser.add_argument("-m", "--metric", type=int, default=1000, help="Number of metrics for one process.")
    parser.add_argument("-i", "--interval", type=int, default=10, help="Publish time interval.")
    parser.add_argument("-d", "--debug", action='store_true', help="Debug mode, send the metrics to terminal.")
    args = parser.parse_args()

    stresser(args)


def stresser(args):
    host, port = args.address.split(":")
    port = int(port)
    metric_args = (host, port, args.format, args.metric, args.interval, args.debug)

    processes = []
    for i in xrange(args.process):
        pname = 'process_%s' % i
        p = Process(target=send_metrics, args=(pname,) + metric_args)
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
        print 'KeyboardInterrupt'


def send_metrics(pname, host, port, format, num_metrics, interval, debug):
    time.sleep(random.random() * interval)
    sock = socket.socket()
    try:
        sock.connect((host, port))
    except socket.error:
        if not debug:
            raise SystemError("Couldn't connect to %s on port %s" %
                              (host, port))
    metrics = list(gen_metrics(pname, num_metrics))
    while True:
        points = gen_metric_points(metrics, format)
        if debug:
            print '\n'.join(map(str, points))
        else:
            if format == 'line':
                msg = '\n'.join(points) + '\n'  # all lines end in a newline
                sock.sendall(msg)
            else:
                # pickle
                package = pickle.dumps(points, 1)
                size = struct.pack('!L', len(package))
                sock.sendall(size)
                sock.sendall(package)
        time.sleep(interval)


def gen_metrics(id_, num_metrics):
    METRIC_PATTERN = 'metric_stresser.{0}.metric_id.%s'.format(id_)
    for i in xrange(num_metrics):
        yield METRIC_PATTERN % str(i)


def gen_metric_points(metrics, format):
    base_val = random.random()
    now = int(time.time())
    points = []
    for i, m in enumerate(metrics):
        val = base_val + i
        if format == 'line':
            points.append("%s %s %s" % (m, val, now))
        else:
            points.append((m, (now, val)))
    return points


if __name__ == '__main__':
    main()
