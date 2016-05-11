# coding: utf-8

import re
import sys
import socket
import time
import subprocess
import pickle
import struct

RUROUNI_SERVER = '127.0.0.1'
RUROUNI_PORT = 2004
DELAY = 60

idx = 0


def get_loadavg():
    cmd = 'uptime'
    output = subprocess.check_output(cmd, shell=True).strip()
    output = re.split("\s+", output)
    # return output[-3:]
    # 发送伪造数据，容易肉眼验证处理结果是否正确
    global idx
    idx += 1
    return idx, 100+idx, 200+idx


def run(sock, delay):
    while True:
        now = int(time.time())
        loadavg = get_loadavg()

        lines = []  # for print info
        tuples = []
        idx2min = [1, 5, 15]
        for i, val in enumerate(loadavg):
            line = "system.loadavg.min_%s.metric_test %s %d" % (idx2min[i], val, now)
            lines.append(line)
            tuples.append(('system.loadavg.min_%s.metric_test' % idx2min[i], (now, val)))
        msg = '\n'.join(lines) + '\n'  # all lines must end in a newline
        print 'sending message'
        print '-' * 80
        print msg
        package = pickle.dumps(tuples, 1)
        size = struct.pack('!L', len(package))
        sock.sendall(size)
        sock.sendall(package)
        time.sleep(delay)


def main():
    if len(sys.argv) > 1:
        delay = int(sys.argv[1])
    else:
        delay = DELAY

    sock = socket.socket()
    try:
        sock.connect((RUROUNI_SERVER, RUROUNI_PORT))
    except socket.error:
        raise SystemError("Couldn't connect to %s on port %s" %
                          (RUROUNI_SERVER, RUROUNI_PORT))

    try:
        run(sock, delay)
    except KeyboardInterrupt:
        sys.stderr.write("\nexiting on CTRL+c\n")
        sys.exit(0)


if __name__ == '__main__':
    main()
