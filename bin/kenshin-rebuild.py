#!/usr/bin/env python
# encoding: utf-8
import re
import time
from os.path import join
from subprocess import check_output


class Status(object):
    def __init__(self, status, pid, time):
        self.status = status
        self.pid = pid
        self.time = int(time)

    def __str__(self):
        return '<Status (%s, %s, %s)>' % (self.status, self.pid, self.time)


def get_service_status(service_name):
    """Return Status(status, pid, time).
    e.g. Status('up', 1024, 12342), Status('down', None, 2)
    """
    cmd = ['svstat', service_name]
    out = check_output(cmd)

    down_pattern = r'down (\d+) seconds, normally up'
    up_pattern = r'up \(pid (\d+)\) (\d+) seconds'

    if re.search(up_pattern, out):
        pid, t = re.search(up_pattern, out).groups()
        return Status('up', pid, t)
    elif re.search(down_pattern, out):
        (t,) = re.search(down_pattern, out).groups()
        return Status('down', None, t)
    else:
        raise Exception('Unkown service status, service=%s, status=%s', service_name, out)


def run_cmd(cmd, user=None):
    if user:
        cmd = 'sudo -u %s %s' % (user, cmd)
    print cmd
    return check_output(cmd, shell=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--storage-dir',
        help='Kenshine storage directory.'
    )
    parser.add_argument(
        '-b', '--begin-bucket',
        type=int,
        help='Begin bucket number.'
    )
    parser.add_argument(
        '-e', '--end-bucket',
        type=int,
        help='End bucket number.'
    )
    parser.add_argument(
        '--skip-rebuild-link',
        action='store_true',
        help='Skip rebuild link.'
    )
    args = parser.parse_args()

    storage_dir = args.storage_dir
    begin = args.begin_bucket
    end = args.end_bucket

    for i in range(begin, end + 1):
        if i != begin:
            time.sleep(10)

        bucket = str(i)
        data_dir = join(storage_dir, 'data', bucket)
        data_idx = join(storage_dir, 'data', bucket + '.idx')
        link_dir = join(storage_dir, 'link', bucket)
        service = '/service/rurouni-cache-%s' % bucket

        run_cmd('svc -d %s' % service)
        while get_service_status(service).status != 'down':
            print 'wating for service down'
            time.sleep(5)

        run_cmd('rm %s' % data_idx)
        run_cmd('kenshin-rebuild-index.py %s %s' % (data_dir, data_idx),
                'graphite')

        if not args.skip_rebuild_link:
            run_cmd('rm -r %s/*' % link_dir)
            run_cmd('kenshin-rebuild-link.py %s %s' % (data_dir, link_dir),
                    'graphite')

        run_cmd('svc -u %s' % service)
        while get_service_status(service).status != 'up':
            print 'wating for service up'
            time.sleep(5)


if __name__ == '__main__':
    main()
