#!/usr/bin/env python
# coding: utf-8
'''
目前 kenshin 不支持动态加载配置，在配置变更时需要重启，此脚本用于重启所有 kenshin 实例。

$ sudo kenshin-restart.py
'''

import re
import time
import glob
from subprocess import check_output


class Status(object):
    def __init__(self, status, pid, time):
        self.status = status
        self.pid = pid
        self.time = int(time)

    def __str__(self):
        return '<Status (%s, %s, %s)>' % (self.status, self.pid, self.time)


def find_cache_services(start_num):
    def get_instance_num(service_path):
        return int(service_path.rsplit('-', 1)[1])

    services = glob.glob('/service/rurouni-cache-*')
    services = [x for x in services if get_instance_num(x) >= start_num]
    return sorted(services, key=get_instance_num)


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


def svc(service_name, arg):
    cmd = ['svc', arg, service_name]
    return check_output(cmd)


def svc_restart(service_name):
    return svc(service_name, '-t')


def restart_service(service_name):
    old = get_service_status(service_name)
    assert old.status == 'up'
    svc_restart(service_name)
    i = 0
    while True:
        time.sleep(2)
        new = get_service_status(service_name)
        print i, new
        if new.status == 'up':
            # 重启成功需要满足下面两个条件:
            # 1. pid 已经发生变化
            # 2. 新的服务已经可用
            #
            # 关于第 2 点，由于 kenshin 没有对外的接口可以查到内部状态，
            # 所以目前是靠时间来估计服务状态。未来计划把 pickle 格式端口去掉，
            # 用 pickle 格式端口作为内部状态查询的接口，也可以通过该接口
            # 实现动态加载配置文件，到时这个脚本的就可以下岗了。
            # PS: 时间的单位是"秒"
            if new.pid != old.pid and new.time > 10:
                break
        i += 1

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--time-interval", default=60, type=int,
        help="time interval between two restarting operations.")
    parser.add_argument("-s", "--start-num", default=0, type=int,
        help="start instance number")
    args = parser.parse_args()
    services = find_cache_services(args.start_num)
    for s in services:
        print 'restarting %s' % s
        print get_service_status(s)
        restart_service(s)
        print get_service_status(s)
        print
        time.sleep(args.time_interval)


if __name__ == '__main__':
    main()
