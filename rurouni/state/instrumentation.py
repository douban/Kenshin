# coding: utf-8
import os
import time
import socket
from resource import getrusage, RUSAGE_SELF

from twisted.application.service import Service
from twisted.internet.task import LoopingCall

from rurouni.conf import settings
from rurouni import log


# consts
HOSTNAME = socket.gethostname().replace('.', '_')
PAGESIZE = os.sysconf('SC_PAGESIZE')

# globals
stats = {}
prior_stats = {}

def _get_usage_info():
    rusage = getrusage(RUSAGE_SELF)
    curr_usage = rusage.ru_utime + rusage.ru_stime
    curr_time = time.time()
    return curr_usage, curr_time

last_usage, last_usage_time = _get_usage_info()


def incr(stat, amount=1):
    stats.setdefault(stat, 0)
    stats[stat] += amount


def max(stat, new_val):
    try:
        if stats[stat] < new_val:
            stats[stat] = new_val
    except KeyError:
        stats[stat] = new_val


def append(stat, val):
    stats.setdefault(stat, [])
    stats[stat].append(val)


def get_cpu_usage():
    global last_usage, last_usage_time
    curr_usage, curr_time = _get_usage_info()

    usage_diff = curr_usage - last_usage
    time_diff = curr_time - last_usage_time
    cpu_usage_percent = (usage_diff / time_diff) * 100.

    last_usage, last_usage_time = curr_usage, curr_time
    return cpu_usage_percent


def get_mem_usage():
    rss_pages = int(open('/proc/self/statm').read().split()[1])
    return rss_pages * PAGESIZE


def record_metrics():
    _stats = stats.copy()
    stats.clear()

    # rurouni cache
    record = cache_record
    update_times = _stats.get('updateTimes', [])
    committed_points = _stats.get('committedPoints', 0)
    creates = _stats.get('creates', 0)
    dropped_creates = _stats.get('droppedCreates', 0)
    errors = _stats.get('errors', 0)
    cache_queries = _stats.get('cacheQueries', 0)
    cache_overflow = _stats.get('cacheOverflow', 0)

    if update_times:
        avg_update_time = sum(update_times) / len(update_times)
        record('avgUpdateTime', avg_update_time)

    if committed_points:
        points_per_update = float(committed_points) / len(update_times)
        record('pointsPerUpdate', points_per_update)

    record('updateOperations', len(update_times))
    record('committedPoints', committed_points)
    record('creates', creates)
    record('droppedCreates', dropped_creates)
    record('errors', errors)
    record('cacheQueries', cache_queries)
    record('cacheOverflow', cache_overflow)

    record('metricReceived', _stats.get('metricReceived', 0))
    record('cpuUsage', get_cpu_usage())
    # this only workds on linux
    try:
        record('memUsage', get_mem_usage())
    except:
        pass


def cache_record(metric_type, val):
    prefix = settings.RUROUNI_METRIC
    metric_tmpl = prefix + '.%s.%s.%s'
    if settings.instance is None:
        metric = metric_tmpl % (HOSTNAME, 'a', metric_type)
    else:
        metric = metric_tmpl % (HOSTNAME, settings.instance, metric_type)
    datapoint = int(time.time()), val
    cache.MetricCache.put(metric, datapoint)


class InstrumentationService(Service):
    def __init__(self):
        self.record_task = LoopingCall(record_metrics)
        self.metric_interval = settings.RUROUNI_METRIC_INTERVAL

    def startService(self):
        if self.metric_interval > 0:
            self.record_task.start(self.metric_interval, False)
        Service.startService(self)

    def stopService(self):
        if self.metric_interval > 0:
            self.record_task.stop()
        Service.stopService(self)


# avoid import circularities
from rurouni import cache
