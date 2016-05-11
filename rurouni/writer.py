# coding: utf-8
import time

from twisted.application.service import Service
from twisted.internet import reactor

import kenshin
from rurouni.cache import MetricCache
from rurouni import log
from rurouni.conf import settings
from rurouni.state import instrumentation
from rurouni.storage import getFilePath


class WriterService(Service):

    def __init__(self):
        pass

    def startService(self):
        reactor.callInThread(writeForever)
        Service.startService(self)

    def stopService(self):
        try:
            file_cache_idxs = MetricCache.getAllFileCaches()
            writeCachedDataPointsWhenStop(file_cache_idxs)
        except Exception as e:
            log.err('write error when stopping service: %s' % e)
        Service.stopService(self)


def writeForever():
    while reactor.running:
        write = False
        try:
            file_cache_idxs = MetricCache.writableFileCaches()
            if file_cache_idxs:
                write = writeCachedDataPoints(file_cache_idxs)
        except Exception as e:
            log.err('write error: %s' % e)
        # The writer thread only sleeps when there is no write
        # or an error occurs
        if not write:
            time.sleep(1)


def writeCachedDataPoints(file_cache_idxs):
    pop_func = MetricCache.pop
    for schema_name, file_idx in file_cache_idxs:
        datapoints = pop_func(schema_name, file_idx)
        file_path = getFilePath(schema_name, file_idx)

        try:
            t1 = time.time()
            kenshin.update(file_path, datapoints)
            update_time = time.time() - t1
        except Exception as e:
            log.err('Error writing to %s: %s' % (file_path, e))
            instrumentation.incr('errors')
        else:
            point_cnt = len(datapoints)
            instrumentation.incr('committedPoints', point_cnt)
            instrumentation.append('updateTimes', update_time)

            if settings.LOG_UPDATES:
                log.updates("wrote %d datapoints for %s in %.5f secs" %
                            (point_cnt, schema_name, update_time))

    return True


def writeCachedDataPointsWhenStop(file_cache_idxs):
    pop_func = MetricCache.pop
    for schema_name, file_idx in file_cache_idxs:
        datapoints = pop_func(schema_name, file_idx, int(time.time()), False)
        if datapoints:
            file_path = getFilePath(schema_name, file_idx)
            try:
                kenshin.update(file_path, datapoints)
            except Exception as e:
                log.err('Error writing to %s: %s' % (file_path, e))
