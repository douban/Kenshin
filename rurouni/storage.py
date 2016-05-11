# coding: utf-8

import os
import re
import glob
from os.path import join, sep, splitext, basename, dirname

import kenshin
from kenshin.utils import mkdir_p
from rurouni import log
from rurouni.conf import settings, OrderedConfigParser


def getFilePath(schema_name, file_idx):
    return join(settings.LOCAL_DATA_DIR, settings['instance'],
                schema_name, '%d.hs' % file_idx)


def getMetricPath(metric):
    path = metric.replace('.', sep)
    return join(settings.LOCAL_LINK_DIR, settings['instance'], path + '.hs')


def createLink(metric, file_path):
    metric_path = getMetricPath(metric)
    _createLinkHelper(metric_path, file_path)


def _createLinkHelper(link_path, file_path):
    """
    Create symlink link_path -> file_path.
    """
    dir_ = dirname(link_path)
    mkdir_p(dir_)
    if os.path.lexists(link_path):
        os.rename(link_path, link_path +'.bak')
    os.symlink(file_path, link_path)


def getFilePathByInstanceDir(instance_data_dir, schema_name, file_idx):
    return join(instance_data_dir, schema_name, "%d.hs" % file_idx)


def getMetricPathByInstanceDir(instance_link_dir, metric):
    path = metric.replace(".", sep)
    return join(instance_link_dir, path + ".hs")


def rebuildIndex(instance_data_dir, instance_index_file):
    """
    Rebuild index file from data file, if a data file has no valid metric,
    we will remove it.
    """
    out = open(instance_index_file, 'w')
    for schema_name in os.listdir(instance_data_dir):
        hs_file_pat = os.path.join(instance_data_dir, schema_name, '*.hs')
        for fp in glob.glob(hs_file_pat):
            with open(fp) as f:
                empty_flag = True
                header = kenshin.header(f)
                metric_list = header['tag_list']
                file_id = splitext(basename(fp))[0]
                for i, metric in enumerate(metric_list):
                    if metric != '':
                        empty_flag = False
                        out.write('%s %s %s %s\n' %
                                  (metric, schema_name, file_id, i))
            if empty_flag:
                os.remove(fp)
    out.close()


def rebuildLink(instance_data_dir, instance_link_dir):
    for schema_name in os.listdir(instance_data_dir):
        hs_file_pat = os.path.join(instance_data_dir, schema_name, '*.hs')
        for fp in glob.glob(hs_file_pat):
            with open(fp) as f:
                header = kenshin.header(f)
                metric_list = header['tag_list']
                for metric in metric_list:
                    if metric != '':
                        link_path = getMetricPathByInstanceDir(instance_link_dir, metric)
                        _createLinkHelper(link_path, fp)


class Archive:
    def __init__(self, secPerPoint, points):
        self.secPerPoint = secPerPoint
        self.points = points

    def __str__(self):
        return 'Archive(%s, %s)' % (self.secPerPoint, self.points)

    def getTuple(self):
        return self.secPerPoint, self.points

    @staticmethod
    def fromString(retentionDef):
        rs = kenshin.parse_retention_def(retentionDef)
        return Archive(*rs)


class Schema(object):
    def match(self, metric):
        raise NotImplementedError()


class DefaultSchema(Schema):
    def __init__(self, name, xFilesFactor, aggregationMethod, archives,
                 cache_retention, metrics_max_num, cache_ratio):
        self.name = name
        self.xFilesFactor = xFilesFactor
        self.aggregationMethod = aggregationMethod
        self.archives = archives
        self.cache_retention = cache_retention
        self.metrics_max_num = metrics_max_num
        self.cache_ratio = cache_ratio

    def match(self, metric):
        return True


class PatternSchema(Schema):
    def __init__(self, name, pattern, xFilesFactor, aggregationMethod, archives,
                 cache_retention, metrics_max_num, cache_ratio):
        self.name = name
        self.pattern = re.compile(pattern)
        self.xFilesFactor = xFilesFactor
        self.aggregationMethod = aggregationMethod
        self.archives = archives
        self.cache_retention = cache_retention
        self.metrics_max_num = metrics_max_num
        self.cache_ratio = cache_ratio

    def match(self, metric):
        return self.pattern.match(metric)


def loadStorageSchemas(conf_file):
    schema_list = []
    config = OrderedConfigParser()
    config.read(conf_file)

    for section in config.sections():
        options = dict(config.items(section))

        pattern = options.get('pattern')
        xff = float(options.get('xfilesfactor'))
        agg = options.get('aggregationmethod')
        retentions = options.get('retentions').split(',')
        archives = [Archive.fromString(s).getTuple() for s in retentions]
        cache_retention = kenshin.RetentionParser.parse_time_str(
            options.get('cacheretention'))
        metrics_max_num = options.get('metricsperfile')
        cache_ratio = 1.2

        try:
            kenshin.validate_archive_list(archives, xff)
        except kenshin.InvalidConfig:
            log.err("Invalid schema found in %s." % section)

        schema = PatternSchema(section, pattern, float(xff), agg, archives,
                               int(cache_retention), int(metrics_max_num),
                               float(cache_ratio))
        schema_list.append(schema)
    schema_list.append(defaultSchema)
    return schema_list


# default schema

defaultSchema = DefaultSchema(
    'default',
    1.0,
    'avg',
    ((60, 60 * 24 * 7)),  # default retention (7 days of minutely data)
    600,
    40,
    1.2
)


class StorageSchemas(object):
    def __init__(self, conf_file):
        self.schemas = loadStorageSchemas(conf_file)

    def getSchemaByMetric(self, metric):
        for schema in self.schemas:
            if schema.match(metric):
                return schema
        return defaultSchema

    def getSchemaByName(self, schema_name):
        for schema in self.schemas:
            if schema.name == schema_name:
                return schema
        return None


if __name__ == '__main__':
    import sys
    loadStorageSchemas(sys.argv[1])
