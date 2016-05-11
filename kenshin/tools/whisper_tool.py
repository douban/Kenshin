# coding: utf-8
import os
import urllib
import re
import struct

import kenshin
from rurouni.conf import OrderedConfigParser


longFormat = "!L"
longSize = struct.calcsize(longFormat)
floatFormat = "!f"
floatSize = struct.calcsize(floatFormat)
valueFormat = "!d"
valueSize = struct.calcsize(valueFormat)
pointFormat = "!Ld"
pointSize = struct.calcsize(pointFormat)
metadataFormat = "!2LfL"
metadataSize = struct.calcsize(metadataFormat)
archiveInfoFormat = "!3L"
archiveInfoSize = struct.calcsize(archiveInfoFormat)

agg_type_dict = dict({
  1: 'average',
  2: 'sum',
  3: 'last',
  4: 'max',
  5: 'min'
})


def get_agg_name(agg_id):
    return agg_type_dict[agg_id]


def remote_url(filepath):
    return filepath.startswith('http://')


def read_header(filename):
    if remote_url(filename):
        fh = urllib.urlopen(filename)
    else:
        fh = open(filename)
    packed_meta = fh.read(metadataSize)

    try:
        agg_type, max_ret, xff, archive_cnt = struct.unpack(
            metadataFormat, packed_meta)
    except:
        raise Exception("Unable to read header", filename)

    archives = []
    for i in xrange(archive_cnt):
        packed_archive_info = fh.read(archiveInfoSize)
        try:
            off, sec, cnt = struct.unpack(archiveInfoFormat, packed_archive_info)
        except:
            raise Exception(
                "Unable to read archive%d metadata" % i, filename)

        archive_info = {
            'offset': off,
            'sec_per_point': sec,
            'count': cnt,
            'size': pointSize * cnt,
            'retention': sec * cnt,
        }
        archives.append(archive_info)

    info = {
        'xff': xff,
        'archives': archives,
        'agg_type': agg_type,
    }
    fh.close()
    return info


### schema (copy from carbon with some small change)

class Schema:
    def match(self, metric):
        raise NotImplementedError()


class DefaultSchema(Schema):
    def __init__(self, name, archives):
        self.name = name
        self.archives = archives

    def match(self, metric):
        return True


class PatternSchema(Schema):
    def __init__(self, name, pattern, archives):
        self.name = name
        self.pattern = pattern
        self.regex = re.compile(pattern)
        self.archives = archives

    def match(self, metric):
        return self.regex.search(metric)

class Archive:
    def __init__(self, secondsPerPoint, points):
        self.secondsPerPoint = int(secondsPerPoint)
        self.points = int(points)

    def __str__(self):
        return "Archive = (Seconds per point: %d, Datapoints to save: %d)" % (
               self.secondsPerPoint, self.points)

    def getTuple(self):
        return (self.secondsPerPoint, self.points)

    @staticmethod
    def fromString(retentionDef):
        rs = kenshin.parse_retention_def(retentionDef)
        return Archive(*rs)


def loadStorageSchemas(storage_schemas_conf):
    schemaList = []
    config = OrderedConfigParser()
    config.read(storage_schemas_conf)

    for section in config.sections():
        options = dict(config.items(section))
        pattern = options.get('pattern')

        retentions = options['retentions'].split(',')
        archives = [Archive.fromString(s).getTuple() for s in retentions]

        mySchema = PatternSchema(section, pattern, archives)
        schemaList.append(mySchema)

    schemaList.append(defaultSchema)
    return schemaList


def loadAggregationSchemas(storage_aggregation_conf):
    schemaList = []
    config = OrderedConfigParser()
    config.read(storage_aggregation_conf)

    for section in config.sections():
        options = dict(config.items(section))
        pattern = options.get('pattern')
        aggregationMethod = options.get('aggregationmethod')
        archives = aggregationMethod
        mySchema = PatternSchema(section, pattern, archives)
        schemaList.append(mySchema)

    schemaList.append(defaultAggregation)
    return schemaList

defaultArchive = Archive(60, 60 * 24 * 7) # default retention for unclassified data (7 days of minutely data)
defaultSchema = DefaultSchema('default', [defaultArchive])
defaultAggregation = DefaultSchema('default', (None, None))


class NewSchema(Schema):
    def __init__(self, name, archives, aggregationMethod):
        self.name = name
        self.archives = archives
        self.aggregationMethod = aggregationMethod


def gen_whisper_schema_func(whisper_conf_dir):
    storage_schemas_conf = os.path.join(whisper_conf_dir, 'storage-schemas.conf')
    storage_aggregation_conf = os.path.join(whisper_conf_dir, 'storage-aggregation.conf')
    storage_schemas = loadStorageSchemas(storage_schemas_conf)
    storage_aggs = loadAggregationSchemas(storage_aggregation_conf)

    def get_schema(schemas, metric):
        for schema in schemas:
            if schema.match(metric):
                return schema

    def _(metric):
        schema = get_schema(storage_schemas, metric)
        agg = get_schema(storage_aggs, metric)
        return NewSchema(schema.name, schema.archives, agg.archives)
    return _
