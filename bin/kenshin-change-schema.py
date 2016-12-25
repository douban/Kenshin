#!/usr/bin/env python
# coding: utf-8
import sys
import os
import time
import glob
import struct

import kenshin
from kenshin.consts import NULL_VALUE
from kenshin.agg import Agg
from rurouni.storage import loadStorageSchemas


# Three action types.
NO_OPERATION, CHANGE_META, REBUILD = range(3)


def get_schema(storage_schemas, schema_name):
    for schema in storage_schemas:
        if schema.name == schema_name:
            return schema


def resize_data_file(schema, data_file):
    print data_file
    with open(data_file) as f:
        header = kenshin.header(f)
    retentions = schema.archives
    old_retentions = [(x['sec_per_point'], x['count'])
                      for x in header['archive_list']]
    msg = []
    action = NO_OPERATION

    # x files factor
    if schema.xFilesFactor != header['x_files_factor']:
        action = CHANGE_META
        msg.append("x_files_factor: %f -> %f" %
                   (header['x_files_factor'], schema.xFilesFactor))

    # agg method
    old_agg_name = Agg.get_agg_name(header['agg_id'])
    if schema.aggregationMethod != old_agg_name:
        action = CHANGE_META
        msg.append("agg_name: %s -> %s" %
                   (old_agg_name, schema.aggregationMethod))

    # retentions
    if retentions != old_retentions:
        action = REBUILD
        msg.append("retentions: %s -> %s" % (old_retentions, retentions))

    if action == NO_OPERATION:
        print "No operation needed."
        return

    elif action == CHANGE_META:
        print 'Change Meta.'
        print '\n'.join(msg)
        change_meta(data_file, schema, header['max_retention'])
        return

    elif action == REBUILD:
        print 'Rebuild File.'
        print '\n'.join(msg)
        rebuild(data_file, schema, header, retentions)

    else:
        raise ValueError(action)


def change_meta(data_file, schema, max_retention):
    with open(data_file, 'r+b') as f:
        format = '!2Lf'
        agg_id = Agg.get_agg_id(schema.aggregationMethod)
        xff = schema.xFilesFactor
        packed_data = struct.pack(format, agg_id, max_retention, xff)
        f.write(packed_data)


def rebuild(data_file, schema, header, retentions):
    now = int(time.time())
    tmpfile = data_file + '.tmp'
    if os.path.exists(tmpfile):
        print "Removing previous temporary database file: %s" % tmpfile
        os.unlink(tmpfile)

    print "Creating new kenshin database: %s" % tmpfile
    kenshin.create(tmpfile,
                   header['tag_list'],
                   retentions,
                   schema.xFilesFactor,
                   schema.aggregationMethod)

    size = os.stat(tmpfile).st_size
    old_size = os.stat(data_file).st_size

    print "Created: %s (%d bytes, was %d bytes)" % (
        tmpfile, size, old_size)

    print "Migrating data to new kenshin database ..."
    for archive in header['archive_list']:
        from_time = now - archive['retention'] + archive['sec_per_point']
        until_time = now
        _, timeinfo, values = kenshin.fetch(data_file, from_time, until_time)
        datapoints = zip(range(*timeinfo), values)
        datapoints = [[p[0], list(p[1])] for p in datapoints if p[1]]
        for _, values in datapoints:
            for i, v in enumerate(values):
                if v is None:
                    values[i] = NULL_VALUE
        kenshin.update(tmpfile, datapoints)
    backup = data_file + ".bak"

    print 'Renaming old database to: %s' % backup
    os.rename(data_file, backup)

    print "Renaming new database to: %s" % data_file
    try:
        os.rename(tmpfile, data_file)
    except Exception as e:
        print "Operation failed, restoring backup"
        os.rename(backup, data_file)
        raise e
        # Notice: by default, '.bak' files are not deleted.


def main():
    usage = ("e.g: kenshin-change-schema.py -d ../graphite-root/conf/ -n default -f '../graphite-root/storage/data/*/default/*.hs'\n"
             "Note: kenshin combined many metrics to one file, "
             "      please check file's meta data before you change it. "
             "      (use keshin-info.py to view file's meta data)")

    import argparse
    parser = argparse.ArgumentParser(description=usage,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "-d", "--conf-dir", required=True, help="kenshin conf directory.")
    parser.add_argument(
        "-n", "--schema-name", required=True, help="schema name.")
    parser.add_argument(
        "-f", "--files", required=True,
        help="metric data file paterns. (e.g. /data/kenshin/storage/data/*/mfs/*.hs)")
    args = parser.parse_args()

    storage_conf_path = os.path.join(args.conf_dir, 'storage-schemas.conf')
    storage_schemas = loadStorageSchemas(storage_conf_path)
    schema = get_schema(storage_schemas, args.schema_name)
    if not schema:
        print 'not matched schema name: %s' % args.schema_name
        sys.exit(1)
    for f in sorted(glob.glob(args.files)):
        resize_data_file(schema, os.path.abspath(f))


if __name__ == '__main__':
    main()
