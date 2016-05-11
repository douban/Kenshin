#!/usr/bin/env python
# coding: utf-8
import sys
import os
import time
import glob

import kenshin
from kenshin.consts import NULL_VALUE
from kenshin.agg import Agg
from rurouni.storage import loadStorageSchemas


def get_schema(storage_schemas, schema_name):
    for schema in storage_schemas:
        if schema.name == schema_name:
            return schema


def resize_data_file(schema, data_file):
    print data_file
    rebuild = False
    with open(data_file) as f:
        header = kenshin.header(f)
    retentions = schema.archives
    old_retentions = [(x['sec_per_point'], x['count'])
                      for x in header['archive_list']]
    msg = ""
    if retentions != old_retentions:
        rebuild = True
        msg += "retentions:\n%s -> %s" % (old_retentions, retentions)

    if not rebuild:
        print "No operation needed."
        return

    print msg
    now = int(time.time())
    tmpfile = data_file + '.tmp'
    if os.path.exists(tmpfile):
        print "Removing previous temporary database file: %s" % tmpfile
        os.unlink(tmpfile)

    print "Creating new kenshin database: %s" % tmpfile
    kenshin.create(tmpfile,
                   [''] * len(header['tag_list']),
                   schema.archives,
                   header['x_files_factor'],
                   Agg.get_agg_name(header['agg_id']))
    for i, t in enumerate(header['tag_list']):
        kenshin.add_tag(t, tmpfile, i)

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
    usage = ("e.g: python bin/kenshin-resize.py -d ../graphite-root/conf/ -n default -f '../graphite-root/storage/data/*/default/*.hs'\n"
             "Note: kenshin combined many metrics to one file, "
             "      please check file's meta data before you resize it. "
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
