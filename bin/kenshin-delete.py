#!/usr/bin/env python
# coding: utf-8
import os
import sys
import glob
import shutil
from subprocess import check_output

from kenshin import header, pack_header
from kenshin.agg import Agg

from rurouni.storage import getFilePathByInstanceDir, getMetricPathByInstanceDir


METRIC_NAME, SCHEMA_NAME, FILE_ID, POS_IDX = range(4)


def try_to_delete_empty_directory(path):
    dirname = os.path.dirname(path)
    try:
        os.rmdir(dirname)
        try_to_delete_empty_directory(dirname)
    except OSError:
        pass


def delete_links(storage_dir, metric_file):
    with open(metric_file) as f:
        for line in f:
            line = line.strip()
            bucket, schema_name, fid, pos, metric = line.split(" ")
            bucket_link_dir = os.path.join(storage_dir, 'link', bucket)
            path = getMetricPathByInstanceDir(bucket_link_dir, metric)
            if os.path.exists(path):
                os.remove(path)
                try_to_delete_empty_directory(path)


def delete_file(storage_dir, index, pos_metrics):
    """
    Note: We do not delete the data file, just delete the tags in data file,
    so the space can reused by new metric.
    """
    bucket, schema_name, fid = index
    bucket_data_dir = os.path.join(storage_dir, 'data', bucket)
    filepath = getFilePathByInstanceDir(bucket_data_dir, schema_name, fid)

    with open(filepath, "r+b") as fh:
        header_info = header(fh)
        tag_list = header_info["tag_list"]
        reserved_size = header_info["reserved_size"]
        archive_list = [(a["sec_per_point"], a["count"])
                        for a in header_info["archive_list"]]
        agg_name = Agg.get_agg_name(header_info["agg_id"])

        released_size = 0
        for pos_idx, tag in pos_metrics:
            if tag == tag_list[pos_idx]:
                tag_list[pos_idx] = ""
                released_size += len(tag)
            elif tag_list[pos_idx] != "":
                print >>sys.stderr, "tag not match: (%s, %d)" % (tag, pos_idx)

        if released_size != 0:
            inter_tag_list = tag_list + ["N" * (reserved_size + released_size)]
            packed_header, _ = pack_header(inter_tag_list,
                                           archive_list,
                                           header_info["x_files_factor"],
                                           agg_name)
            fh.write(packed_header)


def delete(storage_dir, metric_file):
    with open(metric_file) as f:
        group = []
        last_index = None
        for line in f:
            line = line.strip()
            bucket, schema_name, fid, pos, metric = line.split(" ")
            fid = int(fid)
            pos = int(pos)
            index = (bucket, schema_name, fid)
            if index == last_index:
                group.append((pos, metric))
            else:
                if last_index is not None:
                    delete_file(storage_dir, last_index, group)
                group = [(pos, metric)]
                last_index = index
        if last_index is not None:
            delete_file(storage_dir, last_index, group)

    # delete metric-test directory
    metric_test_dirs = glob.glob(os.path.join(storage_dir, '*', 'metric-test'))
    for d in metric_test_dirs:
        shutil.rmtree(d)


def sort_metric_file(metric_file):
    sorted_metric_file = "%s.sorted" % metric_file
    check_output("sort %s > %s" % (metric_file, sorted_metric_file), shell=True)
    return sorted_metric_file


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--storage-dir", help="Kenshin storage directory.")
    parser.add_argument("-m", "--metric-file", help="Metrics that need to be deleted.")
    parser.add_argument("--only-link", action="store_true", help="Only delete link files.")
    args = parser.parse_args()

    sorted_metric_file = sort_metric_file(args.metric_file)
    delete_links(args.storage_dir, sorted_metric_file)
    if not args.only_link:
        delete(args.storage_dir, sorted_metric_file)


if __name__ == '__main__':
    main()
