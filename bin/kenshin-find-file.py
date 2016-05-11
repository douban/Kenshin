#!/usr/bin/env python
# coding: utf-8
import os
import re
import glob
from collections import defaultdict

default_black_list = [
    '.*metric_test.*',
    '^rurouni\.',
    '^carbon\.',
    '^stats\.counters\..*\.count$',
]


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--data-dir', required=True,
        help="data directory.")
    parser.add_argument(
        '-f', '--black-list-file',
        help="black list (regular expression for metric) file."
        )
    args = parser.parse_args()

    idx_files = glob.glob(os.path.join(args.data_dir, '*.idx'))
    black_list_pattern = gen_black_list_pattern(args.black_list_file)
    for idx_file in idx_files:
        dir_, filename = os.path.split(idx_file)
        instance = os.path.splitext(filename)[0]
        for p in yield_kenshin_files(dir_, instance, idx_file, black_list_pattern):
            print p


def gen_black_list_pattern(black_list_file):
    rs = []
    if not black_list_file:
        for x in default_black_list:
            rs.append(re.compile(x))
    else:
        with open(black_list_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    rs.append(re.compile(line))
    return rs


def yield_kenshin_files(dir_, instance, idx_file, black_list_pattern):
    all_fids = defaultdict(set)
    del_fids = defaultdict(set)
    with open(idx_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                metric, schema, fid, _ = line.split()
                fid = int(fid)
            except Exception:
                continue
            all_fids[schema].add(fid)
            for p in black_list_pattern:
                if p.match(metric):
                    del_fids[schema].add(fid)
                    break
    for schema in all_fids:
        valid_fids = all_fids[schema] - del_fids[schema]
        for i in sorted(valid_fids)[:-1]:
            path = os.path.join(dir_, instance, schema, '%s.hs' % i)
            yield path


if __name__ == '__main__':
     main()
