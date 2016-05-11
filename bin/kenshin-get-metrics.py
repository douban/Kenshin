#!/usr/bin/env python
# coding: utf-8
import re
import os
import glob


def match_metrics(index_dir, regexps):
    index_files = glob.glob(os.path.join(index_dir, '*.idx'))
    for index in index_files:
        bucket = os.path.splitext(os.path.basename(index))[0]
        with open(index) as f:
            for line in f:
                line = line.strip()
                try:
                    metric, schema_name, fid, pos = line.split(' ')
                except ValueError:
                    pass
                for p in regexps:
                    if p.match(metric):
                        yield ' '.join([bucket, schema_name, fid, pos, metric])
                        break


def compile_regexp(regexp_file):
    with open(regexp_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                yield re.compile(line)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dirs', required=True, help='directories that contain kenshin index files, seperated by comma.')
    parser.add_argument('-f', '--regexp-file', required=True, help='file that contain regular expressions.')
    args = parser.parse_args()

    regexps = list(compile_regexp(args.regexp_file))

    for dir_ in args.dirs.split(","):
        for m in match_metrics(dir_, regexps):
            print m


if __name__ == '__main__':
    main()
