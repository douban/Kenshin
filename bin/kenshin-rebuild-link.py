#!/usr/bin/env python
# coding: utf-8

import os
import sys
import glob
import kenshin
from os.path import basename, splitext
from kenshin.utils import mkdir_p


def main():
    if len(sys.argv) < 3:
        print ('Need data_dir and link_dir.\n'
               'e.g.: kenshin-rebuild-link.py /kenshin/data/a /kenshin/link/a')
        sys.exit(1)

    data_dir, link_dir = sys.argv[1:]
    data_dir = os.path.abspath(data_dir)
    link_dir = os.path.abspath(link_dir)

    for schema_name in os.listdir(data_dir):
        hs_file_pat = os.path.join(data_dir, schema_name, '*.hs')
        for fp in glob.glob(hs_file_pat):
            with open(fp) as f:
                header = kenshin.header(f)
                metric_list = header['tag_list']
                for metric in metric_list:
                    if metric != '':
                        create_link(metric, link_dir, fp)


def create_link(metric, link_dir, file_path):
    link_path = metric.replace('.', os.path.sep)
    link_path = os.path.join(link_dir, link_path + '.hs')
    dirname = os.path.dirname(link_path)
    mkdir_p(dirname)
    if os.path.exists(link_path):
        os.remove(link_path)
    os.symlink(file_path, link_path)


if __name__ == '__main__':
    main()
