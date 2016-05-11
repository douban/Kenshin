#!/usr/bin/env python
# coding: utf-8

import sys
from rurouni.storage import rebuildIndex


def main():
    if len(sys.argv) < 3:
        print 'need bucket_data_dir and bucket_index_file'
        sys.exit(1)

    data_dir, index_file = sys.argv[1:]
    rebuildIndex(data_dir, index_file)


if __name__ == '__main__':
    main()
