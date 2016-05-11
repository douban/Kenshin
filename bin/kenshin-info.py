#!/usr/bin/env python
# coding: utf-8


from pprint import pprint
import kenshin


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print 'Usage: kenshin-info.py <file_path>'
        sys.exit(1)
    path = sys.argv[1]
    with open(path) as f:
        pprint(kenshin.header(f))
