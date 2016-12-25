#!/usr/bin/env python
# encoding: utf-8

import unittest
from rurouni.fnv1a import get_int32_hash


class TestFnv1a(unittest.TestCase):

    def _cmp_hash(self, int32_h, uint32_h):
        if uint32_h >= 0x80000000:
            uint32_h -= 0x100000000
        self.assertEqual(int32_h, uint32_h)

    def test_fnv1a_hash(self):
        test_cases = [
            ("", 0x811c9dc5),
            ("a", 0xe40c292c),
            ("foobar", 0xbf9cf968),
            ("hello", 0x4f9f2cab),
            (b"\xff\x00\x00\x01", 0xc48fb86d),
        ]

        for s, uint32_h in test_cases:
            int32_h = get_int32_hash(s)
            self._cmp_hash(int32_h, uint32_h)
