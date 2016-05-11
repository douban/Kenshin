# coding: utf-8

import unittest

from kenshin.agg import Agg


class TestAgg(unittest.TestCase):

    def setUp(self):
        self.vals = map(float, range(10))

    def _get_agg_func_by_name(self, name):
        return Agg.get_agg_func(Agg.get_agg_id(name))

    def test_get_agg_id(self):
        for i, agg in enumerate(Agg.get_agg_type_list()):
            id_ = Agg.get_agg_id(agg)
            self.assertEqual(id_, i)

    def test_agg_avg(self):
        func = self._get_agg_func_by_name('average')
        self.assertEqual(func(self.vals), 4.5)

    def test_agg_sum(self):
        func = self._get_agg_func_by_name('sum')
        self.assertEqual(func(self.vals), 45.0)

    def test_agg_last(self):
        func = self._get_agg_func_by_name('last')
        self.assertEqual(func(self.vals), 9.0)

    def test_agg_max(self):
        func = self._get_agg_func_by_name('max')
        self.assertEqual(func(self.vals), 9.0)

    def test_agg_min(self):
        func = self._get_agg_func_by_name('min')
        self.assertEqual(func(self.vals), 0.0)
