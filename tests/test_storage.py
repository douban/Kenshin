# coding: utf-8
import os
import shutil
import struct
import unittest

from kenshin.storage import Storage
from kenshin.agg import Agg
from kenshin.utils import mkdir_p, roundup
from kenshin.consts import NULL_VALUE


class TestStorageBase(unittest.TestCase):
    data_dir = '/tmp/kenshin'

    def setUp(self):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)

        mkdir_p(self.data_dir)
        self.storage = Storage(data_dir=self.data_dir)
        self.basic_setup = self._basic_setup()
        self.storage.create(*self.basic_setup)

        metric_name = self.basic_setup[0]
        self.path = self.storage.gen_path(self.data_dir, metric_name)
        tag_list = self.basic_setup[1]
        self.null_point = (None,) * len(tag_list)

    def tearDown(self):
        shutil.rmtree(self.data_dir)

    @staticmethod
    def _gen_val(i, num=2):
        return [10 * j + i for j in range(num)]

class TestStorage(TestStorageBase):

    def _basic_setup(self):
        metric_name = 'sys.cpu.user'

        tag_list = [
            'host=webserver01,cpu=0',
            'host=webserver01,cpu=1',
        ]
        archive_list = [
            (1, 6),
            (3, 6),
        ]
        x_files_factor = 1.0
        agg_name = 'min'
        return [metric_name, tag_list, archive_list, x_files_factor, agg_name]

    def test_gen_path(self):
        metric_name = 'a.b.c'
        data_dir = '/x/y'
        path = self.storage.gen_path(data_dir, metric_name)
        self.assertEqual(path, '/x/y/a/b/c.hs')

    def test_header(self):
        metric_name, tag_list, archive_list, x_files_factor, agg_name = self.basic_setup
        with open(self.path, 'rb') as f:
            header = self.storage.header(f)

        self.assertEqual(tag_list, header['tag_list'])
        self.assertEqual(x_files_factor, header['x_files_factor'])
        self.assertEqual(Agg.get_agg_id(agg_name), header['agg_id'])

        _archive_list = [(x['sec_per_point'], x['count'])
                         for x in header['archive_list']]
        self.assertEqual(archive_list, _archive_list)

    def test_basic_update_fetch(self):
        now_ts = 1411628779
        num_points = 5
        points = [(now_ts - i, self._gen_val(i)) for i in range(1, num_points+1)]
        self.storage.update(self.path, points, now_ts)

        from_ts = now_ts - num_points
        series = self.storage.fetch(self.path, from_ts, now=now_ts)

        time_info = (from_ts, now_ts, 1)
        vals = [tuple(map(float, v)) for _, v in sorted(points)]
        expected = (time_info, vals)
        self.assertEqual(series[1:], expected)

    def test_update_propagate(self):
        now_ts = 1411628779
        num_points = 6
        points = [(now_ts - i, self._gen_val(i)) for i in range(1, num_points+1)]
        self.storage.update(self.path, points, now_ts)

        from_ts = now_ts - num_points - 1
        series = self.storage.fetch(self.path, from_ts, now=now_ts)
        time_info = (from_ts, roundup(now_ts, 3), 3)
        expected = time_info, [(5.0, 15.0), (2.0, 12.0), self.null_point]
        self.assertEqual(series[1:], expected)

    def test_null_point(self):
        now_ts = 1411628779
        num_points = 6
        points = [(now_ts - i, self._gen_val(i)) for i in range(1, num_points+1)]
        # change the last two points to null value
        points[4] = (now_ts - 5, (NULL_VALUE, NULL_VALUE))
        points[5] = (now_ts - 6, (NULL_VALUE, NULL_VALUE))

        self.storage.update(self.path, points, now_ts)

        from_ts = now_ts - num_points - 1
        series = self.storage.fetch(self.path, from_ts, now=now_ts)
        time_info = (from_ts, roundup(now_ts, 3), 3)
        expected = time_info, [self.null_point, (2.0, 12.0), self.null_point]
        self.assertEqual(series[1:], expected)

    def test_update_old_points(self):
        now_ts = 1411628779
        num_points = 12
        points = [(now_ts - i, self._gen_val(i)) for i in range(7, num_points+1)]
        self.storage.update(self.path, points, now_ts)

        from_ts = now_ts - num_points - 1
        series = self.storage.fetch(self.path, from_ts, now=now_ts)
        time_info = (from_ts, roundup(now_ts, 3), 3)
        expected = time_info, [(12.0, 22.0), (10.0, 20.0), (7.0, 17.0), self.null_point, self.null_point]
        self.assertEqual(series[1:], expected)

    def test_fetch_empty_metric(self):
        now_ts = 1411628779
        from_ts = 1411628775
        series = self.storage.fetch(self.path, from_ts, now=now_ts)
        time_info = (from_ts, now_ts, 1)
        expected = time_info, [self.null_point] * (now_ts - from_ts)
        self.assertEqual(series[1:], expected)

    def print_file_content(self):
        with open(self.path) as f:
            header = self.storage.header(f)
            archive_list = header['archive_list']
            for i, archive in enumerate(archive_list):
                print "--------- archive %d ------------" % i
                print archive
                f.seek(archive['offset'])
                series_str = f.read(archive['size'])
                point_format = header['point_format']
                series_format = point_format[0] + point_format[1:] * archive['count']
                unpacked_series = struct.unpack(series_format, series_str)
                print unpacked_series


class TestLostPoint(TestStorageBase):

    def _basic_setup(self):
        metric_name = 'sys.cpu.user'

        tag_list = [
            'host=webserver01,cpu=0',
            'host=webserver01,cpu=1',
        ]
        archive_list = [
            (1, 60),
            (3, 60),
        ]
        x_files_factor = 5
        agg_name = 'min'
        return [metric_name, tag_list, archive_list, x_files_factor, agg_name]

    def test_update_propagate(self):
        now_ts = 1411628779
        point_seeds_list = [range(30, 45), range(15)]
        mtime = None
        for i, point_seeds in enumerate(point_seeds_list):
            if i != 0:
                mtime = now_ts - max(point_seeds_list[i - 1])
            points = [(now_ts - i, self._gen_val(i)) for i in point_seeds]
            self.storage.update(self.path, points, now_ts, mtime)

        from_ts = now_ts - 60 - 1
        series = self.storage.fetch(self.path, from_ts, now=now_ts)
        time_info = (from_ts, roundup(now_ts, 3), 3)
        null = self.null_point
        values = [null, null, null, null, null, (44.0, 54.0), (41.0, 51.0),
                  (38.0, 48.0), (35.0, 45.0), (32.0, 42.0), (30.0, 40.0),
                  null, null, null, null, (14.0, 24.0), (11.0, 21.0), (8.0, 18.0),
                  (5.0, 15.0), null, null]
        expected = time_info, values
        self.assertEqual(series[1:], expected)

    def test_update_propagate_with_special_start_time(self):
        now_ts = 1411628779
        # start time is 1411628760
        point_seeds_list = [range(10, 20), range(1, 7)]
        mtime = None
        for i, point_seeds in enumerate(point_seeds_list):
            if i != 0:
                mtime = now_ts - max(point_seeds_list[i - 1])
            points = [(now_ts - i, self._gen_val(i)) for i in point_seeds]
            self.storage.update(self.path, points, now_ts, mtime)
        from_ts = 1411628760
        until_ts = from_ts + 15
        series = self.storage.fetch(self.path, from_ts, until_ts,
                                    now=from_ts + 60 + 1)
        time_info = (from_ts, roundup(until_ts, 3), 3)
        values = [(17.0, 27.0), (14.0, 24.0), (11.0, 21.0), (10.0, 20.0), (5.0, 15.0)]
        expected = (time_info, values)
        self.assertEqual(series[1:], expected)

    def test_basic_update(self):
        now_ts = 1411628779
        point_seeds = [1, 2, 4, 5]
        points = [(now_ts - i, self._gen_val(i)) for i in point_seeds]
        self.storage.update(self.path, points, now_ts)

        from_ts = now_ts - 5
        series = self.storage.fetch(self.path, from_ts, now=now_ts)
        time_info = (from_ts, now_ts, 1)
        vals = [(5.0, 15.0), (4.0, 14.0), self.null_point, (2.0, 12.0), (1.0, 11.0)]
        expected = time_info, vals
        self.assertEqual(series[1:], expected)


class TestMultiArchive(TestStorageBase):

    def _basic_setup(self):
        metric_name = 'sys.cpu.user'

        tag_list = [
            'host=webserver01,cpu=0',
            'host=webserver01,cpu=1',
            'host=webserver01,cpu=2',
        ]
        archive_list = [
            (1, 60),
            (3, 60),
            (6, 60),
        ]
        x_files_factor = 5
        agg_name = 'min'
        return [metric_name, tag_list, archive_list, x_files_factor, agg_name]

    def test_time_range(self):
        now_ts = 1411628779
        #  downsample time of chive2: 1411628760 (floor(1411628779. / (6*5)))
        point_seeds_list = [range(19, 30), range(5, 2)]
        mtime = None
        for i, point_seeds in enumerate(point_seeds_list):
            if i != 0:
                mtime = now_ts - max(point_seeds_list[i - 1])
            points = [(now_ts - i, self._gen_val(i, num=3)) for i in point_seeds]
            self.storage.update(self.path, points, now_ts, mtime)
        from_ts = 1411628760 - 2 * 6
        until_ts = 1411628760
        series = self.storage.fetch(self.path, from_ts, until_ts,
                                    now=from_ts + 180 + 1)
        time_info = (from_ts, roundup(until_ts, 6), 6)
        values = [(26.0, 36.0, 46.0), (20.0, 30.0, 40.0)]
        expected = (time_info, values)
        self.assertEqual(series[1:], expected)
