# coding: utf-8
import os
import shutil
import unittest

import kenshin.storage
from kenshin.storage import Storage, enable_debug, RetentionParser
from kenshin.utils import mkdir_p


class TestStorageIO(unittest.TestCase):
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

    def tearDown(self):
        shutil.rmtree(self.data_dir)

    def _basic_setup(self):
        metric_name = 'sys.cpu.user'
        self.file_cnt = 40

        tag_list = ['host=webserver%s,cpu=%s' % (i, i)
                    for i in range(self.file_cnt)]
        archive_list = "1s:1h,60s:2d,300s:7d,15m:25w,12h:5y".split(',')
        archive_list = [RetentionParser.parse_retention_def(x)
                        for x in archive_list]

        x_files_factor = 20
        agg_name = 'min'
        return [metric_name, tag_list, archive_list, x_files_factor, agg_name]

    def _gen_val(self, i):
        res = []
        for j in range(self.file_cnt):
            res.append(i + 10*j)
        return tuple(res)

    def test_io(self):
        """
        test io perfermance.

        (1000 io/s * 3600 s * 24) / (3*10**6 metric) / (40 metric/file) = 1152 io/file
        由于 header 函数在一次写入中被调用了多次，而 header 数据较小，完全可以读取缓存数据，
        因此 enable_debug 中忽略了 header 的读操作。
        """
        enable_debug(ignore_header=True)

        now_ts = 1411628779
        ten_min = 10 * RetentionParser.TIME_UNIT['minutes']
        one_day = RetentionParser.TIME_UNIT['days']
        from_ts = now_ts - one_day

        for i in range(one_day / ten_min):
            points = [(from_ts + i * ten_min + j, self._gen_val(i * ten_min + j))
                      for j in range(ten_min)]
            self.storage.update(self.path, points, from_ts + (i+1) * ten_min)

        open_ = kenshin.storage.open
        io = open_.read_cnt + open_.write_cnt
        io_limit = 1152
        self.assertLessEqual(io, io_limit)
