# coding: utf-8

"""
Copyright 2015-Present Douban Inc.
Copyright 2009-2014 The Graphite Development Team
Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

#
# This module is an implementation of storage API. Here is the
# basic layout of fileformat.
#
# File = Header, Data
#     Header = Metadata, Tag+, ArchiveInfo+
#         Metadata = agg_id, max_retention, x_files_factor, archive_count, tag_size, point_size
#         Tag = metric
#         ArchiveInfo = offset, seconds_per_point, point_count
#     Data = Archive+
#         Archive = Point+
#             Point = timestamp, value
#

import os
import re
import time
import numpy as np
import math
import struct
import operator
import inspect

from kenshin.agg import Agg
from kenshin.utils import mkdir_p, roundup
from kenshin.consts import DEFAULT_TAG_LENGTH, NULL_VALUE, CHUNK_SIZE


LONG_FORMAT = "!L"
LONG_SIZE = struct.calcsize(LONG_FORMAT)
FLOAT_FORMAT = "!f"
FLOAT_SIZE = struct.calcsize(FLOAT_FORMAT)
VALUE_FORMAT = "!d"
VALUE_SIZE = struct.calcsize(VALUE_FORMAT)
POINT_FORMAT = "!L%dd"
METADATA_FORMAT = "!2Lf3L"
METADATA_SIZE = struct.calcsize(METADATA_FORMAT)
ARCHIVEINFO_FORMAT = "!3L"
ARCHIVEINFO_SIZE = struct.calcsize(ARCHIVEINFO_FORMAT)

# reserved tag index for reserved space,
# this is usefull when adding a tag to a file.
RESERVED_INDEX = -1


### Exceptions

class KenshinException(Exception):
    pass


class InvalidTime(KenshinException):
    pass


class InvalidConfig(KenshinException):
    pass


### debug tool

debug = lambda *a, **kw: None


def enable_debug(ignore_header=False):
    """
    监控读写操作.

    由于 header 函数在一次写入中被调用了多次，而 header 数据较小，完全可以读取缓存数据，
    因此 enable_debug 中使用 ignore_header 来忽略了 header 的读操作，从而方便 io
    性能的测试.
    """
    global open, debug

    if not ignore_header:
        def debug(msg):
            print "DEBUG :: %s" % msg

    class open(file):
        write_cnt = 0
        read_cnt = 0

        def __init__(self, *args, **kwargs):
            caller = self.get_caller()
            debug("=========== open in %s ===========" % caller)
            file.__init__(self, *args, **kwargs)

        def write(self, data):
            caller = self.get_caller()
            open.write_cnt += 1
            debug("Write %d bytes #%d in %s" % (len(data),
                                                self.write_cnt, caller))
            return file.write(self, data)

        def read(self, bytes):
            caller = self.get_caller()
            if ignore_header and caller == "header":
                pass
            else:
                open.read_cnt += 1
            debug("Read %d bytes #%d in %s" % (bytes, self.read_cnt, caller))
            return file.read(self, bytes)

        def get_caller(self):
            return inspect.stack()[2][3]


### retention parser

class RetentionParser(object):
    TIME_UNIT = {
        'seconds': 1,
        'minutes': 60,
        'hours': 3600,
        'days': 86400,
        'weeks': 86400 * 7,
        'years': 86400 * 365,
    }
    # time pattern (e.g. 60s, 12h)
    pat = re.compile(r'^(\d+)([a-z]+)$')

    @classmethod
    def get_time_unit_name(cls, s):
        for k in cls.TIME_UNIT.keys():
            if k.startswith(s):
                return k
        raise InvalidTime("Invalid time unit: '%s'" % s)

    @classmethod
    def get_seconds(cls, time_unit):
        return cls.TIME_UNIT[cls.get_time_unit_name(time_unit)]

    @classmethod
    def parse_time_str(cls, s):
        """
        Parse time string to seconds.

        >>> RetentionParser.parse_time_str('12h')
        43200
        """
        if s.isdigit():
            return int(s)

        m = cls.pat.match(s)
        if m:
            num, unit = m.groups()
            return int(num) * cls.get_seconds(unit)
        else:
            raise InvalidTime("Invalid rention specification '%s'" % s)

    @classmethod
    def parse_retention_def(cls, retention_def):
        precision, point_cnt = retention_def.strip().split(':')
        precision = cls.parse_time_str(precision)

        if point_cnt.isdigit():
            point_cnt = int(point_cnt)
        else:
            point_cnt = cls.parse_time_str(point_cnt) / precision

        return precision, point_cnt


### Storage

class Storage(object):

    def __init__(self, data_dir=''):
        self.data_dir = data_dir

    def create(self, metric_name, tag_list, archive_list, x_files_factor=None,
               agg_name=None):
        Storage.validate_archive_list(archive_list, x_files_factor)

        path = self.gen_path(self.data_dir, metric_name)
        if os.path.exists(path):
            raise IOError('file %s already exits.' % path)
        else:
            mkdir_p(os.path.dirname(path))

        # inter_tag_list[RESERVED_INDEX] is reserved space
        # to avoid move data points.
        empty_tag_cnt = sum(1 for t in tag_list if not t)
        inter_tag_list = tag_list + ['N' * DEFAULT_TAG_LENGTH * empty_tag_cnt]

        with open(path, 'wb') as f:
            packed_header, end_offset = self.pack_header(
                inter_tag_list, archive_list, x_files_factor, agg_name)
            f.write(packed_header)

            # init data
            remaining = end_offset - f.tell()
            zeroes = '\x00' * CHUNK_SIZE
            while remaining > CHUNK_SIZE:
                f.write(zeroes)
                remaining -= CHUNK_SIZE
            f.write(zeroes[:remaining])

    @staticmethod
    def validate_archive_list(archive_list, xff):
        """
        Validates an archive_list.

        An archive_list must:
        1. Have at least one archive config.
        2. No duplicates.
        3. Higher precision archives' precision must evenly divide
           all lower precison archives' precision.
        4. Lower precision archives must cover larger time intervals
           than higher precision archives.
        5. Each archive must have at least enough points to the next
           archive.
        """

        # 1
        if not archive_list:
            raise InvalidConfig("must specify at least one archive config")

        archive_list.sort(key=operator.itemgetter(0))

        for i, archive in enumerate(archive_list):
            try:
                next_archive = archive_list[i+1]
            except:
                break
            # 2
            if not archive[0] < next_archive[0]:
                raise InvalidConfig("two same precision config: '%s' and '%s'" %
                                    (archive, next_archive))
            # 3
            if next_archive[0] % archive[0] != 0:
                raise InvalidConfig("higher precision must evenly divide lower "
                                    "precision: %s and %s" %
                                    (archive[0], next_archive[0]))
            # 4
            retention = archive[0] * archive[1]
            next_retention = next_archive[0] * next_archive[1]
            if not next_retention > retention:
                raise InvalidConfig("lower precision archive must cover "
                                    "larger time intervals that higher "
                                    "precision archive: (%d, %s) and (%d, %s)" %
                                    (i, retention, i+1, next_retention))
            # 5
            archive_point_cnt = archive[1]
            point_per_consolidation = next_archive[0] / archive[0]
            if not (archive_point_cnt / xff) >= point_per_consolidation:
                raise InvalidConfig("each archive must have at least enough "
                                    "points to consolidate to the next archive: "
                                    "(%d, %s) and (%d, %s) xff=%s" %
                                    (i, retention, i+1, next_retention, xff))

    @staticmethod
    def gen_path(data_dir, metric_name):
        """
        Generate file path of `metric_name`.

        eg, metric_name is `sys.cpu.user`, the absolute file path will be
        `self.data_dir/sys/cpu/user.hs`
        """
        if metric_name[0] == '/':
            return metric_name
        parts = metric_name.split('.')
        parts[-1] = parts[-1] + '.hs'
        file_path = os.path.sep.join(parts)
        return os.path.join(data_dir, file_path)

    @staticmethod
    def pack_header(inter_tag_list, archive_list, x_files_factor, agg_name):
        # tag
        tag = str('\t'.join(inter_tag_list))

        # metadata
        agg_id = Agg.get_agg_id(agg_name)
        max_retention = reduce(operator.mul, archive_list[-1], 1)
        xff = x_files_factor
        archive_cnt = len(archive_list)
        tag_size = len(tag)
        point_size = struct.calcsize(POINT_FORMAT % (len(inter_tag_list) - 1))
        metadata = struct.pack(METADATA_FORMAT, agg_id, max_retention,
            xff, archive_cnt, tag_size, point_size)

        # archive_info
        header = [metadata, tag]
        offset = METADATA_SIZE + len(tag) + ARCHIVEINFO_SIZE * len(archive_list)

        for sec, cnt in archive_list:
            archive_info = struct.pack(ARCHIVEINFO_FORMAT, offset, sec, cnt)
            header.append(archive_info)
            offset += point_size * cnt
        return ''.join(header), offset

    @staticmethod
    def header(fh):
        origin_offset = fh.tell()
        if origin_offset != 0:
            fh.seek(0)
        packed_metadata = fh.read(METADATA_SIZE)
        agg_id, max_retention, xff, archive_cnt, tag_size, point_size = struct.unpack(
            METADATA_FORMAT, packed_metadata)
        inter_tag_list = fh.read(tag_size).split('\t')

        archives = []
        for i in xrange(archive_cnt):
            packed_archive_info = fh.read(ARCHIVEINFO_SIZE)
            offset, sec, cnt = struct.unpack(
                ARCHIVEINFO_FORMAT, packed_archive_info)
            archive_info = {
                'offset': offset,
                'sec_per_point': sec,
                'count': cnt,
                'size': point_size * cnt,
                'retention': sec * cnt,
            }
            archives.append(archive_info)

        fh.seek(origin_offset)
        tag_list = inter_tag_list[:RESERVED_INDEX]
        info = {
            'agg_id': agg_id,
            'max_retention': max_retention,
            'x_files_factor': xff,
            'tag_list': tag_list,
            'reserved_size': len(inter_tag_list[RESERVED_INDEX]),
            'point_size': point_size,
            'point_format': POINT_FORMAT % len(tag_list),
            'archive_list': archives,
        }
        return info

    @staticmethod
    def add_tag(tag, path, pos_idx):
        with open(path, 'r+b') as fh:
            header_info = Storage.header(fh)
            tag_list = header_info['tag_list']
            reserved_size = header_info['reserved_size']

            archive_list = [(a['sec_per_point'], a['count'])
                            for a in header_info['archive_list']]
            agg_name = Agg.get_agg_name(header_info['agg_id'])

            if len(tag) <= len(tag_list[pos_idx]) + reserved_size:
                diff = len(tag_list[pos_idx]) + reserved_size - len(tag)
                tag_list[pos_idx] = tag
                inter_tag_list = tag_list + ['N' * diff]
                packed_header, _ = Storage.pack_header(
                    inter_tag_list, archive_list, header_info['x_files_factor'], agg_name)
                fh.write(packed_header)
            else:
                tag_list[pos_idx] = tag
                inter_tag_list = tag_list + ['']
                packed_header, _ = Storage.pack_header(
                    inter_tag_list, archive_list, header_info['x_files_factor'], agg_name)
                tmpfile = path + '.tmp'
                with open(tmpfile, 'wb') as fh_tmp:
                    fh_tmp.write(packed_header)
                    fh.seek(header_info['archive_list'][0]['offset'])
                    while True:
                        bytes = fh.read(CHUNK_SIZE)
                        if not bytes:
                            break
                        fh_tmp.write(bytes)
                os.rename(tmpfile, path)

    def update(self, path, points, now=None, mtime=None):
        # order points by timestamp, newest first
        points.sort(key=operator.itemgetter(0), reverse=True)
        mtime = mtime or int(os.stat(path).st_mtime)
        with open(path, 'r+b') as f:
            header = self.header(f)
            if now is None:
                now = int(time.time())
            archive_list = header['archive_list']
            i = 0
            curr_archive = archive_list[i]
            curr_points = []

            for point in points:
                age = now - point[0]

                while age > curr_archive['retention']:
                    # we can't fit any more points in archive i
                    if curr_points:
                        timestamp_range = (min(mtime, curr_points[-1][0]),
                                           curr_points[0][0])
                        self._update_archive(f, header, curr_archive,
                                             curr_points, i, timestamp_range)
                        curr_points = []
                    try:
                        curr_archive = archive_list[i+1]
                        i += 1
                    except IndexError:
                        curr_archive = None
                        break

                if not curr_archive:
                    # drop remaining points that don't fit in the database
                    break

                curr_points.append(point)

            if curr_archive and curr_points:
                timestamp_range = (min(mtime, curr_points[-1][0]),
                                   curr_points[0][0])
                self._update_archive(f, header, curr_archive, curr_points, i,
                                     timestamp_range)

    def _update_archive(self, fh, header, archive, points, archive_idx, timestamp_range):
        step = archive['sec_per_point']
        aligned_points = sorted((p[0] - (p[0] % step), p[1])
                                for p in points if p)
        if not aligned_points:
            return

        # create a packed string for each contiguous sequence of points
        point_format = header['point_format']
        packed_strings = []
        curr_strings = []
        previous_ts = None
        len_aligned_points = len(aligned_points)
        for i in xrange(0, len_aligned_points):
            # take last val of duplicates
            if (i+1 < len_aligned_points and
                aligned_points[i][0] == aligned_points[i+1][0]):
                continue
            (ts, val) = aligned_points[i]
            packed_str = struct.pack(point_format, ts, *val)
            if (not previous_ts) or (ts == previous_ts + step):
                curr_strings.append(packed_str)
            else:
                start_ts = previous_ts - (step * (len(curr_strings) - 1))
                packed_strings.append((start_ts, ''.join(curr_strings)))
                curr_strings = [packed_str]
            previous_ts = ts

        if curr_strings:
            start_ts = previous_ts - (step * (len(curr_strings) - 1))
            packed_strings.append((start_ts, ''.join(curr_strings)))

        # read base point and determine where our writes will start
        base_point = self._read_base_point(fh, archive, header)
        base_ts = base_point[0]

        first_ts = aligned_points[0][0]
        if base_ts == 0:
            # this file's first update, so set it to first timestamp
            base_ts = first_ts

        # write all of our packed strings in locations
        # determined by base_ts
        archive_end = archive['offset'] + archive['size']
        for (ts, packed_str) in packed_strings:
            offset = self._timestamp2offset(ts, base_ts, header, archive)
            bytes_beyond = (offset + len(packed_str)) - archive_end
            fh.seek(offset)
            if bytes_beyond > 0:
                fh.write(packed_str[:-bytes_beyond])
                fh.seek(archive['offset'])
                fh.write(packed_str[-bytes_beyond:])
            else:
                fh.write(packed_str)

        # now we propagate the updates to lower-precision archives
        archive_list = header['archive_list']
        next_archive_idx = archive_idx + 1
        if next_archive_idx < len(archive_list):
            # update timestamp_range
            time_start, time_end = timestamp_range
            time_end = max(time_end, aligned_points[-1][0])
            time_start = min(time_start, aligned_points[0][0])
            timestamp_range = (time_start, time_end)
            self._propagate(fh, header, archive, archive_list[next_archive_idx],
                            timestamp_range, next_archive_idx)

    def _read_base_point(self, fh, archive, header):
        fh.seek(archive['offset'])
        base_point = fh.read(header['point_size'])
        return struct.unpack(header['point_format'], base_point)

    def _timestamp2offset(self, ts, base_ts, header, archive):
        time_distance = ts - base_ts
        point_distince = time_distance / archive['sec_per_point']
        byte_distince =  point_distince * header['point_size']
        return archive['offset'] + (byte_distince % archive['size'])

    @staticmethod
    def get_propagate_timeunit(low_sec_per_point, high_sec_per_point, xff):
        num_point = low_sec_per_point / high_sec_per_point
        return int(math.ceil(num_point * xff)) * high_sec_per_point

    def _propagate(self, fh, header, higher, lower, timestamp_range, lower_idx):
        """
        propagte update to low precision archives.
        """
        from_time, until_time = timestamp_range
        timeunit = Storage.get_propagate_timeunit(lower['sec_per_point'],
                                                  higher['sec_per_point'],
                                                  header['x_files_factor'])
        from_time_boundary = from_time / timeunit
        until_time_boundary = until_time / timeunit
        if (from_time_boundary == until_time_boundary) and (from_time % timeunit) != 0:
            return False

        if lower['sec_per_point'] <= timeunit:
            lower_interval_end = until_time_boundary * timeunit
            lower_interval_start = min(lower_interval_end-timeunit, from_time_boundary*timeunit)
        else:
            lower_interval_end = roundup(until_time, lower['sec_per_point'])
            lower_interval_start = from_time - from_time % lower['sec_per_point']

        fh.seek(higher['offset'])
        packed_base_interval = fh.read(LONG_SIZE)
        higher_base_interval = struct.unpack(LONG_FORMAT, packed_base_interval)[0]

        if higher_base_interval == 0:
            higher_first_offset = higher['offset']
        else:
            higher_first_offset = self._timestamp2offset(lower_interval_start,
                                                         higher_base_interval,
                                                         header,
                                                         higher)

        higher_point_num = (lower_interval_end - lower_interval_start) / higher['sec_per_point']
        higher_size = higher_point_num * header['point_size']
        relative_first_offset = higher_first_offset - higher['offset']
        relative_last_offset = (relative_first_offset + higher_size) % higher['size']
        higher_last_offset = relative_last_offset + higher['offset']

        # get unpacked series str
        # TODO: abstract this to a function
        fh.seek(higher_first_offset)
        if higher_first_offset < higher_last_offset:
            series_str = fh.read(higher_last_offset - higher_first_offset)
        else:
            higher_end = higher['offset'] + higher['size']
            series_str = fh.read(higher_end - higher_first_offset)
            fh.seek(higher['offset'])
            series_str += fh.read(higher_last_offset - higher['offset'])

        # now we unpack the series data we just read
        point_format = header['point_format']
        byte_order, point_type = point_format[0], point_format[1:]
        point_num = len(series_str) / header['point_size']
        # assert point_num == higher_point_num
        series_format = byte_order + (point_type * point_num)
        unpacked_series = struct.unpack(series_format, series_str)

        # and finally we construct a list of values
        point_cnt = (lower_interval_end - lower_interval_start) / lower['sec_per_point']
        tag_cnt = len(header['tag_list'])
        agg_cnt = lower['sec_per_point'] / higher['sec_per_point']
        step = (tag_cnt + 1) * agg_cnt
        lower_points = [None] * point_cnt

        unpacked_series = unpacked_series[::-1]
        ts = lower_interval_end
        for i in xrange(0, len(unpacked_series), step):
            higher_points = unpacked_series[i: i+step]
            ts -= higher['sec_per_point'] * agg_cnt
            agg_value = self._get_agg_value(higher_points, tag_cnt, header['agg_id'],
                                            lower_interval_start, lower_interval_end)
            lower_points[i/step] = (ts, agg_value)

        lower_points = [x for x in lower_points if x and x[0]]  # filter zero item
        timestamp_range = (lower_interval_start, max(lower_interval_end, until_time))
        self._update_archive(fh, header, lower, lower_points, lower_idx,
                             timestamp_range)

    def _get_agg_value(self, higher_points, tag_cnt, agg_id, ts_start, ts_end):
        higher_points = higher_points[::-1]
        agg_func = Agg.get_agg_func(agg_id)
        step = tag_cnt + 1

        # points format:
        # t1 v11 v12,
        # t2 v21 v22,
        # t3 v31 v32,
        points = [higher_points[i: i+step]
                  for i in xrange(0, len(higher_points), step)]
        valid_points = self.filter_points_by_time(points, ts_start, ts_end)
        if not valid_points:
            val = [NULL_VALUE] * tag_cnt
        else:
            points = np.array(valid_points)
            points = points.transpose()
            val = [agg_func(self.filter_values(x)) for x in points[1:]]
        return val

    @staticmethod
    def filter_points_by_time(points, ts_start, ts_end):
        return [p for p in points if ts_start <= p[0] < ts_end]

    @staticmethod
    def filter_values(points):
        rs = [p for p in points if p != NULL_VALUE]
        return rs if rs else [NULL_VALUE]

    def fetch(self, path, from_time, until_time=None, now=None):
        with open(path, 'rb') as f:
            header = self.header(f)

            # validate timestamp
            if now is None:
                now = int(time.time())
            if until_time is None:
                until_time = now
            if from_time >= until_time:
                raise InvalidTime("from_time '%s' is after unitl_time '%s'" %
                                  (from_time, until_time))

            oldest_time = now - header['max_retention']
            if from_time > now:
                return None
            if until_time < oldest_time:
                return None

            until_time = min(now, until_time)
            from_time = max(oldest_time, from_time)

            diff = now - from_time
            for archive in header['archive_list']:
                if archive['retention'] >= diff:
                    break

            return self._archive_fetch(f, header, archive, from_time, until_time)

    def _archive_fetch(self, fh, header, archive, from_time, until_time):
        from_time = roundup(from_time, archive['sec_per_point'])
        until_time = roundup(until_time, archive['sec_per_point'])
        tag_cnt = len(header['tag_list'])
        null_point = (None,) * tag_cnt

        base_point = self._read_base_point(fh, archive, header)
        base_ts = base_point[0]

        if base_ts == 0:
            step = archive['sec_per_point']
            cnt = (until_time - from_time) / step
            time_info = (from_time, until_time, step)
            val_list = [null_point] * cnt
            return (header, time_info, val_list)

        from_offset = self._timestamp2offset(from_time, base_ts, header, archive)
        until_offset = self._timestamp2offset(until_time, base_ts, header, archive)

        fh.seek(from_offset)
        if from_offset < until_offset:
            series_str = fh.read(until_offset - from_offset)
        else:
            archive_end = archive['offset'] + archive['size']
            series_str = fh.read(archive_end - from_offset)
            fh.seek(archive['offset'])
            series_str += fh.read(until_offset - archive['offset'])

        ## unpack series string
        point_format = header['point_format']
        byte_order, point_type = point_format[0], point_format[1:]
        cnt = len(series_str) / header['point_size']
        series_format = byte_order + point_type * cnt
        unpacked_series = struct.unpack(series_format, series_str)

        ## construct value list
        # pre-allocate entire list or speed
        val_list = [null_point] * cnt
        step = tag_cnt + 1
        sec_per_point = archive['sec_per_point']
        for i in xrange(0, len(unpacked_series), step):
            point_ts = unpacked_series[i]
            if from_time <= point_ts < until_time:
                val = unpacked_series[i+1: i+step]
                idx = (point_ts - from_time) / sec_per_point
                val_list[idx] = self._conver_null_value(val)

        time_info = (from_time, until_time, sec_per_point)
        return header, time_info, val_list

    @staticmethod
    def _conver_null_value(point_val):
        val = [None if x == NULL_VALUE else x
               for x in point_val]
        return tuple(val)
