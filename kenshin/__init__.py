# coding: utf-8

from kenshin.storage import (
    Storage, KenshinException, InvalidConfig, InvalidTime,
    RetentionParser)

__version__ = "0.3.0"
__commit__ = "8b465fd"
__author__ = "zzl0"
__email__ = "zhuzhaolong0@gmail.com"
__date__ = "Sun Dec 18 16:09:07 2016 +0800"


_storage = Storage()
validate_archive_list = _storage.validate_archive_list
create = _storage.create
update = _storage.update
fetch = _storage.fetch
header = _storage.header
pack_header = _storage.pack_header
add_tag = _storage.add_tag

parse_retention_def = RetentionParser.parse_retention_def
