# coding: utf-8
#
# This module implements various aggregation method.
#

import operator


class Agg(object):
    agg_funcs = [
        ['average', lambda x: sum(x) / len(x)],
        ['sum', sum],
        ['last', operator.itemgetter(-1)],
        ['max', max],
        ['min', min],
    ]

    agg_type_list = [typ for typ, _ in agg_funcs]
    agg_func_dict = dict(agg_funcs)

    @classmethod
    def get_agg_id(cls, agg_name):
        return cls.agg_type_list.index(agg_name)

    @classmethod
    def get_agg_func(cls, agg_id):
        agg_type = cls.agg_type_list[agg_id]
        return cls.agg_func_dict[agg_type]

    @classmethod
    def get_agg_type_list(cls):
        return cls.agg_type_list

    @classmethod
    def get_agg_name(cls, agg_id):
        return cls.agg_type_list[agg_id]
