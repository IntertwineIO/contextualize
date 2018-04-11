#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple
from datetime import datetime
from utils.structures import FlexEnum


TZINFO_TAG = 'tzinfo'

DatetimeInfo = namedtuple(
    'DatetimeInfo',
    f'year month day hour minute second microsecond {TZINFO_TAG} fold')

TZINFO_IDX = DatetimeInfo._fields.index(TZINFO_TAG)

Granularity = FlexEnum('Granularity',
                       [f.upper() for f in DatetimeInfo._fields[:TZINFO_IDX]])


class Datetime(datetime):
    """
    Datetime is a light-weight datetime subclass that allows custom
    attributes to be set on it.
    """
    @classmethod
    def from_datetime(cls, dt):
        kwds = {f: getattr(dt, f) for f in DatetimeInfo._fields}
        return cls(**kwds)

    def to_datetime(self):
        kwds = {f: getattr(self, f) for f in DatetimeInfo._fields}
        return datetime(**kwds)


def flex_strptime(string, formats):
    """
    Flex strptime attempts datetime.strptime with each of the given
    formats until successful and returns a Datetime with the
    corresponding granularity.

    I/O:
    string:     datetime string to be converted to a Datetime
    formats:    sequence of formats, where each format supports
                the following strptime directives:
                %Y/%y   Year with/without century as zero-padded number
                %B/%b   Month as locale’s full/abbreviated name
                %d      Day of the month as a zero-padded decimal number
                %H      Hour (24-hour) as a zero-padded decimal number
                %M      Minute as a zero-padded decimal number
                %S      Second as a zero-padded decimal number
                %f      Microsecond as a zero-padded decimal number
    return:     Datetime instance with a granularity attribute
    """
    num_templates = len(formats)
    for i, template in enumerate(formats, start=1):
        try:
            dt = datetime.strptime(string, template)
        except ValueError:
            if i == num_templates:
                raise ValueError(f"time data '{string}' does not "
                                 f"match any formats: {formats}")
        else:
            dt_wrapper = Datetime.from_datetime(dt)
            gval = template.count('%')
            dt_wrapper.granularity = Granularity(gval)
            return dt_wrapper
