#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple
from datetime import datetime

from utils.enum import FlexEnum


STANDARD_DATETIME_FORMATS = [
    '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'
]

TZINFO_TAG = 'tzinfo'

DateTimeInfo = namedtuple(
    'DateTimeInfo',
    f'year month day hour minute second microsecond {TZINFO_TAG} fold')

TZINFO_IDX = DateTimeInfo._fields.index(TZINFO_TAG)

Granularity = FlexEnum('Granularity',
                       [f.upper() for f in DateTimeInfo._fields[:TZINFO_IDX]])


class DateTimeWrapper(datetime):
    """
    DateTimeWrapper is a light-weight datetime subclass that allows
    custom attributes to be set on it.
    """
    @classmethod
    def from_datetime(cls, dt):
        kwds = {f: getattr(dt, f) for f in DateTimeInfo._fields}
        return cls(**kwds)

    def to_datetime(self):
        kwds = {f: getattr(self, f) for f in DateTimeInfo._fields}
        return datetime(**kwds)

    @classmethod
    def strptime(cls, string, *formats):
        """
        Strptime attempts datetime.strptime with each of the given
        formats until successful and returns a DateTimeWrapper with the
        corresponding granularity.

        I/O:
        string:   datetime string to be converted to a DateTimeWrapper
        formats:  one or more format strings with these directives:
                  %Y/%y   Year with/without century as 0-padded number
                  %B/%b   Month as locale’s full/abbreviated name
                  %m      Month as a 0-padded decimal number
                  %d      Day of month as a 0-padded decimal number
                  %H      Hour (24-hour) as a 0-padded decimal number
                  %M      Minute as a 0-padded decimal number
                  %S      Second as a 0-padded decimal number
                  %f      Microsecond as a 0-padded decimal number
        return:   DateTimeWrapper instance with a granularity attribute
        """
        num_templates = len(formats)
        for i, template in enumerate(formats, start=1):
            try:
                dt = datetime.strptime(string, template)
            except ValueError:
                if i == num_templates:
                    raise ValueError(f"datetime data '{string}' does not "
                                     f"match any formats: {formats}")
            else:
                dt_wrapper = cls.from_datetime(dt)
                gval = template.count('%')
                dt_wrapper.granularity = Granularity(gval)
                return dt_wrapper
