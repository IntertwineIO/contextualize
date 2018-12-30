#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict, namedtuple
from datetime import datetime

import tzlocal

from utils.enum import FlexEnum


DateTimeInfo = namedtuple(
    'DateTimeInfo', 'year month day hour minute second microsecond tzinfo fold')

TZINFO_TAG = 'tzinfo'
assert TZINFO_TAG in DateTimeInfo._fields

TZINFO_IDX = DateTimeInfo._fields.index(TZINFO_TAG)

Granularity = FlexEnum('Granularity',
                       [f.upper() for f in DateTimeInfo._fields[:TZINFO_IDX]])

MAX_GRANULARITY = Granularity(len(Granularity))

class GranularDateTime(datetime):
    """
    GranularDateTime is a datetime subclass that supports Granularity.

    GranularDateTimes can be created from_datetime() instances and
    converted back to_datetime() instances. However, granularity is lost
    upon conversion to native datetime instances.

    A GranularDateTime instance can serialize() to an ISO-formatted
    substring with appropriate granularity. Such a substring can
    deserialize() back to a GranularDateTime retaining granularity.

    GranularDateTime also provides a granularity-aware strptime() for a
    subset of supported directives. This also supports multiple formats,
    where the first that is successful is returned.
    """
    TEMPLATES = [
        '%Y',
        '%Y-%m',
        '%Y-%m-%d',
        '%Y-%m-%dT%H',
        '%Y-%m-%dT%H:%M',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        ]

    TEMPLATE_LENGTH_MAP = OrderedDict((len(datetime.now().strftime(t)), t)
                                      for t in TEMPLATES)

    @classmethod
    def from_datetime(cls, dt, granularity=None):
        """
        From datetime

        Instantiate from datetime with the specified granularity.

        If granularity is specified, it is stored on the instance, but
        it does NOT truncate datetime field details so as to support
        arbitrary time zone changes beyond just hours.

        I/O:
        dt:                 Native datetime instance
        granularity=None:   Granularity enum option; if not provided,
                            defaults to max granularity.
        """
        kwds = {f: getattr(dt, f) for f in DateTimeInfo._fields}
        granular_dt = cls(**kwds)
        granular_dt.granularity = Granularity(granularity) if granularity else MAX_GRANULARITY
        return granular_dt

    def to_datetime(self):
        """Convert to native datetime, losing granularity in process"""
        kwds = {f: getattr(self, f) for f in DateTimeInfo._fields}
        return datetime(**kwds)

    @classmethod
    def strptime(cls, string, *formats):
        """
        Strptime attempts datetime.strptime with each of the given
        formats until successful and returns a GranularDateTime with the
        corresponding granularity.

        I/O:
        string:   datetime string to be converted to a GranularDateTime
        formats:  one or more format strings with these directives:
                  %Y/%y   Year with/without century as 0-padded number
                  %B/%b   Month as locale’s full/abbreviated name
                  %m      Month as a 0-padded decimal number
                  %d      Day of month as a 0-padded decimal number
                  %H      Hour (24-hour) as a 0-padded decimal number
                  %M      Minute as a 0-padded decimal number
                  %S      Second as a 0-padded decimal number
                  %f      Microsecond as a 0-padded decimal number
        return:   GranularDateTime instance with a granularity attribute
        """
        num_templates = len(formats)
        for i, template in enumerate(formats, start=1):
            try:
                dt = super().strptime(string, template)
            except ValueError:
                if i == num_templates:
                    raise ValueError(f"datetime data '{string}' does not "
                                     f"match any formats: {formats}")
            else:
                granular_dt = cls.from_datetime(dt)
                gval = template.count('%')
                granular_dt.granularity = Granularity(gval)
                return granular_dt

    def serialize(self):
        """Serialize to ISO-formatted substring of proper granularity"""
        try:
            granularity = getattr(self, 'granularity')
        except AttributeError:
            granularity = Granularity.MICROSECOND

        gval = granularity.value
        template = self.TEMPLATES[gval - 1]
        return self.strftime(template)

    @classmethod
    def deserialize(cls, dt_string):
        """Deserialize ISO-formatted substring to granular datetime"""
        try:
            template = cls.TEMPLATE_LENGTH_MAP[len(dt_string)]
            return cls.strptime(dt_string, template)
        except (KeyError, ValueError):
            # 0-pad years since strptime requires it
            dt_list = dt_string.split('-', 1)
            year = dt_list[0]
            if len(year) < 4:
                dt_list[0] = year.zfill(4)
            dt_string = '-'.join(dt_list)
            return cls.strptime(dt_string, *cls.TEMPLATES)

    @classmethod
    def fromtimestamp(cls, timestamp, tz=None, resolution=None):
        """
        From Timestamp

        Construct local datetime from timestamp at the given resolution.

        I/O:
        timestamp:          Unix Epoch time
        tz=None:            Time zone object of localized datetime
        resolution=None:    The number of seconds represented by each
                            timestamp unit. If not specified, 1 second
                            is attempted, failing over to 0.001 seconds.
        """
        # See https://stackoverflow.com/a/40769643/4182210
        timezone = tz or tzlocal.get_localzone()
        if resolution:
            return super().fromtimestamp(timestamp * resolution, tz=timezone)
        try:
            return super().fromtimestamp(timestamp, tz=timezone)
        except ValueError:
            timestamp /= 1000
            return super().fromtimestamp(timestamp, tz=timezone)

    @classmethod
    def utcfromtimestamp(cls, timestamp, resolution=None):
        """
        UTC From Timestamp

        Construct UTC datetime from timestamp at the given resolution.

        I/O:
        timestamp:          Unix Epoch time
        resolution=None:    The number of seconds represented by each
                            timestamp unit. If not specified, 1 second
                            is attempted, failing over to 0.001 seconds.
        """
        if resolution:
            return super().utcfromtimestamp(timestamp * resolution)
        try:
            return super().utcfromtimestamp(timestamp)
        except ValueError:
            timestamp /= 1000
            return super().utcfromtimestamp(timestamp)
