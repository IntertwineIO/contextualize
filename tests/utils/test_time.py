#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from contextualize.utils.time import GranularDateTime, Granularity


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'dt_string',                   'gval', 'exception'),
    [
     # standard formats
     (0,    '2020',                         1,      None),
     (1,    '2020-01',                      2,      None),
     (2,    '2020-01-23',                   3,      None),
     (3,    '2020-01-23T12',                4,      None),
     (4,    '2020-01-23T12:34',             5,      None),
     (5,    '2020-01-23T12:34:56',          6,      None),
     (6,    '2020-01-23T12:34:56.123456',   7,      None),
     # values missing 0-padding
     (7,    '6',                            1,      None),
     (8,    '66',                           1,      None),
     (9,    '666',                          1,      None),
     (10,   '666-1',                        2,      None),
     (11,   '666-1-2',                      3,      None),
     (12,   '666-1-2T3',                    4,      None),
     (13,   '666-1-2T3:4',                  5,      None),
     (14,   '666-1-2T3:4:5',                6,      None),
     (15,   '666-1-2T3:4:5.1',              7,      None),
     (16,   '666-1-2T3:4:5.12',             7,      None),
     (17,   '666-1-2T3:4:5.123',            7,      None),
     (18,   '666-1-2T3:4:5.1234',           7,      None),
     (19,   '666-1-2T3:4:5.12345',          7,      None),
     (20,   '666-1-2T3:4:5.123456',         7,      None),
     (21,   '66-1-2T3:4:5.123456',          7,      None),
     (22,   '6-1-2T3:4:5.123456',           7,      None),
     # out of range
     (23,   '',                             1,      ValueError),
     (23,   '0000',                         1,      ValueError),
     (24,   '20200',                        1,      ValueError),
     (25,   '2020-00',                      2,      ValueError),
     (26,   '2020-13',                      2,      ValueError),
     (27,   '2020-01-00',                   3,      ValueError),
     (28,   '2020-02-30',                   3,      ValueError),
     (29,   '2020-01-23T24',                4,      ValueError),
     (30,   '2020-01-23T12:60',             5,      ValueError),
     (31,   '2020-01-23T12:34:60',          6,      ValueError),
     # unsupported formats
     (32,   '2020-',                        1,      ValueError),
     (33,   '2020-01-',                     2,      ValueError),
     (34,   '2020-01-23 ',                  3,      ValueError),
     (35,   '2020-01-23T',                  3,      ValueError),
     (36,   '2020-01-23T24:',               4,      ValueError),
     (37,   '2020-01-23T12:34:',            5,      ValueError),
     (38,   '2020-01-23T12:34:56.',         6,      ValueError),
     (39,   '2020-01-23T12:34:56.1234567',  7,      ValueError),
     ])
def test_datetime_wrapper_serialization(idx, dt_string, gval, exception):
    """Test GranularDateTime core methods"""
    if exception is None:

        granularity = Granularity(gval)

        granular_dt1 = GranularDateTime.deserialize(dt_string)
        assert granular_dt1.granularity is granularity

        serialized_dt1 = granular_dt1.serialize()

        granular_dt2 = GranularDateTime.deserialize(serialized_dt1)
        assert granular_dt2.granularity is granularity
        assert granular_dt2 == granular_dt1

        serialized_dt2 = granular_dt2.serialize()
        assert serialized_dt2 == serialized_dt1

    else:
        with pytest.raises(exception):
            GranularDateTime.deserialize(dt_string)


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'dt_string'),
    [
     # standard formats
     (0,    '2020'),
     (1,    '2020-01'),
     (2,    '2020-01-23'),
     (3,    '2020-01-23T12'),
     (4,    '2020-01-23T12:34'),
     (5,    '2020-01-23T12:34:56'),
     (6,    '2020-01-23T12:34:56.123456'),
     ])
def test_datetime_wrapper_conversion(idx, dt_string):
    """Test GranularDateTime conversion to/from datetime"""
    granular_dt1 = GranularDateTime.deserialize(dt_string)
    dt1 = granular_dt1.to_datetime()
    granularity = granular_dt1.granularity

    granular_dt2 = GranularDateTime.from_datetime(dt1, granularity)
    assert granular_dt2 == granular_dt1
    assert granular_dt2.granularity is granular_dt1.granularity

    dt2 = granular_dt2.to_datetime()
    assert dt2 == dt1

    granular_dt3 = GranularDateTime.from_datetime(dt1)
    assert granular_dt3 == granular_dt1
