#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import pytest
from datetime import datetime, date, time
from decimal import Decimal
from enum import Enum

from extractor import BaseExtractor
from utils.enum import FlexEnum
from utils.serialization import NULL, serialize, serialize_nonstandard


class Color:
    """Dummy class for testing serialization"""
    AdditivePrimary = FlexEnum('AdditivePrimary', 'RED GREEN BLUE')

    class SubtractivePrimary(Enum):
        RED = 1
        YELLOW = 2
        BLUE = 3

    mix_types = {AdditivePrimary, SubtractivePrimary}

    def to_json(self):
        """Invoked by serialize"""
        return f"Color(Color.{self.mix_type.__name__}, '{self.color.name}')"

    def __init__(self, mix_type, color):
        assert mix_type in self.mix_types
        self.mix_type = mix_type
        self.color = self.mix_type[color]


@pytest.mark.unit
@pytest.mark.parametrize(
    ('value', 'check'), [
    (datetime(2018, 7, 8, 5, 43, 21, 12345), '2018-07-08T05:43:21.012345'),
    (date(1918, 7, 14), '1918-07-14'),
    (time(12, 34, 56, 7890), '12:34:56.007890'),
    (Decimal('2.718281828459'), '2.718281828459'),
    (Color.AdditivePrimary.GREEN, 'test_serialization.Color.AdditivePrimary.GREEN'),
    (Color.SubtractivePrimary.YELLOW, 'test_serialization.Color.SubtractivePrimary.YELLOW'),
    (BaseExtractor.WebDriverType.CHROME, 'extractor.BaseExtractor.WebDriverType.CHROME'),
])
def test_serialize_nonstandard(value, check):
    """Test serialize "nonstandard" (i.e. not handled by json.dumps)"""
    serialized = serialize_nonstandard(value)
    assert serialized == check


@pytest.mark.unit
@pytest.mark.parametrize(
    ('value', 'check'), [
    (Color(Color.AdditivePrimary, 'GREEN'), "Color(Color.AdditivePrimary, 'GREEN')"),
    (Color(Color.SubtractivePrimary, 'YELLOW'), "Color(Color.SubtractivePrimary, 'YELLOW')"),
    (Color.AdditivePrimary.GREEN, 'test_serialization.Color.AdditivePrimary.GREEN'),
    (Color.SubtractivePrimary.YELLOW, 'test_serialization.Color.SubtractivePrimary.YELLOW'),
    (Color.AdditivePrimary, str(Color.AdditivePrimary)),
    (None, NULL),
])
def test_serialize(value, check):
    """Test serialize"""
    serialized = serialize(value)
    assert serialized == check
