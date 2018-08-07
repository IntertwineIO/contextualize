#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from utils.structures import InfinIterator
from utils.tools import isiterator


@pytest.mark.unit
def test_infiniterator():
    """Test that InfinIterator is an iterator that can be reused"""
    length = 3

    infiniterator = InfinIterator(range(length))
    assert isiterator(infiniterator)

    list1 = list(infiniterator)
    assert len(list1) == length

    list2 = list(infiniterator)
    assert list2 == list1
