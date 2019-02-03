#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from contextualize.utils.sentinel import Sentinel


@pytest.mark.unit
def test_sentinel():
    """Test Sentinel via comparisons"""
    sentinel0 = Sentinel()
    same_sentinels = [sentinel0] * 3
    for sentinel in same_sentinels:
        assert sentinel is sentinel0

    unique_sentinels = [Sentinel() for _ in range(3)]
    for sentinel in unique_sentinels:
        assert sentinel is not sentinel0
