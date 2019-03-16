#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from contextualize.utils.structures import DotNotatableDict, DotNotatableOrderedDict


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, dot_notatable_subclass',
    [(0, DotNotatableDict),
     (1, DotNotatableOrderedDict),
     ])
def test_dot_notatable_subclass(idx, dot_notatable_subclass):
    """Test dot_notatable_subclass"""
    dot_notatable = dot_notatable_subclass(a=1)

    assert 'a' in dot_notatable
    assert dot_notatable.a == 1
    assert dot_notatable['a'] == 1

    dot_notatable.b = 2
    assert 'b' in dot_notatable
    assert dot_notatable.b == 2
    assert dot_notatable['b'] == 2

    dot_notatable['c'] = 3
    assert 'c' in dot_notatable
    assert dot_notatable.c == 3
    assert dot_notatable['c'] == 3

    del dot_notatable.b
    assert 'b' not in dot_notatable

    with pytest.raises(AttributeError):
        dot_notatable.b

    with pytest.raises(AttributeError):
        dot_notatable.z
