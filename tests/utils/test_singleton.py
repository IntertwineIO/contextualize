#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from copy import copy

from utils.singleton import Singleton


# Equivalent to the following for A, B, C, etc.:
# class SingletonA(Singleton): pass
ord_A, ord_last = ord('A'), ord('M')
local_dict = locals()
for i in range(ord_last - ord_A + 1):
    letter = chr(ord_A + i)
    name = f'Singleton{letter}'
    local_dict[name] = type(name, (Singleton,), {})


class SingletonX(Singleton):
    """Singleton does not permit extending __new__"""
    def __new__(cls, *args, **kwds):
        instance = super().__new__(cls, *args, **kwds)
        return copy(instance)  # Hey, this isn't a singleton anymore!


@pytest.mark.unit
@pytest.mark.parametrize(
    'singleton_class, args1, kwds1, exception1, args2, kwds2, exception2', [
    (SingletonA, [], {}, None, [], {}, None),
    (SingletonB, [1, 2, 3], {}, None, [], {}, None),
    (SingletonC, [], {'a': 1, 'b': 2}, None, [], {}, None),
    (SingletonD, [1, 2, 3], {'a': 1, 'b': 2}, None, [], {}, None),
    (SingletonE, [1, 2, 3], {}, None, [1, 2, 3], {}, None),
    (SingletonF, [], {'a': 1, 'b': 2}, None, [], {'a': 1, 'b': 2}, None),
    (SingletonG, [1, 2, 3], {'a': 1, 'b': 2}, None, [1, 2, 3], {'a': 1, 'b': 2}, None),
    (SingletonH, [], {}, None, [1], {}, ValueError),
    (SingletonI, [], {}, None, [], {'a': 1}, ValueError),
    (SingletonJ, [1, 2, 3], {}, None, [1], {}, ValueError),
    (SingletonK, [], {'a': 1, 'b': 2}, None, [], {'a': 1, 'b': 2, 'c': 3}, ValueError),
    (SingletonL, [1, 2, 3], {'a': 1, 'b': 2}, None, [1, 2], {'a': 1, 'b': 2}, ValueError),
    (SingletonM, [1, 2, 3], {'a': 1, 'b': 2}, None, [1, 2, 3], {'b': 2}, ValueError),
    (SingletonX, [1, 2, 3], {'a': 1, 'b': 2}, ValueError, [1, 2, 3], {'a': 1, 'b': 2}, None),
])
def test_singleton_instantiation(
    singleton_class, args1, kwds1, exception1, args2, kwds2, exception2):
    """Test Singleton instantiation"""
    if exception1:
        with pytest.raises(exception1):
            singleton1 = singleton_class(*args1, **kwds1)
    else:
        singleton1 = singleton_class(*args1, **kwds1)

        if exception2:
            with pytest.raises(exception2):
                singleton2 = singleton_class(*args2, **kwds2)
        else:
            singleton2 = singleton_class(*args2, **kwds2)
            assert singleton2 is singleton1
