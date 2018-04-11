#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict
from enum import Enum
from functools import lru_cache


class FlexEnum(Enum):
    """
    FlexEnum, the flexible enum class


    """

    @classmethod
    def names(cls, transform=None):
        """Generator of enum names, transformed if function provided"""
        if transform:
            return (transform(en.name) for en in cls)
        return (en.name for en in cls)

    @classmethod
    def values(cls):
        """Generator of enum values"""
        return (en.value for en in cls)

    @classmethod
    def items(cls, transform=None, labels=False, inverse=False):
        """
        Generator of enum name/value 2-tuples

        I/O
        transform=None: function to be applied to each primary enum name
        labels=False: if True, values replaced with secondary enum names
        inverse=False: if True, values (or secondary names) then names
        return: generator of enum name/value 2-tuples
        """
        secondary = 'name' if labels else 'value'

        if transform:
            if inverse:
                return ((getattr(en, secondary), transform(en.name)) for en in cls)
            return ((transform(en.name), getattr(en, secondary)) for en in cls)

        if inverse:
            return ((getattr(en, secondary), en.name) for en in cls)
        return ((en.name, getattr(en, secondary)) for en in cls)

    @classmethod
    @lru_cache(maxsize=None)
    def list(cls, transform=None):
        """List of enum names, transformed if function provided"""
        return list(cls.names(transform))

    @classmethod
    @lru_cache(maxsize=None)
    def set(cls, transform=None):
        """Set of enum names, transformed if function provided"""
        return set(cls.names(transform))

    @classmethod
    @lru_cache(maxsize=None)
    def map(cls, transform=None, inverse=False):
        """
        Ordered enum name/value map

        I/O
        transform=None: function to be applied to each name
        inverse=False: if True, names and values are swapped
        return: OrderedDict of enum name/value pairs
        """
        return OrderedDict(cls.items(transform, labels=False, inverse=inverse))

    @classmethod
    @lru_cache(maxsize=None)
    def labels(cls, transform=None, inverse=False):
        """
        Ordered enum primary/secondary name 2-tuples

        I/O
        transform=None: function to be applied to each primary enum name
        inverse=False: if True, primary and secondary names are swapped
        return: OrderedDict of enum name/value pairs
        """
        return OrderedDict(cls.items(transform, labels=True, inverse=inverse))


class InfinIterator:
    """
    InfinIterator, the infinite iterator class

    Useful for testing functions that work on iterators, since unlike
    most other iterators, this one can be used any number of times.
    """
    def __iter__(self):
        return self

    def __next__(self):
        try:
            value = self.values[self.index]
        except IndexError:
            self.index = 0  # Reset to 0 so it can be used again
            raise StopIteration()
        self.index += 1
        return value

    def __init__(self, iterable):
        self.values = iterable if hasattr(iterable, '__len__') else list(iterable)
        self.index = 0
