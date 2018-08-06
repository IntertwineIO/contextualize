#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
from collections import OrderedDict
from enum import Enum
from functools import lru_cache

from utils.tools import derive_qualname, is_class_name


class FlexEnum(Enum):
    """
    FlexEnum, the flexible Enum

    Enum with helpful cast, accessor, and transformation methods.
    """
    @classmethod
    def cast(cls, option):
        """Cast option name (case-insensitive) or value to enum"""
        if option in cls:
            return option
        if isinstance(option, int):
            return cls(option)
        try:
            return cls[option.upper()]
        except (AttributeError, KeyError):
            return cls(option)

    @classmethod
    def option(cls, name, value):
        """Construct option with given name/value, ensuring validity"""
        try:
            option = cls[name]
        except KeyError:
            raise KeyError(f"Invalid {cls.__qualname__} name '{name}'")
        if option.value != value:
            raise ValueError(f"Invalid {cls.__qualname__} value in option '{name}: {value}'")
        return option

    @classmethod
    def names(cls, *enumables, transform=None):
        """Generator of enum names, transformed if function provided"""
        enums = (cls.cast(x) for x in enumables) if enumables else cls
        if transform:
            return (transform(en.name) for en in enums)
        return (en.name for en in enums)

    @classmethod
    def values(cls, *enumables, transform=None):
        """Generator of enum values, transformed if function provided"""
        enums = (cls.cast(x) for x in enumables) if enumables else cls
        if transform:
            return (transform(en.value) for en in enums)
        return (en.value for en in enums)

    @classmethod
    @lru_cache(maxsize=None)
    def list(cls, *enumables, transform=None):
        """List of enum names, transformed if function provided"""
        return list(cls.names(*enumables, transform=transform))

    @classmethod
    @lru_cache(maxsize=None)
    def tuple(cls, *enumables, transform=None):
        """Tuple of enum names, transformed if function provided"""
        return tuple(cls.names(*enumables, transform=transform))

    @classmethod
    @lru_cache(maxsize=None)
    def set(cls, *enumables, transform=None):
        """Set of enum names, transformed if function provided"""
        return set(cls.names(*enumables, transform=transform))

    @classmethod
    def items(cls, *enumables, swap=False, labels=False, transform=None, inverse=False):
        """
        Generator of enum pair 2-tuples, (name, value) by default

        I/O:
        *enumables:     values to cast to enum cls; if None, use all
        swap=False:     if True, swap a & b: (a, b) = (value, name)
        labels=False:   if True, replace b in each (a, b) pair with name
        transform=None: function to apply to a in each (a, b) pair
        inverse=False:  if True, invert a & b at end: (b, a)
        return:         generator of enum pair 2-tuples
        """
        enums = (cls.cast(x) for x in enumables) if enumables else cls

        if swap:
            pairs = ((en.value, en.name) for en in enums)
        elif labels:
            pairs = ((en.name, en.name) for en in enums)
        else:
            pairs = ((en.name, en.value) for en in enums)

        if transform:
            pairs = ((transform(a), b) for a, b in pairs)

        if inverse:
            pairs = ((b, a) for a, b in pairs)

        return pairs

    @classmethod
    @lru_cache(maxsize=None)
    def map(cls, *enumables, swap=False, labels=False, transform=None, inverse=False):
        """
        Ordered enum map of pair 2-tuples, (name, value) by default

        I/O:
        *enumables:     values to cast to enum cls; if None, use all
        swap=False:     if True, swap names and values first
        labels=False:   if True, replace values with names
        transform=None: function to apply to a in each (a, b) pair
        inverse=False:  if True, invert a & b at end: (b, a)
        return:         OrderedDict of enum pair 2-tuples
        """
        return OrderedDict(
            cls.items(*enumables, swap=swap, labels=labels, transform=transform, inverse=inverse))

    @classmethod
    @lru_cache(maxsize=None)
    def labels(cls, *enumables, transform=None, inverse=True):
        """
        Tuple of labeled enum 2-tuples, (name, transformed(name)) by default

        I/O:
        *enumables:     values to cast to enum cls; if None, use all
        transform=None: function to apply to a in each (a, b) pair
        inverse=True:   if True (default), invert a & b at end: (b, a)
        return:         tuple of labeled enum pair 2-tuples
        """
        return tuple(
            cls.items(*enumables, swap=False, labels=True, transform=transform, inverse=inverse))

    def __init__(self, *args, **kwds):
        super().__init__()
        self.__class__.__qualname__ = derive_qualname(self)

    def __repr__(self):
        qualname = self.__class__.__qualname__
        name, value = self.name, self.value
        value = value if isinstance(value, int) else f"'{value}'"
        return f"{qualname}.option('{name}', {value})"

    def __str__(self):
        return f'{self.__class__.__qualname__}.{self.name}'
