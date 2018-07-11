#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
from collections import OrderedDict
from enum import Enum
from functools import lru_cache
from itertools import chain

from utils.tools import is_class_name


class FlexEnum(Enum):
    """
    FlexEnum, the flexible Enum

    Enum with helpful cast, accessor, and transformation methods.
    """
    @classmethod
    def cast(cls, value):
        """Cast name (case-insensitive) or value to enum"""
        if value in cls:
            return value
        if isinstance(value, int):
            return cls(value)
        try:
            return cls[value.upper()]
        except (AttributeError, KeyError):
            return cls(value)

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
        swap=False:     if True, swap names and values first
        labels=False:   if True, replace values with names
        transform=None: function to apply to each value (or 2nd name)
        inverse=False:  if True, invert (a, b) pair last
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
            pairs = ((a, transform(b)) for a, b in pairs)

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
        transform=None: function to apply to each value (or 2nd name)
        inverse=False:  if True, invert (a, b) pair last
        return:         OrderedDict of enum pair 2-tuples
        """
        return OrderedDict(
            cls.items(*enumables, swap=swap, labels=labels, transform=transform, inverse=inverse))

    @classmethod
    @lru_cache(maxsize=None)
    def labels(cls, *enumables, swap=False, transform=None, inverse=False):
        """
        Tuple of labeled enum 2-tuples, (name, transformed(name)) by default

        I/O:
        *enumables:     values to cast to enum cls; if None, use all
        swap=False:     if True, swap names and values first
        transform=None: function to apply to each name to produce label
        inverse=False:  if True, invert (a, b) pair last
        return:         tuple of labeled enum pair 2-tuples
        """
        return tuple(
            cls.items(*enumables, swap=swap, labels=True, transform=transform, inverse=inverse))

    def _derive_qualname(self):
        """Derive and set __qualname__ on the class"""
        classes = []
        stack = frame = None
        is_eligible = False
        try:
            stack = inspect.stack()
            for frame in stack:
                if frame.function == '__call__':
                    is_eligible = True
                elif frame.function == '<module>':
                    break
                elif is_eligible:
                    if not is_class_name(frame.function):
                        continue
                    classes.append(frame.function)
        finally:
            del frame
            del stack

        outer = '.'.join(reversed(classes))
        qualname = self.__class__.__qualname__

        if classes and not qualname.startswith(outer):
            qualname = '.'.join((outer, qualname))
            self.__class__.__qualname__ = qualname

    def __init__(self, *args, **kwds):
        super().__init__()
        self._derive_qualname()


class InfinIterator:
    """
    InfinIterator, the infinite iterator

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
        self.values = iterable if isinstance(iterable, (list, tuple)) else list(iterable)
        self.index = 0


class Singleton:
    """
    Singleton

    A base class to ensure only a single instance is created. Several
    measures are taken to encourage responsible usage:
    - The instance is only initialized once upon initial creation and
      arguments are permitted but not required
    - The constructor prohibits arguments on subsequent calls unless
      they match the initial ones as state must not change
    - Modifying __new__ in subclasses is not permitted to guard against
      side-stepping the aforementioned restrictions

    Adapted from Guido van Rossum's Singleton:
    https://www.python.org/download/releases/2.2/descrintro/#__new__
    """
    __instance = None
    __arguments = None

    def __new__(cls, *args, **kwds):

        if cls.__new__ is not Singleton.__new__:
            raise ValueError('Singletons may not modify __new__')

        if cls.__instance is not None:
            if args or kwds:
                if (args != cls.__arguments['args'] or kwds != cls.__arguments['kwds']):
                    raise ValueError('Singleton initialization may not change')
            return cls.__instance

        cls.__instance = instance = object.__new__(cls)
        cls.__arguments = {'args': args, 'kwds': kwds}
        instance.initialize(*args, **kwds)
        return instance

    def initialize(self, *args, **kwds):
        pass

    def __repr__(self):
        class_name = self.__class__.__name__
        args, kwds = self.__arguments['args'], self.__arguments['kwds']
        arg_strings = (str(arg) for arg in args)
        kwd_strings = (f'{k}={v}' for k, v in kwds.items()) if kwds else ()
        full_arg_string = ', '.join(chain(arg_strings, kwd_strings))
        return f'{class_name}({full_arg_string})'
