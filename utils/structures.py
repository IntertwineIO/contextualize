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
        """Cast value to enum, where value may be name or value"""
        if value in cls:
            return value
        if isinstance(value, int):
            return cls(value)
        try:
            return cls[value.upper()]
        except (AttributeError, KeyError):
            return cls(value)

    @classmethod
    def names(cls, iterable=None, transform=None):
        """Generator of enum names, transformed if function provided"""
        enums = cls if iterable is None else (cls.cast(i) for i in iterable)
        if transform:
            return (transform(en.name) for en in enums)
        return (en.name for en in enums)

    @classmethod
    def values(cls, iterable=None, transform=None):
        """Generator of enum values, transformed if function provided"""
        enums = cls if iterable is None else (cls.cast(i) for i in iterable)
        if transform:
            return (transform(en.value) for en in enums)
        return (en.value for en in enums)

    @classmethod
    def items(cls, iterable=None, transform=None, labels=False, inverse=False):
        """
        Generator of enum name/value 2-tuples

        I/O:
        iterable=None:  iterable of enum castables; if None, use all
        transform=None: function to be applied to each primary enum name
        labels=False:   if True, replace values with 2nd enum names
        inverse=False:  if True, swap names and values (or 2nd names)
        return:         generator of enum name/value 2-tuples
        """
        enums = cls if iterable is None else (cls.cast(i) for i in iterable)
        secondary = 'name' if labels else 'value'

        if transform:
            if inverse:
                return ((getattr(en, secondary), transform(en.name)) for en in enums)
            return ((transform(en.name), getattr(en, secondary)) for en in enums)

        if inverse:
            return ((getattr(en, secondary), en.name) for en in enums)
        return ((en.name, getattr(en, secondary)) for en in enums)

    @classmethod
    @lru_cache(maxsize=None)
    def list(cls, iterable=None, transform=None):
        """List of enum names, transformed if function provided"""
        return list(cls.names(iterable, transform))

    @classmethod
    @lru_cache(maxsize=None)
    def set(cls, iterable=None, transform=None):
        """Set of enum names, transformed if function provided"""
        return set(cls.names(iterable, transform))

    @classmethod
    @lru_cache(maxsize=None)
    def map(cls, iterable=None, transform=None, labels=False, inverse=False):
        """
        Ordered enum name/value map

        I/O:
        iterable=None:  iterable of enum castables; if None, use all
        transform=None: function to be applied to each primary enum name
        labels=False:   if True, replace values with 2nd enum names
        inverse=False:  if True, swap names and values (or 2nd names)
        return:         OrderedDict of enum name/value pairs
        """
        return OrderedDict(cls.items(iterable, transform, labels, inverse))

    @classmethod
    @lru_cache(maxsize=None)
    def labels(cls, iterable=None, transform=None, inverse=False):
        """
        Ordered enum primary/secondary name 2-tuples

        I/O:
        iterable=None:  iterable of enum castables; if None, use all
        transform=None: function to be applied to each primary enum name
        inverse=False:  if True, swap names and values (or 2nd names)
        return:         OrderedDict of enum name/value pairs
        """
        return OrderedDict(cls.items(iterable, transform, labels=True, inverse=inverse))

    def _derive_qualname(self):
        """
        Derive Qualname

        Inspect stack to derive and set __qualname__ on the class.
        """
        classes = []
        stack = frame = None
        try:
            is_eligible = False
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
