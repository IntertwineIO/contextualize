#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict
from enum import Enum
from functools import lru_cache


class FlexEnum(Enum):
    """
    FlexEnum, the flexible Enum

    Enum with helpful cast, accessor, and transformation methods.
    """
    @classmethod
    def cast(cls, value):
        """Cast value to cls"""
        if value in cls:
            return value
        if isinstance(value, int):
            return cls(value)
        try:
            return cls[value.upper()]
        except (AttributeError, KeyError):
            return cls(value)

    @classmethod
    def names(cls, transform=None):
        """Generator of enum names, transformed if function provided"""
        if transform:
            return (transform(en.name) for en in cls)
        return (en.name for en in cls)

    @classmethod
    def values(cls, transform=None):
        """Generator of enum values, transformed if function provided"""
        if transform:
            return (transform(en.value) for en in cls)
        return (en.values for en in cls)

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
    def map(cls, transform=None, labels=False, inverse=False):
        """
        Ordered enum name/value map

        I/O
        transform=None: function to be applied to each primary enum name
        labels=False: if True, values replaced with secondary enum names
        inverse=False: if True, values (or secondary names) then names
        return: OrderedDict of enum name/value pairs
        """
        return OrderedDict(cls.items(transform, labels=labels, inverse=inverse))

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
        self.values = iterable if isinstance(iterable, (list, tuple)) else list(iterable)
        self.index = 0


class Singleton:
    """
    Singleton

    A base class to ensure only a single instance is created. Additional
    restrictions are enforced to encourage responsible usage:
    - Once instantiated, the constructor prohibits passing arguments to
      discourage subsequent calls from changing state
    - Modifying __new__ in subclasses is not permitted to discourage
      side-stepping the post-initialization argument-passing prohibition

    Adapted from Guido van Rossum's Singleton:
    https://www.python.org/download/releases/2.2/descrintro/#__new__
    """
    __instance = None

    def __new__(cls, *args, **kwds):

        if cls.__new__ is not Singleton.__new__:
            raise ValueError('Singletons may not modify __new__')

        if cls.__instance is not None:
            if args or kwds:
                raise ValueError('Existing singletons accept no arguments')
            return cls.__instance

        cls.__instance = instance = object.__new__(cls)
        instance.initialize(*args, **kwds)
        return cls.__instance

    def initialize(self, *args, **kwds):
        pass
