#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
from collections import OrderedDict
from enum import Enum
from functools import lru_cache

from contextualize.utils.tools import derive_qualname, is_class_name, load_class


class FlexEnum(Enum):
    """
    FlexEnum, the flexible Enum

    Enum with helpful cast, accessor, and transformation methods.
    """
    @classmethod
    def cast(cls, identifier):
        """Cast member name (case-insensitive) or value to enum"""
        if identifier in cls:
            return identifier
        if isinstance(identifier, int):
            return cls(identifier)
        try:
            return cls[identifier.upper()]
        except (AttributeError, KeyError):
            return cls(identifier)

    @classmethod
    def member(cls, name, value):
        """Construct member with given name/value, ensuring validity"""
        try:
            member = cls[name]
        except KeyError:
            raise KeyError(f"Invalid {cls.__qualname__} name '{name}'")
        if member.value != value:
            raise ValueError(f"Invalid {cls.__qualname__} value in member '{name}: {value}'")
        return member

    @classmethod
    def members(cls, *enumables):
        """Generator of specified enum members, defaulting to all"""
        return (cls.cast(x) for x in enumables) if enumables else cls

    @classmethod
    def names(cls, *enumables, transform=None):
        """Generator of enum names, transformed if function provided"""
        enums = cls.members(*enumables)
        if transform:
            return (transform(en.name) for en in enums)
        return (en.name for en in enums)

    @classmethod
    def values(cls, *enumables, transform=None):
        """Generator of enum values, transformed if function provided"""
        enums = cls.members(*enumables)
        if transform:
            return (transform(en.value) for en in enums)
        return (en.value for en in enums)

    @classmethod
    def contain(cls, container, *enumables, names=False, values=False, transform=None, **kwds):
        """Contain given enumables with given container, optionally transformed"""
        if names:
            if values:
                return container(cls.items(*enumables, transform=transform, **kwds))
            return container(cls.names(*enumables, transform=transform))
        if values:
            return container(cls.values(*enumables, transform=transform))
        return container(cls.members(*enumables))

    @classmethod
    @lru_cache(maxsize=None)
    def as_list(cls, *enumables, names=False, values=False, transform=None, **kwds):
        """List of given enumables, optionally transformed"""
        return cls.contain(
            list, *enumables, names=names, values=values, transform=transform, **kwds)

    @classmethod
    @lru_cache(maxsize=None)
    def as_tuple(cls, *enumables, names=False, values=False, transform=None, **kwds):
        """Tuple of given enumables, optionally transformed"""
        return cls.contain(
            tuple, *enumables, names=names, values=values, transform=transform, **kwds)

    @classmethod
    @lru_cache(maxsize=None)
    def as_set(cls, *enumables, names=False, values=False, transform=None, **kwds):
        """Set of given enumables, optionally transformed"""
        return cls.contain(
            set, *enumables, names=names, values=values, transform=transform, **kwds)

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
        enums = cls.members(*enumables)

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
    def as_map(cls, *enumables, swap=False, labels=False, transform=None, inverse=False):
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
    def as_labels(cls, *enumables, transform=None, inverse=True):
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

    def serialize(self):
        """Serialize enum member to specifier"""
        cls = self.__class__
        module = cls.__module__
        qualname = cls.__qualname__
        name = self.name
        return f'{module}.{qualname}.{name}'

    @classmethod
    def deserialize(cls, specifier):
        """Deserialize specifier to enum member"""
        return load_class(specifier)

    def __init__(self, *args, **kwds):
        super().__init__()
        self.__class__.__qualname__ = derive_qualname(self)

    def __repr__(self):
        """Repr contains qualname/name/value and evals to self"""
        qualname = self.__class__.__qualname__
        name, value = self.name, self.value
        value = value if isinstance(value, int) else f"'{value}'"
        return f"{qualname}.member('{name}', {value})"

    def __str__(self):
        """Str contains qualname/name"""
        return f'{self.__class__.__qualname__}.{self.name}'
