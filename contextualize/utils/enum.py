#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
from collections import OrderedDict
from enum import Enum
from functools import lru_cache, reduce

from contextualize.utils.tools import derive_qualname, is_class_name, load_class


class FlexEnum(Enum):
    """
    FlexEnum, the flexible Enum

    Enum with helpful cast, accessor, and transformation methods.
    """

    @classmethod
    def cast(cls, identifier, nullable=False):
        """Cast value or case-insensitive name to enum member, else raise ValueError"""
        try:
            return cls(identifier)
        except ValueError:
            try:
                return cls[identifier.upper()]
            except (AttributeError, KeyError) as e:
                if identifier is None and nullable:
                    return None
                raise ValueError(f'{identifier} is not a valid {cls.__qualname__}') from e

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
    def members(cls, *enumables, nullable=False):
        """Generator of specified enum members, defaulting to all"""
        return (cls.cast(x, nullable=nullable) for x in enumables) if enumables else cls

    @classmethod
    def names(cls, *enumables, nullable=False, transform=None):
        """Generator of enum names, transformed if function provided"""
        enums = cls.members(*enumables, nullable=nullable)
        if transform:
            return (transform(en.name) if en else None for en in enums)
        return (en.name if en else None for en in enums)

    @classmethod
    def values(cls, *enumables, nullable=False, transform=None):
        """Generator of enum values, transformed if function provided"""
        enums = cls.members(*enumables, nullable=nullable)
        if transform:
            return (transform(en.value) if en else None for en in enums)
        return (en.value if en else None for en in enums)

    @classmethod
    def contain(cls, container, *enumables,
                names=False, values=False, nullable=False, transform=None, **kwds):
        """Contain given enumables with given container, optionally transformed"""
        if names:
            if values:
                return container(cls.items(*enumables,
                                           nullable=nullable, transform=transform, **kwds))
            return container(cls.names(*enumables, nullable=nullable, transform=transform))
        if values:
            return container(cls.values(*enumables, nullable=nullable, transform=transform))
        return container(cls.members(*enumables, nullable=nullable))

    @classmethod
    def as_list(cls, *enumables, names=False, values=False, nullable=False, transform=None, **kwds):
        """List of given enumables, optionally transformed"""
        return cls.contain(
            list, *enumables,
            names=names, values=values, nullable=nullable, transform=transform, **kwds)

    @classmethod
    @lru_cache(maxsize=None)
    def as_tuple(cls, *enumables, names=False, values=False, nullable=False, transform=None, **kwds):
        """Tuple of given enumables, optionally transformed; cached"""
        return cls.contain(
            tuple, *enumables,
            names=names, values=values, nullable=nullable, transform=transform, **kwds)

    @classmethod
    def as_set(cls, *enumables, names=False, values=False, nullable=False, transform=None, **kwds):
        """Set of given enumables, optionally transformed"""
        return cls.contain(
            set, *enumables,
            names=names, values=values, nullable=nullable, transform=transform, **kwds)

    @classmethod
    @lru_cache(maxsize=None)
    def as_frozenset(cls, *enumables, names=False, values=False, nullable=False, transform=None,
                     **kwds):
        """Frozen set of given enumables, optionally transformed; cached"""
        return cls.contain(
            frozenset, *enumables,
            names=names, values=values, nullable=nullable, transform=transform, **kwds)

    @classmethod
    def items(cls, *enumables, swap=False, labels=False, nullable=False, transform=None,
              inverse=False):
        """
        Generator of enum pair 2-tuples, (name, value) by default

        I/O:
        *enumables:     values to cast to enum members; if None, use all
        swap=False:     if True, swap a & b: (a, b) = (value, name)
        labels=False:   if True, replace b in each (a, b) pair with name
        transform=None: function to apply to a in each (a, b) pair
        inverse=False:  if True, invert a & b at end: (b, a)
        return:         generator of enum pair 2-tuples
        """
        enums = cls.members(*enumables, nullable=nullable)

        if swap:
            pairs = ((en.value, en.name) if en else (None, None) for en in enums)
        elif labels:
            pairs = ((en.name, en.name) if en else (None, None) for en in enums)
        else:
            pairs = ((en.name, en.value) if en else (None, None) for en in enums)

        if transform:
            pairs = ((transform(a), b) for a, b in pairs)

        if inverse:
            pairs = ((b, a) for a, b in pairs)

        return pairs

    @classmethod
    def as_map(cls, *enumables,
               swap=False, labels=False, nullable=False, transform=None, inverse=False):
        """
        Ordered enum map of pair 2-tuples, (name, value) by default

        I/O:
        *enumables:     values to cast to enum members; if None, use all
        swap=False:     if True, swap names and values first
        labels=False:   if True, replace values with names
        transform=None: function to apply to a in each (a, b) pair
        inverse=False:  if True, invert a & b at end: (b, a)
        return:         OrderedDict of enum pair 2-tuples
        """
        return OrderedDict(cls.items(
            *enumables,
            swap=swap, labels=labels, nullable=nullable, transform=transform, inverse=inverse))

    @classmethod
    @lru_cache(maxsize=None)
    def as_labels(cls, *enumables, nullable=False, transform=None, inverse=True):
        """
        Tuple of labeled enum 2-tuples, (name, transformed(name)) by default

        I/O:
        *enumables:     values to cast to enum members; if None, use all
        transform=None: function to apply to a in each (a, b) pair
        inverse=True:   if True (default), invert a & b at end: (b, a)
        return:         tuple of labeled enum pair 2-tuples
        """
        return tuple(cls.items(
            *enumables,
            swap=False, labels=True, nullable=nullable, transform=transform, inverse=inverse))

    @classmethod
    def consolidate(cls, func, *enumables, names=False, values=False, nullable=False, swallow=None):
        """
        Consolidate enumables into single enum member

        I/O:
        func:           reduce-style function to be applied to enums
        *enumables:     items to be cast to enum cls members
        names=False:    if True, call func on enum names
        values=False:   if True, call func on enum values
        nullable=False: if True, allow None in enumables but filter them
                        out and return None if enumables is empty
        swallow=None:   exception or tuple of exceptions to return None
        return:         enum member corresponding to reduced value
        raise:          ValueError if both names and values are True;
                        cannot be swallowed
        raise:          ValueError if enumables contains invalid value
                        (e.g. None if not nullable) and not swallowed
        raise:          TypeError if enumables is empty and not nullable
                        and not swallowed
        """
        if not enumables:
            consolidatees = enumables
        elif names:
            if values:
                raise ValueError('names and values cannot both be True')
            consolidatees = cls.names(*enumables, nullable=nullable)
        elif values:
            consolidatees = cls.values(*enumables, nullable=nullable)
        else:
            consolidatees = cls.members(*enumables, nullable=nullable)

        if nullable:
            consolidatees = (c for c in consolidatees if c is not None)

        try:
            consolidated = reduce(func, consolidatees)
            return cls.cast(consolidated, nullable=nullable)

        except Exception as e:
            if nullable and not swallow:
                swallow = TypeError
            if swallow and isinstance(e, swallow):
                return None
            raise

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


class MonotonicEnum(FlexEnum):
    """
    MonotonicEnum

    FlexEnum with members conceptually increasing/decreasing as values
    increase. Abstract base class for IncreasingEnum/DecreasingEnum.
    """

    @classmethod
    def maximum(cls, *enumables, nullable=False, swallow=None):
        raise NotImplementedError

    @classmethod
    def minimum(cls, *enumables, nullable=False, swallow=None):
        raise NotImplementedError

    def __ge__(self, other):
        raise NotImplementedError

    def __gt__(self, other):
        raise NotImplementedError

    def __le__(self, other):
        raise NotImplementedError

    def __lt__(self, other):
        raise NotImplementedError

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.value == other.value
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return self.value != other.value
        return NotImplemented


class IncreasingEnum(MonotonicEnum):
    """
    IncreasingEnum

    FlexEnum with members conceptually increasing as values increase.
    """

    @classmethod
    def maximum(cls, *enumables, nullable=False, swallow=None):
        return cls.consolidate(max, *enumables, values=True, nullable=nullable, swallow=swallow)

    @classmethod
    def minimum(cls, *enumables, nullable=False, swallow=None):
        return cls.consolidate(min, *enumables, values=True, nullable=nullable, swallow=swallow)

    def __ge__(self, other):
        if isinstance(other, self.__class__):
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, self.__class__):
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, self.__class__):
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            return self.value < other.value
        return NotImplemented


class DecreasingEnum(MonotonicEnum):
    """
    DecreasingEnum

    FlexEnum with members conceptually decreasing as values increase.
    """

    @classmethod
    def minimum(cls, *enumables, nullable=False, swallow=None):
        return cls.consolidate(max, *enumables, values=True, nullable=nullable, swallow=swallow)

    @classmethod
    def maximum(cls, *enumables, nullable=False, swallow=None):
        return cls.consolidate(min, *enumables, values=True, nullable=nullable, swallow=swallow)

    def __ge__(self, other):
        if isinstance(other, self.__class__):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, self.__class__):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, self.__class__):
            return self.value >= other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            return self.value > other.value
        return NotImplemented
