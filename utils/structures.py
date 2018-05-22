#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict
from enum import Enum
from functools import lru_cache
from itertools import chain


class CacheKey:
    """
    CacheKey

    A class for composing cache keys from fields and/or qualifiers.

    Control characters are used as delimiters to minimize conflicts:

    Field names and values are delimited by Start of Text (2). For
    display purposes, Equal ('=') is used instead.

    Terms (fields/qualifiers) are delimited by Record Separator (30).
    For display purposes, Ampersand ('&') is used instead.

    This allows field values to include any other character, including
    Null (0) and Unit Separator (31).

    I/O:
    fields=None      Ordered dictionary of name/value string pairs
    qualifiers=None  Sequence of strings
    """
    NAME_VALUE_DELIMITER = chr(2)  # Start of Text (STX)
    NAME_VALUE_DELIMITER_DISPLAY = '='

    TERM_DELIMITER = chr(30)  # Record Separator (RS)
    TERM_DELIMITER_DISPLAY = '&'

    NULL = chr(0)
    NULL_DISPLAY = '~'

    @classmethod
    def from_key(cls, key, is_display=None):
        """Construct CacheKey instance from key or display string"""
        if not key:
            raise ValueError('Attempting to instantiate empty CacheKey')

        if is_display is None:
            is_display = (cls.TERM_DELIMITER not in key and
                          cls.NAME_VALUE_DELIMITER not in key)

        term_delimiter, name_value_delimiter, null = (
            (cls.TERM_DELIMITER_DISPLAY, cls.NAME_VALUE_DELIMITER_DISPLAY, cls.NULL_DISPLAY)
            if is_display else (cls.TERM_DELIMITER, cls.NAME_VALUE_DELIMITER, cls.NULL))

        terms = key.split(term_delimiter)
        i = len(terms)
        while i and name_value_delimiter not in terms[i - 1]:
            i -= 1

        def unpack_field(field):
            unpacked = field.split(name_value_delimiter)
            if unpacked[-1] == null:
                unpacked[-1] = None
            return unpacked

        try:
            fields = OrderedDict(unpack_field(field) for field in terms[:i])
        except ValueError:
            raise ValueError('CacheKey fields must precede all qualifiers')

        qualifiers = terms[i:]
        return cls(fields, qualifiers)

    def to_key(self, is_display=False):
        """Form key from CacheKey instance, optionally for display"""
        if not (self.fields or self.qualifiers):
            raise ValueError('Attempting to form empty cache key')

        term_delimiter, name_value_delimiter, null = (
            (self.TERM_DELIMITER_DISPLAY, self.NAME_VALUE_DELIMITER_DISPLAY, self.NULL_DISPLAY)
            if is_display else (self.TERM_DELIMITER, self.NAME_VALUE_DELIMITER, self.NULL))

        def pack_field(name, value):
            serialized_value = null if value is None else str(value)
            return f'{name}{name_value_delimiter}{serialized_value}'

        packed_fields = (pack_field(name, value) for name, value in self.fields.items())
        terms = chain(packed_fields, self.qualifiers)
        return term_delimiter.join(terms)

    @property
    def key(self):
        """Form key string from CacheKey instance"""
        return self.to_key(is_display=False)

    def __repr__(self):
        return self.to_key(is_display=True)

    def __init__(self, fields=None, qualifiers=None):
        self.fields = OrderedDict() if fields is None else fields
        self.qualifiers = [] if qualifiers is None else qualifiers

    def __eq__(self, other):
        return self.fields == other.fields and self.qualifiers == other.qualifiers

    def __ne__(self, other):
        return self.fields != other.fields or self.qualifiers != other.qualifiers


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
