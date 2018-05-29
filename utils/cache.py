#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict
from itertools import chain

import aioredis

from utils.debug import async_debug
from utils.structures import Singleton

ENCODING_DEFAULT = 'utf-8'


class AsyncCache(Singleton):

    @async_debug()
    async def connect(self):
        redis = self.client
        if redis is None:
            redis = await aioredis.create_redis_pool('redis://localhost',
                                                     encoding=ENCODING_DEFAULT)
            self.client = redis
        return redis

    @async_debug()
    async def disconnect(self):
        redis = self.client
        if redis:
            redis.close()
            await redis.wait_closed()
            self.client = None

    def initialize(self):
        self.client = None


class CacheKey:
    """
    CacheKey

    A class for composing cache keys from one or more terms.

    There are two types of supported terms:
    Fields consist of name/value pairs, where names/values are strings.
    Qualifiers are strings.

    Cache keys may include any number of fields and qualifiers, provided
    there is at least one term. Any/all fields always precede any/all
    qualifiers within the key.

    Terms (fields/qualifiers) are delimited by Start of Header (1: SOH).
    For display purposes, Ampersand ('&') is used instead. As such,
    SOH may not be used within field names/values or qualifiers.
    Ampersand is permitted, but when used, `from_key` will not work on
    the display version of the key.

    Field names and values are delimited by Start of Text (2: STX). For
    display purposes, Equals ('=') is used instead. As such, STX may not
    be used within field names/values or qualifiers. Equals is allowed,
    but when used, `from_key` will not work on display keys.

    Field values of None are converted to the Null (0: NUL) character.
    This means field values that are strings consisting of just Null
    will be indistinguishable from None. Null characters may be used as
    part of longer strings without any such collision risk.

    All other characters may be used, including but not limited to any
    printable character and File/Group/Record/Unit Separators (28-31).

    I/O:
    fields=None      Ordered dictionary of name/value string pairs
    qualifiers=None  Sequence of strings
    return           CacheKey instance
    """
    TERM_DELIMITER = chr(1)  # Start of Header (1: SOH)
    TERM_DELIMITER_DISPLAY = '&'

    NAME_VALUE_DELIMITER = chr(2)  # Start of Text (2: STX)
    NAME_VALUE_DELIMITER_DISPLAY = '='

    NULL = chr(0)
    NULL_DISPLAY = '~'

    @classmethod
    def from_key(cls, key, is_display=None, encoding=ENCODING_DEFAULT):
        """Construct CacheKey instance from key or display string"""
        if not key:
            raise ValueError('Attempting to instantiate empty CacheKey')

        if encoding:
            key = str(key, encoding)

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
        return cls(fields, qualifiers, encoding)

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
        key = term_delimiter.join(terms)
        return key if is_display or not self.encoding else key.encode(self.encoding)

    @property
    def key(self):
        """Form key string from CacheKey instance"""
        return self.to_key(is_display=False)

    def __repr__(self):
        return self.to_key(is_display=True)

    def __init__(self, fields=None, qualifiers=None, encoding=ENCODING_DEFAULT):
        self.fields = OrderedDict() if fields is None else fields
        self.qualifiers = [] if qualifiers is None else qualifiers
        self.encoding = encoding

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.fields == other.fields and
                    self.qualifiers == other.qualifiers and
                    self.encoding == other.encoding)
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return (self.fields != other.fields or
                    self.qualifiers != other.qualifiers or
                    self.encoding != other.encoding)
        return NotImplemented
