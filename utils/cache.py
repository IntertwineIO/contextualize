#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
from collections import OrderedDict
from itertools import chain

import aioredis

from utils.debug import async_debug, sync_debug
from utils.structures import Singleton

ENCODING_DEFAULT = 'utf-8'


class AsyncCache(Singleton):
    """AsyncCache manages async connections to Redis key-value store"""
    @sync_debug()
    def initialize(self, loop=None):
        """Initialize AsyncCache singleton"""
        self.client = None
        loop = loop or asyncio.get_event_loop()
        loop.run_until_complete(self.connect(loop))

    @async_debug()
    async def connect(self, loop):
        """Connect to Redis via pool, set client, and return it"""
        redis = self.client
        if not redis:
            redis = await aioredis.create_redis_pool('redis://localhost',
                                                     encoding=ENCODING_DEFAULT,
                                                     loop=loop)
            self.client = redis
        return redis

    @async_debug()
    async def disconnect(self):
        """Disconnect from Redis, await clean up, and remove client"""
        redis = self.client
        if redis:
            redis.close()
            await redis.wait_closed()
            self.client = None

    @sync_debug()
    def shutdown(self, loop=None):
        """Shutdown AsyncCache from outside the event loop"""
        loop = loop or asyncio.get_event_loop()
        loop.run_until_complete(self.disconnect())


class CacheKey:
    """
    CacheKey

    A class for composing cache keys from one or more terms.

    There are two types of supported terms:
    Qualifiers are strings.
    Fields consist of name/value pairs, where names/values are strings.

    Cache keys may include any number of qualifiers and fields, provided
    there is at least one term. Any/all qualifiers always precede any/all
    fields within the key.

    Terms (qualifiers/fields) are delimited by Start of Header (1: SOH).
    For display purposes, Ampersand ('&') is used instead. As such,
    SOH may not be used within qualifiers or field names/values.
    Ampersand is permitted, but when used, `from_key` will not work on
    the display version of the key.

    Field names and values are delimited by Start of Text (2: STX). For
    display purposes, Equals ('=') is used instead. As such, STX may not
    be used within field names/values or qualifiers. Equals is allowed,
    but when used, `from_key` will not work on display keys.

    Field values of None are converted to the Null (0: NUL) character.
    For display, Tilde ('~') is used instead. This means field values
    consisting of just NUL will be indistinguishable from None. NUL may
    be used as part of longer strings without any such collision risk.

    All other characters may be used, including but not limited to any
    printable character and File/Group/Record/Unit Separators (28-31).

    Python 3.6+ required to preserve order in fields parameter:
    https://www.python.org/dev/peps/pep-0468/

    I/O:
    *qualifiers     strings to be included in key in order
    encoding='utf8' encoding for serialization; key not encoded if None
    **fields        name/value strings to be included in key in order
    return          CacheKey instance
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

        if encoding and isinstance(key, bytes):
            key = str(key, encoding)

        if is_display is None:
            is_display = (cls.TERM_DELIMITER not in key and
                          cls.NAME_VALUE_DELIMITER not in key)

        term_delimiter, name_value_delimiter, null = (
            (cls.TERM_DELIMITER_DISPLAY, cls.NAME_VALUE_DELIMITER_DISPLAY, cls.NULL_DISPLAY)
            if is_display else (cls.TERM_DELIMITER, cls.NAME_VALUE_DELIMITER, cls.NULL))

        terms = key.split(term_delimiter)
        for i, term in enumerate(terms):
            if name_value_delimiter in term:
                break

        qualifiers = terms[:i]

        def unpack_field(field):
            unpacked = field.split(name_value_delimiter)
            if unpacked[-1] == null:
                unpacked[-1] = None
            return unpacked

        try:
            fields = OrderedDict(unpack_field(field) for field in terms[i:])
        except ValueError:
            raise ValueError('CacheKey fields must precede all qualifiers')

        return cls(*qualifiers, encoding=encoding, **fields)

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
        terms = chain(self.qualifiers, packed_fields)
        key = term_delimiter.join(terms)
        return key if is_display or not self.encoding else key.encode(self.encoding)

    @property
    def key(self):
        """Form key byte string from CacheKey instance"""
        return self.to_key(is_display=False)

    def __repr__(self):
        return self.to_key(is_display=True)

    def __init__(self, *qualifiers, encoding=ENCODING_DEFAULT, **fields):
        self.qualifiers = qualifiers
        self.fields = fields
        self.encoding = encoding

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.qualifiers == other.qualifiers and
                    self.fields == other.fields and
                    self.encoding == other.encoding)
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return (self.qualifiers != other.qualifiers or
                    self.fields != other.fields or
                    self.encoding != other.encoding)
        return NotImplemented
