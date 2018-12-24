#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import inspect
import os
from collections import OrderedDict, namedtuple
from itertools import chain
from functools import lru_cache

import aioredis
import wrapt

import settings
from utils.debug import debug
from utils.signature import CallSign
from utils.singleton import Singleton
from utils.tools import is_instance_method, is_nonstring_sequence

ENCODING_DEFAULT = 'utf-8'


class AsyncCache(Singleton):
    """AsyncCache manages async connections to Redis key-value store"""
    @debug
    def initialize(self, loop=None):
        """Initialize AsyncCache singleton"""
        self.client = None
        self.loop = loop = loop or asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(self.connect(loop))
        else:
            loop.run_until_complete(self.connect(loop))

    @debug
    async def connect(self, loop=None):
        """Connect to Redis via pool, set client, and return it"""
        loop = loop or self.loop or asyncio.get_event_loop()
        redis = self.client
        if not redis:
            redis = await aioredis.create_redis_pool(settings.REDIS_ADDRESS,
                                                     encoding=ENCODING_DEFAULT,
                                                     loop=loop)
            self.client = redis
        return redis

    @debug
    async def disconnect(self):
        """Disconnect from Redis, await clean up, and remove client"""
        redis = self.client
        if redis:
            redis.close()
            await redis.wait_closed()
            self.client = None

    @debug
    def terminate(self, loop=None):
        """Terminate AsyncCache from outside the event loop"""
        loop = loop or self.loop or asyncio.get_event_loop()
        loop.run_until_complete(self.disconnect())


class CacheKey:
    """
    CacheKey

    A class for composing cache keys from one or more terms.

    There are two types of supported terms:
    Qualifiers are strings.
    Fields consist of name/value pairs, where names are strings and
    values are either strings or non-string sequences of strings.

    Cache keys may include any number of qualifiers and fields, provided
    there is at least one term. All qualifiers always precede all fields
    within the key.

    Terms (qualifiers/fields) are delimited by Start of Header (1: SOH).
    For display purposes, Ampersand ('&') is used instead. As such,
    SOH may not be used within qualifiers or field names/values. While
    Ampersand is permitted, it will prevent `from_string` from working
    on the display version of the key.

    Field names are assigned values via Start of Text (2: STX). For
    display purposes, Equals ('=') is used instead. As such, STX may not
    be used within field names/values or qualifiers. Equals is allowed,
    but when used, `from_string` will not work on display keys.

    Field values that are non-string sequences (lists/tuples/etc.) are
    serialized via concatenation with End of Text (ETX) as delimiter.
    For display purposes, Vertical Bar ('|') is used instead. While ETX
    and Vertical Bar are permitted, usage within field values will
    prevent `from_string` from working as expected.

    Field values of None are converted to the Null (0: NUL) character.
    For display, Tilde ('~') is used instead. This means field values
    consisting of just NUL will be indistinguishable from None. NUL may
    be used as part of longer strings without any such collision risk.

    All other characters may be used, including but not limited to any
    printable character and File/Group/Record/Unit Separators (28-31).

    Python 3.6+ required to preserve order in fields parameter:
    https://www.python.org/dev/peps/pep-0468/

    I/O:
    *qualifiers      strings to be included in key in positional order
    encoding_=None   encoding for serialization; key not encoded if None
    **fields         name/value pairs to be included in key in keyword
                     order, where values are strings or string sequences
    return           CacheKey instance
    """
    TERM_DELIMITER = chr(1)  # Start of Header (SOH)
    TERM_DELIMITER_DISPLAY = '&'

    FIELD_ASSIGNER = chr(2)  # Start of Text (STX)
    FIELD_ASSIGNER_DISPLAY = '='

    VALUE_SEPARATOR = chr(3)  # End of Text (ETX)
    VALUE_SEPARATOR_DISPLAY = '|'

    NULL = chr(0)  # Null (NUL)
    NULL_DISPLAY = '~'

    @classmethod
    def from_string(cls, key, is_display=None, encoding=None):
        """Construct CacheKey instance from key or display string"""
        if isinstance(key, bytes):
            encoding = encoding or ENCODING_DEFAULT
            key = str(key, encoding)

        if is_display is None:
            is_display = (cls.TERM_DELIMITER not in key and cls.FIELD_ASSIGNER not in key)

        term_delimiter, field_assigner, value_separator, null = cls.special_characters(is_display)

        terms = key.split(term_delimiter)
        for i, term in enumerate(terms):
            if field_assigner in term:
                qualifiers = terms[:i]
                break
        else:
            return cls(*terms, encoding_=encoding)

        def unpack_field(field):
            unpacked = field.split(field_assigner)
            value = unpacked[-1]
            if value == null:
                unpacked[-1] = None
            elif value_separator in value:
                unpacked[-1] = value.split(value_separator)
            return unpacked

        try:
            fields = OrderedDict(unpack_field(field) for field in terms[i:])
        except ValueError:
            raise ValueError('CacheKey qualifiers must precede all fields')

        return cls(*qualifiers, encoding_=encoding, **fields)

    def to_string(self, is_display=False, encoding=None):
        """Form key string or encoded bytes, optionally for display"""
        term_delimiter, field_assigner, value_separator, null = self.special_characters(is_display)

        def pack_field(name, value):
            # serialized_value = null if value is None else str(value)
            if value is None:
                serialized_value = null
            elif is_nonstring_sequence(value):
                serialized_value = value_separator.join(str(v) for v in value)
            else:
                serialized_value = str(value)
            return f'{name}{field_assigner}{serialized_value}'

        packed_fields = (pack_field(name, value) for name, value in self.fields.items())
        qualifiers = (str(qualifier) for qualifier in self.qualifiers)
        terms = chain(qualifiers, packed_fields)
        key = term_delimiter.join(terms)
        return key.encode(encoding) if encoding else key

    @classmethod
    def special_characters(cls, is_display=False):
        if is_display:
            return (cls.TERM_DELIMITER_DISPLAY,
                    cls.FIELD_ASSIGNER_DISPLAY,
                    cls.VALUE_SEPARATOR_DISPLAY,
                    cls.NULL_DISPLAY)
        else:
            return (cls.TERM_DELIMITER,
                    cls.FIELD_ASSIGNER,
                    cls.VALUE_SEPARATOR,
                    cls.NULL)

    @property
    def key(self):
        """Form key from CacheKey instance"""
        return self.to_string(is_display=False, encoding=self.encoding)

    @property
    def string(self):
        """Form key string from CacheKey instance"""
        return self.to_string(is_display=False, encoding=None)

    @property
    def bytes(self):
        """Form key bytes from instance; encoding defaults to utf-8"""
        encoding = self.encoding or ENCODING_DEFAULT
        return self.to_string(is_display=False, encoding=encoding)

    @property
    def display(self):
        """Form display key string from CacheKey instance"""
        return self.to_string(is_display=True, encoding=None)

    @property
    def tuple(self):
        """Form tuple key from CacheKey instance"""
        field_generator = ((name, value) for name, value in self.fields.items())
        return tuple(chain(self.qualifiers, field_generator))

    def __str__(self):
        return self.display

    def __repr__(self):
        class_name = self.__class__.__name__
        qualifiers = ', '.join(repr(qualifier) for qualifier in self.qualifiers)
        delimiter1 = ', ' if qualifiers else ''
        encoding = "encoding_='{self.encoding}'" if self.encoding else ''
        delimiter2 = ', ' if encoding and self.fields else ''
        fields = ', '.join(f'{name}={repr(value)}' for name, value in self.fields.items())
        try:
            eval(f"dict({fields})")
            return f"{class_name}({qualifiers}{delimiter1}{encoding}{delimiter2}{fields})"
        except SyntaxError:
            return f"{class_name}({qualifiers}{delimiter1}{encoding}{delimiter2}**{self.fields})"

    def __init__(self, *qualifiers, encoding_=None, **fields):
        self.qualifiers = qualifiers
        self.fields = OrderedDict(fields)
        self.encoding = encoding_
        if not (self.qualifiers or self.fields):
            raise ValueError('Unable to instantiate empty CacheKey')

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


class LyricalCache(OrderedDict):
    """
    Lyrical Cache

    "Lyrical" is a homonym of LRU-CAL, an acronym for:

        Least Recently Used - Clear Any Loaded

    Lyrical is an LRU cache supporting individual key invalidation and
    key normalization based on the function call signature.

    Inspired by Raymond Hettinger's LRU:
    https://bugs.python.org/issue30153
    """
    def invalidate(self, *args, **kwargs):
        """Invalidate cache key corresponding to given inputs"""
        key = self.form_key(*args, **kwargs)
        self.pop(key, None)

    def form_key(self, *args, **kwargs):
        """Form normalized cache key from args and kwargs"""
        normalized = self.call_sign.normalize(*args, **kwargs)
        cache_key = CacheKey(*normalized.args, **normalized.kwargs)
        return cache_key.tuple

    @wrapt.decorator
    def __call__(self, func, instance, args, kwargs):
        """Decorate function, returning decorated version of function"""
        if asyncio.iscoroutinefunction(func):
            raise TypeError('Function decorated with LyricalCache must not be async.')

        if self.func is None:
            self._initialize_decorated(func)

        key = self.form_key(*args, **kwargs)

        if key in self:
            value = self[key]
            self.move_to_end(key)
            return value

        value = self.func(*args, **kwargs)
        self[key] = value

        if len(self) > self.maxsize:
            self.popitem(last=False)

        return value

    def _initialize_decorated(self, func):
        """Initialize attributes from decorated function"""
        self.func = func
        self.call_sign = CallSign.manifest(func) if func else None

    def __init__(self, func=None, *, maxsize=128):
        super().__init__()
        self.maxsize = maxsize or float('inf')
        self._initialize_decorated(func)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.func == other.func and
                    self.maxsize == other.maxsize)
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return (self.func != other.func or
                    self.maxsize != other.maxsize)
        return NotImplemented


FileInfo = namedtuple('FileInfo', 'last_modified size')


class FileCache:
    """
    File Cache

    The FileCache decorator provides least-recently-used (LRU) caching
    with automatic file invalidation upon file modification.

    Each FileCache applies to a single function and must not be reused.
    The decorated function must accept the file path as a parameter.
    If the file has been modified since the last call, the cache is
    invalidated for just that file and the file is reloaded.

    Both last modified time and file size are used to determine whether
    a file has been modified. The granularity of last modified time is
    one second on some operating systems, which works for most real
    world use cases, but not for tests. File size addresses this issue.

    I/O:
    maxsize=None:        Max size of the LRU cache
    path_parameter=None: Argument name containing file path. Defaults to
                         first parameter that is not self/cls/meta.
    """
    @wrapt.decorator
    def _file_cache_wrapper(self, lyrical_func, instance, args, kwargs):
        """Clear cached file if modified and call lyrical func"""
        if asyncio.iscoroutinefunction(lyrical_func):
            raise TypeError('Function decorated with FileCache must not be async.')

        file_path = (args[self.path_parameter_index] if self.path_parameter_index < len(args)
                     else kwargs[self.path_parameter])

        cached_file_info = self.file_info.get(file_path, FileInfo(0, 0))
        current_file_stats = os.stat(file_path)
        current_file_info = FileInfo(current_file_stats.st_mtime, current_file_stats.st_size)

        if current_file_info != cached_file_info:
            self.file_info[file_path] = current_file_info
            if cached_file_info > (0, 0):
                self.lyrical_cache.invalidate(file_path)

        return lyrical_func(*args, **kwargs)

    @wrapt.decorator
    def __call__(self, func, instance, args, kwargs):
        """Decorate function, returning decorated version of function"""
        if asyncio.iscoroutinefunction(func):
            raise TypeError('Function decorated with FileCache must not be async.')

        if self.func is None:
            self._initialize_decorated(func, self.path_parameter)

        file_cache = self._file_cache_wrapper(self.lyrical_cache(func))
        return file_cache(*args, **kwargs)

    def _initialize_decorated(self, func, path_parameter):
        """Initialize attributes from decorated function"""
        self.func = func
        if not func:
            self.signature = None
            self.path_parameter_index = None
            return

        self.signature = inspect.signature(func)
        parameters = self.signature.parameters

        if not path_parameter:
            self.path_parameter_index = 0
            self.path_parameter = next(iter(parameters.keys()))
            return

        for index, key in enumerate(parameters.keys()):
            if key == path_parameter:
                self.path_parameter_index = index
                break
        else:
            raise ValueError(f'path parameter not found in decorated signature: {self.signature}')

    def __init__(self, func=None, *, maxsize=None, path_parameter=None):
        super().__init__()
        self.maxsize = maxsize or float('inf')
        self.path_parameter = path_parameter
        self.lyrical_cache = LyricalCache(maxsize=maxsize)
        self.file_info = {}
        self._initialize_decorated(func, path_parameter)

    def __repr__(self):
        """
        The FileCache repr evaluates to 'itself':

        FileCache({func}, maxsize={size}, path_parameter={param})

        For a class or static method, the func includes the underlying
        class name and the evaluated repr equates to the original.

        For an instance method, the instance to which it is bound is
        referenced by its repr. If it evaluates to an instance, the
        FileCache repr will evaluate to a FileCache instance.

        If the bound instance's repr evaluates to a new instance, the
        methods will be bound to different instances, so they may not
        equate, but they should return the same values when called.
        """
        class_name = self.__class__.__name__
        if not self.func:
            function = ''
        elif is_instance_method(self.func):
            base = repr(self.func.__self__)
            function = f'{base}.{self.func.__name__}'
        else:
            function = self.func.__qualname__
        maxsize = f'maxsize={self.maxsize}' if self.maxsize < float('inf') else ''
        path_parameter = f"path_parameter='{self.path_parameter}'" if self.path_parameter else ''
        delimiter1 = ', ' if function and maxsize else ''
        delimiter2 = ', ' if (function or maxsize) and path_parameter else ''
        return f"{class_name}({function}{delimiter1}{maxsize}{delimiter2}{path_parameter})"

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.func == other.func and
                    self.maxsize == other.maxsize and
                    self.path_parameter == other.path_parameter)
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return (self.func != other.func or
                    self.maxsize != other.maxsize or
                    self.path_parameter != other.path_parameter)
        return NotImplemented
