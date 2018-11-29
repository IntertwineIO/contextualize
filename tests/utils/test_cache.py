#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import math
import os
import pytest
from contextlib import contextmanager
from collections import OrderedDict, namedtuple  # noqa: F401 OrderedDict used by eval
from datetime import date
from enum import Enum
from unittest.mock import Mock, patch

from utils.cache import CacheKey, FileCache, LyricalCache
from utils.signature import CallSign
from utils.tools import is_instance_method


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
A_FILE = 'a_file.txt'
A_PATH = os.path.join(CURRENT_DIR, A_FILE)
B_FILE = 'b_file.txt'
B_PATH = os.path.join(CURRENT_DIR, B_FILE)
C_FILE = 'c_file.txt'
C_PATH = os.path.join(CURRENT_DIR, C_FILE)


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, qualifiers, fields, encoding, exception, display_check, string_check, bytes_check',
    [(0,
        [],                                            # qualifiers
        {},                                            # fields
        None,                                          # encoding
        ValueError,                                    # exception
        None,                                          # display_check
        None,                                          # string_check
        None),                                         # bytes_check
     (1,
        ['alpha', 'beta'],                             # qualifiers
        {},                                            # fields
        None,                                          # encoding
        None,                                          # exception
        'alpha&beta',                                  # display_check
        'alpha\x01beta',                               # string_check
        b'alpha\x01beta'),                             # bytes_check
     (2,
        [],                                            # qualifiers
        dict(a='1', b='2'),                            # fields
        None,                                          # encoding
        None,                                          # exception
        'a=1&b=2',                                     # display_check
        'a\x021\x01b\x022',                            # string_check
        b'a\x021\x01b\x022'),                          # bytes_check
     (3,
        ['alpha', 'beta'],                             # qualifiers
        {'a': '1', 'b': 2},                            # fields
        None,                                          # encoding
        None,                                          # exception
        'alpha&beta&a=1&b=2',                          # display_check
        'alpha\x01beta\x01a\x021\x01b\x022',           # string_check
        b'alpha\x01beta\x01a\x021\x01b\x022'),         # bytes_check
     (4,
        ['alpha', 'beta'],                             # qualifiers
        {'a': 1, 'b': None},                           # fields
        None,                                          # encoding
        None,                                          # exception
        'alpha&beta&a=1&b=~',                          # display_check
        'alpha\x01beta\x01a\x021\x01b\x02\x00',        # string_check
        b'alpha\x01beta\x01a\x021\x01b\x02\x00'),      # bytes_check
     (5,
        ['alpha', 'beta'],                             # qualifiers
        {'a': 1, 'b': ['b1', 'b2']},                   # fields
        None,                                          # encoding
        None,                                          # exception
        'alpha&beta&a=1&b=b1|b2',                      # display_check
        'alpha\x01beta\x01a\x021\x01b\x02b1\x03b2',    # string_check
        b'alpha\x01beta\x01a\x021\x01b\x02b1\x03b2'),  # bytes_check
     (6,
        ['🐍', 'beta'],                                # qualifiers
        {'a': '1', '🐲': '🐉'},                        # fields
        None,                                          # encoding
        None,                                          # exception
        '🐍&beta&a=1&🐲=🐉',                           # display_check
        '🐍\x01beta\x01a\x021\x01🐲\x02🐉',            # string_check | bytes_check ↴
        b'\xf0\x9f\x90\x8d\x01beta\x01a\x021\x01\xf0\x9f\x90\xb2\x02\xf0\x9f\x90\x89'),
     (7,
        ['🐍', 'beta'],                                # qualifiers
        {'a': '1', '🐲': '🐉'},                        # fields
        'utf-8',                                       # encoding
        None,                                          # exception
        '🐍&beta&a=1&🐲=🐉',                           # display_check
        '🐍\x01beta\x01a\x021\x01🐲\x02🐉',            # string_check | bytes_check ↴
        b'\xf0\x9f\x90\x8d\x01beta\x01a\x021\x01\xf0\x9f\x90\xb2\x02\xf0\x9f\x90\x89'),
     (8,
        ['🐍', 'beta'],                                # qualifiers
        {'a': '1', 'dragon': ['🐲', '🐉']},            # fields
        None,                                          # encoding
        None,                                          # exception
        '🐍&beta&a=1&dragon=🐲|🐉',                    # display_check
        '🐍\x01beta\x01a\x021\x01dragon\x02🐲\x03🐉',  # string_check | bytes_check ↴
        b'\xf0\x9f\x90\x8d\x01beta\x01a\x021\x01dragon\x02\xf0\x9f\x90\xb2\x03\xf0\x9f\x90\x89'),
     (9,
        ['🐍', 'beta'],                                # qualifiers
        {'a': '1', 'dragon': ['🐲', '🐉']},            # fields
        'utf-8',                                       # encoding
        None,                                          # exception
        '🐍&beta&a=1&dragon=🐲|🐉',                    # display_check
        '🐍\x01beta\x01a\x021\x01dragon\x02🐲\x03🐉',  # string_check | bytes_check ↴
        b'\xf0\x9f\x90\x8d\x01beta\x01a\x021\x01dragon\x02\xf0\x9f\x90\xb2\x03\xf0\x9f\x90\x89'),
     ])
def test_cache_key(idx, qualifiers, fields, encoding, exception,
                   display_check, string_check, bytes_check):
    """Test CacheKey"""
    if exception:
        with pytest.raises(exception):
            CacheKey(*qualifiers, encoding_=encoding, **fields)
        return

    cache_key1 = CacheKey(*qualifiers, encoding_=encoding, **fields)
    key1_display = str(cache_key1)
    assert key1_display == display_check
    key1_string = cache_key1.string
    assert key1_string == string_check
    key1_bytes = cache_key1.bytes
    assert key1_bytes == bytes_check
    key1 = cache_key1.key
    assert key1 == bytes_check if encoding else string_check

    key1_repr = repr(cache_key1)
    assert repr(eval(key1_repr)) == key1_repr

    cache_key2 = CacheKey.from_string(key1_display)
    assert str(cache_key2) == key1_display
    assert cache_key2.string == key1_string
    assert cache_key2.bytes == key1_bytes

    cache_key3 = CacheKey.from_string(key1_string)
    assert str(cache_key3) == key1_display
    assert cache_key3.string == key1_string
    assert cache_key3.bytes == key1_bytes

    cache_key4 = CacheKey.from_string(key1_bytes)
    assert str(cache_key4) == key1_display
    assert cache_key4.string == key1_string
    assert cache_key4.bytes == key1_bytes


def validate_lyrical_cache_calls(idx, func, maxsize, call_args_and_cache_hits):
    """Validate LyricalCache hits without any mocking"""
    lyrical_cache = LyricalCache(maxsize=maxsize)
    cached = lyrical_cache(func)

    for call_count, (args, kwargs, cache_hit) in enumerate(call_args_and_cache_hits, start=1):
        check = func(*args, **kwargs)
        value = cached(*args, **kwargs)
        assert value == check


def validate_lyrical_cache_hits(idx, func, maxsize, call_args_and_cache_hits):
    """Validate LyricalCache hits via Mock wrapper"""
    wrapped = Mock(wraps=func)

    # Use call sign of func, not call sign of wrapped
    call_sign = CallSign.manifest(func)
    with patch('utils.cache.CallSign.manifest') as mock_manifest_call_sign:
        mock_manifest_call_sign.return_value = call_sign
        lyrical_cache = LyricalCache(maxsize=maxsize)
        wrapped_and_cached = lyrical_cache(wrapped)

        expected_cache_hits = expected_cache_misses = 0
        assert len(wrapped.call_args_list) == expected_cache_misses

        for call_count, (args, kwargs, cache_hit) in enumerate(call_args_and_cache_hits, start=1):
            check = func(*args, **kwargs)
            value = wrapped_and_cached(*args, **kwargs)
            assert value == check

            if cache_hit:
                expected_cache_hits += 1

            actual_cache_misses = len(wrapped.call_args_list)

            actual_cache_hits = call_count - actual_cache_misses
            assert actual_cache_hits == expected_cache_hits

            expected_cache_misses = call_count - expected_cache_hits
            assert actual_cache_misses == expected_cache_misses


Y, M, D = 1999, 3, 21
Y2, M2, D2 = 2018, 1, 23


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'func', 'maxsize',  'call_args_and_cache_hits'),
    [
     (0,     ord,    3,         [(['A'], {}, 0), (['A'], {}, 1), (['A'], {}, 1)]),
     (1,     ord,    3,         [(['A'], {}, 0), (['B'], {}, 0), (['C'], {}, 0)]),
     (2,     ord,    3,         [(['A'], {}, 0), (['B'], {}, 0), (['C'], {}, 0),
                                 (['A'], {}, 1), (['B'], {}, 1), (['C'], {}, 1)]),
     (3,     ord,    2,         [(['A'], {}, 0), (['B'], {}, 0), (['C'], {}, 0),
                                 (['A'], {}, 0), (['B'], {}, 0), (['C'], {}, 0)]),
     (4,     ord,    2,         [(['A'], {}, 0), (['B'], {}, 0), (['C'], {}, 0),
                                 (['C'], {}, 1), (['B'], {}, 1), (['A'], {}, 0)]),
     (5,     date,   3,         [([Y, M, D], {}, 0), ([Y, M, D], {}, 1), ([Y, M, D], {}, 1)]),
     (6,     date,   3,         [([Y, M], dict(day=D), 0), ([Y, M], dict(day=D2), 0),
                                 ([Y, M], dict(day=D), 1), ([Y, M], dict(day=D2), 1)]),
     (7,     date,   3,         [([Y], dict(month=M, day=D), 0), ([Y], dict(month=M, day=D2), 0),
                                 ([Y], dict(month=M, day=D), 1), ([Y], dict(day=D2, month=M), 1)]),
     (8,     date,   3,         [([], dict(year=Y, month=M, day=D), 0),
                                 ([], dict(year=Y, month=M, day=D2), 0),
                                 ([], dict(year=Y, month=M, day=D), 1),
                                 ([], dict(year=Y, day=D2, month=M), 1)]),
     ])
def test_lyrical_cache_on_builtins(idx, func, maxsize, call_args_and_cache_hits):
    validate_lyrical_cache_calls(idx, func, maxsize, call_args_and_cache_hits)
    validate_lyrical_cache_hits(idx, func, maxsize, call_args_and_cache_hits)


def quadratic_roots(a, b, c):
    try:
        sqrt_term = math.sqrt(b ** 2 - 4 * a * c)
    except ValueError:
        return ()

    x1 = (-b + sqrt_term) / (2 * a)
    rx1 = round(x1)
    x1 = rx1 if rx1 == x1 else x1
    if not sqrt_term:
        return (x1,)

    x2 = (-b - sqrt_term) / (2 * a)
    rx2 = round(x2)
    x2 = rx2 if rx2 == x2 else x2
    return (x1, x2) if x1 < x2 else (x2, x1)


class C:
    def imethod(self, a, b, c):
        return quadratic_roots(a, b, c)

    @classmethod
    def cmethod(cls, a, b, c):
        return quadratic_roots(a, b, c)

    @staticmethod
    def smethod(a, b, c):
        return quadratic_roots(a, b, c)


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'maxsize',  'call_args_and_cache_hits'),
    [
     (0,     2,         [([1, -5, 6], {}, 0), ([1, -5, 6], {}, 1), ([1, -5, 6], {}, 1),
                         ([1, -7, 12], {}, 0), ([1, -7, 12], {}, 1), ([1, -7, 12], {}, 1),
                         ([1, -9, 20], {}, 0), ([1, -9, 20], {}, 1), ([1, -9, 20], {}, 1)]),
     (1,     2,         [([1, -5, 6], {}, 0), ([1, -7, 12], {}, 0), ([1, -9, 20], {}, 0),
                         ([1, -9, 20], {}, 1), ([1, -7, 12], {}, 1), ([1, -5, 6], {}, 0)]),
     (2,     2,         [([1, -5, 6], {}, 0), ([1, -7, 12], {}, 0), ([1, -9, 20], {}, 0),
                         ([1, -5, 6], {}, 0), ([1, -7, 12], {}, 0), ([1, -9, 20], {}, 0)]),
     (3,     3,         [([1, -5, 6], {}, 0), ([1, -7, 12], {}, 0), ([1, -9, 20], {}, 0),
                         ([1, -5, 6], {}, 1), ([1, -7, 12], {}, 1), ([1, -9, 20], {}, 1)]),
     (4,     2,         [([1, -5, 6], {}, 0), ([1, -5], dict(c=6), 1),
                         ([1], dict(b=-5, c=6), 1), ([1], dict(c=6, b=-5), 1),
                         ([], dict(a=1, b=-5, c=6), 1), ([], dict(c=6, b=-5, a=1), 1)]),
     (5,     2,         [([1], dict(b=-5, c=6), 0), ([], dict(c=6, a=1, b=-5), 1),
                         ([1, -7], dict(c=12), 0), ([1], dict(c=12, b=-7), 1),
                         ([1, -9, 20], {}, 0), ([1, -9], dict(c=20), 1)]),
     (6,     2,         [([1, -5, 6], {}, 0),
                         ([1, -7, 12], {}, 0),
                         ([1, -9, 20], {}, 0),
                         ([1, -9], dict(c=20), 1),
                         ([1], dict(c=12, b=-7), 1),
                         ([], dict(c=6, a=1, b=-5), 0)]),
     (7,     2,         [([1, -5, 6], {}, 0),
                         ([1, -7, 12], {}, 0),
                         ([1, -9, 20], {}, 0),
                         ([], dict(c=6, a=1, b=-5), 0),
                         ([1], dict(c=12, b=-7), 0),
                         ([1, -9], dict(c=20), 0)]),
     (8,     3,         [([1, -5, 6], {}, 0),
                         ([1, -7, 12], {}, 0),
                         ([1, -9, 20], {}, 0),
                         ([], dict(c=6, a=1, b=-5), 1),
                         ([1], dict(c=12, b=-7), 1),
                         ([1, -9], dict(c=20), 1)]),
     ])
@pytest.mark.parametrize(
    ('func'), [quadratic_roots, C().imethod, C().cmethod, C().smethod])
def test_lyrical_cache_on_custom_functions(idx, func, maxsize, call_args_and_cache_hits):
    validate_lyrical_cache_calls(idx, func, maxsize, call_args_and_cache_hits)
    validate_lyrical_cache_hits(idx, func, maxsize, call_args_and_cache_hits)


FileIO = Enum('FileIO', 'READ WRITE')


def read_file(file_path):
    with open(file_path) as file:
        return file.read()


class FR:
    def imethod(self, file_path):
        return read_file(file_path)

    @classmethod
    def cmethod(cls, file_path):
        return read_file(file_path)

    @staticmethod
    def smethod(file_path):
        return read_file(file_path)

    def __repr__(self):
        return f'{self.__class__.__qualname__}()'


def write_file(file_path, content):
    with open(file_path, 'w') as file:
        file.write(content)


@contextmanager
def reset_files(paths):

    initial_results = {}
    initial_results = {path: None for path in paths}
    for path in initial_results.keys():
        initial_results[path] = read_file(path)

    try:
        yield initial_results.items()

    finally:
        for path, content in initial_results.items():
            write_file(path, content)


def validate_file_cache_calls(idx, func, maxsize, procedures):
    """Validate FileCache calls without any mocking"""
    file_cache = FileCache(maxsize=maxsize, path_parameter=None)
    assert eval(repr(file_cache)) == file_cache
    file_cached = file_cache(func)
    assert eval(repr(file_cache)) == file_cache

    paths = (procedure.path for procedure in procedures if procedure.io is FileIO.WRITE)
    with reset_files(paths):
        for io, path, content, cache_hit in procedures:
            if io is FileIO.READ:
                check = func(path)
                value = file_cached(path)
                assert value == check
                assert value == content

                eval_repr_file_cache = eval(repr(file_cache))
                # methods bound to different instances do not equate...
                if not is_instance_method(func):
                    assert eval_repr_file_cache == file_cache
                # ...but they should return the same value
                eval_repr_file_cached = eval_repr_file_cache(func)
                eval_repr_value = eval_repr_file_cached(path)
                assert eval_repr_value == check

            elif io is FileIO.WRITE:
                write_file(path, content)
            else:
                raise ValueError('io must be a FileIO enum option')


def validate_file_cache_hits(idx, func, maxsize, procedures):
    """Validate FileCache hits via Mock wrapper"""
    wrapped = Mock(wraps=func)

    # Use signature of func, not signature of wrapped
    signature = inspect.signature(func)
    with patch('utils.cache.inspect.signature') as mock_signature:
        mock_signature.return_value = signature

        file_cache = FileCache(maxsize=maxsize, path_parameter='file_path')
        assert eval(repr(file_cache)) == file_cache
        wrapped_and_cached = file_cache(wrapped)

        expected_cache_hits = expected_cache_misses = 0
        assert len(wrapped.call_args_list) == expected_cache_misses

        paths = (procedure.path for procedure in procedures if procedure.io is FileIO.WRITE)
        with reset_files(paths):
            call_count = 1

            for io, path, content, cache_hit in procedures:

                if io is FileIO.READ:
                    check = func(path)
                    value = wrapped_and_cached(path)
                    assert value == check
                    assert value == content

                    if cache_hit:
                        expected_cache_hits += 1

                    actual_cache_misses = len(wrapped.call_args_list)

                    actual_cache_hits = call_count - actual_cache_misses
                    assert actual_cache_hits == expected_cache_hits

                    expected_cache_misses = call_count - expected_cache_hits
                    assert actual_cache_misses == expected_cache_misses

                    call_count += 1

                elif io is FileIO.WRITE:
                    write_file(path, content)
                else:
                    raise ValueError('io must be a FileIO enum option')


FileProcedure = namedtuple('FileProcedure', 'io path content cache_hit')


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'maxsize',  'procedures'),
    [
     (0,     2,         [FileProcedure(FileIO.READ, A_PATH, 'apple', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         ]),
     (1,     2,         [FileProcedure(FileIO.READ, A_PATH, 'apple', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.WRITE, A_PATH, 'avocado', None),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 1),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 1),
                         ]),
     (2,     2,         [FileProcedure(FileIO.READ, A_PATH, 'apple', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.WRITE, A_PATH, 'avocado', None),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 0),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 1),
                         FileProcedure(FileIO.WRITE, B_PATH, 'blueberry', None),
                         FileProcedure(FileIO.READ, B_PATH, 'blueberry', 0),
                         FileProcedure(FileIO.READ, B_PATH, 'blueberry', 1),
                         ]),
     (3,     2,         [FileProcedure(FileIO.READ, A_PATH, 'apple', 0),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 1),
                         FileProcedure(FileIO.WRITE, A_PATH, 'avocado', None),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 0),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 1),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 1),
                         FileProcedure(FileIO.WRITE, B_PATH, 'blueberry', None),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'blueberry', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'blueberry', 1),
                         ]),
     (4,     2,         [FileProcedure(FileIO.READ, A_PATH, 'apple', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 0),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 1),
                         FileProcedure(FileIO.READ, C_PATH, 'cantaloupe', 0),
                         FileProcedure(FileIO.READ, C_PATH, 'cantaloupe', 1),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 0),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 0),
                         FileProcedure(FileIO.READ, C_PATH, 'cantaloupe', 0),
                         ]),
     (5,     None,      [FileProcedure(FileIO.READ, A_PATH, 'apple', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 0),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 1),
                         FileProcedure(FileIO.READ, C_PATH, 'cantaloupe', 0),
                         FileProcedure(FileIO.READ, C_PATH, 'cantaloupe', 1),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 1),
                         FileProcedure(FileIO.READ, C_PATH, 'cantaloupe', 1),
                         ]),
     (6,     None,      [FileProcedure(FileIO.READ, A_PATH, 'apple', 0),
                         FileProcedure(FileIO.READ, B_PATH, 'banana', 0),
                         FileProcedure(FileIO.READ, C_PATH, 'cantaloupe', 0),
                         FileProcedure(FileIO.WRITE, B_PATH, 'blueberry', None),
                         FileProcedure(FileIO.READ, C_PATH, 'cantaloupe', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'blueberry', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.WRITE, C_PATH, 'coconut', None),
                         FileProcedure(FileIO.READ, A_PATH, 'apple', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'blueberry', 1),
                         FileProcedure(FileIO.READ, C_PATH, 'coconut', 0),
                         FileProcedure(FileIO.WRITE, A_PATH, 'avocado', None),
                         FileProcedure(FileIO.READ, C_PATH, 'coconut', 1),
                         FileProcedure(FileIO.READ, B_PATH, 'blueberry', 1),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 0),
                         FileProcedure(FileIO.READ, A_PATH, 'avocado', 1),
                         ]),
     ])
@pytest.mark.parametrize(
    ('func'), [read_file, FR().imethod, FR().cmethod, FR().smethod])
def test_file_cache(idx, func, maxsize, procedures):
    validate_file_cache_calls(idx, func, maxsize, procedures)
    validate_file_cache_hits(idx, func, maxsize, procedures)
