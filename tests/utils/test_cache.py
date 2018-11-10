#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from utils.cache import CacheKey


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

    cache_key2 = CacheKey.from_key(key1_display)
    assert str(cache_key2) == key1_display
    assert cache_key2.string == key1_string
    assert cache_key2.bytes == key1_bytes

    cache_key3 = CacheKey.from_key(key1_string)
    assert str(cache_key3) == key1_display
    assert cache_key3.string == key1_string
    assert cache_key3.bytes == key1_bytes

    cache_key4 = CacheKey.from_key(key1_bytes)
    assert str(cache_key4) == key1_display
    assert cache_key4.string == key1_string
    assert cache_key4.bytes == key1_bytes
