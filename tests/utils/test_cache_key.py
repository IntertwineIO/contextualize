#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from utils.cache import CacheKey


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, qualifiers, fields, encoding, exception, display_check, string_check, bytes_check', [
    (0, [], {}, None, ValueError, None, None, None),
    (1, ['alpha', 'beta'], {}, None, None, 'alpha&beta', 'alpha\x01beta', b'alpha\x01beta'),
    (2, [], dict(a='1', b='2'), None, None, 'a=1&b=2', 'a\x021\x01b\x022', b'a\x021\x01b\x022'),
    (3, ['alpha', 'beta'], {'a': '1', 'b': 2}, None, None, 'alpha&beta&a=1&b=2',
        'alpha\x01beta\x01a\x021\x01b\x022', b'alpha\x01beta\x01a\x021\x01b\x022'),
    (4, ['alpha', 'beta'], {'a': 1, 'b': None}, None, None, 'alpha&beta&a=1&b=~',
        'alpha\x01beta\x01a\x021\x01b\x02\x00', b'alpha\x01beta\x01a\x021\x01b\x02\x00'),
    (5, ['alpha', 'beta'], {'a': 1, 'b': ['b1', 'b2']}, None, None, 'alpha&beta&a=1&b=b1|b2',
        'alpha\x01beta\x01a\x021\x01b\x02b1\x03b2', b'alpha\x01beta\x01a\x021\x01b\x02b1\x03b2'),
    (6, ['🐍', 'beta'], {'a': '1', '🐲': '🐉'}, None, None,
        '🐍&beta&a=1&🐲=🐉', '🐍\x01beta\x01a\x021\x01🐲\x02🐉',
        b'\xf0\x9f\x90\x8d\x01beta\x01a\x021\x01\xf0\x9f\x90\xb2\x02\xf0\x9f\x90\x89'),
    (7, ['🐍', 'beta'], {'a': '1', '🐲': '🐉'}, 'utf-8', None,
        '🐍&beta&a=1&🐲=🐉', '🐍\x01beta\x01a\x021\x01🐲\x02🐉',
        b'\xf0\x9f\x90\x8d\x01beta\x01a\x021\x01\xf0\x9f\x90\xb2\x02\xf0\x9f\x90\x89'),
    (8, ['🐍', 'beta'], {'a': '1', 'dragon': ['🐲', '🐉']}, None, None,
        '🐍&beta&a=1&dragon=🐲|🐉', '🐍\x01beta\x01a\x021\x01dragon\x02🐲\x03🐉',
        b'\xf0\x9f\x90\x8d\x01beta\x01a\x021\x01dragon\x02\xf0\x9f\x90\xb2\x03\xf0\x9f\x90\x89'),
    (9, ['🐍', 'beta'], {'a': '1', 'dragon': ['🐲', '🐉']}, 'utf-8', None,
        '🐍&beta&a=1&dragon=🐲|🐉', '🐍\x01beta\x01a\x021\x01dragon\x02🐲\x03🐉',
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
