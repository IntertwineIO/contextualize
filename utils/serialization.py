#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime, date, time
from decimal import Decimal
from enum import Enum

ENCODING_DEFAULT = 'utf-8'
NULL = 'null'


def safe_encode(val, encoding):
    """Encode value, unless null"""
    return NULL.encode(encoding) if val is None else val.encode(encoding)


def safe_decode(val, encoding):
    """Decode value unless null"""
    null_bytes = NULL.encode(encoding)
    return None if val == null_bytes or val is None else val.decode(encoding)


def serialize_nonstandard(obj):
    """
    Serialize Nonstandard

    Convert nonstandard object to unicode string; default for json.dumps
    Supported types:
    - datetime/date/time: isoformat
    - Decimal: str
    - Enum: module.qualname.name
    Raise TypeError for all other types
    """
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return str(obj)

    if isinstance(obj, Enum):
        enum_class = obj.__class__
        module = enum_class.__module__
        qualname = enum_class.__qualname__
        name = obj.name
        return f'{module}.{qualname}.{name}'

    raise TypeError(f'Type {type(obj)} is not JSON serializable')


def serialize(obj, encoding=None):
    """Serialize object, optionally encoding as well"""
    if obj is None:
        serialized = NULL
    elif hasattr(obj, 'to_json'):
        serialized = obj.to_json()
    else:
        try:
            serialized = serialize_nonstandard(obj)
        except TypeError:
            serialized = str(obj)

    return serialized.encode(encoding) if encoding else serialized
