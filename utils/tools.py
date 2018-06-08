#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import re
from collections import OrderedDict
from datetime import datetime, date, time
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from itertools import chain, islice
from pprint import PrettyPrinter

from parse import parse

from exceptions import TooFewValuesError, TooManyValuesError

VALUE_DELIMITER = ', '
MORE_VALUES = '...'

INDENT = 4
WIDTH = 200
PP = PrettyPrinter(indent=INDENT, width=WIDTH)

CLASS_NAME_PATTERN = re.compile(r'[A-Z][a-zA-Z0-9]*$')

def is_class_name(name):
    return CLASS_NAME_PATTERN.match(name)


SELF_REFERENTIAL_PARAMS = {'self', 'cls', 'meta'}

def derive_args(func):
    """Derive args from the given function"""
    args = inspect.getfullargspec(func).args
    if args and args[0] in SELF_REFERENTIAL_PARAMS:
        del args[0]
    return args


def derive_attributes(cls, _mro=None):
    """
    Derive attributes

    Given a class, derive all instance attributes the class declares or
    inherits by inspecting __init__ methods. Attributes are listed
    by initial declaration order, taking into account super calls.

    Does NOT detect the following:
    - attributes set in methods other than __init__
    - attributes set via setattr
    - attributes only conditionally set (they will always be included)
    """
    mro = cls.mro() if _mro is None else _mro
    len_mro = len(mro)
    attributes = OrderedDict()

    try:
        lines = inspect.getsource(cls.__init__).split('\n')

    except TypeError:  # class definition does not contain __init__
        if len_mro > 2:
            super_attributes = derive_attributes(mro[1], mro[1:])
            attributes.update(super_attributes)
        else:
            return attributes

    for line in lines:
        line = line.strip()
        parsed = parse('self.{field} = {}', line)

        if parsed is not None:
            field = parsed.named['field']
            attributes[field] = None
            continue

        if len_mro > 2:  # No attributes on object class
            parsed = (parse('super().__init__({}', line) or
                      parse(f'super({cls.__name__}, self).__init__({{}}', line))
            if parsed is not None:
                super_attributes = derive_attributes(mro[1], mro[1:])
                attributes.update(super_attributes)

    return [k for k in attributes] if _mro is None else attributes


def delist(obj):
    """
    Delist

    Unpack the given obj as follows:
    If obj is not a list, return the obj.
    If obj is a list with 2 or more items, return the list.
    If obj is a list with a single item, return the item.
    If obj is an empty list, return None.
    """
    if isinstance(obj, list):
        if not obj:
            return None
        if len(obj) == 1:
            return obj[0]

    return obj


def enlist(obj):
    """
    Enlist

    Package the given obj as a list as follows:
    If obj is already a list, return the obj.
    If obj is not a list, return list containing obj.
    If obj is None, return an empty list.
    """
    if obj is None:
        return []
    return obj if isinstance(obj, list) else [obj]


def default_serialize(obj):
    """
    Default Serialize

    Convert object to unicode string; use as default in json.dumps
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


def serialize(obj):
    """Serialize object to unicode string"""
    if hasattr(obj, 'jsonify'):
        return obj.jsonify()
    try:
        return default_serialize(obj)
    except TypeError:
        return str(obj)


def multi_parse(templates, string):
    """
    Multi parse attempts to parse the string with each of the templates
    until successful and returns the parse result.

    I/O:
    templates:  sequence of templates
    string:     string to be parsed
    return:     first successful parse result
    """
    for template in templates:
        parsed = parse(template, string)
        if parsed:
            return parsed

    raise ValueError(
        f"'{string}' does not match any template: {templates}")


def form_range_string(minimum, maximum):
    """Form range string (or single value) from minimum and maximum"""
    return f'{minimum}-{maximum}' if minimum < maximum else maximum


def form_values_string(values, iterator=None):
    """Form values string, with ellipsis if iterator has a next value"""
    values_string = VALUE_DELIMITER.join(str(v) for v in values)
    has_more = False

    if iterator:
        try:
            next(iterator)
            has_more = True
        except StopIteration:
            pass

    if has_more:
        values_string += MORE_VALUES

    return values_string


def constrain(iterable, exact=None, minimum=None, maximum=None):
    """
    Constrain

    Given an iterable, confirm the number of values meet the
    constraints and if so return the values.

    I/O:
    exact=None:     Exact number of values required
    minimum=None:   Minimum number of values required
    maximum=None:   Maximum number of values required
    return:         Iterable if it has a length, else convert to a list
                    (i.e. the iterable is an iterator)
    raise:          TooFewValuesError if less than minimum number
                    TooManyValuesError if more than maximum number
    """
    if exact is not None:
        if minimum or maximum:
            raise ValueError(
                f'Incompatible arguments: {minimum}, {maximum}, {exact}')
        minimum = maximum = exact
    else:
        minimum = minimum if minimum is not None else 0
        maximum = maximum if maximum is not None else float('Inf')

    if hasattr(iterable, '__len__'):
        if minimum <= len(iterable) <= maximum:
            return iterable
        iterator = iter(iterable)
    else:
        iterator = iterable

    values = list(islice(iterator, maximum)) if maximum < float('Inf') else list(iterator)

    if len(values) < minimum:
        range_string = form_range_string(minimum, maximum)
        values_string = form_values_string(values)
        raise TooFewValuesError(expected=range_string, received=values_string)

    if len(values) == maximum:
        try:
            values.append(next(iterator))
            range_string = form_range_string(minimum, maximum)
            values_string = form_values_string(values)
            raise TooManyValuesError(expected=range_string, received=values_string)
        except StopIteration:
            pass

    return values


def one(iterable):
    """
    One

    Given an iterable, confirm there is only one value and return it.
    Otherwise, raise TooFewValuesError or TooManyValuesError.
    """
    if hasattr(iterable, '__len__'):
        if len(iterable) == 1:
            try:
                return iterable[0]
            except (TypeError, KeyError):  # sets, dicts, etc.
                return next(iter(iterable))
        iterator = iter(iterable)
    else:
        iterator = iterable

    try:
        first = next(iterator)
    except StopIteration:
        raise TooFewValuesError(expected=1, received='')
    try:
        second = next(iterator)
        values_string = form_values_string((first, second), iterator)
        raise TooManyValuesError(expected=1, received=values_string)
    except StopIteration:
        return first


def one_max(iterable):
    """
    One Max

    Given an iterable, confirm there is at most one value and return it
    if one exists, else None. If there are 2 or more values, raise
    TooManyValuesError.
    """
    if hasattr(iterable, '__len__'):
        if len(iterable) == 1:
            try:
                return iterable[0]
            except (TypeError, KeyError):  # sets, dicts, etc.
                return next(iter(iterable))
        elif not iterable:
            return None
        iterator = iter(iterable)
    else:
        iterator = iterable

    try:
        first = next(iterator)
    except StopIteration:
        return None
    try:
        second = next(iterator)
        values_string = form_values_string((first, second), iterator)
        raise TooManyValuesError(expected=1, received=values_string)
    except StopIteration:
        return first


def one_min(iterable):
    """
    One Min

    Given an iterable, confirm there is at least one value. Return the
    iterable if it has a length; if not, convert it to a list first
    (i.e. the iterable is an iterator). If there are no values, raise
    TooFewValuesError.
    """
    values = iterable if hasattr(iterable, '__len__') else list(iterable)
    if not values:
        raise TooFewValuesError(expected=1, received='')
    return values


def logical_xor(a, b):
    return bool(a) ^ bool(b)


def xor_constrain(a, b):
    if a and not b:
        return a
    if b and not a:
        return b
    if a and b:
        raise ValueError('xor error: both values cannot be True')
    raise ValueError('xor error: both values cannot be False')
