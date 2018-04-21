#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import math
import random
from functools import lru_cache
from itertools import islice

from parse import parse

from exceptions import TooFewValuesError, TooManyValuesError

VALUE_DELIMITER = ', '
MORE_VALUES = '...'

# https://searchenginewatch.com/sew/study/2276184/no-1-position-in-google-gets-33-of-search-traffic-study
FIRST_SELECTION_PROBABILITY = 0.325
# Cumulative exponential distribution:
#      F = 1 - exp(- lambda * x)
# lambda = - log(1 - F) / x
#        = - log(1 - 0.325) / 1 = 0.393
HUMAN_SELECTION_LAMBDA = 0.393

SELF_REFERENTIAL_PARAMS = {'self', 'cls', 'meta'}


def derive_args(func):
    args = inspect.getfullargspec(func).args
    if args and args[0] in SELF_REFERENTIAL_PARAMS:
        del args[0]
    return args


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


def multi_parse(templates, string):
    """
    Multi parse attempts to parse the string with each of the templates
    until successful and returns the parse result.

    I/O:
    templates:  sequence of templates
    string:     string to be parsed
    return:     first successful parse result
    """
    num_templates = len(templates)
    for i, template in enumerate(templates, start=1):
        parsed = parse(template, string)
        if parsed:
            return parsed
        if i == num_templates:
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


def random_exponential_index(lambd=1, length=None, attempts=10):
    """
    Random Exponential Index

    Use exponential probability distribution to generate a random index.

    I/O:
    lambd=1:        Exponential distribution lambda aka rate parameter
    length=None:    Length of implied list for which index is generated
    attempts=10:    Max attempts to generate index less than length
    return:         Random integer less than length using  exponential
                    probability distribution with given lambda. If all
                    attempts fail, return 0.
    raise:          ValueError if length less than 1
    """
    length = float('Inf') if length is None else length
    if length < 1:
        raise ValueError('Length must be at least 1 to generate index')
    if length > 1:
        for _ in range(attempts):
            value = random.expovariate(lambd)
            if value < length:
                return math.floor(value)
    return 0


def human_selection_shuffle(values, lambd=HUMAN_SELECTION_LAMBDA):
    """
    Human Selection Shuffle

    Shuffle list of values in place via exponential distribution that
    approximates likelihood of user clicking the nth element. The
    distribution is used repeatedly to select multiple values. Once a
    value has been selected, it is no longer eligible for selection and
    any following elements have their indices lowered by 1.

    I/O:
    values:         List of values to be shuffled in place
    lambd=0.393:    Exponential distribution lambda aka rate parameter.
                    Default is HUMAN_SELECTION_LAMBDA with value 0.393,
                    derived from first selection probability of 32.5%
    return:         None
    """
    num_values = len(values)
    if num_values < 2:
        return
    num_remaining = num_values
    while num_remaining:
        selected_index = random_exponential_index(lambd, length=num_remaining)
        selected = values.pop(selected_index)
        values.append(selected)
        num_remaining -= 1


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
