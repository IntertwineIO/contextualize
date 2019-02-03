#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import deque
from itertools import islice

from contextualize.exceptions import TooFewValuesError, TooManyValuesError
from contextualize.utils.tools import is_iterator

VALUE_DELIMITER = ', '
MORE_VALUES = '...'


class InfinIterator:
    """
    InfinIterator, the infinite iterator

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


def _determine_constraints(exact, minimum, maximum):
    if exact is not None:
        if minimum is not None or maximum is not None:
            raise ValueError(
                f'Incompatible arguments: {exact}, {minimum}, {maximum}')
        minimum = maximum = exact
    else:
        minimum = minimum if minimum is not None else 0
        maximum = maximum if maximum is not None else float('Inf')
        if minimum > maximum:
            raise ValueError(
                f'Minimum ({minimum}) may not be greater than maximum ({maximum})')

    return minimum, maximum


def _form_range_string(minimum, maximum):
    """Form range string (or single value) from minimum and maximum"""
    return f'{minimum}-{maximum}' if minimum < maximum else maximum


def _form_values_string(values, iterator=None):
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


def _raise_constrain_error(error, values, minimum, maximum):
    range_string = _form_range_string(minimum, maximum)
    values_string = _form_values_string(values)
    raise error(expected=range_string, received=values_string)


def _obtain_constrained_values(iterator, minimum, maximum):
    values = list(islice(iterator, maximum)) if maximum < float('Inf') else list(iterator)

    if len(values) < minimum:
        _raise_constrain_error(TooFewValuesError, values, minimum, maximum)

    if len(values) == maximum:
        try:
            values.append(next(iterator))
            _raise_constrain_error(TooManyValuesError, values, minimum, maximum)
        except StopIteration:
            pass

    return values


def consume(iterator, n=None):
    """
    Consume

    Advance the iterator n-steps ahead. If n is None, consume entirely.

    Reference:
    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)


def constrain(iterable, exact=None, minimum=None, maximum=None):
    """
    Constrain

    Given an iterable, confirm the number of values meet the constraints
    and if so return the iterable, casting to a list if an iterator.

    I/O:
    exact=None:     Exact number of values required
    minimum=None:   Minimum number of values required
    maximum=None:   Maximum number of values required
    return:         Iterable if it has a length, else convert to a list
                    (i.e. the iterable is an iterator)
    raise:          TooFewValuesError if less than minimum number
                    TooManyValuesError if more than maximum number
    """
    minimum, maximum = _determine_constraints(exact, minimum, maximum)

    if is_iterator(iterable):
        iterator = iterable
    else:
        if minimum <= len(iterable) <= maximum:
            return iterable
        iterator = iter(iterable)

    return _obtain_constrained_values(iterator, minimum, maximum)


def one(iterable):
    """
    One

    Given an iterable, confirm there is only one value and return it.
    Otherwise, raise TooFewValuesError or TooManyValuesError.

    Comparable to the snippet below, but ~40% faster:
        next(iter(constrain(iterable, exact=1)))
    """
    if is_iterator(iterable):
        iterator = iterable
    else:
        if len(iterable) == 1:
            try:
                return iterable[0]
            except (TypeError, KeyError):  # sets, dicts, etc.
                return next(iter(iterable))
        iterator = iter(iterable)

    try:
        first = next(iterator)
    except StopIteration:
        raise TooFewValuesError(expected=1, received='')
    try:
        second = next(iterator)
        values_string = _form_values_string((first, second), iterator)
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
    try:
        return one(iterable)
    except TooFewValuesError:
        return None


def one_min(iterable):
    """
    One Min

    Given an iterable, confirm there is at least one value. Return the
    iterable if it has a length; if not (i.e. it's an iterator), convert
    it to a list first. If there are no values, raise TooFewValuesError.
    """
    values = list(iterable) if is_iterator(iterable) else iterable
    if not values:
        raise TooFewValuesError(expected=1, received='')
    return values
