#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import lru_cache
from itertools import islice


def delist(obj):
    if isinstance(obj, list):
        if not obj:
            return None
        if len(obj) == 1:
            return obj[0]

    return obj


def enlist(obj):
    return obj if isinstance(obj, list) else [obj]


def one(generator):
    first = next(generator)
    try:
        second = next(generator)
        raise ValueError('Expected 1 value, but received 2 or more: '
                         f'{first}, {second},...')
    except StopIteration:
        return first


def one_max(generator):
    try:
        return one(generator)
    except StopIteration:
        return None


def n_values(generator, n, minimum=None):
    values = list(islice(generator, n))
    min_num = n if minimum is None else minimum
    if len(values) < min_num:
        num_values = '1 value' if min_num == 1 else f'{min_num} values'
        raise ValueError(f'Expected {num_values}, but received '
                         f'{len(values)}: {values}')
    try:
        next_value = next(generator)
        values.append(next_value)
        string_values = ', '.join(str(v) for v in values)
        num_values = '1 value' if n == 1 else f'{n} values'
        raise ValueError(f'Expected {num_values}, but received '
                         f'{n + 1} or more: {string_values},...')
    except StopIteration:
        return values
