#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from collections import namedtuple
from enum import Enum

from contextualize.exceptions import TooFewValuesError, TooManyValuesError
from contextualize.utils.iterable import InfinIterator, constrain, one, one_max, one_min
from contextualize.utils.tools import is_child_class, is_iterator


MAX_ITERABLE_SIZE = 5


def generator_fn(iterable):
    return (x for x in iterable)


def range_fn(iterable):
    return range(len(iterable))


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'iterable_fn'),
    [
     (0,      generator_fn),
     (1,      range_fn),
     (2,      list),
     (3,      tuple),
     ])
def test_infiniterator(idx, iterable_fn):
    """Test that InfinIterator is an iterator that can be reused"""
    max_range = range(MAX_ITERABLE_SIZE)
    for length in max_range:
        iterable_check = list(range(length))
        iterable = iterable_fn(iterable_check)

        infiniterator = InfinIterator(iterable)
        assert is_iterator(infiniterator)

        list1 = list(infiniterator)
        assert list1 == iterable_check

        list2 = list(infiniterator)
        assert list2 == iterable_check


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'iterable_fn'),
    [
     (0,      generator_fn),
     (1,      range_fn),
     (2,      list),
     (3,      tuple),
     (4,      set),
     ])
def test_constrain_exact(idx, iterable_fn):
    max_range = range(MAX_ITERABLE_SIZE)
    for length in max_range:
        args = list(range(length))

        if iterable_fn is generator_fn:
            iterable_check = list(args)
        else:
            iterable = iterable_check = iterable_fn(args)

        for exact in max_range:
            if iterable_fn is generator_fn:
                iterable = iterable_fn(args)

            print(exact, length)

            if length == exact:
                assert constrain(iterable, exact) == iterable_check
            else:
                error = TooFewValuesError if length < exact else TooManyValuesError
                with pytest.raises(error):
                    constrain(iterable, exact)


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'iterable_fn'),
    [
     (0,      generator_fn),
     (1,      range_fn),
     (2,      list),
     (3,      tuple),
     (4,      set),
     ])
def test_constrain_minimum(idx, iterable_fn):
    max_range = range(MAX_ITERABLE_SIZE)
    for length in max_range:
        args = list(range(length))

        if iterable_fn is generator_fn:
            iterable_check = list(args)
        else:
            iterable = iterable_check = iterable_fn(args)

        for minimum in max_range:
            if iterable_fn is generator_fn:
                iterable = iterable_fn(args)

            print(minimum, length)

            if length >= minimum:
                assert constrain(iterable, minimum=minimum) == iterable_check
            else:
                with pytest.raises(TooFewValuesError):
                    constrain(iterable, minimum=minimum)


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'iterable_fn'),
    [
     (0,      generator_fn),
     (1,      range_fn),
     (2,      list),
     (3,      tuple),
     (4,      set),
     ])
def test_constrain_maximum(idx, iterable_fn):
    max_range = range(MAX_ITERABLE_SIZE)
    for length in max_range:
        args = list(range(length))

        if iterable_fn is generator_fn:
            iterable_check = list(args)
        else:
            iterable = iterable_check = iterable_fn(args)

        for maximum in max_range:
            if iterable_fn is generator_fn:
                iterable = iterable_fn(args)

            print(maximum, length)

            if length <= maximum:
                assert constrain(iterable, maximum=maximum) == iterable_check
            else:
                with pytest.raises(TooManyValuesError):
                    constrain(iterable, maximum=maximum)


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'iterable_fn'),
    [
     (0,      generator_fn),
     (1,      range_fn),
     (2,      list),
     (3,      tuple),
     (4,      set),
     ])
def test_constrain_minimum_and_maximum(idx, iterable_fn):
    max_range = range(MAX_ITERABLE_SIZE)
    for length in max_range:
        args = list(range(length))

        if iterable_fn is generator_fn:
            iterable_check = list(args)
        else:
            iterable = iterable_check = iterable_fn(args)

        for minimum in max_range:
            for maximum in max_range:
                if iterable_fn is generator_fn:
                    iterable = iterable_fn(args)

                print(minimum, maximum, length)

                if length >= minimum and length <= maximum:
                    assert constrain(iterable, minimum=minimum, maximum=maximum) == iterable_check
                else:
                    if minimum > maximum:
                        error = ValueError
                    else:
                        error = TooFewValuesError if length < minimum else TooManyValuesError
                    with pytest.raises(error):
                        constrain(iterable, minimum=minimum, maximum=maximum)


SingleTuple = namedtuple('SingleTuple', 'first')
DoubleTuple = namedtuple('DoubleTuple', 'first second')

SingleEnum = Enum('SingleEnum', 'FIRST')
DoubleEnum = Enum('DoubleEnum', 'FIRST SECOND')


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'iterable',               'check'),
    [
     (0,      (x for x in range(0)),    TooFewValuesError),
     (1,      (x for x in range(1)),    0),
     (2,      (x for x in range(2)),    TooManyValuesError),
     (3,      [],                       TooFewValuesError),
     (4,      ['a'],                   'a'),
     (5,      ['a', 'b'],               TooManyValuesError),
     (6,      (),                       TooFewValuesError),
     (7,      ('a',),                  'a'),
     (8,      ('a', 'b'),               TooManyValuesError),
     (9,      set(),                    TooFewValuesError),
     (10,     {'a'},                   'a'),
     (11,     {'a', 'b'},               TooManyValuesError),
     (12,     dict(),                   TooFewValuesError),
     (13,     {'a': 0},                'a'),
     (14,     {'a': 0, 'b': 1},         TooManyValuesError),
     (15,     range(0),                 TooFewValuesError),
     (16,     range(1),                 0),
     (17,     range(2),                 TooManyValuesError),
     (15,     range(0),                 TooFewValuesError),
     (16,     range(1),                 0),
     (17,     range(2),                 TooManyValuesError),
     (18,     SingleTuple('a'),        'a'),
     (19,     DoubleTuple('a', 'b'),    TooManyValuesError),
     (20,     SingleEnum,               SingleEnum.FIRST),
     (21,     DoubleEnum,               TooManyValuesError),
     ])
def test_one(idx, iterable, check):
    if is_child_class(check, Exception):
        with pytest.raises(check):
            one(iterable)
    else:
        assert one(iterable) == check


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'iterable',               'check'),
    [
     (0,      (x for x in range(0)),    None),
     (1,      (x for x in range(1)),    0),
     (2,      (x for x in range(2)),    TooManyValuesError),
     (3,      [],                       None),
     (4,      ['a'],                   'a'),
     (5,      ['a', 'b'],               TooManyValuesError),
     (6,      (),                       None),
     (7,      ('a',),                  'a'),
     (8,      ('a', 'b'),               TooManyValuesError),
     (9,      set(),                    None),
     (10,     {'a'},                   'a'),
     (11,     {'a', 'b'},               TooManyValuesError),
     (12,     dict(),                   None),
     (13,     {'a': 0},                'a'),
     (14,     {'a': 0, 'b': 1},         TooManyValuesError),
     (15,     range(0),                 None),
     (16,     range(1),                 0),
     (17,     range(2),                 TooManyValuesError),
     (15,     range(0),                 None),
     (16,     range(1),                 0),
     (17,     range(2),                 TooManyValuesError),
     (18,     SingleTuple('a'),        'a'),
     (19,     DoubleTuple('a', 'b'),    TooManyValuesError),
     (20,     SingleEnum,               SingleEnum.FIRST),
     (21,     DoubleEnum,               TooManyValuesError),
     ])
def test_one_max(idx, iterable, check):
    if is_child_class(check, Exception):
        with pytest.raises(check):
            one_max(iterable)
    else:
        assert one_max(iterable) == check


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'iterable',               'check'),
    [
     (0,      (x for x in range(0)),    TooFewValuesError),
     (1,      (x for x in range(1)),    [0]),
     (2,      (x for x in range(2)),    [0, 1]),
     (3,      [],                       TooFewValuesError),
     (4,      ['a'],                    ['a']),
     (5,      ['a', 'b'],               ['a', 'b']),
     (6,      (),                       TooFewValuesError),
     (7,      ('a',),                   ('a',)),
     (8,      ('a', 'b'),               ('a', 'b')),
     (9,      set(),                    TooFewValuesError),
     (10,     {'a'},                    {'a'}),
     (11,     {'a', 'b'},               {'a', 'b'}),
     (12,     dict(),                   TooFewValuesError),
     (13,     {'a': 0},                 {'a': 0}),
     (14,     {'a': 0, 'b': 1},         {'a': 0, 'b': 1}),
     (15,     range(0),                 TooFewValuesError),
     (16,     range(1),                 range(1)),
     (17,     range(2),                 range(2)),
     (18,     SingleTuple('a'),         SingleTuple('a')),
     (19,     DoubleTuple('a', 'b'),    DoubleTuple('a', 'b')),
     (20,     SingleEnum,               SingleEnum),
     (21,     DoubleEnum,               DoubleEnum),
     ])
def test_one_min(idx, iterable, check):
    if is_child_class(check, Exception):
        with pytest.raises(check):
            one_min(iterable)
    else:
        assert one_min(iterable) == check
