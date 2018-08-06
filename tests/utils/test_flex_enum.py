#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from collections import OrderedDict

from utils.enum import FlexEnum
from utils.tools import isiterator


Fruit = FlexEnum('Fruit', 'APPLE BANANA CANTALOUPE')


class Roshambo(FlexEnum):
    ROCK = 'SCISSORS'
    PAPER = 'ROCK'
    SCISSORS = 'PAPER'


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, value, enum_option, exception', [
    (Fruit, 'APPLE', Fruit.APPLE, None),
    (Fruit, 'banana', Fruit.BANANA, None),
    (Fruit, 3, Fruit.CANTALOUPE, None),
    (Roshambo, 'ROCK', Roshambo.ROCK, None),  # Name trumps value
    (Roshambo, 'STONE', None, ValueError),
    (Roshambo, 1, None, ValueError),
])
def test_flex_enum_cast(enum_class, value, enum_option, exception):
    """Test FlexEnum cast"""
    if enum_option:
        assert enum_class.cast(value) is enum_option
    elif exception:
        with pytest.raises(exception):
            enum_class.cast(value)


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, name, value, enum_option, exception', [
    (Fruit, 'APPLE', 1, Fruit.APPLE, None),
    (Fruit, 'BANANA', 2, Fruit.BANANA, None),
    (Fruit, 'CANTALOUPE', 3, Fruit.CANTALOUPE, None),
    (Fruit, 'DRAGONFRUIT', 1, None, KeyError),
    (Fruit, 'DRAGONFRUIT', 4, None, KeyError),
    (Fruit, 'APPLE', 0, None, ValueError),
    (Roshambo, 'ROCK', 'SCISSORS', Roshambo.ROCK, None),
    (Roshambo, 'PAPER', 'ROCK', Roshambo.PAPER, None),
    (Roshambo, 'SCISSORS', 'PAPER', Roshambo.SCISSORS, None),
    (Roshambo, 'STONE', 'SCISSORS', None, KeyError),
    (Roshambo, 'STONE', 'SCISSORZ', None, KeyError),
    (Roshambo, 'ROCK', 'SCISSORZ', None, ValueError),
])
def test_flex_enum_option(enum_class, name, value, enum_option, exception):
    """Test FlexEnum cast"""
    if enum_option:
        assert enum_class.option(name, value) is enum_option
    elif exception:
        with pytest.raises(exception):
            enum_class.option(name, value)


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, enumables, transform, enum_names', [
    (Fruit, [], None, ['APPLE', 'BANANA', 'CANTALOUPE']),
    (Fruit, [], str.lower, ['apple', 'banana', 'cantaloupe']),
    (Fruit, [Fruit.CANTALOUPE], None, ['CANTALOUPE']),
    (Fruit, [Fruit.CANTALOUPE, 'BANANA', 1], str.capitalize, ['Cantaloupe', 'Banana', 'Apple']),
    (Roshambo, [Roshambo.ROCK, 'ROCK', 'PAPER'], str.capitalize, ['Rock', 'Rock', 'Paper']),
])
def test_flex_enum_names(enum_class, enumables, transform, enum_names):
    """Test FlexEnum names, list, tuple, and set"""
    names = enum_class.names(*enumables, transform=transform)
    assert isiterator(names)
    count = 0
    for name, check in zip(names, enum_names):
        assert name == check
        count += 1

    assert count == len(enum_names)
    assert enum_class.list(*enumables, transform=transform) == enum_names
    assert enum_class.tuple(*enumables, transform=transform) == tuple(enum_names)
    assert enum_class.set(*enumables, transform=transform) == set(enum_names)


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, enumables, transform, enum_values', [
    (Fruit, [], None, [1, 2, 3]),
    (Fruit, [], lambda x: x - 1, [0, 1, 2]),
    (Fruit, [Fruit.CANTALOUPE], None, [3]),
    (Fruit, [Fruit.CANTALOUPE, 'BANANA', 1], lambda x: x ** 2, [9, 4, 1]),
    (Roshambo, [Roshambo.PAPER, 'PAPER'], str.capitalize, ['Rock', 'Rock']),
])
def test_flex_enum_values(enum_class, enumables, transform, enum_values):
    """Test FlexEnum values"""
    values = enum_class.values(*enumables, transform=transform)
    assert isiterator(values)
    count = 0
    for value, check in zip(values, enum_values):
        assert value == check
        count += 1

    assert count == len(enum_values)


def minus_1(x): return x - 1


def square(x): return x ** 2


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, swap, labels, transform, inverse, enumables, enum_items', [
    (Fruit, False, False, None, False, [], [('APPLE', 1), ('BANANA', 2), ('CANTALOUPE', 3)]),
    (Fruit, False, False, str.lower, False, [], [('apple', 1), ('banana', 2), ('cantaloupe', 3)]),
    (Fruit, False, False, str.lower, True, [], [(1, 'apple'), (2, 'banana'), (3, 'cantaloupe')]),
    (Fruit, True, False, minus_1, False, [], [(0, 'APPLE'), (1, 'BANANA'), (2, 'CANTALOUPE')]),
    (Fruit, True, False, minus_1, True, [], [('APPLE', 0), ('BANANA', 1), ('CANTALOUPE', 2)]),
    (Fruit, False, True, str.lower, False, [],
        [('apple', 'APPLE'), ('banana', 'BANANA'), ('cantaloupe', 'CANTALOUPE')]),
    (Fruit, False, True, str.lower, True, [],
        [('APPLE', 'apple'), ('BANANA', 'banana'), ('CANTALOUPE', 'cantaloupe')]),
    (Fruit, False, False, str.capitalize, False, [Fruit.CANTALOUPE, 'BANANA', 1],
        [('Cantaloupe', 3), ('Banana', 2), ('Apple', 1)]),
    (Fruit, False, True, str.capitalize, False, [Fruit.CANTALOUPE, 'BANANA', 1],
        [('Cantaloupe', 'CANTALOUPE'), ('Banana', 'BANANA'), ('Apple', 'APPLE')]),
    (Fruit, False, True, str.capitalize, True, [Fruit.CANTALOUPE, 'BANANA', 1],
        [('CANTALOUPE', 'Cantaloupe'), ('BANANA', 'Banana'), ('APPLE', 'Apple')]),
    (Fruit, True, False, square, False, [Fruit.CANTALOUPE, 'BANANA', 1],
        [(9, 'CANTALOUPE'), (4, 'BANANA'), (1, 'APPLE')]),
    (Fruit, True, True, square, True, [Fruit.CANTALOUPE, 'BANANA', 1],
        [('CANTALOUPE', 9), ('BANANA', 4), ('APPLE', 1)]),
    (Roshambo, True, False, str.capitalize, True, [Roshambo.SCISSORS, 'ROCK'],
        [('SCISSORS', 'Paper'), ('ROCK', 'Scissors')]),
])
def test_flex_enum_items(enum_class, swap, labels, transform, inverse, enumables, enum_items):
    """Test FlexEnum items, map, and labels"""
    items = enum_class.items(
        *enumables, swap=swap, labels=labels, transform=transform, inverse=inverse)
    assert isiterator(items)
    count = 0
    for item, check in zip(items, enum_items):
        assert item == check
        count += 1

    assert count == len(enum_items)
    assert OrderedDict(enum_items) == enum_class.map(
        *enumables, swap=swap, labels=labels, transform=transform, inverse=inverse)
    if labels and not swap:
        assert tuple(enum_items) == enum_class.labels(
            *enumables, transform=transform, inverse=inverse)


class First:
    Fruit = FlexEnum('Fruit', 'APPLE BANANA CANTALOUPE')

    class Roshambo(FlexEnum):
        ROCK = 'SCISSORS'
        PAPER = 'ROCK'
        SCISSORS = 'PAPER'

    class Second:
        Fruit = FlexEnum('Fruit', 'APPLE BANANA CANTALOUPE')

        class Roshambo(FlexEnum):
            ROCK = 'SCISSORS'
            PAPER = 'ROCK'
            SCISSORS = 'PAPER'

        class Third:
            Fruit = FlexEnum('Fruit', 'APPLE BANANA CANTALOUPE')

            class Roshambo(FlexEnum):
                ROCK = 'SCISSORS'
                PAPER = 'ROCK'
                SCISSORS = 'PAPER'


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, qualname', [
    (First.Fruit, 'First.Fruit'),
    (First.Roshambo, 'First.Roshambo'),
    (First.Second.Fruit, 'First.Second.Fruit'),
    (First.Second.Roshambo, 'First.Second.Roshambo'),
    (First.Second.Third.Fruit, 'First.Second.Third.Fruit'),
    (First.Second.Third.Roshambo, 'First.Second.Third.Roshambo'),
])
def test_flex_enum_qualname(enum_class, qualname):
    """Test that FlexEnum qualname is properly set"""
    assert enum_class.__qualname__ == qualname


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_option', [
    First.Fruit.APPLE,
    First.Roshambo.ROCK,
    First.Second.Fruit.BANANA,
    First.Second.Roshambo.PAPER,
    First.Second.Third.Fruit.CANTALOUPE,
    First.Second.Third.Roshambo.SCISSORS
])
def test_flex_enum_repr(enum_option):
    """Confirm FlexEnum repr has qualname/name/value & evals to self"""
    option_repr = repr(enum_option)
    assert enum_option.__class__.__qualname__ in option_repr
    assert enum_option.name in option_repr
    assert str(enum_option.value) in option_repr
    assert eval(option_repr) is enum_option


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_option', [
    First.Fruit.APPLE,
    First.Roshambo.ROCK,
    First.Second.Fruit.BANANA,
    First.Second.Roshambo.PAPER,
    First.Second.Third.Fruit.CANTALOUPE,
    First.Second.Third.Roshambo.SCISSORS
])
def test_flex_enum_str(enum_option):
    """Confirm FlexEnum str contains qualname and name"""
    option_str = str(enum_option)
    assert enum_option.__class__.__qualname__ in option_str
    assert enum_option.name in option_str
