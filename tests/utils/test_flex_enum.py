#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from collections import OrderedDict

from utils.enum import FlexEnum
from utils.tools import ischildclass, isiterator


# Functional enum declaration
Fruit = FlexEnum('Fruit', 'APPLE BANANA CANTALOUPE')


class Roshambo(FlexEnum):
    """Enum with values that are strings and rotating"""
    ROCK = 'SCISSORS'
    PAPER = 'ROCK'
    SCISSORS = 'PAPER'


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, value,   check',
    [(Fruit,    'APPLE',  Fruit.APPLE),
     (Fruit,    'banana', Fruit.BANANA),
     (Fruit,     3,       Fruit.CANTALOUPE),
     (Roshambo, 'ROCK',   Roshambo.ROCK),  # Name trumps value
     (Roshambo, 'STONE',  ValueError),
     (Roshambo,  1,       ValueError),
     ])
def test_flex_enum_cast(enum_class, value, check):
    """Test FlexEnum cast"""
    if ischildclass(check, Exception):
        with pytest.raises(check):
            enum_class.cast(value)
    else:
        assert enum_class.cast(value) is check


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, name,         value,       check',
    [(Fruit,    'APPLE',       1,           Fruit.APPLE),
     (Fruit,    'BANANA',      2,           Fruit.BANANA),
     (Fruit,    'CANTALOUPE',  3,           Fruit.CANTALOUPE),
     (Fruit,    'DRAGONFRUIT', 1,           KeyError),
     (Fruit,    'DRAGONFRUIT', 4,           KeyError),
     (Fruit,    'APPLE',       0,           ValueError),
     (Roshambo, 'ROCK',       'SCISSORS',   Roshambo.ROCK),
     (Roshambo, 'PAPER',      'ROCK',       Roshambo.PAPER),
     (Roshambo, 'SCISSORS',   'PAPER',      Roshambo.SCISSORS),
     (Roshambo, 'STONE',      'SCISSORS',   KeyError),
     (Roshambo, 'STONE',      'SCISSORZ',   KeyError),
     (Roshambo, 'ROCK',       'SCISSORZ',   ValueError),
     ])
def test_flex_enum_option(enum_class, name, value, check):
    """Confirm FlexEnum option creates enums or raises as expected"""
    if ischildclass(check, Exception):
        with pytest.raises(check):
            enum_class.option(name, value)
    else:
        assert enum_class.option(name, value) is check


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, enumables, transform, enum_names',
    [(Fruit, [], None, ['APPLE', 'BANANA', 'CANTALOUPE']),
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
    'enum_class, enumables, transform, enum_values',
    [(Fruit, [], None, [1, 2, 3]),
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
    'enum_class, swap, labels, transform, inverse, enumables, enum_items',
    [(Fruit, False, False, None, False, [], [('APPLE', 1), ('BANANA', 2), ('CANTALOUPE', 3)]),
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
    'enum_class, qualname',
    [(First.Fruit, 'First.Fruit'),
     (First.Roshambo, 'First.Roshambo'),
     (First.Second.Fruit, 'First.Second.Fruit'),
     (First.Second.Roshambo, 'First.Second.Roshambo'),
     (First.Second.Third.Fruit, 'First.Second.Third.Fruit'),
     (First.Second.Third.Roshambo, 'First.Second.Third.Roshambo'),
     ])
def test_flex_enum_qualname(enum_class, qualname):
    """Confirm FlexEnum qualname is properly set"""
    assert enum_class.__qualname__ == qualname


ENUM_OPTIONS = [
    First.Fruit.APPLE,
    First.Roshambo.ROCK,
    First.Second.Fruit.BANANA,
    First.Second.Roshambo.PAPER,
    First.Second.Third.Fruit.CANTALOUPE,
    First.Second.Third.Roshambo.SCISSORS,
]


@pytest.mark.unit
@pytest.mark.parametrize('enum_option', ENUM_OPTIONS)
def test_flex_enum_serialization(enum_option):
    """Confirm FlexEnum serializes and deserializes to self"""
    serialized_option = enum_option.serialize()
    assert isinstance(serialized_option, str)
    enum_class = enum_option.__class__
    assert enum_class.__module__ in serialized_option
    assert enum_class.__qualname__ in serialized_option
    assert enum_option.name in serialized_option
    assert FlexEnum.deserialize(serialized_option) is enum_option
    assert enum_class.deserialize(serialized_option) is enum_option


@pytest.mark.unit
@pytest.mark.parametrize('enum_option', ENUM_OPTIONS)
def test_flex_enum_repr(enum_option):
    """Confirm FlexEnum repr has qualname/name/value & evals to self"""
    option_repr = repr(enum_option)
    assert enum_option.__class__.__qualname__ in option_repr
    assert enum_option.name in option_repr
    assert str(enum_option.value) in option_repr
    assert eval(option_repr) is enum_option


@pytest.mark.unit
@pytest.mark.parametrize('enum_option', ENUM_OPTIONS)
def test_flex_enum_str(enum_option):
    """Confirm FlexEnum str contains qualname and name"""
    option_str = str(enum_option)
    assert enum_option.__class__.__qualname__ in option_str
    assert enum_option.name in option_str
