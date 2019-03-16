#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from collections import OrderedDict

from contextualize.utils.enum import FlexEnum
from contextualize.utils.tools import is_child_class, is_iterator


# Functional enum declaration
Fruit = FlexEnum('Fruit', 'APPLE BANANA CANTALOUPE')


class Roshambo(FlexEnum):
    """Roshambo enum for breaking gridlock"""
    ROCK = 'SCISSORS'
    PAPER = 'ROCK'
    SCISSORS = 'PAPER'

    @property
    def beats(self):
        return self.__class__[self.value]

    @classmethod
    def shoot(cls, hand1, hand2):
        if hand1 is hand2:
            return (1, 1)
        if hand1 > hand2:
            return (1, 0)
        elif hand1 < hand2:
            return (0, 1)

    def __gt__(self, other):
        return self.beats is other

    def __ge__(self, other):
        return not self < other

    def __lt__(self, other):
        return other.beats is self

    def __le__(self, other):
        return not self > other


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, enum_class,    value,   check',
    [(0,  Fruit,        'APPLE',  Fruit.APPLE),
     (1,  Fruit,        'banana', Fruit.BANANA),
     (2,  Fruit,         3,       Fruit.CANTALOUPE),
     (3,  Roshambo,     'ROCK',   Roshambo.PAPER),  # value trumps name
     (4,  Roshambo,     'STONE',  ValueError),
     (5,  Roshambo,      1,       ValueError),
     ])
def test_flex_enum_cast(idx, enum_class, value, check):
    """Test FlexEnum cast"""
    if is_child_class(check, Exception):
        with pytest.raises(check):
            enum_class.cast(value)
    else:
        assert enum_class.cast(value) is check


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, enum_class,    name,         value,       check',
    [(0,  Fruit,        'APPLE',       1,           Fruit.APPLE),
     (1,  Fruit,        'BANANA',      2,           Fruit.BANANA),
     (2,  Fruit,        'CANTALOUPE',  3,           Fruit.CANTALOUPE),
     (3,  Fruit,        'DRAGONFRUIT', 1,           KeyError),
     (4,  Fruit,        'DRAGONFRUIT', 4,           KeyError),
     (5,  Fruit,        'APPLE',       0,           ValueError),
     (6,  Roshambo,     'ROCK',       'SCISSORS',   Roshambo.ROCK),
     (7,  Roshambo,     'PAPER',      'ROCK',       Roshambo.PAPER),
     (8,  Roshambo,     'SCISSORS',   'PAPER',      Roshambo.SCISSORS),
     (9,  Roshambo,     'STONE',      'SCISSORS',   KeyError),
     (10, Roshambo,     'STONE',      'SCISSORZ',   KeyError),
     (11, Roshambo,     'ROCK',       'SCISSORZ',   ValueError),
     ])
def test_flex_enum_member(idx, enum_class, name, value, check):
    """Confirm FlexEnum member creates enums or raises as expected"""
    if is_child_class(check, Exception):
        with pytest.raises(check):
            enum_class.member(name, value)
    else:
        assert enum_class.member(name, value) is check


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, enumables, transform, enum_names',
    [(Fruit, [], None, ['APPLE', 'BANANA', 'CANTALOUPE']),
     (Fruit, [], str.lower, ['apple', 'banana', 'cantaloupe']),
     (Fruit, [Fruit.CANTALOUPE], None, ['CANTALOUPE']),
     (Fruit, [Fruit.CANTALOUPE, 'BANANA', 1], str.capitalize, ['Cantaloupe', 'Banana', 'Apple']),
     (Roshambo, [Roshambo.ROCK, 'ROCK', 'PAPER'], str.capitalize, ['Rock', 'Paper', 'Scissors']),
     ])
def test_flex_enum_names(enum_class, enumables, transform, enum_names):
    """Test FlexEnum names, list, tuple, and set"""
    names = enum_class.names(*enumables, transform=transform)
    assert is_iterator(names)
    count = 0
    for name, check in zip(names, enum_names):
        assert name == check
        count += 1

    assert count == len(enum_names)

    enum_as_tuple = enum_class.as_tuple(*enumables, names=True, transform=transform)
    assert enum_as_tuple == tuple(enum_names)

    enum_as_frozenset = enum_class.as_frozenset(*enumables, names=True, transform=transform)
    assert enum_as_frozenset == frozenset(enum_names)

    enum_as_list = enum_class.as_list(*enumables, names=True, transform=transform)
    assert enum_as_list == enum_names
    enum_as_list.append('extra')
    enum_as_list2 = enum_class.as_list(*enumables, names=True, transform=transform)
    assert enum_as_list2 == enum_names

    enum_as_set = enum_class.as_set(*enumables, names=True, transform=transform)
    assert enum_as_set == set(enum_names)
    enum_as_set.add('extra')
    enum_as_set2 = enum_class.as_set(*enumables, names=True, transform=transform)
    assert enum_as_set2 == set(enum_names)


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, enumables, transform, enum_values',
    [(Fruit, [], None, [1, 2, 3]),
     (Fruit, [], lambda x: x - 1, [0, 1, 2]),
     (Fruit, [Fruit.CANTALOUPE], None, [3]),
     (Fruit, [Fruit.CANTALOUPE, 'BANANA', 1], lambda x: x ** 2, [9, 4, 1]),
     (Roshambo, [Roshambo.PAPER, 'PAPER'], str.capitalize, ['Rock', 'Paper']),
     ])
def test_flex_enum_values(enum_class, enumables, transform, enum_values):
    """Test FlexEnum values"""
    values = enum_class.values(*enumables, transform=transform)
    assert is_iterator(values)
    count = 0
    for value, check in zip(values, enum_values):
        assert value == check
        count += 1

    assert count == len(enum_values)
    assert enum_class.as_list(*enumables, values=True, transform=transform) == enum_values
    assert enum_class.as_tuple(*enumables, values=True, transform=transform) == tuple(enum_values)
    assert enum_class.as_set(*enumables, values=True, transform=transform) == set(enum_values)
    assert enum_class.as_frozenset(*enumables, values=True, transform=transform) == set(enum_values)


def minus_1(x): return x - 1


def square(x): return x ** 2


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, enum_class, swap, labels, transform, inverse, enumables, enum_items',
    [(0, Fruit, False, False, None, False, [],
        [('APPLE', 1), ('BANANA', 2), ('CANTALOUPE', 3)]),
     (1, Fruit, False, False, str.lower, False, [],
        [('apple', 1), ('banana', 2), ('cantaloupe', 3)]),
     (2, Fruit, False, False, str.lower, True, [],
        [(1, 'apple'), (2, 'banana'), (3, 'cantaloupe')]),
     (3, Fruit, True, False, minus_1, False, [],
        [(0, 'APPLE'), (1, 'BANANA'), (2, 'CANTALOUPE')]),
     (4, Fruit, True, False, minus_1, True, [],
        [('APPLE', 0), ('BANANA', 1), ('CANTALOUPE', 2)]),
     (5, Fruit, False, True, str.lower, False, [],
        [('apple', 'APPLE'), ('banana', 'BANANA'), ('cantaloupe', 'CANTALOUPE')]),
     (6, Fruit, False, True, str.lower, True, [],
        [('APPLE', 'apple'), ('BANANA', 'banana'), ('CANTALOUPE', 'cantaloupe')]),
     (7, Fruit, False, False, str.capitalize, False, [Fruit.CANTALOUPE, 'BANANA', 1],
        [('Cantaloupe', 3), ('Banana', 2), ('Apple', 1)]),
     (8, Fruit, False, True, str.capitalize, False, [Fruit.CANTALOUPE, 'BANANA', 1],
        [('Cantaloupe', 'CANTALOUPE'), ('Banana', 'BANANA'), ('Apple', 'APPLE')]),
     (9, Fruit, False, True, str.capitalize, True, [Fruit.CANTALOUPE, 'BANANA', 1],
        [('CANTALOUPE', 'Cantaloupe'), ('BANANA', 'Banana'), ('APPLE', 'Apple')]),
     (10, Fruit, True, False, square, False, [Fruit.CANTALOUPE, 'BANANA', 1],
        [(9, 'CANTALOUPE'), (4, 'BANANA'), (1, 'APPLE')]),
     (11, Fruit, True, True, square, True, [Fruit.CANTALOUPE, 'BANANA', 1],
        [('CANTALOUPE', 9), ('BANANA', 4), ('APPLE', 1)]),
     (12, Roshambo, True, False, str.capitalize, True, [Roshambo.SCISSORS, 'ROCK'],
        [('SCISSORS', 'Paper'), ('PAPER', 'Rock')]),
     ])
def test_flex_enum_items(idx, enum_class, swap, labels, transform, inverse, enumables, enum_items):
    """Test FlexEnum items, map, and labels"""
    items = enum_class.items(
        *enumables, swap=swap, labels=labels, transform=transform, inverse=inverse)
    assert is_iterator(items)
    count = 0
    for item, check in zip(items, enum_items):
        assert item == check
        count += 1

    assert count == len(enum_items)
    assert OrderedDict(enum_items) == enum_class.as_map(
        *enumables, swap=swap, labels=labels, transform=transform, inverse=inverse)
    if labels and not swap:
        assert tuple(enum_items) == enum_class.as_labels(
            *enumables, transform=transform, inverse=inverse)


@pytest.mark.unit
@pytest.mark.parametrize(
    'enum_class, enumables, transform',
    [(Fruit, [], None),
     (Fruit, [], str.lower),
     (Fruit, [Fruit.CANTALOUPE], None),
     (Fruit, [Fruit.CANTALOUPE, 'BANANA', 1], str.capitalize),
     (Roshambo, [Roshambo.ROCK, 'ROCK', 'PAPER'], str.capitalize),
     ])
def test_flex_enum_mutable_container_not_cached(enum_class, enumables, transform):
    """Test ensuring FlexEnum mutable container methods not cached"""
    enum_names = list(enum_class.names(*enumables, transform=transform))

    enum_as_list = enum_class.as_list(*enumables, names=True, transform=transform)
    assert enum_as_list == enum_names
    enum_as_list.append('extra')
    enum_as_list2 = enum_class.as_list(*enumables, names=True, transform=transform)
    assert enum_as_list2 == enum_names

    enum_as_set = enum_class.as_set(*enumables, names=True, transform=transform)
    assert enum_as_set == set(enum_names)
    enum_as_set.add('extra')
    enum_as_set2 = enum_class.as_set(*enumables, names=True, transform=transform)
    assert enum_as_set2 == set(enum_names)

    enum_dict = OrderedDict(enum_class.items(*enumables, transform=transform))
    enum_as_map = enum_class.as_map(*enumables, transform=transform)
    assert enum_as_map == enum_dict
    enum_as_map['extra'] = 'value'
    enum_as_map2 = enum_class.as_map(*enumables, transform=transform)
    assert enum_as_map2 == enum_dict


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
    'idx, enum_class, qualname',
    [(0,  First.Fruit, 'First.Fruit'),
     (1,  First.Roshambo, 'First.Roshambo'),
     (2,  First.Second.Fruit, 'First.Second.Fruit'),
     (3,  First.Second.Roshambo, 'First.Second.Roshambo'),
     (4,  First.Second.Third.Fruit, 'First.Second.Third.Fruit'),
     (5,  First.Second.Third.Roshambo, 'First.Second.Third.Roshambo'),
     ])
def test_flex_enum_qualname(idx, enum_class, qualname):
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
@pytest.mark.parametrize('enum_member', ENUM_OPTIONS)
def test_flex_enum_serialization(enum_member):
    """Confirm FlexEnum serializes and deserializes to self"""
    serialized_member = enum_member.serialize()
    assert isinstance(serialized_member, str)
    enum_class = enum_member.__class__
    assert enum_class.__module__ in serialized_member
    assert enum_class.__qualname__ in serialized_member
    assert enum_member.name in serialized_member
    assert FlexEnum.deserialize(serialized_member) is enum_member
    assert enum_class.deserialize(serialized_member) is enum_member


@pytest.mark.unit
@pytest.mark.parametrize('enum_member', ENUM_OPTIONS)
def test_flex_enum_repr(enum_member):
    """Confirm FlexEnum repr has qualname/name/value & evals to self"""
    member_repr = repr(enum_member)
    assert enum_member.__class__.__qualname__ in member_repr
    assert enum_member.name in member_repr
    assert str(enum_member.value) in member_repr
    assert eval(member_repr) is enum_member


@pytest.mark.unit
@pytest.mark.parametrize('enum_member', ENUM_OPTIONS)
def test_flex_enum_str(enum_member):
    """Confirm FlexEnum str contains qualname and name"""
    member_str = str(enum_member)
    assert enum_member.__class__.__qualname__ in member_str
    assert enum_member.name in member_str
