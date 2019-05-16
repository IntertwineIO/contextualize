#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from collections import OrderedDict

from contextualize.utils.enum import FlexEnum, IncreasingEnum, DecreasingEnum
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


class Color(FlexEnum):
    WHITE = -4
    GRAY = -3
    BLACK = -2
    BROWN = -1
    RED = 1
    ORANGE = 2
    YELLOW = 3
    GREEN = 4
    BLUE = 5
    INDIGO = 6
    VIOLET = 7

    @classmethod
    def grays(cls):
        return frozenset({cls.WHITE, cls.GRAY, cls.BLACK})

    @classmethod
    def browns(cls):
        return frozenset({cls.BROWN})

    @classmethod
    def oranges(cls):
        return frozenset({cls.RED, cls.ORANGE, cls.YELLOW})

    @classmethod
    def greens(cls):
        return frozenset({cls.YELLOW, cls.GREEN, cls.BLUE})

    @classmethod
    def violets(cls):
        return frozenset({cls.BLUE, cls.INDIGO, cls.VIOLET, cls.RED})

    @classmethod
    def rainbow(cls):
        return tuple(color for color in Color if color.value > 0)

    @classmethod
    def in_rainbow(cls, color):
        return color is not None and color.value > 0

    @classmethod
    def mix(cls, color1, color2):
        if color1 is color2:
            return color1
        if color1 in cls.grays() and color2 in cls.grays():
            return cls.GRAY
        if color1 is None or color1 in cls.grays():
            return color2
        if color2 is None or color2 in cls.grays():
            return color1
        if color1 in cls.oranges() and color2 in cls.oranges():
            return cls.ORANGE
        if color1 in cls.greens() and color2 in cls.greens():
            return cls.GREEN
        if color1 in cls.violets() and color2 in cls.violets():
            return cls.VIOLET
        else:
            return cls.BROWN

    @classmethod
    def low(cls, color1, color2):
        if not cls.in_rainbow(color1) and not cls.in_rainbow(color2):
            return None
        if not cls.in_rainbow(color1):
            return color2
        if not cls.in_rainbow(color2):
            return color1
        return color1 if color1.value < color2.value else color2

    @classmethod
    def high(cls, color1, color2):
        if not cls.in_rainbow(color1) and not cls.in_rainbow(color2):
            return None
        if not cls.in_rainbow(color1):
            return color2
        if not cls.in_rainbow(color2):
            return color1
        return color1 if color1.value > color2.value else color2


def longest(x, y):
    len_x, len_y = len(x), len(y)
    if len_x == len_y:  # tie-breaker
        return x if x > y else y
    return x if len_x > len_y else y


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, cls,   func,        names, values, nullable, swallow,     enumables,       check',
    [
     (0,  Color,  Color.mix,  False, False,  False,    None,        Color,           Color.BROWN),
     (1,  Color,  Color.mix,  False, False,  False,    None,        Color.rainbow(), Color.BROWN),
     (2,  Color,  Color.mix,  False, False,  False,    None,        Color.oranges(), Color.ORANGE),
     (3,  Color,  Color.mix,  False, False,  False,    None,        Color.greens(),  Color.GREEN),
     (4,  Color,  Color.mix,  False, False,  False,    None,        Color.violets(), Color.VIOLET),
     (5,  Color,  Color.mix,  False, False,  False,    None,        Color.browns(),  Color.BROWN),
     (6,  Color,  Color.mix,  False, False,  False,    None,        Color.grays(),   Color.GRAY),

     (7,  Color,  Color.low,  False, False,  False,    None,        Color,           Color.RED),
     (8,  Color,  Color.low,  False, False,  False,    None,        Color.rainbow(), Color.RED),
     (9,  Color,  Color.low,  False, False,  False,    None,        Color.oranges(), Color.RED),
     (10, Color,  Color.low,  False, False,  False,    None,        Color.greens(),  Color.YELLOW),
     (11, Color,  Color.low,  False, False,  False,    None,        Color.violets(), Color.RED),
     (12, Color,  Color.low,  False, False,  False,    None,        Color.browns(),  Color.BROWN),
     (13, Color,  Color.low,  False, False,  False,    None,        Color.grays(),   ValueError),
     (14, Color,  Color.low,  False, False,  False,    ValueError,  Color.grays(),   None),
     (15, Color,  Color.low,  False, False,  True,     None,        Color.grays(),   None),

     (16, Color,  Color.high, False, False,  False,    None,        Color,           Color.VIOLET),
     (17, Color,  Color.high, False, False,  False,    None,        Color.rainbow(), Color.VIOLET),
     (18, Color,  Color.high, False, False,  False,    None,        Color.oranges(), Color.YELLOW),
     (19, Color,  Color.high, False, False,  False,    None,        Color.greens(),  Color.BLUE),
     (20, Color,  Color.high, False, False,  False,    None,        Color.violets(), Color.VIOLET),
     (21, Color,  Color.high, False, False,  False,    None,        Color.browns(),  Color.BROWN),
     (22, Color,  Color.high, False, False,  False,    None,        Color.grays(),   ValueError),
     (23, Color,  Color.high, False, False,  False,    ValueError,  Color.grays(),   None),
     (24, Color,  Color.high, False, False,  True,     None,        Color.grays(),   None),

    # idx, cls,   func,       names, values, nullable, swallow,     enumables,       check',
     (25, Color,  longest,    True,  False,  False,    None,        Color,           Color.YELLOW),
     (26, Color,  longest,    True,  False,  False,    None,        Color.rainbow(), Color.YELLOW),
     (27, Color,  longest,    True,  False,  False,    None,        Color.oranges(), Color.YELLOW),
     (28, Color,  longest,    True,  False,  False,    None,        Color.greens(),  Color.YELLOW),
     (29, Color,  longest,    True,  False,  False,    None,        Color.violets(), Color.VIOLET),
     (30, Color,  longest,    True,  False,  False,    None,        Color.browns(),  Color.BROWN),
     (31, Color,  longest,    True,  False,  False,    None,        Color.grays(),   Color.WHITE),

     (32, Color,  min,        True,  False,  False,    None,        Color,           Color.BLACK),
     (33, Color,  min,        True,  False,  False,    None,        Color.rainbow(), Color.BLUE),
     (34, Color,  min,        True,  False,  False,    None,        Color.oranges(), Color.ORANGE),
     (35, Color,  min,        True,  False,  False,    None,        Color.greens(),  Color.BLUE),
     (36, Color,  min,        True,  False,  False,    None,        Color.violets(), Color.BLUE),
     (37, Color,  min,        True,  False,  False,    None,        Color.browns(),  Color.BROWN),
     (38, Color,  min,        True,  False,  False,    None,        Color.grays(),   Color.BLACK),

     (39, Color,  max,        True,  False,  False,    None,        Color,           Color.YELLOW),
     (40, Color,  max,        True,  False,  False,    None,        Color.rainbow(), Color.YELLOW),
     (41, Color,  max,        True,  False,  False,    None,        Color.oranges(), Color.YELLOW),
     (42, Color,  max,        True,  False,  False,    None,        Color.greens(),  Color.YELLOW),
     (43, Color,  max,        True,  False,  False,    None,        Color.violets(), Color.VIOLET),
     (44, Color,  max,        True,  False,  False,    None,        Color.browns(),  Color.BROWN),
     (45, Color,  max,        True,  False,  False,    None,        Color.grays(),   Color.WHITE),

    # idx, cls,   func,       names, values, nullable, swallow,     enumables,       check',
     (46, Color,  min,        False, True,   False,    None,        Color,           Color.WHITE),
     (47, Color,  min,        False, True,   False,    None,        Color.rainbow(), Color.RED),
     (48, Color,  min,        False, True,   False,    None,        Color.oranges(), Color.RED),
     (49, Color,  min,        False, True,   False,    None,        Color.greens(),  Color.YELLOW),
     (50, Color,  min,        False, True,   False,    None,        Color.violets(), Color.RED),
     (51, Color,  min,        False, True,   False,    None,        Color.browns(),  Color.BROWN),
     (52, Color,  min,        False, True,   False,    None,        Color.grays(),   Color.WHITE),

     (53, Color,  max,        False, True,   False,    None,        Color,           Color.VIOLET),
     (54, Color,  max,        False, True,   False,    None,        Color.rainbow(), Color.VIOLET),
     (55, Color,  max,        False, True,   False,    None,        Color.oranges(), Color.YELLOW),
     (56, Color,  max,        False, True,   False,    None,        Color.greens(),  Color.BLUE),
     (57, Color,  max,        False, True,   False,    None,        Color.violets(), Color.VIOLET),
     (58, Color,  max,        False, True,   False,    None,        Color.browns(),  Color.BROWN),
     (59, Color,  max,        False, True,   False,    None,        Color.grays(),   Color.BLACK),

     (60, Color,  max,        True,  True,   False,    None,        Color,           ValueError),
     ])
def test_flex_enum_consolidate(
        idx, cls, func, names, values, nullable, swallow, enumables, check):
    """Test FlexEnum consolidate"""
    kwargs = {}
    if names:
        kwargs['names'] = True
    if values:
        kwargs['values'] = True
    if nullable:
        kwargs['nullable'] = True
    if swallow:
        kwargs['swallow'] = swallow

    if is_child_class(check, Exception):
        with pytest.raises(check):
            cls.consolidate(func, *enumables, **kwargs)
    else:
        assert cls.consolidate(func, *enumables, **kwargs) is check


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


class Grade(IncreasingEnum):
    A = 4
    B = 3
    C = 2
    D = 1
    E = 0
    F = 0


class Place(DecreasingEnum):
    FIRST = 1
    SECOND = 2
    THIRD = 3
    FOURTH = 4
    FIFTH = 5
    LAST = 5


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, cls,   nullable, swallow,     enumables,          check',
    [
     (0,  Grade, False,    None,        Grade,              (Grade.F, Grade.A)),
     (1,  Grade, False,    None,        ['B', 'A', 'C'],    (Grade.C, Grade.A)),
     (2,  Grade, False,    None,        ['C', 'C', 'C'],    (Grade.C, Grade.C)),
     (3,  Grade, False,    None,        [None, 'A', 'F'],   ValueError),
     (4,  Grade, False,    ValueError,  [None, 'A', 'F'],   (None, None)),
     (5,  Grade, True,     None,        [None, 'A', 'F'],   (Grade.F, Grade.A)),
     (6,  Grade, False,    None,        [None],             ValueError),
     (7,  Grade, False,    ValueError,  [None],             (None, None)),
     (8,  Grade, True,     None,        [None],             (None, None)),
     (9,  Grade, False,    None,        [],                 TypeError),
     (10, Grade, False,    TypeError,   [],                 (None, None)),
     (11, Grade, True,     None,        [],                 (None, None)),

     (12, Place, False,    None,        Place,              (Place.FIFTH, Place.FIRST)),
     (13, Place, False,    None,        [2, 'THIRD', 5],    (Place.FIFTH, Place.SECOND)),
     (14, Place, False,    None,        [1, 1, 1, 1, 1],    (Place.FIRST, Place.FIRST)),
     (15, Place, False,    None,        [None, 1, 3],       ValueError),
     (16, Place, False,    ValueError,  [None, 1, 3],       (None, None)),
     (17, Place, True,     None,        [None, 1, 3],       (Place.THIRD, Place.FIRST)),
     (18, Place, False,    None,        [None],             ValueError),
     (19, Place, False,    ValueError,  [None],             (None, None)),
     (20, Place, True,     None,        [None],             (None, None)),
     (21, Place, False,    None,        [],                 TypeError),
     (22, Place, False,    TypeError,   [],                 (None, None)),
     (23, Place, True,     None,        [],                 (None, None)),
     ])
def test_monotonic_enum_min_max(idx, cls, nullable, swallow, enumables, check):
    """Test minimum/maximum for IncreasingEnum and DecreasingEnum"""
    kwargs = {}
    if nullable:
        kwargs['nullable'] = True
    if swallow:
        kwargs['swallow'] = swallow

    if is_child_class(check, Exception):
        with pytest.raises(check):
            cls.minimum(*enumables, **kwargs)
        with pytest.raises(check):
            cls.maximum(*enumables, **kwargs)
    else:
        min_check, max_check = check
        assert cls.minimum(*enumables, **kwargs) is min_check
        assert cls.maximum(*enumables, **kwargs) is max_check


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, lesser,           greater,            error',
    [
     (0,  Grade.B,          Grade.A,            None),
     (1,  Grade.C,          Grade.A,            None),
     (2,  Grade.D,          Grade.A,            None),
     (3,  Grade.F,          Grade.A,            None),
     (4,  Grade.C,          Grade.B,            None),
     (5,  Grade.D,          Grade.B,            None),
     (6,  Grade.F,          Grade.B,            None),
     (7,  Grade.D,          Grade.C,            None),
     (8,  Grade.F,          Grade.C,            None),
     (9,  Grade.F,          Grade.D,            None),

     (10, Place.SECOND,     Place.FIRST,        None),
     (11, Place.THIRD,      Place.FIRST,        None),
     (12, Place.FOURTH,     Place.FIRST,        None),
     (13, Place.FIFTH,      Place.FIRST,        None),
     (14, Place.THIRD,      Place.SECOND,       None),
     (15, Place.FOURTH,     Place.SECOND,       None),
     (16, Place.FIFTH,      Place.SECOND,       None),
     (17, Place.FOURTH,     Place.THIRD,        None),
     (18, Place.FIFTH,      Place.THIRD,        None),
     (19, Place.FIFTH,      Place.FOURTH,       None),

     (20, Place.FIRST,      Grade.A,            TypeError),
     (21, Grade.A,          Place.FIRST,        TypeError),
     ])
def test_monotonic_enum_inequality(idx, lesser, greater, error):
    """Test inequality for IncreasingEnum and DecreasingEnum"""
    if error:
        with pytest.raises(error):
            lesser < greater
        with pytest.raises(error):
            lesser <= greater
        with pytest.raises(error):
            greater > lesser
        with pytest.raises(error):
            greater >= lesser
        assert lesser != greater
        assert greater != lesser
        assert not lesser == greater
        assert not greater == lesser
    else:
        assert lesser < greater
        assert lesser <= greater
        assert greater > lesser
        assert greater >= lesser
        assert lesser != greater
        assert not lesser > greater
        assert not lesser >= greater
        assert not greater < lesser
        assert not greater <= lesser
        assert not lesser == greater


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, cls',
    [
     (0,  Grade),
     (1,  Place),
     ])
def test_monotonic_enum_equality(idx, cls):
    """Test equality for IncreasingEnum and DecreasingEnum"""
    for member in cls:
        assert member == member
        assert member >= member
        assert member <= member
        assert not member != member
        assert not member > member
        assert not member < member

    if cls is Grade:
        assert Grade.E == Grade.F
        assert Grade.F == Grade.E
        assert Grade.E >= Grade.F
        assert Grade.F >= Grade.E
        assert Grade.E <= Grade.F
        assert Grade.F <= Grade.E
        assert not Grade.E != Grade.F
        assert not Grade.F != Grade.E
        assert not Grade.E > Grade.F
        assert not Grade.F > Grade.E
        assert not Grade.E < Grade.F
        assert not Grade.F < Grade.E

    elif cls is Place:
        assert Place.FIFTH == Place.LAST
        assert Place.LAST == Place.FIFTH
        assert Place.FIFTH >= Place.LAST
        assert Place.LAST >= Place.FIFTH
        assert Place.FIFTH <= Place.LAST
        assert Place.LAST <= Place.FIFTH
        assert not Place.FIFTH != Place.LAST
        assert not Place.LAST != Place.FIFTH
        assert not Place.FIFTH > Place.LAST
        assert not Place.LAST > Place.FIFTH
        assert not Place.FIFTH < Place.LAST
        assert not Place.LAST < Place.FIFTH
