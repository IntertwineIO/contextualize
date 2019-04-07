#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
from enum import Enum

import pytest

from contextualize.utils.context import FlexContext
from contextualize.utils.tools import is_child_class


def validate_flex_context(delta_a, delta_b):
    check_ab = {**delta_a, **delta_b}
    check_ba = {**delta_b, **delta_a}

    context0 = FlexContext.get_context()
    assert context0 == {}

    with FlexContext(**delta_a) as flex_context_a:
        context1 = FlexContext.get_context()
        validate_context(flex_context_a, delta_a, baseline={}, context=delta_a, **check_ba)

        with FlexContext(**delta_b) as flex_context_b:
            context2 = FlexContext.get_context()
            validate_context(flex_context_a, delta_a, baseline={}, context=delta_a, **check_ba)
            validate_context(flex_context_b, delta_b, baseline=delta_a, context=check_ab,
                             **check_ab)

            context2.update(z=26)
            context2z = FlexContext.get_context()
            assert context2z == check_ab, 'get_context should return a copy'

        context3 = FlexContext.get_context()
        assert context3 is not context1
        assert flex_context_a.context == context3 == context1 == delta_a
        validate_context(flex_context_a, delta_a, baseline={}, context=delta_a, **check_ba)
        validate_context(flex_context_b, delta_b, baseline=None, context=None, **check_ab)

    context4 = FlexContext.get_context()
    assert context4 == {}
    validate_context(flex_context_a, delta_a, baseline=None, context=None, **check_ba)
    validate_context(flex_context_b, delta_b, baseline=None, context=None, **check_ab)

    with flex_context_b:
        context5 = FlexContext.get_context()
        assert flex_context_b.context == context5 == delta_b
        validate_context(flex_context_a, delta_a, baseline=None, context=None, **check_ba)
        validate_context(flex_context_b, delta_b, baseline={}, context=delta_b, **check_ab)

        with flex_context_a:
            context6 = FlexContext.get_context()
            assert flex_context_a.context == context6 == check_ba
            validate_context(flex_context_a, delta_a, baseline=delta_b, context=check_ba,
                             **check_ba)
            validate_context(flex_context_b, delta_b, baseline={}, context=delta_b, **check_ab)

        context7 = FlexContext.get_context()
        assert context7 is not context5
        assert flex_context_b.context == context7 == context5 == delta_b
        validate_context(flex_context_a, delta_a, baseline=None, context=None, **check_ba)
        validate_context(flex_context_b, delta_b, baseline={}, context=delta_b, **check_ab)

    context8 = FlexContext.get_context()
    assert context8 == {}
    assert context8 is not context4 is not context0 is not context8
    validate_context(flex_context_a, delta_a, baseline=None, context=None, **check_ba)
    validate_context(flex_context_b, delta_b, baseline=None, context=None, **check_ab)


def validate_context(flex_context, delta, baseline, context, **check_kwargs):
    assert flex_context.delta == delta
    assert flex_context.baseline == baseline
    assert flex_context.context == context

    with pytest.raises(AttributeError):
        getattr(flex_context, 'not_a_var')

    if context is not None:
        assert flex_context.baseline is not None
        assert flex_context.context is not None
        validate_attributes(flex_context, attributes=context, **check_kwargs)
    else:
        assert flex_context.baseline is None
        assert flex_context.context is None
        validate_attributes(flex_context, attributes=delta, **check_kwargs)


def validate_attributes(flex_context, attributes, **check_kwargs):
    for name, check in check_kwargs.items():
        if name in attributes:
            assert name in flex_context
            assert getattr(flex_context, name) == check
        else:
            assert name not in flex_context
            with pytest.raises(AttributeError):
                getattr(flex_context, name)


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'delta_a',         'delta_b',                 'error'),
    [
     (0,    {},                 {},                          None),
     (1,    {},                 {'b': 2},                    None),
     (2,    {'a': 1},           {},                          None),
     (3,    {'a': 1},           {'b': 2},                    None),
     (4,    {'a': 1},           {'a': 42},                   None),
     (5,    {'a': 1},           {'a': 42, 'b': 2},           None),
     (6,    {'a': 1, 'b': 2},   {'c': 3},                    None),
     (7,    {'a': 1, 'b': 2},   {'a': 0, 'b': 42},           None),
     (8,    {'a': 1, 'b': 2},   {'b': 42},                   None),
     (9,    {'a': 1, 'b': 2},   {'b': 42, 'c': 3},           None),
     (10,   {'a': 1, 'b': 2},   {'a': 0, 'b': 42, 'c': 3},   None),
     (11,   {'a': 1, 'b': 2},   {'a': 1, 'b': 2, 'c': 3},    None),
     ])
def test_sync_flex_context(idx, delta_a, delta_b, error):
    if error is None:
        validate_flex_context(delta_a, delta_b)
    else:
        with pytest.raises(error):
            validate_flex_context(delta_a, delta_b)


Color = Enum('Color', 'RED ORANGE YELLOW GREEN BLUE INDIGO VIOLET')


async def get_context():
    return FlexContext.get_context()


async def apply_context(**delta):
    with FlexContext(**delta) as flex_context:
        await asyncio.sleep(0.05)
        context = await get_context()
        await asyncio.sleep(0.05)
        return context


async def rainbow():
    context_futures = [apply_context(color=color) for color in Color]
    return await asyncio.gather(*context_futures)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_flex_context():
    start = await get_context()
    assert not start

    contexts = await rainbow()
    for context, color in zip(contexts, Color):
        assert context['color'] is color

    finish = await get_context()
    assert not finish


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_flex_context_within_a_context():
    start = await get_context()
    assert not start

    with FlexContext(color=Color.RED, number=42) as flex_context:
        before = await get_context()
        assert before['color'] is Color.RED
        assert before['number'] == 42
        contexts = await rainbow()
        after = await get_context()
        assert after['color'] is Color.RED
        assert after['number'] == 42

    finish = await get_context()
    assert not finish

    for context, color in zip(contexts, Color):
        assert context['color'] is color
        assert context['number'] == 42
