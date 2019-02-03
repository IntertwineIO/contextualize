#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import inspect
import pytest
from asyncio import Future
from unittest.mock import Mock, patch

from contextualize.exceptions import TooManyValuesError
from contextualize.extraction.operation import ExtractionOperation as EO
from contextualize.utils.tools import is_child_class
from testing.builders.extraction_operation_builder import ExtractionOperationBuilder

METHOD_TYPES = {method_type for name, method_type in inspect.getmembers(EO)
                if name.endswith('Method')}


@pytest.mark.asyncio
async def test_example(event_loop):
    """An async test!"""
    await asyncio.sleep(0, loop=event_loop)


def side_effect_execute_in_future(func, *args, **kwds):
    future = Future()
    future.set_result(func(*args, **kwds))
    return future


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, method,                           arguments,          values',
    [(0,  EO.ExtractionMethod.GETATTR,      ['text'],           ['value1']),
     (1,  EO.ExtractionMethod.GETATTR,      ['text'],           ['value1', 'value2']),
     (2,  EO.ExtractionMethod.ATTRIBUTE,    ['href'],           ['value1']),
     (3,  EO.ExtractionMethod.ATTRIBUTE,    ['href'],           ['value1', 'value2']),
     (4,  EO.ExtractionMethod.PROPERTY,     ['content'],        ['value1']),
     (5,  EO.ExtractionMethod.PROPERTY,     ['content'],        ['value1', 'value2']),
     ])
@pytest.mark.asyncio
async def test_extract_values(idx, method, arguments, values):
    """Test extract values"""
    elements = [Mock() for _ in values]

    for mock_element, value in zip(elements, values):

        if method is EO.ExtractionMethod.GETATTR:
            setattr(mock_element, arguments[0], value)

        elif method is EO.ExtractionMethod.ATTRIBUTE:
            def side_effect_get_attribute(name, value=value):  # hack due to closure late-binding
                return value if name == arguments[0] else 'something else'
            mock_element.get_attribute = Mock(side_effect=side_effect_get_attribute)

        elif method is EO.ExtractionMethod.PROPERTY:
            def side_effect_get_property(name, value=value):  # hack due to closure late-binding
                return value if name == arguments[0] else 'something else'
            mock_element.get_property = Mock(side_effect=side_effect_get_property)

        else:
            raise ValueError('Unknown extraction method')

    builder = ExtractionOperationBuilder(extract_method=method, extract_args=arguments)
    operation = builder.build()

    with patch(f'{EO.__module__}.ExtractionOperation._execute_in_future') as mock_execute:
        mock_execute.side_effect = side_effect_execute_in_future
        extracted_values = await operation._extract_values(elements)
        assert extracted_values == values


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, method,               arguments,                          values',
    [(0,  EO.GetMethod.GET,     ['<alpha>'],                        ['dog']),
     (1,  EO.GetMethod.GET,     ['<alpha>', '<beta.max>'],          ['dog', 1975]),
     ])
@pytest.mark.asyncio
async def test_get_values(idx, method, arguments, values):
    """Test get values"""
    builder = ExtractionOperationBuilder(get_method=method, get_args=arguments)
    operation = builder.build()

    extracted_values = await operation._get_values('this is ignored'.split())
    assert extracted_values == values


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, reference,                 check',
    [(0, 'alpha',                   'dog'),
     (1, 'beta.max',                 1975),
     (2, 'gamma.ray.burst',         '110328A'),
     (3, 'delta.delta.delta',       'ΔΔΔ'),
     (4, 'aleph',                    KeyError),
     (5, 'beta.carotene',            AttributeError),
     ])
def test_get_by_reference(idx, reference, check):
    """Test _get_by_reference and _get_by_reference_tag)"""
    builder = ExtractionOperationBuilder()
    operation = builder.build()

    reference_tag = EO.REFERENCE_TEMPLATE.format(reference)

    if is_child_class(check, Exception):
        with pytest.raises(check):
            operation._get_by_reference(reference)

        with pytest.raises(check):
            operation._get_by_reference_tag(reference_tag)

    else:
        value_by_reference = operation._get_by_reference(reference)
        assert value_by_reference == check

        value_by_reference_tag = operation._get_by_reference_tag(reference_tag)
        assert value_by_reference_tag == check


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, template,                                  check',
    [(0, '<alpha>',                                 'dog'),
     (1, 'hot <alpha>',                             'hot dog'),
     (2, '<alpha> house',                           'dog house'),
     (3, 'Hot<alpha>haus',                          'Hotdoghaus'),
     (4, '<beta.max> hot <alpha>s',                 '1975 hot dogs'),
     (5, 'hot <alpha> cooked by <gamma.ray.burst>', 'hot dog cooked by 110328A'),
     (6, '<delta.delta.delta> circa <beta.max>',    'ΔΔΔ circa 1975'),
     ])
def test_render_references(idx, template, check):
    """Test ExtractionOperation._render_references"""
    builder = ExtractionOperationBuilder()
    operation = builder.build()

    if is_child_class(check, Exception):
        with pytest.raises(check):
            rendered = operation._render_references(template)

    else:
        rendered = operation._render_references(template)
        assert rendered == check


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx,  latest,     prior,      parent,     driver,      scope,              check',
    [(0,  'latest',   'prior',    'parent',    'driver',    EO.Scope.LATEST,    ['latest']),
     (1,  'latest',   'prior',    'parent',    'driver',    EO.Scope.PRIOR,     ['prior']),
     (2,  'latest',   'prior',    'parent',    'driver',    EO.Scope.PARENT,    ['parent']),
     (2,  'latest',   'prior',    'parent',    'driver',    EO.Scope.PAGE,      ['driver']),
     (3,  [1, 2, 3],  [4, 5, 6],   7,           8,          EO.Scope.LATEST,    [1, 2, 3]),
     (4,  [1, 2, 3],  [4, 5, 6],   7,           8,          EO.Scope.PRIOR,     [4, 5, 6]),
     (5,  [1, 2, 3],  [4, 5, 6],   7,           8,          EO.Scope.PARENT,    [7]),
     (5,  [1, 2, 3],  [4, 5, 6],   7,           8,          EO.Scope.PAGE,      [8]),
     ])
def test_select_targets(idx, latest, prior, parent, driver, scope, check):
    """Test ExtractionOperation._select_targets"""
    builder = ExtractionOperationBuilder()
    operation = builder.build()
    operation.web_driver = driver
    operation.scope = scope

    targets = operation._select_targets(latest, prior, parent)
    assert targets == check


@pytest.mark.unit
def test_configure_scope():
    for scope in EO.Scope:
        configuration = {EO.SCOPE_TAG: scope.name.lower()}
        configured = EO._configure_scope(configuration)
        assert configured is scope

    configuration = {'not_scope': 'foo'}
    configured = EO._configure_scope(configuration)
    assert configured is EO.SCOPE_DEFAULT

    with pytest.raises(KeyError):
        configuration = {EO.SCOPE_TAG: 'foo'}
        configured = EO._configure_scope(configuration)


CLASS_NAME = EO.FindMethod.CLASS_NAME
XPATH = EO.FindMethod.XPATH


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, configuration, method_enum, check',
    [
     (0, {'class_name': 'a-1'}, EO.FindMethod, (CLASS_NAME, ['a-1'])),
     (1, {'x': 'x-1', 'class_name': 'a-1'}, EO.FindMethod, (CLASS_NAME, ['a-1'])),
     (2, {'class_name': 'a-1', 'y': 'y-1'}, EO.FindMethod, (CLASS_NAME, ['a-1'])),
     (3, {'x': 'x-1', 'class_name': 'a-1', 'y': 'y-1'}, EO.FindMethod, (CLASS_NAME, ['a-1'])),
     (4, {'class_name': ['a-1', 'a-2']}, EO.FindMethod, (CLASS_NAME, ['a-1', 'a-2'])),
     (5, {'class_name': None}, EO.FindMethod, (CLASS_NAME, [])),
     (6, {'no_method_name': 'a-1'}, EO.FindMethod, (None, None)),
     (7, {'no_method_name': ['a-1', 'a-2']}, EO.FindMethod, (None, None)),
     (8, {'no_method_name': None}, EO.FindMethod, (None, None)),
     (9, {'xpath': '//div'}, EO.FindMethod, (XPATH, ['//div'])),
     (10, {'class_name': 'a-1', 'xpath': '//div'}, EO.FindMethod, TooManyValuesError),
     (11, {'no_method_name': None}, 'not_an_enum', AttributeError),
     ])
def test_configure_method(idx, configuration, method_enum, check):
    if is_child_class(check, Exception):
        with pytest.raises(check):
            EO._configure_method(configuration, method_enum)
        return
    method_type, method_args = EO._configure_method(configuration, method_enum)
    assert method_type is check[0], 'Unexpected method type'
    assert method_args == check[1], 'Unexpected method args'


@pytest.mark.unit
def test_names_are_unique_across_method_types():
    """Confirm names are unique across method types (non-case-sensitive)"""
    all_method_names = set()
    for method_type in METHOD_TYPES:
        method_names = set(method_type.names(transform=str.lower))
        names_in_common = all_method_names & method_names
        assert (not names_in_common), 'Duplicate method name(s)'
        all_method_names |= method_names
