#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import pytest
from unittest.mock import MagicMock

from exceptions import TooManyValuesError
from extraction.operation import ExtractionOperation as EO
from utils.tools import is_child_class

METHOD_TYPES = {method_type for name, method_type in inspect.getmembers(EO)
                if name.endswith('Method')}


class ReferencePath:

    def __init__(self, path, value):
        path_component_names = path.split('.')
        path_length = len(path_component_names)
        if not path_length:
            raise ValueError(f'Invalid path: {path}')
        self.name = path_component_names[0]
        if path_length == 1:
            setattr(self, self.name, value)
        else:
            sub_path = '.'.join(path_component_names[1:])
            cls = self.__class__
            sub_component = cls(sub_path, value)
            setattr(self, self.name, sub_component)

    def __repr__(self):
        path_component_names = []
        value = self
        while isinstance(value, type(self)):
            name = value.name
            path_component_names.append(name)
            value = getattr(value, name)

        path = '.'.join(path_component_names)
        return f"ReferencePath('{path}', {value})"


content_map = {
    'alpha': 'dog',
    'beta': ReferencePath('max', 1975),
    'gamma': ReferencePath('ray.burst', '110328A'),
    'delta': ReferencePath('delta.delta', 'ΔΔΔ')
}


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, reference, check',
    [(0, 'alpha', 'dog'),
     (1, 'beta.max', 1975),
     (2, 'gamma.ray.burst', '110328A'),
     (3, 'delta.delta.delta', 'ΔΔΔ'),
     (4, 'aleph', KeyError),
     (5, 'beta.carotene', AttributeError),
     ])
def test_get_by_reference(idx, reference, check):
    """Test ExtractionOperation._get_by_reference"""
    mock_extractor = MagicMock()
    mock_extractor.content_map = content_map

    operation = EO.from_configuration(
        {}, field='test_field', source='test_source', extractor=mock_extractor)

    if is_child_class(check, Exception):
        with pytest.raises(check):
            value = operation._get_by_reference(reference)

    else:
        value = operation._get_by_reference(reference)
        assert value == check


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
def test_method_types_have_unique_names():
    """Confirm method types have unique names (non-case-sensitive)"""
    all_method_names = set()
    for method_type in METHOD_TYPES:
        method_names = set(method_type.names(transform=str.lower))
        names_in_common = all_method_names & method_names
        assert (not names_in_common), 'Duplicate method name(s)'
        all_method_names |= method_names
