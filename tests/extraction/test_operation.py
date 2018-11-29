#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from unittest.mock import MagicMock

from extraction.operation import ExtractionOperation
from utils.tools import is_child_class


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

    operation = ExtractionOperation.from_configuration(
        {}, field='test_field', source='test_source', extractor=mock_extractor)

    if is_child_class(check, Exception):
        with pytest.raises(check):
            value = operation._get_by_reference(reference)

    else:
        value = operation._get_by_reference(reference)
        assert value == check
