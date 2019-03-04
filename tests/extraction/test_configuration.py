#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict
from unittest.mock import Mock

import pytest

from contextualize.extraction.configuration import (
    ExtractorConfiguration, MultiExtractorConfiguration, SourceExtractorConfiguration
)
from contextualize.utils.tools import is_child_class


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, file_path, check', [
    (0, 'contextualize/providers/academic_oup_com/multi.yaml', MultiExtractorConfiguration),
    (1, 'contextualize/providers/ncbi_nlm_nih_gov/multi.yaml', MultiExtractorConfiguration),
    (2, 'contextualize/providers/ncbi_nlm_nih_gov/source.yaml', SourceExtractorConfiguration),
    (3, 'contextualize/services/secret_service/providers/developers_whatismybrowser_com/multi.yaml',
        MultiExtractorConfiguration),
    (4, 'contextualize/providers/academic_oup_com/multiple.yaml', ValueError),
    (5, 'contextualize/providers/invalid/path/multi.yaml', FileNotFoundError),
])
def test_extractor_configuration(idx, file_path, check):
    """Test Extractor Configuration"""
    if is_child_class(check, Exception):
        with pytest.raises(check):
            ExtractorConfiguration.from_file(file_path=file_path, source='source', extractor=Mock())

    else:
        extractor_configuration = ExtractorConfiguration.from_file(
            file_path=file_path, source='source', extractor=Mock())
        assert isinstance(extractor_configuration, check)
