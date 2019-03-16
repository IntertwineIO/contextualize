#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict

import pytest

from contextualize.extraction.url import URLConstructor
from contextualize.utils.tools import is_child_class


URL_CONFIGURATION_A = {
    'url_template': 'https://academic.oup.com/journals/search-results?page=1&qb={{query}}',
    'query': {
        'series': 'TOPIC',
        'templates': ['{primary_clause}', '{qualifier_clause}'],
        'delimiter': ','
    },
    'primary_clause': '%22Keywords{index}%22:%22{term}%22',
    'qualifier_clause': '%22FullText{index}%22:%22{term}%22',
}

URL_CONFIGURATION_B = {
    'url_template': 'https://www.ncbi.nlm.nih.gov/pmc/?term={query}',
    'query': {
        'series': 'TOPIC',
        'templates': ['({topic})'],
        'delimiter': '+AND+',
    },
    'topic': {
        'series': 'TERM',
        'templates': ['{term}%5BAbstract%5D'],
        'delimiter': '+OR+',
    },
}


@pytest.mark.unit
@pytest.mark.parametrize(
    'idx, configuration, search_data, check', [
    (
        0,  # URL construction involving index and TOPIC URL clause
        URL_CONFIGURATION_A,
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        'https://academic.oup.com/journals/search-results?page=1&qb='
        '{%22Keywords1%22:%22Homelessness%22,%22FullText2%22:%22Texas%22}'
     ),
    (
        1,  # URL construction involving TOPIC and TERM URL clauses
        URL_CONFIGURATION_B,
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        'https://www.ncbi.nlm.nih.gov/pmc/?term='
        '(Homelessness%5BAbstract%5D)+AND+(Texas%5BAbstract%5D+OR+TX%5BAbstract%5D)'
     ),
    (
        2,  # Hardcoded URL
        'http://www.example.com',
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        'http://www.example.com'
     ),
    (
        3,  # Empty configuration dictionary
        {},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        TypeError
     ),
    (
        4,  # Missing URL_TEMPLATE_TAG key
        {k: v for k, v in URL_CONFIGURATION_A.items() if k != URLConstructor.URL_TEMPLATE_TAG},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        TypeError
     ),
    (
        5,  # Missing 'query' key
        {k: v for k, v in URL_CONFIGURATION_A.items() if k != 'query'},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        ValueError
     ),
    (
        6,  # Missing 'primary_clause' key
        {k: v for k, v in URL_CONFIGURATION_A.items() if k != 'primary_clause'},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        ValueError
     ),
    (
        7,  # Missing 'qualifier_clause' key
        {k: v for k, v in URL_CONFIGURATION_A.items() if k != 'qualifier_clause'},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        ValueError
     ),
    (
        8,  # Missing 'series' key from 'query' URL clause
        {k: v if k != 'query' else {k2: v2 for k2, v2 in v.items() if k2 != 'series'}
         for k, v in URL_CONFIGURATION_A.items()},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        KeyError
     ),
    (
        9,  # Missing 'templates' key from 'query' URL clause
        {k: v if k != 'query' else {k2: v2 for k2, v2 in v.items() if k2 != 'templates'}
         for k, v in URL_CONFIGURATION_A.items()},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        KeyError
     ),
    (
        10,  # Missing 'delimiter' key from 'query' URL clause
        {k: v if k != 'query' else {k2: v2 for k2, v2 in v.items() if k2 != 'delimiter'}
         for k, v in URL_CONFIGURATION_A.items()},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        KeyError
     ),
    (
        11,  # 'TOPIC' misspelled as 'TROPIC' in 'query' URL clause
        {k: v if k != 'query' else {k2: v2 if k2 != 'series' else 'TROPIC' for k2, v2 in v.items()}
         for k, v in URL_CONFIGURATION_A.items()},
        OrderedDict([('problem', 'Homelessness'), ('org', None), ('geo', ['Texas', 'TX'])]),
        KeyError
     ),
])
def test_url_constructor(idx, configuration, search_data, check):
    """Test URLConstructor"""
    if is_child_class(check, Exception):
        with pytest.raises(check):
            url_constructor = URLConstructor.from_dict(configuration)
            url = url_constructor.construct(search_data)

    else:
        url_constructor = URLConstructor.from_dict(configuration)
        url = url_constructor.construct(search_data)
        assert url == check
