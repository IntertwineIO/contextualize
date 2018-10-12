#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import pytest
from enum import Enum

from content import ResearchArticle
from utils.mixins import Extractable, Hashable


HASHED_CONTENT = {
    '__model__': 'content.ResearchArticle',
    'author_names': 'Erin Roark Murphy; Brittany H Eghaneyan',
    'doi': 'https://doi.org/10.1093/bjsw/bcx163',
    'granularity_published': 'utils.time.Granularity.DAY',
    'publication': 'The British Journal of Social Work',
    'published_timestamp': '2018-02-22T00:00:00',
    'publisher': 'Oxford University Press',
    'source_url': 'https://academic.oup.com/bjsw/advance-article/doi/10.1093/bjsw/bcx163/4902995',
    'summary':
        'Research demonstrates that homelessness among older adults will significantly increase in '
        'the coming decades due to population ageing, a trend of first-time homelessness at mid-'
        'life and continued economic vulnerability into old age without appropriate prevention and '
        'policy response. The objective of this study was to generate a rich description of '
        'homelessness as told by older adults using a qualitative interpretive meta-synthesis '
        '(QIMS). This approach is appropriate to synthesise multiple qualitative studies into a '
        'more holistic, broader understanding of the phenomenon (Aguirre and Bolton, 2014). An '
        'exhaustive search yielded 144 potentially relevant studies. Eight of these studies met '
        'the inclusion criteria for further analyses. A priori inclusion criteria included '
        'studies: (i) published in peer-reviewed journals or dissertations; (ii) published in '
        'English prior to January 2017; (iii) sampling older adults experiencing homelessness in '
        'the USA or Canada; (iv) conducted using qualitative or mixed-method designs, and (v) '
        'including the voices of participants through direct quotes. Synthesis of the eight '
        'studies resulted in two subthemes that describe older adult homelessness: systemic '
        'failings and coping mechanisms and survival behaviours. Micro- and macro-level practice '
        'and policy recommendations are addressed.',
    'title':
        'Understanding the Phenomenon of Older Adult Homelessness in North America: A Qualitative '
        'Interpretive Meta-Synthesis',
    'tzinfo_published': 'UTC',
}


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'model_class',     'hashed_content'),
    [(0,     ResearchArticle,   HASHED_CONTENT),
     ])
def test_field_mixin(idx, model_class, hashed_content):
    """Test field mixin core methods"""
    from utils.time import DateTimeWrapper, Granularity  # noqa: F401

    hashed_content = hashed_content.copy()

    content = model_class.from_hash(hashed_content)
    del hashed_content['__model__']

    content_fields = {f for f in content.fields() if getattr(content, f) is not None}
    assert content_fields == set(hashed_content.keys())

    for item, field, value in zip(content.items(), content.fields(), content.values()):
        assert item == (field, value)

    content2 = eval(repr(content))
    assert content2 == content


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'model_class',     'hashed_content', 'to_encoding', 'from_encoding', 'exception'),
    [(0,     ResearchArticle,   HASHED_CONTENT,   None,          None,            None),
     (1,     Extractable,       HASHED_CONTENT,   None,          None,            None),
     (2,     Hashable,          HASHED_CONTENT,   None,          None,            None),
     (3,     ResearchArticle,   HASHED_CONTENT,  'utf-8',       'utf-8',          None),
     (4,     Extractable,       HASHED_CONTENT,  'utf-8',       'utf-8',          None),
     (5,     Hashable,          HASHED_CONTENT,  'utf-8',       'utf-8',          None),
     (6,     ResearchArticle,   HASHED_CONTENT,  'utf-16',      'utf-16',         None),
     (7,     Extractable,       HASHED_CONTENT,  'utf-16',      'utf-16',         None),
     (8,     Hashable,          HASHED_CONTENT,  'utf-16',      'utf-16',         None),
     (9,     ResearchArticle,   HASHED_CONTENT,   None,         'utf-8',          TypeError),
     (10,    ResearchArticle,   HASHED_CONTENT,   None,         'utf-16',         TypeError),
     (11,    ResearchArticle,   HASHED_CONTENT,  'utf-8',       'utf-16',         TypeError),
     (12,    ResearchArticle,   HASHED_CONTENT,  'utf-8',        None,            None),  # default
     (13,    ResearchArticle,   HASHED_CONTENT,  'utf-16',       None,            TypeError),
     (14,    ResearchArticle,   HASHED_CONTENT,  'utf-16',      'utf-8',          TypeError),
     ])
def test_hashable(idx, model_class, hashed_content, to_encoding, from_encoding, exception):
    """Test Hashable core methods to_hash and from_hash"""
    original_content = hashed_content
    hashed_content = hashed_content.copy()

    if exception is None:
        hashed_content['issue'] = None  # Test explicit None
        assert 'issue_date' not in hashed_content  # Test no key
        content1 = model_class.from_hash(hashed_content)
        assert content1.issue is None  # Confirm None when explicit
        assert content1.issue_date is None  # Confirm None if no key

        rehashed1 = content1.to_hash(encoding=to_encoding)
        if to_encoding is None:
            assert rehashed1 == original_content

        content2 = model_class.from_hash(rehashed1, encoding=from_encoding)
        assert isinstance(content2, ResearchArticle)
        assert isinstance(content2.published_timestamp, datetime.datetime)
        assert isinstance(content2.granularity_published, Enum)

        rehashed2 = content2.to_hash(encoding=to_encoding)
        assert rehashed2 == rehashed1
    else:
        with pytest.raises(exception):
            content1 = model_class.from_hash(hashed_content)
            rehashed1 = content1.to_hash(encoding=to_encoding)
            content2 = model_class.from_hash(rehashed1, encoding=from_encoding)
            rehashed2 = content2.to_hash(encoding=to_encoding)
