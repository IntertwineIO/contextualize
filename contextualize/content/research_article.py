#!/usr/bin/env python
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from datetime import datetime

from contextualize.content.base import Extractable
from contextualize.utils.time import Granularity


@dataclass
class ResearchArticle(Extractable):
    """ResearchArticle"""
    PROVIDER_DIRECTORY = 'contextualize/providers'

    title: str = None
    author_names: str = None
    publication: str = None
    volume: str = None
    issue: str = None
    issue_date: datetime = None
    issue_date_granularity: Granularity = None
    first_page: str = None
    last_page: str = None
    doi: str = None
    published_timestamp: datetime = None
    granularity_published: Granularity = None
    tzinfo_published: str = None
    publisher: str = None
    summary: str = None
    full_text: str = None

    @classmethod
    def deserialize_issue_date(cls, dt_string, **field_hash):
        dt = cls.datetime_from_string(dt_string)
        granularity_string = field_hash.get('issue_date_granularity')
        dt.granularity = cls.enum_from_string(granularity_string)
        return dt

    @classmethod
    def deserialize_issue_date_granularity(cls, granularity_string, **field_hash):
        return cls.enum_from_string(granularity_string)

    @classmethod
    def deserialize_published_timestamp(cls, dt_string, **field_hash):
        dt = cls.datetime_from_string(dt_string)
        granularity_string = field_hash.get('granularity_published')
        dt.granularity = cls.enum_from_string(granularity_string)
        return dt

    @classmethod
    def deserialize_granularity_published(cls, granularity_string, **field_hash):
        return cls.enum_from_string(granularity_string)
