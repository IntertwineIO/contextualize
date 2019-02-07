#!/usr/bin/env python
# -*- coding: utf-8 -*-
from contextualize.content.base import Extractable


class ResearchArticle(Extractable):
    """ResearchArticle"""
    PROVIDER_DIRECTORY = 'contextualize/providers'

    @classmethod
    def deserialize_issue_date(cls, dt_string, **field_hash):
        dt = cls.deserialize_datetime(dt_string)
        granularity_string = field_hash.get('issue_date_granularity')
        dt.granularity = cls.deserialize_enum(granularity_string)
        return dt

    @classmethod
    def deserialize_issue_date_granularity(cls, granularity_string, **field_hash):
        return cls.deserialize_enum(granularity_string)

    @classmethod
    def deserialize_published_timestamp(cls, dt_string, **field_hash):
        dt = cls.deserialize_datetime(dt_string)
        granularity_string = field_hash.get('granularity_published')
        dt.granularity = cls.deserialize_enum(granularity_string)
        return dt

    @classmethod
    def deserialize_granularity_published(cls, granularity_string, **field_hash):
        return cls.deserialize_enum(granularity_string)

    def __init__(self, source_url=None, title=None, author_names=None, publication=None,
                 volume=None, issue=None, issue_date=None, issue_date_granularity=None,
                 first_page=None, last_page=None, doi=None, published_timestamp=None,
                 granularity_published=None, tzinfo_published=None, publisher=None,
                 summary=None, full_text=None, *args, **kwds):
        super().__init__(source_url, *args, **kwds)
        self.title = title
        self.author_names = author_names
        self.publication = publication
        self.volume = volume
        self.issue = issue
        self.issue_date = issue_date
        self.issue_date_granularity = issue_date_granularity
        self.first_page = first_page
        self.last_page = last_page
        self.doi = doi
        self.published_timestamp = published_timestamp
        self.granularity_published = granularity_published
        self.tzinfo_published = tzinfo_published
        self.publisher = publisher
        self.summary = summary
        self.full_text = full_text
