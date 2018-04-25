#!/usr/bin/env python
# -*- coding: utf-8 -*-
from utils.mixins import Extractable


class ResearchArticle(Extractable):
    """ResearchArticle"""
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
