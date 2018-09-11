#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict

from extraction.caching import ContentCache
from services.extraction import ExtractionService

ENCODING_DEFAULT = 'utf-8'


class CommunityService(ExtractionService):

    @classmethod
    def _derive_search_data(cls, payload):
        """Derive search data from a community payload"""
        community_key = payload['root']
        community = payload[community_key]
        problem_key = community['problem']
        problem_terms = payload[problem_key]['name'] if problem_key else None
        org_key = community['org']
        org_terms = payload[org_key]['name'] if org_key else None
        geo_key = community['geo']
        if geo_key:
            geo = payload[geo_key]
            geo_terms = [geo['name'], geo['abbrev']] if geo['abbrev'] else geo['name']
        else:
            geo_terms = None

        search_data = OrderedDict(problem=problem_terms, org=org_terms, geo=geo_terms)
        return search_data

    @classmethod
    def from_payload(cls, payload, loop=None):
        """Construct CommunityService instance from a community payload"""
        search_data = cls._derive_search_data(payload)
        return cls(search_data, loop)


    def __init__(self, search_data, loop=None):
        super().__init__(search_data, loop)
        self.cache = ContentCache(self.search_data, self.loop)
