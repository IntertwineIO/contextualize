#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict
from itertools import product

from contextualize.extraction.caching import ContentCache
from contextualize.services.extraction_service import ExtractionService
from contextualize.utils.tools import get_related_json

ENCODING_DEFAULT = 'utf-8'


class CommunityService(ExtractionService):

    TOP_GEO_LEVELS = {'country', 'subdivision1'}

    @classmethod
    def _derive_search_data(cls, payload):
        """Derive search data from a community payload"""
        community = get_related_json(payload, 'root', payload)
        problem = get_related_json(community, 'problem', payload)
        problem_terms = problem['name'] if problem else None
        org = get_related_json(community, 'org', payload)
        org_terms = org['name'] if org else None
        geo_terms = cls._derive_geo_search_terms(community, payload)
        search_data = OrderedDict(problem=problem_terms, org=org_terms, geo=geo_terms)
        return search_data

    @classmethod
    def _derive_geo_search_terms(cls, community, payload):
        geo = get_related_json(community, 'geo', payload)

        if not geo:
            return

        geo_terms = [geo['name'], geo['abbrev']] if geo['abbrev'] else geo['name']
        geo_levels = get_related_json(geo, 'levels', payload)
        geo_max_level = next(iter(geo_levels)) if geo_levels else None
        geo_parent = get_related_json(geo, 'path_parent', payload)

        if geo_max_level in cls.TOP_GEO_LEVELS or not geo_parent:
            return geo_terms

        geo_parent_terms = (
            [geo_parent['name'], geo_parent['abbrev']]
            if geo_parent['abbrev'] else geo_parent['name'])
        geo_terms = geo_terms if isinstance(geo_terms, list) else [geo_terms]
        geo_pairs = product(geo_terms, geo_parent_terms)
        geo_terms = [', '.join(geo_pair) for geo_pair in geo_pairs]
        return geo_terms

    @classmethod
    def from_payload(cls, payload, loop=None):
        """Construct CommunityService instance from a community payload"""
        search_data = cls._derive_search_data(payload)
        return cls(search_data, loop)

    def __init__(self, search_data, loop=None):
        super().__init__(search_data, loop)
        self.cache = ContentCache(self.search_data, self.loop)
