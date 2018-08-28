#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict

from service import Service


class CommunityService(Service):

    def derive_search_data(self):
        payload = self.community_payload
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

    async def extract_content(self):
        return await super().extract_content(**self.search_data)

    def __init__(self, community_payload, cache=None, loop=None):
        super().__init__(cache, loop)
        self.community_payload = community_payload
        self.search_data = self.derive_search_data()
