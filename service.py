#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
from collections import OrderedDict

from content import ResearchArticle
from extractor import MultiExtractor
from utils.cache import AsyncCache
from utils.debug import async_debug, sync_debug
from utils.tools import PP


class Service:

    @async_debug()
    async def contextualize(self):
        search_terms = OrderedDict(problem=self.problem_name, org=self.org_name, geo=self.geo_name)
        extractors = MultiExtractor.provision_extractors(ResearchArticle, search_terms,
                                                         cache=self.cache, loop=self.loop)
        futures = {extractor.extract() for extractor in extractors}
        done, pending = await asyncio.wait(futures)
        PP.pprint([task.result() for task in done])
        return [task.result() for task in done]

    def __init__(self, problem_name=None, org_name=None, geo_name=None, cache=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.cache = cache or AsyncCache(self.loop)
        self.problem_name = problem_name
        self.org_name = org_name
        self.geo_name = geo_name
