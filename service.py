#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

from content import ResearchArticle
from extractor import MultiExtractor
from utils.cache import AsyncCache
from utils.tools import PP


class Service:

    cache = AsyncCache()

    async def contextualize(self):
        url_fragments = dict(problem=self.problem_name, org=self.org_name, geo=self.geo_name)
        extractors = MultiExtractor.provision_extractors(ResearchArticle, url_fragments,
                                                         cache=self.cache, loop=self.loop)
        futures = {extractor.extract() for extractor in extractors}
        done, pending = await asyncio.wait(futures)
        PP.pprint([task.result() for task in done])
        return [task.result() for task in done]

    def __init__(self, loop=None, problem_name=None, org_name=None, geo_name=None):
        self.loop = loop or asyncio.get_event_loop()
        self.problem_name = problem_name
        self.org_name = org_name
        self.geo_name = geo_name

    @classmethod
    def shutdown(cls, loop=None):
        loop = loop or asyncio.get_event_loop()
        cls.cache.shutdown(loop)
