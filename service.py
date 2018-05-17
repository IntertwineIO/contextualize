#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

from content import ResearchArticle
from extractor import MultiExtractor
from utils.tools import PP


class Service:

    def contextualize(self):
        url_fragments = dict(problem=self.problem_name, org=self.org_name, geo=self.geo_name)
        extractors = MultiExtractor.provision_extractors(ResearchArticle, url_fragments)
        futures = {extractor.extract() for extractor in extractors}
        done, pending = self.loop.run_until_complete(asyncio.wait(futures))
        PP.pprint([task.result() for task in done])
        return [task.result() for task in done]

    def __init__(self, loop=None, problem_name=None, org_name=None, geo_name=None):
        self.loop = loop or asyncio.get_event_loop()
        self.problem_name = problem_name
        self.org_name = org_name
        self.geo_name = geo_name
