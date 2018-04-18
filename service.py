#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

from pprint import PrettyPrinter

from extractor import SearchExtractor


class Service:

    def contextualize(self):
        extractors = SearchExtractor.provision_extractors(
            self.problem_name, self.org_name, self.geo_name)
        futures = {extractor.extract() for extractor in extractors}
        done, pending = self.loop.run_until_complete(asyncio.wait(futures))
        pp = PrettyPrinter(indent=4)
        pp.pprint([task.result() for task in done])
        return [task.result() for task in done]

    def __init__(self, loop=None, problem_name=None, org_name=None, geo_name=None):
        self.loop = loop or asyncio.get_event_loop()
        self.problem_name = problem_name
        self.org_name = org_name
        self.geo_name = geo_name
