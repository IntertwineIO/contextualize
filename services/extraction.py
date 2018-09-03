#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
from collections import OrderedDict

from content import ResearchArticle
from extraction.extractor import MultiExtractor
from utils.cache import AsyncCache
from utils.debug import async_debug, sync_debug
from utils.tools import PP


class ExtractionService:

    @async_debug()
    async def extract_content(self):
        extractors = MultiExtractor.provision_extractors(
            ResearchArticle, self.search_data, loop=self.loop)
        futures = {extractor.extract() for extractor in extractors}
        done, pending = await asyncio.wait(futures)
        PP.pprint([task.result() for task in done])
        return [task.result() for task in done]

    def __init__(self, search_data, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.search_data = search_data
