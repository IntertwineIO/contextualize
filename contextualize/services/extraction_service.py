#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

from contextualize.content.research_article import ResearchArticle
from contextualize.extraction.extractor import MultiExtractor
from contextualize.utils.debug import debug
from contextualize.utils.tools import PP


class ExtractionService:

    @debug
    async def extract_content(self):
        extractors = MultiExtractor.provision_extractors(
            ResearchArticle, self.search_data, use_cache=True, loop=self.loop)
        futures = {extractor.extract() for extractor in extractors}
        done, pending = await asyncio.wait(futures)
        PP.pprint([task.result() for task in done])
        return [task.result() for task in done]

    def __init__(self, search_data, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.search_data = search_data
