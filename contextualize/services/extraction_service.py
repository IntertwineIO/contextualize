#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

from contextualize.content.research_article import ResearchArticle
from contextualize.extraction.caching import ContentCache
from contextualize.extraction.definitions import ExtractionStatus
from contextualize.extraction.extractor import MultiExtractor
from contextualize.utils.debug import debug
from contextualize.utils.tools import PP


class ExtractionService:

    @debug
    def provision_extractors(self):
        extractors = MultiExtractor.provision_extractors(
            ResearchArticle, self.search_data, use_cache=True, loop=self.loop)
        self.extractors = list(extractors)

    @debug
    async def determine_status(self):
        """
        Determine status

        Calculate overall status by retrieving individual extractor
        statuses since the set of active extractors changes over time.

        Provision extractors to determine the active set. For each,
        determine if we should use cached content based on extractor
        status and last extracted timestamp.

        If ALL extractors should use cached content, return COMPLETED.
        Else if ANY extractors should use cached content, return
        PRELIMINARY. Otherwise, return INITIATED.
        """
        if not self.extractors:
            self.provision_extractors()

        extractors = self.extractors
        directories = [extractor.directory for extractor in extractors]

        cache = self.cache
        info_map = await cache.retrieve_extraction_info_map(directories)

        if all(extractor.should_use_cached_content(info)
               for extractor, info in zip(extractors, info_map.values())):
            return ExtractionStatus.COMPLETED

        aggregate_status = ExtractionStatus.aggregate(
            *(info.status for info in info_map.values()))

        if not aggregate_status:
            return ExtractionStatus.INITIATED

        if aggregate_status >= ExtractionStatus.PRELIMINARY:
            return ExtractionStatus.PRELIMINARY

        return aggregate_status

    @debug
    async def extract_content(self):
        if not self.extractors:
            self.provision_extractors()
        futures = {extractor.extract() for extractor in self.extractors}
        done, pending = await asyncio.wait(futures)
        PP.pprint([task.result() for task in done])
        return [task.result() for task in done]

    def __init__(self, search_data, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.search_data = search_data
        self.cache = ContentCache(self.search_data, self.loop)
        self.extractors = None
