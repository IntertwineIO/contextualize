#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import datetime
from collections import OrderedDict

from contextualize.content.base import Hashable
from contextualize.extraction.definitions import ExtractionStatus
from contextualize.extraction.info import ExtractionInfo
from contextualize.utils.cache import AsyncCache, CacheKey
from contextualize.utils.debug import debug

ENCODING_DEFAULT = 'utf-8'


class BaseContentCache:

    CONTENT_KEY = 'content'

    @property
    def client(self):
        return self.core.client

    def _prepare_content(self, content):
        """Prepare content for storage and return hash"""
        content.cache_version = self.cache_version
        content.last_extracted = datetime.datetime.utcnow()
        content_hash = content.to_hash()
        return content_hash

    def _form_content_key(self, source_url):
        return CacheKey(self.CONTENT_KEY, source_url=source_url).key

    def __init__(self, cache_version=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.core = AsyncCache(self.loop)
        self.cache_version = cache_version


class SourceExtractorCache(BaseContentCache):

    async def store_content_item(self, content):
        """Store content item in cache"""
        content_key = self._form_content_key(self.source_url)
        content_hash = self._prepare_content(content)
        return await self.client.hmset_dict(content_key, content_hash)

    async def retrieve_content_item(self):
        """Retrieve cached content item from cache"""
        content_key = self._form_content_key(self.source_url)
        content_hash = await self.client.hgetall(content_key)
        return Hashable.from_hash(content_hash) if content_hash else None

    def __init__(self, source_url, cache_version, loop=None):
        super().__init__(cache_version=cache_version, loop=loop)
        self.source_url = source_url


class ContentCache(BaseContentCache):

    # Cache keys
    EXTRACTION_KEY = 'extraction'
    INFO_KEY = 'info'
    RESULTS_KEY = 'results'

    # Hash keys
    SOURCE_URL_KEY = 'source_url'
    STATUS_KEY = 'status'
    CACHE_VERSION_KEY = 'cache_version'
    LAST_EXTRACTED_KEY = 'last_extracted'

    async def retrieve_search_results(self):
        """Retrieve all cached content hashes for the search data"""
        content_keys = await self.client.zrange(self.search_results_key)
        if content_keys:
            return await self.retrieve_content_hashes(content_keys)
        return []

    async def retrieve_content_hashes(self, content_keys):
        """Retrieve cached content hashes for given content keys"""
        pipe = self.client.pipeline()
        cached_content = [pipe.hgetall(key) for key in content_keys]
        await pipe.execute()
        return await asyncio.gather(*cached_content)

    async def retrieve_content_map(self, content_keys):
        """Retrieve dictionary of content items keyed by source URL"""
        content_hashes = await self.retrieve_content_hashes(content_keys)
        return {content_hash[self.SOURCE_URL_KEY]: Hashable.from_hash(content_hash)
                for content_hash in content_hashes}

    async def retrieve_extraction_info_map(self, directories):
        """Retrieve map of extraction info by extractor directory"""
        pipe = self.client.pipeline()
        extraction_info_keys = (self._form_extraction_info_key(directory, **self.search_data)
                                for directory in directories)
        cached_info_futures = [pipe.hgetall(key) for key in extraction_info_keys]
        await pipe.execute()
        info_hashes = await asyncio.gather(*cached_info_futures)
        info_items = (ExtractionInfo.from_hash(info_hash) for info_hash in info_hashes)
        return {directory: info for directory, info in zip(directories, info_items)}

    def _form_extraction_info_key(self, directory, **search_data):
        return CacheKey(self.EXTRACTION_KEY, self.INFO_KEY, extractor=directory, **search_data).key

    def _form_search_results_key(self, **search_data):
        return CacheKey(self.EXTRACTION_KEY, self.RESULTS_KEY, **search_data).key

    def __init__(self, search_data, cache_version=None, loop=None):
        super().__init__(cache_version=cache_version, loop=loop)
        self.search_data = search_data
        self.search_results_key = self._form_search_results_key(**self.search_data)


class MultiExtractorCache(ContentCache):

    async def store_extraction_info(self, status):
        """
        Store extraction info

        Format:
            extraction&info&extractor=academic_oup_com&**search_data: {
                status: status,
                cache_version: '2019-04-01-01',
                last_extracted: '2019-04-20T23:52:11.810493',
            }
        """
        info_hash = {}
        info_hash[self.STATUS_KEY] = status.name

        if status is not ExtractionStatus.INITIATED:
            info_hash[self.LAST_EXTRACTED_KEY] = datetime.datetime.utcnow().isoformat()

        if self.cache_version:
            info_hash[self.CACHE_VERSION_KEY] = self.cache_version

        return await self.client.hmset_dict(self.extraction_info_key, info_hash)

    async def retrieve_extraction_info(self):
        """Retrieve extraction info from cache"""
        info_hash = await self.client.hgetall(self.extraction_info_key)
        return ExtractionInfo.from_hash(info_hash)

    @debug
    async def store_extraction_result(self, content, rank):
        """Cache extraction result scored by rank and associated content"""
        content_key = self._form_content_key(content.source_url)

        pipe = self.client.pipeline()
        cache_search_result = pipe.zadd(self.search_results_key, rank, content_key)
        cache_extractor_result = pipe.zadd(self.extraction_results_key, rank, content_key)

        await pipe.execute()
        return await asyncio.gather(cache_search_result, cache_extractor_result)

    @debug
    async def store_extraction_content_result(self, content, rank):
        """Cache extraction result scored by rank and associated content"""
        content_key = self._form_content_key(content.source_url)
        content_hash = self._prepare_content(content)

        pipe = self.client.pipeline()
        cache_content = pipe.hmset_dict(content_key, content_hash)
        cache_search_result = pipe.zadd(self.search_results_key, rank, content_key)
        cache_extractor_result = pipe.zadd(self.extraction_results_key, rank, content_key)

        await pipe.execute()
        return await asyncio.gather(cache_content, cache_search_result, cache_extractor_result)

    async def retrieve_extraction_results(self):
        """Retrieve all cached content for extractor and search data"""
        content_keys = await self.client.zrange(self.extraction_results_key)
        if not content_keys:
            return {}
        return await self.retrieve_content_map(content_keys)

    def _form_extraction_results_key(self, directory, **search_data):
        return CacheKey(self.EXTRACTION_KEY, self.RESULTS_KEY, extractor=directory,
                        **search_data).key

    def __init__(self, directory, search_data, cache_version, loop=None):
        super().__init__(search_data=search_data, cache_version=cache_version, loop=loop)
        self.directory = directory
        self.extraction_info_key = self._form_extraction_info_key(self.directory,
                                                                  **self.search_data)
        self.extraction_results_key = self._form_extraction_results_key(self.directory,
                                                                        **self.search_data)
