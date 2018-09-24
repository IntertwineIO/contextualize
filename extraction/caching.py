#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
from collections import OrderedDict

from extraction.definitions import ExtractionStatus
from utils.debug import async_debug, sync_debug
from utils.cache import AsyncCache, CacheKey

ENCODING_DEFAULT = 'utf-8'


class BaseContentCache:

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.core = AsyncCache(self.loop)

    @property
    def client(self):
        return self.core.client


class ModelCacheMixin:

    # Cache Keys
    CONTENT_KEY = 'content'

    # @async_debug(context="getattr(content, 'source_url', None)")
    async def store_content(self, content):
        content_key, content_hash = self._prepare_content(content)
        rv = await self.client.hmset_dict(content_key, content_hash)
        return rv

    # @sync_debug(context="getattr(content, 'source_url', None)")
    def _prepare_content(self, content):
        """Prepare content for storage by returning key and hash"""
        unique_field = self.model.UNIQUE_FIELD
        fields = OrderedDict()
        fields[unique_field] = getattr(content, unique_field)
        content_key = CacheKey(self.CONTENT_KEY, **fields).key
        content_hash = content.to_hash()
        return content_key, content_hash

    def __init__(self, model, loop=None, *args, **kwds):
        super().__init__(loop=loop, *args, **kwds)
        self.model = model


class SearchCacheMixin:

    # Cache Keys
    EXTRACTION_KEY = 'extraction'
    RESULTS_KEY = 'results'
    INFO_KEY = 'info'
    STATUS_KEY = 'status'

    # @async_debug(context="getattr(self, 'search_data', None)")
    async def retrieve_status(self):
        """Retrieve cached extraction status as enum or None"""
        redis = self.client
        status_info = await redis.hgetall(self.info_key)
        if status_info and self.status_key in status_info:
            status_name = status_info[self.status_key]
            return ExtractionStatus[status_name]

    # @async_debug(context="getattr(self, 'search_data', None)")
    async def retrieve_content(self):
        """Retrieve all cached content for the search data"""
        redis = self.client
        content_keys = await redis.zrange(self.search_results_key)
        if not content_keys:
            return []

        pipe = redis.pipeline()
        cached_content = [pipe.hgetall(key) for key in content_keys]
        result = await pipe.execute()
        content_items = await asyncio.gather(*cached_content)
        return content_items

    def _form_info_key(self):
        return CacheKey(self.EXTRACTION_KEY, self.INFO_KEY, **self.search_data).key

    def _form_status_key(self):
        return CacheKey(self.STATUS_KEY).key

    def _form_search_results_key(self):
        return CacheKey(self.EXTRACTION_KEY, self.RESULTS_KEY, **self.search_data).key

    def __init__(self, search_data, loop=None, *args, **kwds):
        super().__init__(loop=loop, *args, **kwds)
        self.search_data = search_data
        self.info_key = self._form_info_key()
        self.status_key = self._form_status_key()
        self.search_results_key = self._form_search_results_key()


class DirectoryCacheMixin(SearchCacheMixin, ModelCacheMixin):

    # @async_debug(context="getattr(content, 'source_url', None)")
    async def store_search_result(self, content, rank):
        """Cache search result scored by rank and associated content"""
        content_key, content_hash = self._prepare_content(content)

        pipe = self.client.pipeline()
        cache_content = pipe.hmset_dict(content_key, content_hash)
        cache_result = pipe.zadd(self.search_results_key, rank, content_key)
        result = await pipe.execute()
        rv = await asyncio.gather(cache_content, cache_result)
        return rv

    # @async_debug()
    async def store_extraction_status(self, status, overall_status, has_changed):
        """
        Store extraction status

        Format:
            info: {
                status: overall_status,
                status&extractor=academic_oup_com: status
            }
        TODO: add last_extracted
        """
        info_hash = {}
        extractor_status_key = CacheKey(self.STATUS_KEY, extractor=self.directory).key
        info_hash[extractor_status_key] = status.name

        if has_changed:
            info_hash[self.status_key] = overall_status.name

        rv = await self.client.hmset_dict(self.info_key, info_hash)
        return rv

    def __init__(self, directory, search_data, model, loop=None, *args, **kwds):
        super().__init__(search_data=search_data, model=model, loop=loop, *args, **kwds)
        self.directory = directory


class SourceExtractorCache(ModelCacheMixin, BaseContentCache):
    pass


class MultiExtractorCache(DirectoryCacheMixin, BaseContentCache):
    pass


class ContentCache(SearchCacheMixin, BaseContentCache):
    pass
