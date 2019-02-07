#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import datetime
from collections import OrderedDict

from contextualize.extraction.definitions import ExtractionStatus
from contextualize.utils.cache import AsyncCache, CacheKey
from contextualize.utils.debug import debug
from contextualize.utils.time import GranularDateTime

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

    # @debug(context="getattr(content, 'source_url', None)")
    async def store_content(self, content):
        content_key, content_hash = self._prepare_content(content)
        rv = await self.client.hmset_dict(content_key, content_hash)
        return rv

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
    LAST_EXTRACTED_KEY = 'last_extracted'

    # @debug(context="getattr(self, 'search_data', None)")
    async def retrieve_extraction_info(self):
        """Retrieve cached extraction info or None"""
        redis = self.client
        return await redis.hgetall(self.info_key)

    # @debug(context="getattr(self, 'search_data', None)")
    async def retrieve_status(self):
        """Retrieve cached extraction status enum or None"""
        redis = self.client
        status_info = await redis.hgetall(self.info_key)
        if status_info and self.status_key in status_info:
            status_name = status_info[self.status_key]
            return ExtractionStatus[status_name]

    # @debug(context="getattr(self, 'search_data', None)")
    async def retrieve_search_results(self):
        """Retrieve all cached content for the search data"""
        redis = self.client
        content_keys = await redis.zrange(self.search_results_key)
        if content_keys:
            return await self.retrieve_content(content_keys)
        return []

    async def retrieve_content(self, content_keys):
        """Retrieve cached content for given content keys"""
        pipe = self.client.pipeline()
        cached_content = [pipe.hgetall(key) for key in content_keys]
        await pipe.execute()
        return await asyncio.gather(*cached_content)

    def _form_info_key(self):
        return CacheKey(self.EXTRACTION_KEY, self.INFO_KEY, **self.search_data).key

    def _form_status_key(self):
        return CacheKey(self.STATUS_KEY).key

    def _form_last_extracted_key(self):
        return CacheKey(self.LAST_EXTRACTED_KEY).key

    def _form_search_results_key(self):
        return CacheKey(self.EXTRACTION_KEY, self.RESULTS_KEY, **self.search_data).key

    def __init__(self, search_data, loop=None, *args, **kwds):
        super().__init__(loop=loop, *args, **kwds)
        self.search_data = search_data
        self.info_key = self._form_info_key()
        self.status_key = self._form_status_key()
        self.last_extracted_key = self._form_last_extracted_key()
        self.search_results_key = self._form_search_results_key()


class DirectoryCacheMixin(SearchCacheMixin, ModelCacheMixin):

    @debug(context="getattr(content, 'source_url', None)")
    async def store_search_result(self, content, rank):
        """Cache search result scored by rank and associated content"""
        content_key, content_hash = self._prepare_content(content)

        pipe = self.client.pipeline()
        cache_content = pipe.hmset_dict(content_key, content_hash)
        cache_search_result = pipe.zadd(self.search_results_key, rank, content_key)
        cache_extractor_result = pipe.zadd(self.extractor_results_key, rank, content_key)

        await pipe.execute()
        rv = await asyncio.gather(cache_content, cache_search_result, cache_extractor_result)
        return rv

    # @debug
    async def store_extraction_status(self, extractor_status, overall_status, has_changed):
        """
        Store extraction status

        Format:
            extraction&info&**search_data: {
                status: overall_status,
                last_extracted: overall_timestamp,
                status&extractor=academic_oup_com: extractor_status,
                last_extracted&extractor=academic_oup_com: extractor_timestamp,
            }
        """
        info_hash = {}
        info_hash[self.extractor_status_key] = extractor_status.name
        now = datetime.datetime.utcnow()
        timestamp = now.isoformat()

        if extractor_status.indicates_results():
            info_hash[self.extractor_last_extracted_key] = timestamp

        if has_changed:
            info_hash[self.status_key] = overall_status.name
            if overall_status.indicates_results():
                info_hash[self.last_extracted_key] = timestamp

        rv = await self.client.hmset_dict(self.info_key, info_hash)
        return rv

    # @debug(context="getattr(self, 'search_data', None)")
    async def retrieve_extractor_status(self):
        """Retrieve cached extractor status enum or None"""
        redis = self.client
        info_hash = await redis.hgetall(self.info_key)

        if info_hash and self.extractor_status_key in info_hash:
            status_name = info_hash[self.extractor_status_key]
            return ExtractionStatus[status_name]

    # @debug(context="getattr(self, 'search_data', None)")
    async def retrieve_extractor_info(self):
        """Retrieve cached extractor status enum and last extracted"""
        redis = self.client
        info_hash = await redis.hgetall(self.info_key)

        if not info_hash:
            return None, None

        try:
            status_name = info_hash[self.extractor_status_key]
            status = ExtractionStatus[status_name]
        except KeyError:
            status = None

        try:
            last_extracted_string = info_hash[self.extractor_last_extracted_key]
            last_extracted = GranularDateTime.deserialize(last_extracted_string)
        except KeyError:
            last_extracted = None

        return status, last_extracted

    # @debug(context="getattr(self, 'search_data', None)")
    async def retrieve_extractor_results(self):
        """Retrieve all cached content for extractor and search data"""
        redis = self.client
        content_keys = await redis.zrange(self.extractor_results_key)
        if content_keys:
            return await self.retrieve_content(content_keys)
        return []

    def _form_extractor_status_key(self):
        return CacheKey(self.STATUS_KEY, extractor=self.directory).key

    def _form_extractor_last_extracted_key(self):
        return CacheKey(self.LAST_EXTRACTED_KEY, extractor=self.directory).key

    def _form_extractor_results_key(self):
        return CacheKey(self.EXTRACTION_KEY, self.RESULTS_KEY, extractor=self.directory,
                        **self.search_data).key

    def __init__(self, directory, search_data, model, loop=None, *args, **kwds):
        super().__init__(search_data=search_data, model=model, loop=loop, *args, **kwds)
        self.directory = directory
        self.extractor_status_key = self._form_extractor_status_key()
        self.extractor_last_extracted_key = self._form_extractor_last_extracted_key()
        self.extractor_results_key = self._form_extractor_results_key()


class SourceExtractorCache(ModelCacheMixin, BaseContentCache):
    pass


class MultiExtractorCache(DirectoryCacheMixin, BaseContentCache):
    pass


class ContentCache(SearchCacheMixin, BaseContentCache):
    pass
