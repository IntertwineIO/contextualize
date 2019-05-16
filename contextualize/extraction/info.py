#!/usr/bin/env python
# -*- coding: utf-8 -*-
from contextualize.extraction.definitions import ExtractionStatus
from contextualize.utils.time import GranularDateTime


class ExtractionInfo:

    STATUS_KEY = 'status'
    LAST_EXTRACTED_KEY = 'last_extracted'
    CACHE_VERSION_TAG = 'cache_version'

    def __init__(self, status=None, last_extracted=None, cache_version=None):
        self.status = status
        self.last_extracted = last_extracted
        self.cache_version = cache_version

    @classmethod
    def from_hash(cls, info_hash):
        if not info_hash:
            return cls()

        try:
            status_name = info_hash[cls.STATUS_KEY]
            status = ExtractionStatus[status_name]
        except KeyError:
            status = None

        try:
            last_extracted_string = info_hash[cls.LAST_EXTRACTED_KEY]
            last_extracted = GranularDateTime.deserialize(last_extracted_string)
        except KeyError:
            last_extracted = None

        cache_version = info_hash.get(cls.CACHE_VERSION_TAG)

        return cls(status=status, last_extracted=last_extracted, cache_version=cache_version)

    @classmethod
    def from_content(cls, content):
        if not content:
            return cls()

        return cls(status=ExtractionStatus.COMPLETED,
                   last_extracted=content.last_extracted,
                   cache_version=content.cache_version)
