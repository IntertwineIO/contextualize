#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime

from ruamel import yaml

from contextualize.extraction.operation import ExtractionOperation
from contextualize.extraction.url import URLConstructor
from contextualize.utils.cache import FileCache
from contextualize.utils.enum import FlexEnum
from contextualize.utils.statistics import HumanDwellTime
from contextualize.utils.structures import DotNotatableOrderedDict
from contextualize.utils.tools import PP, xor_constrain


class BaseConfiguration(DotNotatableOrderedDict):

    @classmethod
    def configure_field(cls, configuration, field, extractor):
        if isinstance(configuration, dict):
            return ExtractionOperation.from_dict(configuration=configuration,
                                                 field=field,
                                                 extractor=extractor)

        if isinstance(configuration, list):
            return [ExtractionOperation.from_dict(configuration=operation_config,
                                                  field=field,
                                                  extractor=extractor)
                    for operation_config in configuration]

        return configuration


class ExtractorConfiguration(BaseConfiguration):

    IS_ENABLED_TAG = 'is_enabled'
    IMPLICIT_WAIT_TAG = 'wait'

    IMPLICIT_WAIT_DEFAULT = 3  # seconds

    file_cache = FileCache(maxsize=None)

    def __init__(self, is_enabled, implicit_wait, delay, content):

        self.is_enabled = is_enabled
        self.implicit_wait = implicit_wait
        self.delay = delay
        self.content = content

    @classmethod
    def from_file(cls, file_path, extractor):
        constructor_class = (cls if cls is not ExtractorConfiguration
                             else cls._derive_class_from_file_path(file_path))
        marshalled = constructor_class._marshal_from_file(file_path)
        return constructor_class.from_dict(configuration=marshalled, extractor=extractor)

    @classmethod
    def _derive_class_from_file_path(cls, file_path):
        if file_path.endswith(MultiExtractorConfiguration.FILE_NAME):
            return MultiExtractorConfiguration
        elif file_path.endswith(SourceExtractorConfiguration.FILE_NAME):
            return SourceExtractorConfiguration
        else:
            raise ValueError(f'Invalid file path: {file_path}')

    @file_cache
    @classmethod
    def _marshal_from_file(cls, file_path):
        """Marshall configuration dictionary from file, given a path"""
        with open(file_path) as stream:
            return yaml.safe_load(stream)

    @classmethod
    def from_dict(cls, configuration, extractor):
        init_kwargs = cls._derive_init_kwargs(configuration, extractor=extractor)
        return cls(**init_kwargs)

    @classmethod
    def _derive_init_kwargs(cls, configuration, extractor):
        return dict(
            is_enabled=configuration.get(cls.IS_ENABLED_TAG, True),
            implicit_wait=configuration.get(cls.IMPLICIT_WAIT_TAG, cls.IMPLICIT_WAIT_DEFAULT),
            delay=DelayConfiguration.from_dict(configuration.get(DelayConfiguration.DELAY_TAG)),
            content=ContentConfiguration.from_dict(
                configuration[ContentConfiguration.CONTENT_TAG],
                extractor=extractor))


class SourceExtractorConfiguration(ExtractorConfiguration):
    """SourceExtractorConfiguration is currently a subset of Multi..."""
    FILE_NAME = 'source.yaml'

    @classmethod
    def _derive_init_kwargs(cls, configuration, extractor):
        return super()._derive_init_kwargs(configuration, extractor=extractor)


class MultiExtractorConfiguration(ExtractorConfiguration):

    FILE_NAME = 'multi.yaml'
    CONTENT_ITEMS_TAG = 'items'
    EXTRACT_SOURCES_TAG = 'extract_sources'
    FRESHNESS_THRESHOLD_TAG = 'freshness_threshold'

    FRESHNESS_THRESHOLD_DEFAULT = 30  # days

    def __init__(self,
                 is_enabled,
                 implicit_wait,
                 extract_sources,
                 freshness_threshold,
                 delay,
                 content,
                 url,
                 pagination,
                 content_items):

        super().__init__(is_enabled=is_enabled,
                         implicit_wait=implicit_wait,
                         delay=delay,
                         content=content)

        self.extract_sources = extract_sources
        self.freshness_threshold = freshness_threshold
        self.url = url
        self.pagination = pagination
        self.content_items = content_items

    @classmethod
    def from_dict(cls, configuration, extractor):
        return super().from_dict(configuration=configuration, extractor=extractor)

    @classmethod
    def _derive_init_kwargs(cls, configuration, extractor):
        url = URLConstructor.from_dict(configuration[URLConstructor.URL_TAG])

        init_kwargs = super()._derive_init_kwargs(configuration, extractor=extractor)

        init_kwargs.update(
            extract_sources=configuration.get(cls.EXTRACT_SOURCES_TAG, True),
            freshness_threshold=cls._derive_freshness_threshold(configuration),
            url=url,
            pagination=PaginationConfiguration.from_dict(
                configuration=PaginationConfiguration.get_dict(configuration),
                extractor=extractor),
            content_items=cls.configure_field(configuration=configuration[cls.CONTENT_ITEMS_TAG],
                                              field=cls.CONTENT_ITEMS_TAG,
                                              extractor=extractor))

        return init_kwargs

    @classmethod
    def _derive_freshness_threshold(cls, configuration):
        freshness_threshold = configuration.get(cls.FRESHNESS_THRESHOLD_TAG,
                                                cls.FRESHNESS_THRESHOLD_DEFAULT)
        return datetime.timedelta(days=freshness_threshold)


class DelayConfiguration(HumanDwellTime, BaseConfiguration):

    DELAY_TAG = 'delay'

    @classmethod
    def from_dict(cls, configuration=None):
        delay_kwargs = cls.ARGUMENT_DEFAULTS._asdict()
        if configuration:
            delay_kwargs.update(configuration)
            cls.cleanse_kwargs(delay_kwargs)

        return cls(**delay_kwargs)

    @classmethod
    def cleanse_kwargs(cls, kwargs):
        if len(kwargs) > len(cls.ARGUMENT_DEFAULTS):
            unsupported = kwargs.keys() - cls.ARGUMENT_DEFAULTS._asdict().keys()
            for key in unsupported:
                del kwargs[key]
            PP.pprint(dict(
                msg='Unsupported keys in delay configuration',
                type='unsupported_keys_in_delay_configuration',
                unsupported=unsupported))


class ContentConfiguration(BaseConfiguration):

    CONTENT_TAG = 'content'

    @classmethod
    def from_dict(cls, configuration, extractor):
        inst = cls()
        for field, value in configuration.items():
            inst[field] = cls.configure_field(configuration=value,
                                              field=field,
                                              extractor=extractor)
        return inst


class PaginationConfiguration(BaseConfiguration):

    PAGINATION_TAG = 'pagination'
    PAGES_TAG = 'pages'
    PAGE_SIZE_TAG = 'page_size'
    NEXT_PAGE_CLICK_TAG = 'next_page_click'
    NEXT_PAGE_URL_TAG = 'next_page_url'

    PAGES_MINIMUM_DEFAULT = 1
    PAGES_MAXIMUM_DEFAULT = 10

    NextPageVia = FlexEnum('NextPageVia', 'CLICK URL')

    def __init__(self, pages=PAGES_MINIMUM_DEFAULT, page_size=None, next_page=None, next_page_method=None):
        self.pages = pages
        self.page_size = page_size
        self.next_page = next_page
        self.next_page_method = next_page_method

    @classmethod
    def from_dict(cls, configuration, extractor):
        if not configuration:
            return cls()

        next_page_click = configuration.get(cls.NEXT_PAGE_CLICK_TAG)
        next_page_url = configuration.get(cls.NEXT_PAGE_URL_TAG)
        next_page_configuration = xor_constrain(next_page_click, next_page_url)
        next_page_method = cls.NextPageVia.CLICK if next_page_click else cls.NextPageVia.URL
        next_page_field = cls.NEXT_PAGE_CLICK_TAG if next_page_click else cls.NEXT_PAGE_URL_TAG

        next_page = cls.configure_field(configuration=next_page_configuration,
                                        field=next_page_field,
                                        extractor=extractor)

        return cls(pages=configuration.get(cls.PAGES_TAG, cls.PAGES_MAXIMUM_DEFAULT),
                   page_size=configuration[cls.PAGE_SIZE_TAG],
                   next_page=next_page,
                   next_page_method=next_page_method)

    @classmethod
    def get_dict(cls, configuration):
        return configuration.get(cls.PAGINATION_TAG)

    @property
    def next_page_tag(self):
        return self.NEXT_PAGE_CLICK_TAG if self.via_click else self.NEXT_PAGE_URL_TAG

    @property
    def via_click(self):
        return self.next_page_method is self.NextPageVia.CLICK

    @property
    def via_url(self):
        return self.next_page_method is self.NextPageVia.URL
