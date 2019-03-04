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
    pass


class BaseExtractorConfiguration(BaseConfiguration):

    IS_ENABLED_TAG = 'is_enabled'
    IMPLICIT_WAIT_TAG = 'wait'

    # All wait times and delays in seconds
    IMPLICIT_WAIT_DEFAULT = 3

    configuration_file_cache = FileCache(maxsize=None)

    def __init__(self, is_enabled, implicit_wait, delay, content):

        self.is_enabled = is_enabled
        self.implicit_wait = implicit_wait
        self.delay = delay
        self.content = content

    @classmethod
    def from_file(cls, file_path, source, extractor):
        constructor_class = (cls if cls is not BaseExtractorConfiguration
                             else cls._derive_class_from_file_path(file_path))
        marshalled = constructor_class._marshal_from_file(file_path)
        return constructor_class.from_dict(marshalled, source, extractor)

    @classmethod
    def _derive_class_from_file_path(cls, file_path):
        if file_path.endswith(MultiExtractorConfiguration.FILE_NAME):
            return MultiExtractorConfiguration
        elif file_path.endswith(SourceExtractorConfiguration.FILE_NAME):
            return SourceExtractorConfiguration
        else:
            raise ValueError(f'Invalid file path: {file_path}')

    @classmethod
    @configuration_file_cache
    def _marshal_from_file(cls, file_path):
        """Marshall configuration dictionary from file, given a path"""
        with open(file_path) as stream:
            return yaml.safe_load(stream)

    @classmethod
    def _derive_init_kwargs(cls, configuration, source, extractor):
        return dict(
            is_enabled=configuration.get(cls.IS_ENABLED_TAG, True),
            implicit_wait=configuration.get(cls.IMPLICIT_WAIT_TAG, cls.IMPLICIT_WAIT_DEFAULT),
            delay=DelayConfiguration.from_dict(configuration.get(DelayConfiguration.DELAY_TAG)),
            content=ContentConfiguration.from_dict(
                configuration[ContentConfiguration.CONTENT_TAG],
                source,
                extractor))

    @classmethod
    def from_dict(cls, configuration, source, extractor):
        init_kwargs = cls._derive_init_kwargs(configuration, source, extractor)
        return cls(**init_kwargs)


class SourceExtractorConfiguration(BaseExtractorConfiguration):
    """SourceExtractorConfiguration is currently a subset of Multi..."""
    FILE_NAME = 'source.yaml'


class MultiExtractorConfiguration(BaseExtractorConfiguration):

    FILE_NAME = 'multi.yaml'
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
                 items):

        super().__init__(is_enabled=is_enabled,
                         implicit_wait=implicit_wait,
                         delay=delay,
                         content=content)

        self.extract_sources = extract_sources
        self.freshness_threshold = freshness_threshold
        self.url = url
        self.pagination = pagination
        self.items = items

    @classmethod
    def _derive_init_kwargs(cls, configuration, source, extractor):
        init_kwargs = super()._derive_init_kwargs(configuration, source, extractor)

        init_kwargs.update(
            extract_sources=configuration.get(cls.EXTRACT_SOURCES_TAG, True),
            freshness_threshold=cls._derive_freshness_threshold(configuration),
            url=URLConstructor.from_dict(configuration[URLConstructor.URL_TAG]),
            pagination=PaginationConfiguration.from_dict(
                configuration.get(PaginationConfiguration.PAGINATION_TAG),
                source,
                extractor),
            items=ItemsConfiguration.from_dict(
                configuration[ItemsConfiguration.ITEMS_TAG],
                source,
                extractor))

        return init_kwargs

    @classmethod
    def _derive_freshness_threshold(cls, configuration):
        freshness_threshold = configuration.get(cls.FRESHNESS_THRESHOLD_TAG,
                                                cls.FRESHNESS_THRESHOLD_DEFAULT)
        return datetime.timedelta(days=freshness_threshold)


class DelayConfiguration(BaseConfiguration):

    DELAY_TAG = 'delay'

    DELAY_DEFAULTS = HumanDwellTime(
        mu=0, sigma=0.5, base=1, multiplier=1, minimum=1, maximum=3)

    def __init__(self, mu=None, sigma=None, base=None, multiplier=None, minimum=None, maximum=None):
        self.mu = mu
        self.sigma = sigma
        self.base = base
        self.multiplier = multiplier
        self.minimum = minimum
        self.maximum = maximum

    @classmethod
    def from_dict(cls, configuration=None):
        delay_kwargs = cls.DELAY_DEFAULTS._asdict()
        if configuration:
            delay_kwargs.update(configuration)
            cls.cleanse_kwargs(delay_kwargs)

        return cls(**delay_kwargs)

    @classmethod
    def cleanse_kwargs(cls, kwargs):
        if len(kwargs) > len(cls.DELAY_DEFAULTS):
            unsupported = kwargs.keys() - cls.DELAY_DEFAULTS._asdict().keys()
            for key in unsupported:
                del kwargs[key]
            PP.pprint(dict(
                msg='Unsupported keys in delay configuration',
                type='unsupported_keys_in_delay_configuration',
                unsupported=unsupported))

    def as_human_dwell_time(self):
        return HumanDwellTime(**self)


class FieldConfigurable:

    @classmethod
    def configure_field(cls, configuration, field, source, extractor):
        # TODO: utilize contextvars for field/source (py3.7)
        if isinstance(configuration, dict):
            return ExtractionOperation.from_dict(configuration=configuration,
                                                 field=field,
                                                 source=source,
                                                 extractor=extractor)

        if isinstance(configuration, list):
            return [ExtractionOperation.from_dict(configuration=operation_config,
                                                  field=field,
                                                  source=source,
                                                  extractor=extractor)
                    for operation_config in configuration]

        return configuration


class ItemsConfiguration(FieldConfigurable, BaseConfiguration):

    ITEMS_TAG = 'items'

    def __init__(self, items):
        self.items = items

    @classmethod
    def from_dict(cls, configuration, source, extractor):
        items = cls.configure_field(configuration=configuration,
                                    field=cls.ITEMS_TAG,
                                    source=source,
                                    extractor=extractor)
        return cls(items=items)


class ContentConfiguration(FieldConfigurable, BaseConfiguration):

    CONTENT_TAG = 'content'

    @classmethod
    def from_dict(cls, configuration, source, extractor):
        inst = cls()
        for field, value in configuration.items():
            inst[field] = cls.configure_field(value, field, source, extractor)
        return inst


class PaginationConfiguration(FieldConfigurable, BaseConfiguration):

    PAGINATION_TAG = 'pagination'
    PAGES_TAG = 'pages'
    PAGE_SIZE_TAG = 'page_size'
    NEXT_PAGE_TAG = 'next_page'  # TODO: replace with click/url below
    NEXT_PAGE_CLICK_TAG = 'next_page_click'
    NEXT_PAGE_URL_TAG = 'next_page_url'

    PAGES_MAXIMUM_DEFAULT = 10

    NextPageMethod = FlexEnum('NextPageMethod', 'CLICK URL')

    def __init__(self, pages=1, page_size=None, next_page=None, next_page_method=None):
        self.pages = pages
        self.page_size = page_size
        self.next_page = next_page
        self.next_page_method = next_page_method

    @classmethod
    def from_dict(cls, configuration, source, extractor):
        if not configuration:
            return cls()

        next_page_click = configuration.get(cls.NEXT_PAGE_CLICK_TAG)
        next_page_url = configuration.get(cls.NEXT_PAGE_URL_TAG)
        next_page_configuration = xor_constrain(next_page_click, next_page_url)
        next_page_method = cls.NextPageMethod.CLICK if next_page_click else cls.NextPageMethod.URL
        next_page_field = cls.NEXT_PAGE_CLICK_TAG if next_page_click else cls.NEXT_PAGE_URL_TAG

        next_page = cls.configure_field(configuration=next_page_configuration,
                                        field=next_page_field,
                                        source=source,
                                        extractor=extractor)

        return cls(pages=configuration.get(cls.PAGES_TAG, cls.PAGES_MAXIMUM_DEFAULT),
                   page_size=configuration[cls.PAGE_SIZE_TAG],
                   next_page=next_page,
                   next_page_method=next_page_method)
