#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import datetime
import os
import urllib
from collections import OrderedDict, defaultdict, namedtuple
from functools import lru_cache
from itertools import chain
from pathlib import Path

from ruamel import yaml
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from url_normalize import url_normalize

import settings
from exceptions import NoneValueError
from extraction.caching import MultiExtractorCache, SourceExtractorCache
from extraction.definitions import ExtractionStatus
from extraction.operation import ExtractionOperation
from secret_service.agency import SecretService
from utils.async import run_in_executor
from utils.debug import async_debug, sync_debug
from utils.enum import FlexEnum
from utils.iterable import one
from utils.statistics import HumanDwellTime, human_dwell_time, human_selection_shuffle
from utils.tools import PP, derive_domain, enlist, isnonstringsequence, xor_constrain


class BaseExtractor:

    FILE_NAME = NotImplementedError

    CONTENT_TAG = 'content'
    DELAY_TAG = 'delay'
    ELEMENTS_TAG = 'elements'
    IS_ENABLED_TAG = 'is_enabled'
    SOURCE_URL_TAG = 'source_url'
    WAIT_TAG = 'wait'

    WebDriverBrand = FlexEnum('WebDriverBrand', 'CHROME FIREFOX')
    WEB_DRIVER_BRAND_DEFAULT = WebDriverBrand.CHROME

    WebDriverInfo = namedtuple('WebDriverInfo', 'brand type kwargs')

    DELAY_DEFAULTS = HumanDwellTime(
        mu=0, sigma=0.5, base=1, multiplier=1, minimum=1, maximum=3)

    Status = ExtractionStatus

    @async_debug()
    async def extract(self):
        """
        Extract

        Extract content via the configured extractor. Return content if
        extractor is enabled and extraction is successful, else None.
        """
        if not self.is_enabled:
            PP.pprint(dict(
                msg='WARNING: extracting with disabled extractor',
                type='extractor_disabled_warning', extractor=repr(self)))
            return
        try:
            # TODO: Add WebDriverPool with context manager to provision
            if not self.web_driver:
                await self._acquire_web_driver()
            await self._update_status(ExtractionStatus.STARTED)
            await self._perform_extraction(self.page_url)
            await self._update_status(ExtractionStatus.COMPLETED)
            return self.extracted_content
        finally:
            if not self.reuse_web_driver:
                await self._release_web_driver()

    @async_debug()
    async def _acquire_web_driver(self):
        """Acquire web driver"""
        implicit_wait = self.configuration.get(self.WAIT_TAG, settings.WAIT_IMPLICIT_DEFAULT)
        self.web_driver = await self._provision_web_driver(
            web_driver_type=self.web_driver_type,
            web_driver_kwargs=self.web_driver_kwargs,
            implicit_wait=implicit_wait,
            loop=self.loop)

    @async_debug()
    async def _release_web_driver(self):
        """Release web driver"""
        await self._deprovision_web_driver(web_driver=self.web_driver, loop=self.loop)

    @async_debug()
    @classmethod
    async def _provision_web_driver(cls, web_driver_brand=None, web_driver_type=None,
                                    web_driver_kwargs=None, implicit_wait=None, loop=None):
        """Provision web driver"""
        loop = loop or asyncio.get_event_loop()
        _, web_driver_type, web_driver_kwargs = cls._derive_web_driver_info(
            web_driver_brand, web_driver_type, web_driver_kwargs)

        web_driver = await run_in_executor(loop, None, web_driver_type, **web_driver_kwargs)
        # Configure web driver to allow waiting on each operation
        implicit_wait = settings.WAIT_IMPLICIT_DEFAULT if implicit_wait is None else implicit_wait
        web_driver.implicitly_wait(implicit_wait)
        web_driver.last_fetch_timestamp = None
        return web_driver

    @async_debug()
    @classmethod
    async def _deprovision_web_driver(cls, web_driver, loop=None):
        """Deprovision web driver"""
        loop = loop or asyncio.get_event_loop()
        if web_driver:
            await run_in_executor(loop, None, web_driver.quit)

    @async_debug()
    async def _perform_page_fetch(self, url):
        """Perform page fetch of given URL by running in executor"""
        future_page = self._execute_in_future(self.web_driver.get, url)
        self.web_driver.last_fetch_timestamp = datetime.datetime.utcnow()
        await future_page

    async def _perform_extraction(self, url=None, page=1):
        """Perform extraction (abstract method)"""
        raise NotImplementedError

    async def _perform_page_extraction(self, *args, **kwds):
        """Perform page extraction (abstract method)"""
        raise NotImplementedError

    # @async_debug()
    async def _extract_content(self, element, configuration, index=1, **kwds):
        """
        Extract content

        Extract content from the given web element and configuration.

        I/O:
        element:        Selenium web driver or element
        configuration:  Configuration dictionary in which keys are
                        content fields and values are extraction
                        operations
        index=1:        Index of given element within a series
        return:         Instance of content model (e.g. ResearchArticle)
        """
        self.content_map = content_map = OrderedDict(kwds)
        # Allow field to be otherwise set without overwriting
        fields_to_extract = (field for field in self.model.fields() if field not in content_map)

        for field in fields_to_extract:
            try:
                field_config = configuration[field]

            except KeyError as e:
                field_config = None
                PP.pprint(dict(
                    msg='Extract field configuration missing', type='extract_field_config_missing',
                    error=e, field=field, content_map=content_map, extractor=repr(self)))

            try:
                content_map[field] = await self._extract_field(field, element, field_config, index)

            except Exception as e:  # e.g. NoSuchElementException
                PP.pprint(dict(
                    msg='Extract field failure', type='extract_field_failure',
                    error=e, field=field, content_map=content_map, extractor=repr(self)))

        self.content_map = None
        instance = self.model(**content_map)
        return instance

    # @async_debug(context="self.content_map.get('source_url')")
    async def _extract_field(self, field, element, configuration, index=1):
        """
        Extract field

        Extract specified field via given element and configuration.

        I/O:
        field:          Name of field within content
        element:        Selenium web driver or element
        configuration:  Configuration consisting of an operation
                        dictionary, list of operation dictionaries, or
                        hard-coded field value
        index=1:        Index of given element within a series
        return:         Extracted field value
        """
        source = self.content_map.get(self.model.UNIQUE_FIELD) if self.content_map else None
        if isinstance(configuration, list):
            return await self._execute_operation_series(
                field, source, element, configuration, index)
        if isinstance(configuration, dict):
            return await self._execute_operation(
                field, source, element, configuration, index)
        return configuration

    # @async_debug()
    async def _execute_operation_series(self, field, source, target, configuration, index=1):
        """
        Execute operation series

        Execute series of operations to extract the specified field of
        the given content source based on the target and configuration.

        I/O:
        field:          Name of field within content
        source:         Unique field for content item (e.g. source_url)
        target:         Selenium web driver or element
        configuration:  A list of operation dictionaries
        index=1:        Index of given content element within a series
        return:         Extracted field value
        """
        latest = prior = parent = target
        for operation_config in configuration:
            operation = ExtractionOperation.from_configuration(
                configuration=operation_config,
                field=field,
                source=source,
                extractor=self)

            new_targets = operation._select_targets(latest, prior, parent)
            prior = latest
            if operation.is_multiple:
                latest = await operation.execute(new_targets, index)
            else:
                for new_target in new_targets:
                    latest = await operation.execute(new_target, index)
        return latest

    # @async_debug()
    async def _execute_operation(self, field, source, target, configuration, index=1):
        """
        Execute operation

        Execute operation to extract the specified field of the given
        content source based on the target and configuration.

        I/O:
        field:          Name of field within content
        source:         Unique field for content item (e.g. source_url)
        target:         Selenium web driver or element
        configuration:  An operation dictionary
        index=1:        Index of given content element within a series
        return:         Extracted field value
        """
        operation = ExtractionOperation.from_configuration(
            configuration=configuration,
            field=field,
            source=source,
            extractor=self)

        return await operation.execute(target, index)

    # @async_debug(context="self.content_map.get('source_url')")
    async def _update_status(self, status):
        """Update status in memory only"""
        if status is self.status:
            return False
        self.status = ExtractionStatus(status)
        return True

    # @sync_debug(context="self.content_map.get('source_url')")
    def _execute_in_future(self, func, *args, **kwds):
        """Run in executor with kwds support & default loop/executor"""
        return run_in_executor(self.loop, None, func, *args, **kwds)

    # Initialization Methods

    def _form_file_path(self, base, directory):
        """Form file path by combining base and directory"""
        return os.path.join(base, directory, self.FILE_NAME)

    @lru_cache(maxsize=None, typed=False)
    def _marshall_configuration(self, file_path):
        """Marshall configuration from file, given a file path"""
        with open(file_path) as stream:
            return yaml.safe_load(stream)

    def _configure_delay(self, configuration):
        """Return delay configuration based on defaults and extractor configuration"""
        delay_config = self.DELAY_DEFAULTS._asdict()
        if self.DELAY_TAG not in configuration:
            return delay_config
        delay_overrides = configuration[self.DELAY_TAG]
        unsupported = delay_overrides.keys() - delay_config.keys()
        delay_config.update(delay_overrides)

        if unsupported:
            for key in unsupported:
                del delay_config[key]
            PP.pprint(dict(
                msg='Unsupported keys in delay configuration',
                type='unsupported_keys_in_delay_configuration',
                configuration=delay_overrides, unsupported=unsupported, extractor=repr(self)))

        return delay_config

    @classmethod
    def _derive_web_driver_brand(cls, web_driver_type=None):
        """Derive web driver brand, given a web driver type"""
        if web_driver_type:
            module_path = web_driver_type.__module__
            web_driver_brand_name = module_path.split('.')[-2]
            return cls.WebDriverBrand.cast(web_driver_brand_name)
        return cls.WEB_DRIVER_BRAND_DEFAULT

    @classmethod
    def _derive_web_driver_type(cls, web_driver_brand=None):
        """Derive web driver type, given a web driver brand enum"""
        web_driver_brand = web_driver_brand or cls.WEB_DRIVER_BRAND_DEFAULT
        return getattr(webdriver, web_driver_brand.name.capitalize())

    @classmethod
    def _derive_web_driver_kwargs(cls, web_driver_brand=None):
        """Derive web driver kwargs, given a web driver brand enum"""
        web_driver_brand = web_driver_brand or cls._derive_web_driver_brand()
        secret_service = SecretService(web_driver_brand.name)
        user_agent = secret_service.random
        if web_driver_brand is cls.WebDriverBrand.CHROME:
            chrome_options = webdriver.ChromeOptions()
            # chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument(f'user-agent={user_agent}')
            return dict(chrome_options=chrome_options)
        elif web_driver_brand is cls.WebDriverBrand.FIREFOX:
            raise NotImplementedError('Firefox not yet supported')

    @classmethod
    def _derive_web_driver_info(cls, web_driver_brand=None, web_driver_type=None,
                                web_driver_kwargs=None, web_driver=None):
        """Derive web driver info from any subset of info or web_driver"""
        web_driver_type = type(web_driver) if web_driver else web_driver_type
        web_driver_brand = web_driver_brand or cls._derive_web_driver_brand(web_driver_type)
        web_driver_type = web_driver_type or cls._derive_web_driver_type(web_driver_brand)
        web_driver_kwargs = web_driver_kwargs or cls._derive_web_driver_kwargs(web_driver_brand)
        return cls.WebDriverInfo(web_driver_brand, web_driver_type, web_driver_kwargs)

    def __init__(self, model, directory, web_driver=None, web_driver_brand=None,
                 reuse_web_driver=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.created_timestamp = self.loop.time()
        self.status = None

        self.model = model
        self.base_directory = model.BASE_DIRECTORY
        self.directory = directory
        self.file_path = self._form_file_path(self.base_directory, self.directory)
        self.configuration = self._marshall_configuration(self.file_path)
        self.is_enabled = self.configuration.get(self.IS_ENABLED_TAG, True)
        self.delay_configuration = self._configure_delay(self.configuration)
        self.content_configuration = self.configuration[self.CONTENT_TAG]

        self.web_driver = web_driver
        self.reuse_web_driver = bool(web_driver) if reuse_web_driver is None else reuse_web_driver
        web_driver_info = self._derive_web_driver_info(web_driver_brand, None, None, web_driver)
        self.web_driver_brand = web_driver_info.brand
        self.web_driver_type = web_driver_info.type
        self.web_driver_kwargs = web_driver_info.kwargs

        self.content_map = None  # Temporary storage for extracted fields
        self.extracted_content = None  # Permanent storage for extracted content

    def __repr__(self):
        class_name = self.__class__.__name__
        # getattr in case not yet set, e.g. logging during construction
        directory = getattr(self, 'directory', None)
        created_timestamp = getattr(self, 'created_timestamp', None)
        return (f'<{class_name}: {directory}, {created_timestamp}>')


class SourceExtractor(BaseExtractor):

    FILE_NAME = 'source.yaml'

    HTTPS_TAG = 'https://'
    HTTP_TAG = 'http://'
    WWW_DOT_TAG = 'www.'
    DOMAIN_DELIMITER = '.'
    DIRECTORY_NAME_DELIMITER = '_'
    PATH_DELIMITER = '/'
    QUERY_STRING_DELIMITER = '?'

    @async_debug()
    async def _perform_extraction(self, url=None):
        """Perform extraction by fetching & extracting page given URL"""
        url = url or self.page_url
        await self._perform_page_fetch(url)
        await self._perform_page_extraction()

    @async_debug()
    async def _perform_page_extraction(self, *args, **kwds):
        """Perform page extraction for single content source"""
        try:
            content = await self._extract_content(self.web_driver,
                                                  self.content_configuration,
                                                  source_url=self.page_url)
        except Exception as e:
            PP.pprint(dict(
                msg='Extract content failure', type='extract_content_failure',
                error=e, extractor=repr(self), configuration=self.content_configuration))
            raise
        else:
            self.extracted_content = content
            await self.cache.store_content(content)

    @async_debug()
    @classmethod
    async def extract_in_parallel(cls, model, urls_by_domain, search_domain,
                                  search_web_driver=None, delay_configuration=None, loop=None):
        """
        Extract in parallel

        Extract content in parallel from multiple domains. For  each
        domain, source URLs are extracted in series with delays.

        I/O:
        model:                      Extractable content class

        urls_by_domain:             Dict of URL lists keyed by domain

        search_domain:              Domain of search page for urls

        search_web_driver=None:     Selenium webdriver used by search;
                                    urls matching the search domain use
                                    the search web driver

        delay_configuration=None:   Configuration to stagger extractions

        loop=None:                  Event loop (optional)

        return:                     List of extracted content instances
        """
        web_driver_brand = cls._derive_web_driver_brand(type(search_web_driver))

        futures = [cls.extract_in_series(
                   model=model,
                   urls=urls,
                   web_driver=search_web_driver if domain == search_domain else None,
                   web_driver_brand=web_driver_brand,
                   delay_configuration=delay_configuration,
                   loop=loop)
                   for domain, urls in urls_by_domain.items()]

        if not futures:
            return []
        done, pending = await asyncio.wait(futures)
        series_results = [task.result() for task in done]
        source_results = chain(*series_results)
        return source_results

    @async_debug()
    @classmethod
    async def extract_in_series(cls, model, urls, web_driver=None, web_driver_brand=None,
                                reuse_web_driver=None, delay_configuration=None, loop=None):
        """
        Extract in series

        Extract content from source URLs in series with delays. Used to
        extract multiple sources from the same domain.

        I/O:
        model:                      Extractable content class

        urls:                       List of content URL strings

        web_driver=None:            Selenium webdriver (optional);
                                    if not provided, one is provisioned.

        web_driver_brand=None:      WebDriverBrand, default: CHROME

        reuse_web_driver=None:      If True, driver not released after
                                    series extraction. Default is True
                                    if web driver provided, else False.

        delay_configuration=None:   Configuration to stagger extractions

        loop=None:                  Event loop (optional)

        return:                     List of extracted content instances
        """
        if web_driver:
            reuse_web_driver = reuse_web_driver if reuse_web_driver is not None else True
        else:
            reuse_web_driver = reuse_web_driver or False
            web_driver = await cls._provision_web_driver(web_driver_brand=web_driver_brand,
                                                         loop=loop)
        # TODO: replace with WebDriverPool.provision_web_driver(web_driver) as web_driver:
        # Context manager should set web_driver._should_reuse on web driver returned
        try:
            delay_config = delay_configuration or cls.DELAY_DEFAULTS._asdict()
            source_results = []

            source_extractors = cls.provision_extractors(
                model=model,
                urls=urls,
                web_driver=web_driver,
                reuse_web_driver=True,  # use same web driver for series
                loop=loop)

            for source_extractor in source_extractors:
                await cls._delay_if_necessary(delay_config, web_driver.last_fetch_timestamp)
                source_result = await source_extractor.extract()
                if source_result:
                    source_results.append(source_result)

        finally:
            if not reuse_web_driver:
                await cls._deprovision_web_driver(web_driver=web_driver, loop=loop)

        return source_results

    @async_debug()
    @classmethod
    async def _delay_if_necessary(cls, delay_config, last_fetch_timestamp):
        delay = human_dwell_time(**delay_config)
        now = datetime.datetime.utcnow()
        delta_since_last_fetch = now - last_fetch_timestamp
        elapsed_seconds = delta_since_last_fetch.total_seconds()
        remaining_delay = delay - elapsed_seconds if delay > elapsed_seconds else 0
        if remaining_delay:
            await asyncio.sleep(delay)

    @sync_debug()
    @classmethod
    def provision_extractors(cls, model, urls=None, web_driver=None,
                             web_driver_brand=None, reuse_web_driver=None, loop=None):
        """
        Provision Extractors

        Instantiate and yield source extractors for the given urls.

        I/O:
        model:                  Extractable content class

        urls=None:              List of content URL strings

        web_driver=None:        Selenium webdriver (optional);
                                if not provided, one is provisioned.

        web_driver_brand=None:  WebDriverBrand, default: CHROME

        reuse_web_driver=None:  If True, web driver is not released
                                after extraction. Defaults to True
                                if web driver provided, else False.

        loop=None:              Event loop (optional)

        yield:                  Fully configured source extractors
        """
        for url in urls:
            try:
                extractor = cls(model=model,
                                page_url=url,
                                web_driver=web_driver,
                                web_driver_brand=web_driver_brand,
                                reuse_web_driver=reuse_web_driver,
                                loop=loop)

                if extractor.is_enabled:
                    yield extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

    def _derive_directory(self, model, page_url):
        """Derive directory from base set on model and page URL"""
        base_directory = model.BASE_DIRECTORY
        clipped_url = self._clip_url(page_url)
        url_path = clipped_url.split(self.PATH_DELIMITER)
        base_url = url_path[0]
        base_url_directory = base_url.replace(self.DOMAIN_DELIMITER,
                                              self.DIRECTORY_NAME_DELIMITER)
        path_components = [base_url_directory] + url_path[1:]
        num_components = deepest_index = len(path_components)

        # Find deepest directory
        for i in range(num_components):
            sub_directory = self.PATH_DELIMITER.join(path_components[:i + 1])
            path = os.path.join(base_directory, sub_directory)
            if not Path(path).is_dir():
                deepest_index = i
                break

        # Look for source configuration directory, starting with deepest
        for i in range(deepest_index, 0, -1):
            sub_directory = self.PATH_DELIMITER.join(path_components[:i])
            path = os.path.join(base_directory, sub_directory, self.FILE_NAME)
            if Path(path).is_file():
                return sub_directory

        raise FileNotFoundError(f'Source extractor configuration not found for {page_url}')

    # TODO: rewrite to use urlparse and include www in directories
    def _clip_url(self, url):
        """Clip URL to just contain host and path"""
        start = 0
        if url.startswith(self.HTTPS_TAG):
            start = len(self.HTTPS_TAG)
        elif url.startswith(self.HTTP_TAG):
            start = len(self.HTTP_TAG)
        www_index = url.find(self.WWW_DOT_TAG)
        if www_index == start:
            start += len(self.WWW_DOT_TAG)
        query_index = url.find(self.QUERY_STRING_DELIMITER)
        end = query_index if query_index > -1 else len(url)
        if url[end - 1] == self.PATH_DELIMITER:
            end -= 1
        clipped_url = url[start:end]
        return clipped_url

    def __init__(self, model, page_url, web_driver=None, web_driver_brand=None,
                 reuse_web_driver=None, loop=None):

        page_url = url_normalize(page_url)
        directory = self._derive_directory(model, page_url)

        super().__init__(model=model,
                         directory=directory,
                         web_driver=web_driver,
                         web_driver_brand=web_driver_brand,
                         reuse_web_driver=reuse_web_driver,
                         loop=loop)

        self.page_url = page_url
        self.cache = SourceExtractorCache(model=self.model, loop=self.loop)
        self.status = ExtractionStatus.INITIALIZED


class MultiExtractor(BaseExtractor):

    FILE_NAME = 'multi.yaml'

    # URL keys
    URL_TAG = 'url'
    URL_TEMPLATE_TAG = 'url_template'
    INDEX_TAG = 'index'
    SERIES_TAG = 'series'
    TEMPLATES_TAG = 'templates'
    DELIMITER_TAG = 'delimiter'
    TERM_TAG = 'term'
    TOKEN_TEMPLATE = '{{{}}}'
    URLClauseSeries = FlexEnum('URLClauseSeries', 'TOPIC TERM CUSTOM')

    # Pagination keys
    PAGINATION_TAG = 'pagination'
    PAGES_TAG = 'pages'
    PAGE_SIZE_TAG = 'page_size'
    NEXT_PAGE_TAG = 'next_page'
    NEXT_PAGE_CLICK_TAG = 'next_page_click'
    NEXT_PAGE_URL_TAG = 'next_page_url'

    EXTRACT_SOURCES_TAG = 'extract_sources'
    ITEMS_TAG = 'items'

    @async_debug()
    async def _perform_extraction(self, url=None):
        """
        Perform extraction

        Given a URL, fetch all pages up to the configured maximum and
        extract all available content. As with other "perform" methods,
        content is extracted, but not returned.
        """
        url = url or self.page_url
        await self._perform_page_fetch(url)
        await self._perform_page_extraction(page=1)

        if self.pages > 1:
            via_url = bool(self.next_page_url_configuration)
            more_pages = True
            page = 2

            while more_pages:
                more_pages = await self._perform_next_page_extraction(page, via_url)
                page += 1

    @async_debug()
    async def _perform_next_page_extraction(self, page, via_url):
        """
        Perform next page extraction

        Load and extract next page via next page operation. Loading the
        page involves either clicking a link (i.e. "next") or extracting
        a URL that is subsequently fetched. As with other "perform"
        methods, content is extracted, but not returned.

        I/O:
        page:     Page number of results to be extracted
        via_url:  True if a page must be fetched via an extracted URL
        return:   True if another page should be extracted, else False
        """
        try:
            next_page_result = await self._extract_field(
                self.NEXT_PAGE_TAG, self.web_driver, self.next_page_configuration, page - 1)
        except NoSuchElementException:
            return False  # Last page always fails to find next element

        delay = human_dwell_time(**self.delay_configuration)
        await asyncio.sleep(delay)

        # TODO: Simplify logic by allowing an operation to fetch a page
        if via_url:
            await self._perform_page_fetch(url=next_page_result)

        await self._perform_page_extraction(page=page)
        return page < self.pages

    @async_debug()
    async def _perform_page_extraction(self, page=1):
        """
        Perform page extraction

        Perform extraction of page containing multiple content items. If
        items contain source URLs and the extractor is so configured,
        source pages are extracted as well. Source page content is
        typically more accurate and granular, so such content overrides
        multi-item content on a field by field basis.

        As with other "perform" methods, content is extracted, but not
        returned.

        I/O:
        page=1:  Page number of content search results to be extracted
        """
        content_config = self.content_configuration
        items_config = self.items_configuration
        unique_field = self.model.UNIQUE_FIELD

        elements = await self._extract_field(self.ELEMENTS_TAG, self.web_driver, items_config)

        if elements is not None:
            for index, element in enumerate(elements, start=1):
                rank = (page - 1) * self.page_size + index
                try:
                    content = await self._extract_content(element, content_config, index)

                    unique_key = getattr(content, unique_field)
                    if not unique_key:
                        raise ValueError(f"Content missing value for '{unique_field}'")

                except Exception as e:
                    PP.pprint(dict(
                        msg='Extract content failure', type='extract_content_failure',
                        error=e, page=page, index=index, rank=rank,
                        extractor=repr(self), configuration=content_config))

                if unique_key in self.extracted_content:
                    PP.pprint(dict(
                        msg='Unique key collision', type='unique_key_collision',
                        field=unique_field, unique_key=unique_key,
                        old_content=self.extracted_content[unique_key], new_content=content,
                        page=page, index=index, rank=rank,
                        extractor=repr(self), configuration=content_config))

                # TODO: store all results for a page at once instead of incrementally
                await self.cache.store_search_result(content, rank)
                self.extracted_content[unique_key] = content

        else:
            PP.pprint(dict(
                msg='Extract item results failure', type='extract_item_results_failure',
                extractor=repr(self), configuration=items_config))

        if self.configuration.get(self.EXTRACT_SOURCES_TAG, True):
            if unique_field != self.SOURCE_URL_TAG:
                raise ValueError('Unique field must be '
                                 f"'{self.SOURCE_URL_TAG}' to extract sources")
            # PRELIMINARY
            source_results = await self._extract_sources(self.extracted_content)
            await self._combine_results(self.extracted_content, source_results)

    @async_debug()
    async def _extract_sources(self, extracted_content):
        """Extract sources given extracted content"""
        search_domain = derive_domain(self.page_url)
        source_urls = (content.source_url for content in extracted_content.values())
        urls_by_domain = defaultdict(list)

        for source_url in source_urls:
            source_domain = derive_domain(source_url, base=search_domain)
            urls_by_domain[source_domain].append(source_url)

        for domain, urls in urls_by_domain.items():
            human_selection_shuffle(urls)

        return await SourceExtractor.extract_in_parallel(
            model=self.model,
            urls_by_domain=urls_by_domain,
            search_domain=search_domain,
            search_web_driver=self.web_driver,
            delay_configuration=self.delay_configuration,
            loop=self.loop)

    @async_debug()
    async def _combine_results(self, extracted_content, source_results):
        """Combine results extracted from search with source content"""
        for source_result in source_results:
            source_url = source_result.source_url
            content_result = extracted_content[source_url]
            source_overrides = ((k, v) for k, v in source_result.items() if v is not None)
            for field, source_value in source_overrides:
                item_value = getattr(content_result, field)
                if item_value is not None and item_value != source_value:
                    PP.pprint(dict(
                        msg='Overwriting content field value from source',
                        type='overwriting_content_field_value_from_source',
                        extractor=repr(self), field=field,
                        item_value=item_value, source_value=source_value))

                setattr(content_result, field, source_value)

    # @async_debug(context="self.content_map.get('source_url')")
    async def _update_status(self, status):
        """Update extractor/overall status, caching as necessary"""
        if status.value < self.status.value:
            raise ValueError(f'Invalid status change: {self.status} -> {status}')

        if status is self.status:
            return False

        overall_status, has_changed = await self._apply_status_change(status)
        await self.cache.store_extraction_status(status, overall_status, has_changed)
        return True

    # @async_debug(context="self.content_map.get('source_url')")
    async def _apply_status_change(self, status):
        """Apply status change; return overall status & has_changed"""
        old_overall_status = await self._determine_overall_status()
        if old_overall_status is ExtractionStatus.COMPLETED:
            return ExtractionStatus.COMPLETED, False

        self.status = ExtractionStatus(status)
        new_overall_status = await self._determine_overall_status()
        has_changed = new_overall_status is not old_overall_status
        return new_overall_status, has_changed

    # @async_debug(context="self.content_map.get('source_url')")
    async def _determine_overall_status(self):
        """Determine overall extraction status of the cohort"""
        minimum = min(self.cohort_status_values)
        if ExtractionStatus(minimum) is ExtractionStatus.COMPLETED:
            return ExtractionStatus.COMPLETED

        maximum = max(self.cohort_status_values)
        if maximum >= ExtractionStatus.PRELIMINARY.value:
            return ExtractionStatus.PRELIMINARY

        return ExtractionStatus(maximum)

    @property
    def cohort_status_values(self):
        """Cohort status values are emitted by the returned generator"""
        return (extractor.status.value for extractor in self.cohort.values())

    # @sync_debug()
    @classmethod
    def provision_extractors(cls, model, search_data=None, web_driver=None, web_driver_brand=None,
                             reuse_web_driver=None, loop=None):
        """
        Provision Extractors

        Instantiate and yield all multi extractors configured with the
        given model and search terms. All extractors provisioned
        together are part of the same cohort, a dictionary of extractors
        keyed by directory.

        I/O:
        model:                  Any Extractable (mixin) content class;
                                defines base configuration directory,
                                fields to extract, and unique field.

        search_data=None:       Ordered dictionary of all search terms
                                used to render urls and form hash keys.
                                Keys specify 'topics' whose values are
                                queried via 'AND'. Values are 'terms'
                                that are queried via 'OR' when multiple.

                                Example:
                                    OrderedDict(problem='Homelessness',
                                                org=None,
                                                geo=['Texas', 'TX'])

        web_driver=None:        Selenium webdriver (optional); if not
                                provided, one is provisioned.

        web_driver_brand=None:  WebDriverBrand, default: CHROME

        reuse_web_driver=None:  If True, web driver is not released
                                after extraction. Defaults to True
                                if web driver provided, else False.

        loop=None:              Event loop (optional)

        yield:                  Fully configured search extractors
        """
        base = model.BASE_DIRECTORY
        dir_nodes = os.walk(base)
        directories = (cls._debase_directory(base, dn[0]) for dn in dir_nodes
                       if cls.FILE_NAME in dn[2])

        extractors = {}
        for directory in directories:
            try:
                extractor = cls(model=model,
                                directory=directory,
                                search_data=search_data,
                                web_driver=web_driver,
                                web_driver_brand=web_driver_brand,
                                reuse_web_driver=reuse_web_driver,
                                loop=loop)

                if extractor.is_enabled:
                    extractors[directory] = extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

        for extractor in extractors.values():
            extractor._set_cohort(extractors)
            yield extractor

    @classmethod
    def _debase_directory(cls, base, path):
        """Remove base from directory path"""
        base = os.path.join(base, '')  # Add slash
        if not path.startswith(base):
            raise ValueError(f"'{path}' must start with '{base}'")
        directory = path.replace(base, '', 1)
        return directory

    def _set_cohort(self, extractors):
        """Set cohort to dictionary of extractors keyed by directory"""
        self.cohort = extractors

    @staticmethod
    def _prepare_search_data(search_data):
        """Prepare search terms by ensuring they are an ordered dict"""
        if isinstance(search_data, OrderedDict):
            return search_data
        if search_data is None:
            return OrderedDict()
        return OrderedDict(search_data)

    @sync_debug()
    def _form_page_url(self, configuration, search_data):
        """Form page URL given extractor configuration & search terms"""
        url_config = self.configuration[self.URL_TAG]
        # Support shorthand form for hard-coded urls
        if isinstance(url_config, str):
            return url_config

        encoded_search_data = OrderedDict(self._encode_search_data(k, v)
                                          for k, v in search_data.items())
        url_template = url_config[self.URL_TEMPLATE_TAG]

        return self._form_url_clause(url_template, url_config, encoded_search_data)

    def _encode_search_data(self, key, value):
        """Return key & URL-encoded value, a list of strings or None"""
        if value is None:
            return key, None
        if isinstance(value, str):
            return key, [urllib.parse.quote(value)]
        if isnonstringsequence(value):
            return key, [urllib.parse.quote(v) for v in value]
        raise TypeError(f"Expected string or list/tuple for '{key}'; "
                        f"received '{type(value)}': {value}")

    def _form_url_clause(self, template, url_config, search_data, topic=None, term=None, index=1):
        """
        Form URL clause

        Form URL clause by recursing depth-first to replace template
        tokens via URL configuration, search data & clause index.

        I/O:
        template:     string with 1+ tokens specified via {}
        url_config:   value of 'url' key in configuration
        search_data:  encoded search data, an ordered dict
        topic=None:   search data key to specify any terms
        term=None:    search data term
        index=1:      index specified by a URL clause series
        return:       rendered URL template
        raise:        NoneValueError if token value is None term
                      ValueError if unknown token
                      TypeError if unexpected type in configuration
        """
        rendered = template
        tokens = self._find_tokens(template)
        for token in tokens:
            if token == self.INDEX_TAG:
                value = str(index)

            elif token in search_data or token == self.TERM_TAG:
                terms = self._get_relevant_search_terms(token, topic, search_data)
                term_index = index - 1 if term else 0
                value = terms[term_index]

            elif token in url_config:
                token_config = url_config[token]

                if isinstance(token_config, str):
                    value = self._form_url_clause(
                        token_config, url_config, search_data, topic, term, index)

                elif isinstance(token_config, dict):
                    value = self._form_url_clause_series(
                        token_config, url_config, search_data, topic)

                else:
                    raise TypeError(f"Expected str or dict for '{token}'; "
                                    f"received '{type(series_config)}': {series_config}")
            else:
                raise ValueError(f'Unknown token: {token}')

            rendered = rendered.replace(self.TOKEN_TEMPLATE.format(token), value)

        return rendered

    def _form_url_clause_series(self, series_config, url_config, search_data, topic=None):
        """Form URL clause series based on series configuration"""
        series = self.URLClauseSeries[series_config[self.SERIES_TAG]]
        templates = enlist(series_config[self.TEMPLATES_TAG])
        delimiter = series_config[self.DELIMITER_TAG]
        iterable = search_data if series is self.URLClauseSeries.TOPIC else templates

        if series is self.URLClauseSeries.TERM:
            token = one(token for token in self._find_tokens(templates[0])
                        if token in search_data or token == self.TERM_TAG)
            iterable = self._get_relevant_search_terms(token, topic, search_data)

        clauses = []
        index = 1
        for i, value in enumerate(iterable):
            template = templates[i] if i < len(templates) else templates[-1]
            topic = value if series is self.URLClauseSeries.TOPIC else topic
            term = value if series is self.URLClauseSeries.TERM else None
            try:
                clause = self._form_url_clause(
                    template, url_config, search_data, topic, term, index)
            except NoneValueError:
                continue  # skip clauses with null search topics
            else:
                clauses.append(clause)
                index += 1

        return delimiter.join(clauses)

    def _find_tokens(self, template):
        """Find & yield tokens in template as defined by {}"""
        length = len(template)
        start = end = -1
        while end < length:
            start = template.find('{', start + 1)
            if start == -1:
                break
            end = template.find('}', start + 1)
            if end == -1:
                break
            # tokens may not contain tokens, so find innermost
            new_start = template.rfind('{', start + 1, end)
            if new_start > start:
                start = new_start
            yield template[start + 1:end]

    def _get_relevant_search_terms(self, token, topic, search_data):
        """Get relevant search terms based on token and topic"""
        topic = token if token in search_data else topic
        terms = search_data[topic]
        if terms is None:
            raise NoneValueError
        return terms

    def __init__(self, model, directory, search_data=None, web_driver=None, web_driver_brand=None,
                 reuse_web_driver=None, loop=None):

        super().__init__(model=model,
                         directory=directory,
                         web_driver=web_driver,
                         web_driver_brand=web_driver_brand,
                         reuse_web_driver=reuse_web_driver,
                         loop=loop)

        self.search_data = self._prepare_search_data(search_data)
        self.cache = MultiExtractorCache(directory=self.directory, search_data=self.search_data,
                                         model=self.model, loop=self.loop)
        self.page_url = self._form_page_url(self.configuration, self.search_data)

        pagination_config = self.configuration.get(self.PAGINATION_TAG)

        if pagination_config:
            self.pages = pagination_config.get(self.PAGES_TAG, float('Inf'))
            self.page_size = pagination_config[self.PAGE_SIZE_TAG]
            self.next_page_click_configuration = pagination_config.get(self.NEXT_PAGE_CLICK_TAG)
            self.next_page_url_configuration = pagination_config.get(self.NEXT_PAGE_URL_TAG)
            self.next_page_configuration = xor_constrain(
                self.next_page_click_configuration, self.next_page_url_configuration)
        else:
            self.pages = 1
            self.page_size = self.next_page_configuration = None
            self.next_page_click_configuration = self.next_page_url_configuration = None

        self.items_configuration = self.configuration[self.ITEMS_TAG]
        self.extracted_content = OrderedDict()

        self.cohort = None  # Only set after instantiation
        self.status = ExtractionStatus.INITIALIZED

    def __repr__(self):
        class_name = self.__class__.__name__
        directory = getattr(self, 'directory', None)
        search_data = getattr(self, 'search_data', None)
        created_timestamp = getattr(self, 'created_timestamp', None)
        return (f'<{class_name} | {directory} | {search_data!r} | {created_timestamp}>')
