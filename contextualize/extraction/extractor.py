#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import datetime
import os
import urllib
from collections import OrderedDict, defaultdict, namedtuple
from itertools import chain
from pathlib import Path

from ruamel import yaml
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from url_normalize import url_normalize

from contextualize.content.base import Hashable
from contextualize.exceptions import NoneValueError
from contextualize.extraction.caching import MultiExtractorCache, SourceExtractorCache
from contextualize.extraction.configuration import (
    DelayConfiguration, ExtractorConfiguration, MultiExtractorConfiguration,
    SourceExtractorConfiguration
)
from contextualize.extraction.definitions import ExtractionStatus
from contextualize.extraction.info import ExtractionInfo
from contextualize.extraction.operation import ExtractionOperation
from contextualize.services.secret_service.agency import SecretService
from contextualize.utils.asynchronous import run_in_executor
from contextualize.utils.cache import FileCache
from contextualize.utils.context import FlexContext
from contextualize.utils.debug import debug
from contextualize.utils.enum import FlexEnum
from contextualize.utils.iterable import one
from contextualize.utils.statistics import (
    human_dwell_time, human_selection_shuffle
)
from contextualize.utils.time import GranularDateTime
from contextualize.utils.tools import (
    PP, derive_domain, enlist, is_nonstring_sequence, xor_constrain
)


class BaseExtractor:

    FILE_NAME = NotImplementedError

    SOURCE_URL_TAG = 'source_url'

    WebDriverBrand = FlexEnum('WebDriverBrand', 'CHROME FIREFOX')
    WEB_DRIVER_BRAND_DEFAULT = WebDriverBrand.CHROME

    WebDriverInfo = namedtuple('WebDriverInfo', 'brand type kwargs')

    Status = ExtractionStatus

    configuration_file_cache = FileCache(maxsize=None)

    @debug
    async def extract(self):
        """
        Extract

        Extract content via the configured extractor. Return content if
        extractor is enabled and extraction is successful, else None.
        """
        if not self.configuration.is_enabled:
            PP.pprint(dict(
                msg='WARNING: extracting with disabled extractor',
                type='extractor_disabled_warning', extractor=repr(self)))
            return

        if await self._load_cached_content():
            return self.extracted_content

        # TODO: Add WebDriverPool context manager to acquire/release
        try:
            if not self.web_driver:
                await self._acquire_web_driver()
            await self._handle_extraction_start()
            await self._perform_extraction(self.page_url)
            await self._update_status(ExtractionStatus.COMPLETED)
            return self.extracted_content

        finally:
            if not self.reuse_web_driver:
                await self._release_web_driver()

    @debug
    async def _acquire_web_driver(self):
        """Acquire web driver"""
        self.web_driver = await self._provision_web_driver(
            web_driver_type=self.web_driver_type,
            web_driver_kwargs=self.web_driver_kwargs,
            implicit_wait=self.configuration.implicit_wait,
            loop=self.loop)

    @debug
    async def _release_web_driver(self):
        """Release web driver"""
        await self._deprovision_web_driver(web_driver=self.web_driver, loop=self.loop)

    @classmethod
    @debug
    async def _provision_web_driver(cls, web_driver_brand=None, web_driver_type=None,
                                    web_driver_kwargs=None, implicit_wait=None, loop=None):
        """Provision web driver"""
        loop = loop or asyncio.get_event_loop()
        _, web_driver_type, web_driver_kwargs = cls._derive_web_driver_info(
            web_driver_brand, web_driver_type, web_driver_kwargs)

        web_driver = await run_in_executor(loop, None, web_driver_type, **web_driver_kwargs)
        # Configure web driver to allow waiting on each operation
        implicit_wait = (ExtractorConfiguration.IMPLICIT_WAIT_DEFAULT if implicit_wait is None
                         else implicit_wait)
        web_driver.implicitly_wait(implicit_wait)
        web_driver.last_fetch_timestamp = None
        return web_driver

    @classmethod
    @debug
    async def _deprovision_web_driver(cls, web_driver, loop=None):
        """Deprovision web driver"""
        loop = loop or asyncio.get_event_loop()
        if web_driver:
            await run_in_executor(loop, None, web_driver.quit)

    @debug
    async def _perform_page_fetch(self, url):
        """Perform page fetch of given URL by running in executor"""
        future_page = self._execute_in_future(self.web_driver.get, url)
        self.web_driver.last_fetch_timestamp = datetime.datetime.utcnow()
        await future_page

    @debug
    async def _extract_content(self, element, index=1, **kwds):
        """
        Extract content

        Extract content from the given web element and configuration.

        I/O:
        element:    Selenium web driver or element
        index=1:    Index of given element within a series
        **kwds:     Additional keyword args passed to content
        return:     Instance of content model (e.g. ResearchArticle)
        """
        self.content_map = content_map = OrderedDict(kwds)
        source_url = content_map.get(self.SOURCE_URL_TAG)
        if not source_url:
            source_url = await self._extract_content_field(field=self.SOURCE_URL_TAG,
                                                           element=element,
                                                           index=index)

        with FlexContext(source_url=source_url):
            # Allow field to be otherwise set without overwriting
            fields_to_extract = (f for f in self.model.field_names() if f not in content_map)
            for field in fields_to_extract:
                await self._extract_content_field(field=field, element=element, index=index)

        self.content_map = None
        instance = self.model(**content_map)
        return instance

    @debug()
    async def _extract_content_field(self, field, element, index=1):
        content_configuration = self.configuration.content
        try:
            field_configuration = content_configuration[field]

        except KeyError as e:
            self.content_map[field] = None
            PP.pprint(dict(
                msg='Extract field configuration missing',
                type='extract_field_configuration_missing',
                error=e, field=field, content_map=self.content_map, extractor=repr(self)))
        else:
            try:
                field_value = await self._extract_field(field=field,
                                                        element=element,
                                                        configuration=field_configuration,
                                                        index=index)
                self.content_map[field] = field_value
                return field_value

            except Exception as e:  # e.g. NoSuchElementException
                PP.pprint(dict(
                    msg='Extract field failure', type='extract_field_failure',
                    error=e, field=field, content_map=self.content_map, extractor=repr(self)))
                # TODO: re-raise if required field

    @debug
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
        if isinstance(configuration, ExtractionOperation):
            return await configuration.execute(target=element, index=index)
        if isinstance(configuration, list):
            return await self._execute_operation_series(
                target=element, configuration=configuration, index=index)
        return configuration

    # @debug
    async def _execute_operation_series(self, target, configuration, index=1):
        """
        Execute operation series

        Execute series of operations to extract a value based on the
        target and configuration.

        I/O:
        target:         Selenium web driver or element
        configuration:  A list of operation dictionaries
        index=1:        Index of given content element within a series
        return:         Extracted value
        """
        latest = prior = parent = target
        for operation in configuration:
            new_targets = operation._select_targets(latest, prior, parent)
            prior = latest
            if operation.is_multiple:
                latest = await operation.execute(new_targets, index)
            else:
                for new_target in new_targets:
                    latest = await operation.execute(new_target, index)
        return latest

    async def _load_cached_content(self):
        """Load cached content"""
        raise NotImplementedError

    @debug
    def should_use_cached_content(self, info):
        """Determine if cached content is available, fresh, and valid given extraction info"""
        status, last_extracted, cache_version = info.status, info.last_extracted, info.cache_version

        if not self.use_cache or not last_extracted:
            return False
        if not status or status < ExtractionStatus.PRELIMINARY:
            return False

        now = datetime.datetime.utcnow()
        freshness = now - last_extracted
        return (freshness.days < self.configuration.freshness_threshold and
                cache_version == self.configuration.cache_version)

    async def _handle_extraction_start(self, status=ExtractionStatus.INITIATED):
        """Handle extraction start by updating extraction status"""
        await self._update_status(status)

    async def _update_status(self, status):
        """Update status in memory only"""
        if status is self.status:
            return False
        self.status = ExtractionStatus(status)
        return True

    def _execute_in_future(self, func, *args, **kwds):
        """Run in executor with kwds support & default loop/executor"""
        return run_in_executor(self.loop, None, func, *args, **kwds)

    # Initialization Methods

    def _form_file_path(self, base, directory):
        """Form file path by combining base, directory, and file name"""
        return os.path.join(base, directory, self.FILE_NAME)

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
            options = webdriver.ChromeOptions()
            # Waits for js loads on click are unreliable when headless
            # options.add_argument('--headless')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-infobars')
            options.add_argument(f'user-agent={user_agent}')
            return dict(options=options)
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
                 reuse_web_driver=None, use_cache=True, loop=None):

        self.loop = loop or asyncio.get_event_loop()
        self.created_timestamp = self.loop.time()
        self.use_cache = use_cache
        self.status = None

        self.model = model
        self.base_directory = model.PROVIDER_DIRECTORY
        self.directory = directory
        self.file_path = self._form_file_path(self.base_directory, self.directory)
        self.configuration = ExtractorConfiguration.from_file(file_path=self.file_path,
                                                              extractor=self)

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

    FILE_NAME = SourceExtractorConfiguration.FILE_NAME

    HTTPS_TAG = 'https://'
    HTTP_TAG = 'http://'
    WWW_DOT_TAG = 'www.'
    DOMAIN_DELIMITER = '.'
    DIRECTORY_NAME_DELIMITER = '_'
    PATH_DELIMITER = '/'
    QUERY_STRING_DELIMITER = '?'

    async def extract(self):
        """Extract within source extractor context"""
        with FlexContext(provider_directory=self.directory, source_url=self.page_url):
            return await super().extract()

    async def _load_cached_content(self):
        """Load cached content"""
        if not self.use_cache:
            return False
        content = await self.cache.retrieve_content_item()
        if content:
            info = ExtractionInfo.from_content(content)
            if self.should_use_cached_content(info):
                self.extracted_content = content
                return True

        return False

    @debug
    async def _perform_extraction(self, url=None):
        """Perform extraction by fetching & extracting page given URL"""
        url = url or self.page_url
        await self._perform_page_fetch(url)
        await self._perform_page_extraction()

    @debug
    async def _perform_page_extraction(self):
        """Perform page extraction for single content source"""
        try:
            content = await self._extract_content(element=self.web_driver,
                                                  source_url=self.page_url,
                                                  rank=None)
        except Exception as e:
            PP.pprint(dict(
                msg='Extract content failure', type='extract_content_failure',
                error=e, extractor=repr(self), configuration=self.configuration.content))
            raise
        else:
            self.extracted_content = content
            if self.use_cache:
                await self.cache.store_content_item(content)

    @classmethod
    @debug
    async def extract_in_parallel(cls, model, urls_by_domain, search_domain,
                                  search_web_driver, use_cache=True, loop=None):
        """
        Extract in parallel

        Extract content in parallel from multiple domains. For  each
        domain, source URLs are extracted in series with delays.

        I/O:
        model:                  Extractable content class

        urls_by_domain:         Dict of URL lists keyed by domain

        search_domain:          Domain of search page for urls

        search_web_driver:      Selenium webdriver used by search; used
                                to fetch urls matching the search domain

        use_cache=True:         If True (default), cache results and
                                check for previously cached results.

        loop=None:              Event loop (optional)

        return:                 List of extracted content instances
        """
        web_driver_brand = cls._derive_web_driver_brand(type(search_web_driver))

        futures = [cls.extract_in_series(
                   model=model,
                   urls=urls,
                   web_driver=search_web_driver if domain == search_domain else None,
                   web_driver_brand=web_driver_brand,
                   use_cache=use_cache,
                   loop=loop)
                   for domain, urls in urls_by_domain.items()]

        if not futures:
            return []
        done, pending = await asyncio.wait(futures)
        series_results = [task.result() for task in done]
        source_results = chain(*series_results)
        return source_results

    @classmethod
    @debug
    async def extract_in_series(cls, model, urls, web_driver=None, web_driver_brand=None,
                                reuse_web_driver=None, use_cache=True, loop=None):
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

        use_cache=True:             If True (default), cache results and
                                    check for previously cached results.

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
            source_results = []

            source_extractors = cls.provision_extractors(
                model=model,
                urls=urls,
                web_driver=web_driver,
                reuse_web_driver=True,  # use same web driver for series
                use_cache=use_cache,
                loop=loop)

            for source_extractor in source_extractors:
                source_result = await source_extractor.extract()
                if source_result:
                    source_results.append(source_result)

        finally:
            if not reuse_web_driver:
                await cls._deprovision_web_driver(web_driver=web_driver, loop=loop)

        return source_results

    @debug
    async def _handle_extraction_start(self):
        """Handle extraction start by updating extraction status and delaying if necessary"""
        await super()._handle_extraction_start()
        await self._delay_if_necessary()

    @debug
    async def _delay_if_necessary(self):
        last_fetch_timestamp = self.web_driver.last_fetch_timestamp
        if not last_fetch_timestamp:  # no delay the first time
            return
        delay = self.configuration.delay.random_delay()
        now = datetime.datetime.utcnow()
        delta_since_last_fetch = now - last_fetch_timestamp
        elapsed_seconds = delta_since_last_fetch.total_seconds()
        remaining_delay = delay - elapsed_seconds if delay > elapsed_seconds else 0
        if remaining_delay:
            await asyncio.sleep(remaining_delay)

    @classmethod
    @debug
    def provision_extractors(cls, model, urls=None, web_driver=None,
                             web_driver_brand=None, reuse_web_driver=None,
                             use_cache=True, loop=None):
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

        use_cache=True:         If True (default), cache results and
                                check for previously cached results.

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
                                use_cache=use_cache,
                                loop=loop)

                if extractor.configuration.is_enabled:
                    yield extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

    def _derive_directory(self, model, page_url):
        """Derive directory from base set on model and page URL"""
        base_directory = model.PROVIDER_DIRECTORY
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

    # TODO: rewrite to use urlparse and include www in directories?
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
                 reuse_web_driver=None, use_cache=True, loop=None):

        self.page_url = url_normalize(page_url)
        directory = self._derive_directory(model, page_url)

        with FlexContext(provider_directory=directory, source_url=self.page_url):

            super().__init__(model=model,
                             directory=directory,
                             web_driver=web_driver,
                             web_driver_brand=web_driver_brand,
                             reuse_web_driver=reuse_web_driver,
                             use_cache=use_cache,
                             loop=loop)

            self.cache = SourceExtractorCache(source_url=self.page_url,
                                              cache_version=self.configuration.cache_version,
                                              loop=self.loop) if self.use_cache else None


class MultiExtractor(BaseExtractor):

    FILE_NAME = MultiExtractorConfiguration.FILE_NAME

    async def extract(self):
        """Extract within multi-extractor context"""
        with FlexContext(provider_directory=self.directory, search_data=self.search_data):
            return await super().extract()

    @debug
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

        if self.configuration.pagination.pages > 1:
            more_pages = True
            page = 2

            while more_pages:
                more_pages = await self._perform_next_page_extraction(page)
                page += 1

    @debug
    async def _perform_next_page_extraction(self, page):
        """
        Perform next page extraction

        Load and extract next page via next page configuration. Loading
        the page involves either clicking a link (e.g. "next") or
        extracting a URL that is subsequently fetched. As with other
        "perform" methods, content is extracted, but not returned.

        I/O:
        page:     Page number of results to be extracted
        return:   True if another page should be extracted, else False
        """
        pagination_configuration = self.configuration.pagination
        try:
            next_page_result = await self._extract_field(
                field=pagination_configuration.next_page_tag,
                element=self.web_driver,
                configuration=pagination_configuration.next_page,
                index=page - 1)

        except NoSuchElementException:
            return False  # Last page always fails to find next element

        delay = self.configuration.delay.random_delay()
        await asyncio.sleep(delay)

        # TODO: Simplify logic by allowing an operation to fetch a page?
        if pagination_configuration.via_url:
            await self._perform_page_fetch(url=next_page_result)

        await self._perform_page_extraction(page=page)
        return page < pagination_configuration.pages

    @debug
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
        elements = await self._extract_field(field=self.configuration.CONTENT_ITEMS_TAG,
                                             element=self.web_driver,
                                             configuration=self.configuration.content_items)

        if elements is None:
            PP.pprint(dict(
                msg='Extract content items failure', type='extract_content_items_failure',
                extractor=repr(self), configuration=self.configuration.content_items))
            return

        extract_sources = self.configuration.extract_sources

        for index, element in enumerate(elements, start=1):
            rank = (page - 1) * self.configuration.pagination.page_size + index
            try:
                content = await self._extract_content(element, index, rank=rank)
                source_url = content.source_url
                if not source_url:
                    raise ValueError(f"Content missing source_url")

            except Exception as e:
                PP.pprint(dict(
                    msg='Extract content failure', type='extract_content_failure',
                    error=e, page=page, index=index, rank=rank, extractor=repr(self)))
                continue

            if source_url in self.extracted_content:
                PP.pprint(dict(
                    msg='Source url collision; keeping new content', type='source_url_collision',
                    source_url=source_url, old_content=self.extracted_content[source_url],
                    new_content=content, page=page, index=index, rank=rank, extractor=repr(self)))

            # TODO: store all page results at once instead of incrementally?
            if self.use_cache:
                await self.cache.store_extraction_result(
                    content=content, rank=rank, store_content=not extract_sources)

            self.extracted_content[source_url] = content

        if extract_sources:
            await self._update_status(ExtractionStatus.PRELIMINARY)
            source_results = await self._extract_sources(self.extracted_content)
            await self._combine_results(self.extracted_content, source_results)

    @debug
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
            use_cache=self.use_cache,
            loop=self.loop)

    @debug
    async def _combine_results(self, extracted_content, source_results):
        """Combine results extracted from search with source content"""
        for source_result in source_results:
            source_url = source_result.source_url
            content_result = extracted_content[source_url]
            source_overrides = ((k, v) for k, v in source_result.field_items() if v is not None)
            for field, source_value in source_overrides:
                item_value = getattr(content_result, field)
                if item_value is not None and item_value != source_value:
                    PP.pprint(dict(
                        msg='Overwriting content field value from source',
                        type='overwriting_content_field_value_from_source',
                        extractor=repr(self), field=field,
                        item_value=item_value, source_value=source_value))

                setattr(content_result, field, source_value)

    @debug
    async def _load_cached_content(self):
        """Load cached content, returning True if available and fresh"""
        if not self.use_cache:
            return False

        info = await self.cache.retrieve_extraction_info()

        if self.should_use_cached_content(info):
            self.extracted_content = await self.cache.retrieve_extraction_results()
            return True
        return False

    @debug
    async def _handle_extraction_start(self):
        """Handle extraction start by updating extraction status"""
        status = ExtractionStatus.INITIATED
        if self.use_cache:
            info = await self.cache.retrieve_extraction_info()
            if not info.status or info.status >= ExtractionStatus.PRELIMINARY:
                status = ExtractionStatus.PRELIMINARY
        await super()._handle_extraction_start(status)

    @debug
    async def _update_status(self, status):
        """Update extractor/overall status, caching as necessary"""
        updated = await super()._update_status(status)
        if not updated:
            return False
        if self.use_cache:
            await self.cache.store_extraction_info(status)
        return True

    @classmethod
    @debug
    def provision_extractors(cls, model, search_data=None, web_driver=None, web_driver_brand=None,
                             reuse_web_driver=None, use_cache=True, loop=None):
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

        use_cache=True:         If True (default), cache results and
                                check for previously cached results.

        loop=None:              Event loop (optional)

        yield:                  Fully configured search extractors
        """
        base = model.PROVIDER_DIRECTORY
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
                                use_cache=use_cache,
                                loop=loop)

                if extractor.configuration.is_enabled:
                    extractors[directory] = extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

        for extractor in extractors.values():
            extractor._set_cohort(extractors)
            yield extractor

    def _set_cohort(self, extractors):
        """Set cohort to dictionary of extractors keyed by directory"""
        self.cohort = extractors

    @property
    def cohort_status_values(self):
        """Cohort status values are emitted by the returned generator"""
        return (extractor.status.value for extractor in self.cohort.values())

    @classmethod
    def _debase_directory(cls, base, path):
        """Remove base from directory path"""
        base = os.path.join(base, '')  # Add slash
        if not path.startswith(base):
            raise ValueError(f"'{path}' must start with '{base}'")
        directory = path.replace(base, '', 1)
        return directory

    @staticmethod
    def _prepare_search_data(search_data):
        """Prepare search terms by ensuring they are an ordered dict"""
        if isinstance(search_data, OrderedDict):
            return search_data
        if search_data is None:
            return OrderedDict()
        return OrderedDict(search_data)

    def __init__(self, model, directory, search_data=None, web_driver=None, web_driver_brand=None,
                 reuse_web_driver=None, use_cache=True, loop=None):

        self.search_data = self._prepare_search_data(search_data)

        with FlexContext(provider_directory=directory,
                         search_data=self.search_data):

            super().__init__(model=model,
                             directory=directory,
                             web_driver=web_driver,
                             web_driver_brand=web_driver_brand,
                             reuse_web_driver=reuse_web_driver,
                             use_cache=use_cache,
                             loop=loop)

            self.page_url = self.configuration.url.construct(self.search_data)
            self.cache = MultiExtractorCache(directory=self.directory,
                                             search_data=self.search_data,
                                             cache_version=self.configuration.cache_version,
                                             loop=self.loop) if self.use_cache else None

            self.extracted_content = OrderedDict()
            self.cohort = None  # Only set after instantiation

    def __repr__(self):
        class_name = self.__class__.__name__
        directory = getattr(self, 'directory', None)
        search_data = getattr(self, 'search_data', None)
        created_timestamp = getattr(self, 'created_timestamp', None)
        return (f'<{class_name} | {directory} | {search_data!r} | {created_timestamp}>')
