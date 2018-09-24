#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import os
import urllib
from collections import OrderedDict
from functools import lru_cache
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
from utils.statistics import HumanDwellTime, human_dwell_time, human_selection_shuffle
from utils.tools import PP, enlist, isnonstringsequence, one, xor_constrain


class BaseExtractor:

    FILE_NAME = NotImplementedError

    CONTENT_TAG = 'content'
    DELAY_TAG = 'delay'
    ELEMENTS_TAG = 'elements'
    IS_ENABLED_TAG = 'is_enabled'
    SOURCE_URL_TAG = 'source_url'
    WAIT_TAG = 'wait'

    WebDriverType = FlexEnum('WebDriverType', 'CHROME FIREFOX')
    WEB_DRIVER_TYPE_DEFAULT = WebDriverType.CHROME

    DELAY_DEFAULTS = HumanDwellTime(
        mu=0, sigma=0.5, base=1, multiplier=1, minimum=1, maximum=3)

    Status = ExtractionStatus

    @async_debug()
    async def extract(self):
        if not self.is_enabled:
            PP.pprint(dict(
                msg='WARNING: extracting with disabled extractor',
                type='extractor_disabled_warning', extractor=repr(self)))
            return
        try:
            await self._provision_web_driver()
            await self._update_status(ExtractionStatus.STARTED)
            return await self._perform_extraction(self.page_url)
        finally:
            await self._update_status(ExtractionStatus.COMPLETED)
            await self._dispose_web_driver()

    # @async_debug()
    async def _provision_web_driver(self):
        # TODO: retrieve from web driver pool
        future_web_driver = self._execute_in_future(self.web_driver_class,
                                                    **self.web_driver_kwargs)
        self.web_driver = await future_web_driver
        max_implicit_wait = self.configuration.get(self.WAIT_TAG, settings.WAIT_MAXIMUM_DEFAULT)
        # Configure web driver to allow waiting on each operation
        self.web_driver.implicitly_wait(max_implicit_wait)

    # @async_debug()
    async def _perform_extraction(self, url=None, page=1):
        if url:
            await self._fetch_page(url)
        return await self._extract_page(page=page)

    # @async_debug()
    async def _fetch_page(self, url):
        future_page = self._execute_in_future(self.web_driver.get, url)
        await future_page

    # @async_debug()
    async def _extract_page(self, *args, **kwds):
        raise NotImplementedError

    # @async_debug()
    async def _dispose_web_driver(self):
        if self.web_driver:
            future_web_driver_quit = self._execute_in_future(self.web_driver.quit)
            await future_web_driver_quit

    # @async_debug()
    async def _extract_content(self, element, configuration, index=1, **kwds):
        self.content_map = content_map = OrderedDict(kwds)
        for field in self.model.fields():
            try:
                field_config = configuration[field]

            except KeyError as e:
                field_config = None
                PP.pprint(dict(
                    msg='Extract field configuration missing', type='extract_field_config_missing',
                    error=e, field=field, content_map=content_map, extractor=repr(self)))

            # Allow field to be set by other field without overwriting
            if field_config is None and content_map.get(field):
                continue

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
        source = self.content_map.get(self.model.UNIQUE_FIELD) if self.content_map else None
        if isinstance(configuration, list):
            return await self._perform_operation_series(
                field, source, element, configuration, index)
        if isinstance(configuration, dict):
            return await self._perform_operation(
                field, source, element, configuration, index)
        return configuration

    # @async_debug()
    async def _perform_operation_series(self, field, source, target, configuration, index=1):
        latest = prior = parent = target
        for op_config in configuration:
            operation = ExtractionOperation.from_configuration(op_config, field, source, self)
            new_targets = operation._select_targets(latest, prior, parent)
            prior = latest
            if operation.is_multiple:
                latest = await operation.perform(new_targets, index)
            else:
                for new_target in new_targets:
                    latest = await operation.perform(new_target, index)
        return latest

    # @async_debug()
    async def _perform_operation(self, field, source, target, configuration, index=1):
        operation = ExtractionOperation.from_configuration(configuration, field, source, self)
        return await operation.perform(target, index)

    # @async_debug(context="self.content_map.get('source_url')")
    async def _update_status(self, status):
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
        return os.path.join(base, directory, self.FILE_NAME)

    @lru_cache(maxsize=None, typed=False)
    def _marshall_configuration(self, file_path):
        with open(file_path) as stream:
            return yaml.safe_load(stream)

    def _configure_delay(self, configuration):
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

    def _derive_web_driver_class(self, web_driver_type):
        return getattr(webdriver, web_driver_type.name.capitalize())

    def _derive_web_driver_kwargs(self, web_driver_type):
        secret_service = SecretService(web_driver_type.name)
        user_agent = secret_service.random
        if web_driver_type is self.WebDriverType.CHROME:
            chrome_options = webdriver.ChromeOptions()
            # chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument(f'user-agent={user_agent}')
            return dict(chrome_options=chrome_options)
        elif web_driver_type is self.WebDriverType.FIREFOX:
            raise NotImplementedError('Firefox not yet supported')

    def __init__(self, model, directory, web_driver_type=None, loop=None):
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

        self.web_driver_type = web_driver_type or self.WEB_DRIVER_TYPE_DEFAULT
        self.web_driver_class = self._derive_web_driver_class(self.web_driver_type)
        self.web_driver_kwargs = self._derive_web_driver_kwargs(self.web_driver_type)
        self.web_driver = None

        self.content_map = None  # Temporary storage for content being extracted

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
    async def extract(self):
        if self.initial_delay:
            await asyncio.sleep(self.initial_delay)
        return await super().extract()

    # @async_debug()
    async def _extract_page(self, *args, **kwds):
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
            await self.cache.store_content(content)
            return content

    # @sync_debug()
    @classmethod
    def provision_extractors(cls, model, urls=None, delay_configuration=None,
                             web_driver_type=None, loop=None):
        """
        Provision Extractors

        Instantiate and yield source extractors for the given urls.

        I/O:
        model:                      Extractable content class
        urls=None:                  List of content URL strings
        delay_configuration=None:   Configuration to stagger extractions
        web_driver_type=None:       WebDriverType, e.g. CHROME (default)
        loop=None:                  Event loop (optional)
        yield:                      Fully configured source extractors
        """
        loop = loop or asyncio.get_event_loop()
        web_driver_type = web_driver_type or cls.WEB_DRIVER_TYPE_DEFAULT

        human_selection_shuffle(urls)
        delay_config = delay_configuration or cls.DELAY_DEFAULTS._asdict()
        initial_delay = 0
        for url in urls:
            try:
                initial_delay += human_dwell_time(**delay_config)
                extractor = cls(model, page_url=url, initial_delay=initial_delay,
                                web_driver_type=web_driver_type, loop=loop)
                if extractor.is_enabled:
                    yield extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

    def _derive_directory(self, model, page_url):
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

    def _clip_url(self, url):
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

    def __init__(self, model, page_url, initial_delay=0, web_driver_type=None, loop=None):
        self.initial_delay = initial_delay
        self.page_url = url_normalize(page_url)
        directory = self._derive_directory(model, self.page_url)
        super().__init__(model, directory, web_driver_type, loop)
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

    # @async_debug()
    async def _perform_extraction(self, url=None):
        url = url or self.page_url
        results = await super()._perform_extraction(url, page=1)

        pagination_config = self.configuration.get(self.PAGINATION_TAG)
        if not pagination_config:
            return results

        if self.pages < 2:
            return results

        next_page_via_url = bool(self.next_page_url_configuration)
        updated_results = await self._extract_following_pages(
            self.next_page_configuration, next_page_via_url)

        return updated_results if updated_results else results

    # @async_debug()
    async def _extract_following_pages(self, configuration, via_url=False):
        updated_results = None
        page, pages = 1, self.pages
        while page < pages:
            try:
                next_page_result = await self._extract_field(
                    self.NEXT_PAGE_TAG, self.web_driver, configuration, page)
            except NoSuchElementException:
                break  # Last page always fails to find next element

            page += 1
            delay = human_dwell_time(**self.delay_configuration)
            await asyncio.sleep(delay)
            url = next_page_result if via_url else None
            updated_results = await super()._perform_extraction(url, page=page)

        return updated_results

    # @async_debug()
    async def _extract_page(self, page=1):
        return await self._extract_multiple(page=page)

    # @async_debug()
    async def _extract_multiple(self, page=1):
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

                if unique_key in self.item_results:
                    PP.pprint(dict(
                        msg='Unique key collision', type='unique_key_collision',
                        field=unique_field, unique_key=unique_key,
                        old_content=self.item_results[unique_key], new_content=content,
                        page=page, index=index, rank=rank,
                        extractor=repr(self), configuration=content_config))

                # TODO: store all results for a page at once instead of incrementally
                await self.cache.store_search_result(content, rank)
                self.item_results[unique_key] = content

        else:
            PP.pprint(dict(
                msg='Extract item results failure', type='extract_item_results_failure',
                extractor=repr(self), configuration=items_config))

        if self.configuration.get(self.EXTRACT_SOURCES_TAG, True):
            if unique_field != self.SOURCE_URL_TAG:
                raise ValueError('Unique field must be '
                                 f"'{self.SOURCE_URL_TAG}' to extract sources")
            # PRELIMINARY
            source_results = await self._extract_sources(self.item_results)
            await self._combine_results(self.item_results, source_results)

        return self.item_results

    # @async_debug()
    async def _extract_sources(self, item_results):
        source_urls = [content.source_url for content in item_results.values()]
        source_extractors = SourceExtractor.provision_extractors(
            self.model, source_urls, self.delay_configuration,
            self.web_driver_type, self.loop)
        futures = [extractor.extract() for extractor in source_extractors]
        if not futures:
            return []
        done, pending = await asyncio.wait(futures)
        source_results = [task.result() for task in done]
        return source_results

    # @async_debug()
    async def _combine_results(self, item_results, source_results):
        for source_result in source_results:
            source_url = source_result.source_url
            item_result = item_results[source_url]
            source_overrides = ((k, v) for k, v in source_result.items() if v is not None)
            for field, source_value in source_overrides:
                item_value = getattr(item_result, field)
                if item_value is not None and item_value != source_value:
                    PP.pprint(dict(
                        msg='Overwriting item value from source',
                        type='overwriting_item_value_from_source',
                        extractor=repr(self), field=field,
                        item_value=item_value, source_value=source_value))

                setattr(item_result, field, source_value)

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
    def provision_extractors(cls, model, search_data=None, web_driver_type=None, loop=None):
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

        web_driver_type=None:   WebDriverType, e.g. CHROME (default)
        loop=None:              Event loop (optional)
        yield:                  Fully configured search extractors
        """
        loop = loop or asyncio.get_event_loop()
        web_driver_type = web_driver_type or cls.WEB_DRIVER_TYPE_DEFAULT

        base = model.BASE_DIRECTORY
        dir_nodes = os.walk(base)
        directories = (cls._debase_directory(base, dn[0]) for dn in dir_nodes
                       if cls.FILE_NAME in dn[2])

        extractors = {}
        for directory in directories:
            try:
                extractor = cls(model, directory, search_data, web_driver_type, loop)
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

    def __init__(self, model, directory, search_data=None, web_driver_type=None, loop=None):
        self.search_data = self._prepare_search_data(search_data)
        super().__init__(model, directory, web_driver_type, loop)

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
            self.pages = self.page_size = self.next_page_configuration = None
            self.next_page_click_configuration = self.next_page_url_configuration = None

        self.items_configuration = self.configuration[self.ITEMS_TAG]
        self.item_results = OrderedDict()  # Store results after extraction

        self.cohort = None  # Only set after instantiation
        self.status = ExtractionStatus.INITIALIZED

    def __repr__(self):
        class_name = self.__class__.__name__
        directory = getattr(self, 'directory', None)
        search_data = getattr(self, 'search_data', None)
        created_timestamp = getattr(self, 'created_timestamp', None)
        return (f'<{class_name} | {directory} | {search_data!r} | {created_timestamp}>')
