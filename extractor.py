#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import os
import random
import urllib
from collections import OrderedDict, namedtuple
from datetime import datetime
from functools import lru_cache, partial
from pathlib import Path

from parse import parse
from pprint import PrettyPrinter
from ruamel import yaml
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from url_normalize import url_normalize

from secret_service.agency import SecretService
from utils.async import run_in_executor
from utils.debug import async_debug
from utils.structures import FlexEnum
from utils.time import flex_strptime
from utils.tools import (
    PP, delist, enlist, human_selection_shuffle, multi_parse,
    one, one_max, one_min
)


class BaseExtractor:

    FILE_PATH_BASE = 'extractors'
    FILE_NAME = NotImplementedError

    OPTIONS_TAG = 'options'

    IS_ENABLED_TAG = 'is_enabled'
    CONTENT_TAG = 'content'
    PAGE_URL_TAG = 'page_url'
    CLAUSE_DELIMITER_TAG = 'clause_delimiter'
    ITEMS_TAG = 'items'

    ELEMENT_TAG = 'element'
    ELEMENTS_TAG = 'elements'
    INDEX_TAG = 'index'
    TEXT_TAG = 'text'
    VALUE_TAG = 'value'

    REFERENCE_TEMPLATE = '<{}>'
    LEFT_REFERENCE_TOKEN = REFERENCE_TEMPLATE[0]
    RIGHT_REFERENCE_TOKEN = REFERENCE_TEMPLATE[-1]
    REFERENCE_DELIMITER = '.'

    # TODO: Make Content a class/model
    CONTENT_FIELDS = [
        'source_url', 'title', 'author_names', 'publication', 'volume',
        'issue', 'issue_date', 'issue_date_granularity',
        'first_page', 'last_page', 'doi',
        'published_timestamp', 'granularity_published', 'tzinfo_published',
        'publisher', 'summary', 'full_text']
    SOURCE_URL_TAG = 'source_url'

    SearchComponent = FlexEnum('SearchComponent', 'PROBLEM ORG GEO')

    WebDriverType = FlexEnum('WebDriverType', 'CHROME FIREFOX')
    DEFAULT_WEB_DRIVER_TYPE = WebDriverType.CHROME
    DEFAULT_MAX_WAIT = 10
    WAIT_POLL_INTERVAL = 0.2

    ####################################################################
    # TODO: Make ExtractOperation a class & encapsulate relevant methods
    ####################################################################

    SCOPE_TAG = 'scope'
    IS_MULTIPLE_TAG = 'is_multiple'
    WAIT_TAG = 'wait'
    CLICK_TAG = 'click'
    ATTRIBUTE_TAG = 'attribute'

    OperationScope = FlexEnum('OperationScope', 'PAGE PARENT PRIOR LATEST')
    DEFAULT_OPERATION_SCOPE = OperationScope.LATEST

    FindMethod = FlexEnum('FindMethod',
                          'CLASS_NAME CSS_SELECTOR ID LINK_TEXT NAME '
                          'PARTIAL_LINK_TEXT TAG_NAME XPATH')

    WaitMethod = FlexEnum('WaitMethod', [
        'ELEMENT_LOCATED_TO_BE_SELECTED',           # locator
        'ELEMENT_TO_BE_CLICKABLE',                  # locator
        'FRAME_TO_BE_AVAILABLE_AND_SWITCH_TO_IT',   # locator
        'INVISIBILITY_OF_ELEMENT_LOCATED',          # locator
        'PRESENCE_OF_ALL_ELEMENTS_LOCATED',         # locator
        'PRESENCE_OF_ELEMENT_LOCATED',              # locator
        'VISIBILITY_OF_ALL_ELEMENTS_LOCATED',       # locator
        'VISIBILITY_OF_ANY_ELEMENTS_LOCATED',       # locator
        'VISIBILITY_OF_ELEMENT_LOCATED',            # locator

        # 'ALERT_IS_PRESENT',                         # (no args)
        # 'ELEMENT_LOCATED_SELECTION_STATE_TO_BE',    # locator, is_selected
        # 'ELEMENT_SELECTION_STATE_TO_BE',            # element, is_selected
        # 'ELEMENT_TO_BE_SELECTED',                   # element
        # 'NEW_WINDOW_IS_OPENED',                     # current_handles
        # 'NUMBER_OF_WINDOWS_TO_BE',                  # num_windows
        # 'STALENESS_OF',                             # element
        # 'TEXT_TO_BE_PRESENT_IN_ELEMENT',            # locator, text_
        # 'TEXT_TO_BE_PRESENT_IN_ELEMENT_VALUE',      # locator, text_
        # 'TITLE_CONTAINS',                           # title
        # 'TITLE_IS',                                 # title
        # 'URL_CHANGES',                              # url
        # 'URL_CONTAINS',                             # url
        # 'URL_MATCHES',                              # pattern
        # 'URL_TO_BE',                                # url
        # 'VISIBILITY_OF',                            # element
    ])

    ExtractMethod = FlexEnum('ExtractMethod', 'GETATTR ATTRIBUTE PROPERTY')
    GetMethod = FlexEnum('GetMethod', 'GET')
    ParseMethod = FlexEnum('ParseMethod', 'PARSE STRPTIME')
    FormatMethod = FlexEnum('FormatMethod', 'FORMAT STRFTIME')
    TransformMethod = FlexEnum('TransformMethod', 'JOIN SPLIT')
    # SetMethod = FlexEnum('SetMethod', 'SET')

    ExtractOperation = namedtuple('ExtractOperation',
                                  'is_multiple find_method find_args '
                                  'wait_method wait_args '
                                  'wait click '
                                  'extract_method extract_args '
                                  'get_method get_args '
                                  'parse_method parse_args '
                                  'format_method format_args '
                                  'transform_method transform_args')

    ####################################################################

    @async_debug()
    async def extract(self):
        if not self.is_enabled:
            PP.pprint(dict(
                msg='Extractor disabled warning',
                type='extractor_disabled_warning', extractor=repr(self)))
        try:
            await self._provision_web_driver()
            await self._fetch_page()
            return await self._extract_page()
        finally:
            await self._dispose_web_driver()

    @async_debug()
    async def _provision_web_driver(self):
        # TODO: retrieve from web driver pool
        future_web_driver = self._execute_in_future(self.web_driver_class,
                                                    **self.web_driver_kwargs)
        self.web_driver = await future_web_driver
        max_implicit_wait = self.configuration.get(self.WAIT_TAG, self.DEFAULT_MAX_WAIT)
        # Configure web driver to allow waiting on each operation
        self.web_driver.implicitly_wait(max_implicit_wait)

    @async_debug()
    async def _fetch_page(self):
        future_page = self._execute_in_future(self.web_driver.get, self.page_url)
        await future_page

    @async_debug()
    async def _extract_page(self):
        raise NotImplementedError

    @async_debug()
    async def _dispose_web_driver(self):
        if self.web_driver:
            future_web_driver_quit = self._execute_in_future(self.web_driver.quit)
            await future_web_driver_quit

    @async_debug()
    async def _extract_content(self, element, config, index=1):
        self.content = content = OrderedDict()
        for field in self.CONTENT_FIELDS:
            field_config = config.get(field)
            # Allow field to be set by other field without overwriting
            if field_config is None and content.get(field):
                continue
            elif not isinstance(field_config, (list, dict)):
                content[field] = field_config
                continue

            try:
                content[field] = await self._perform_operation(
                    element, field_config, index)
            # e.g. NoSuchElementException
            except Exception as e:
                PP.pprint(dict(
                    msg='Extract field failure',
                    type='extract_field_failure',
                    error=str(e), field=field, content=content,
                    extractor=repr(self)))

        self.content = None
        return content

    @async_debug()
    async def _perform_operation(self, target, config, index=1):
        if isinstance(config, list):
            latest = prior = parent = target
            for operation_config in config:
                new_targets = self._select_targets(operation_config, latest, prior, parent)
                prior = latest
                if operation_config.get(self.IS_MULTIPLE_TAG, False):
                    latest = await self._perform_operation(new_targets, operation_config, index)
                    continue
                for new_target in new_targets:
                    latest = await self._perform_operation(new_target, operation_config, index)
            return latest

        if isinstance(config, dict):
            operation = self._configure_operation(config)

            if operation.find_method:
                new_targets = await self._find_elements(operation, target, index)
            else:
                new_targets = enlist(target)
                if operation.wait:
                    await asyncio.sleep(operation.wait)

            if operation.click:
                await self._click_elements(new_targets)

            if operation.extract_method:
                values = await self._extract_values(operation, new_targets)
            else:
                values = new_targets

            if operation.get_method:
                values = await self._get_values(operation, values)

            if operation.parse_method:
                values = await self._parse_values(operation, values)

            if operation.format_method:
                values = await self._format_values(operation, values)

            if operation.transform_method:
                values = await self._transform_values(operation, values)

            return delist(values)

    @async_debug()
    async def _find_elements(self, operation, element, index=1):
        self._validate_element(element)
        find_method, find_by = self._derive_find_method(operation, element)
        args = (self._render_references(a) for a in operation.find_args)
        template = one(args)
        selector = template.format(index=index)

        if operation.wait_method:
            max_explicit_wait = operation.wait or self.DEFAULT_MAX_WAIT
            wait = WebDriverWait(self.web_driver, max_explicit_wait,
                                 poll_frequency=self.WAIT_POLL_INTERVAL)
            wait_method_name = operation.wait_method.name.lower()
            wait_condition_method = getattr(expected_conditions, wait_method_name)
            locator = (find_by, selector)
            wait_condition = wait_condition_method(locator)
            # wait_condition = expected_conditions.presence_of_element_located((find_by, selector))
            future_elements = self._execute_in_future(wait.until, wait_condition)
        else:
            future_elements = self._execute_in_future(find_method, selector)

        new_elements = await future_elements
        return enlist(new_elements)

    @async_debug()
    async def _click_elements(self, elements):
        """
        Click elements sequentially

        Subsequent operations may require web driver to wait for DOM
        changes via either implicit or explicit waits.
        """
        for element in elements:
            future_dom = self._execute_in_future(element.click)
            await future_dom

    @async_debug()
    async def _extract_values(self, operation, elements):
        extracted_values = []
        args = (self._render_references(a) for a in operation.extract_args)
        field = one(args)
        extract_method = operation.extract_method

        if extract_method is self.ExtractMethod.GETATTR:
            for element in elements:
                func = partial(getattr, element)
                future_value = self._execute_in_future(func, field)
                value = await future_value
                extracted_values.append(value)

        elif (extract_method is self.ExtractMethod.ATTRIBUTE or
              extract_method is self.ExtractMethod.PROPERTY):
            extract_method_name = f'get_{extract_method.name.lower()}'
            for element in elements:
                func = getattr(element, extract_method_name)
                future_value = self._execute_in_future(func, field)
                value = await future_value
                extracted_values.append(value)

        return extracted_values

    @async_debug()
    async def _get_values(self, operation, values):
        retrieved_values = []
        args = operation.get_args

        if operation.get_method is self.GetMethod.GET:
            # Ignore current values; get new ones based on get_args
            tags = one_min(args)
            retrieved_values = [self._get_by_reference_tag(t) for t in tags]

        return retrieved_values

    @async_debug()
    async def _parse_values(self, operation, values):
        parsed_values = []
        args = (self._render_references(a) for a in operation.parse_args)

        if operation.parse_method is self.ParseMethod.PARSE:
            templates = one_min(args)
            for value in values:
                if value is None:
                    parsed_values.append(None)
                    continue
                parsed = None
                try:
                    parsed = multi_parse(templates, value)
                    parsed_values.append(parsed.named[self.VALUE_TAG])

                except (ValueError, AttributeError, KeyError) as e:
                    PP.pprint(dict(
                        msg='Extractor parse failure',
                        type='extractor_parse_failure',
                        templates=templates, value=value, parsed=parsed,
                        error=e, extractor=repr(self)))

        elif operation.parse_method is self.ParseMethod.STRPTIME:
            templates = one_min(args)
            for value in values:
                if value is None:
                    parsed_values.append(None)
                    continue
                parsed = flex_strptime(value, templates)
                parsed_values.append(parsed)

        return parsed_values

    @async_debug()
    async def _format_values(self, operation, values):
        formatted_values = []
        args = (self._render_references(a) for a in operation.format_args)

        if operation.format_method is self.FormatMethod.FORMAT:
            template = one(args)
            if self.VALUE_TAG in self.content:
                raise ValueError(
                    "Reserved word '{self.VALUE_TAG}' cannot be content field")
            self.content[self.VALUE_TAG] = None
            for value in values:
                self.content[self.VALUE_TAG] = value
                formatted = template.format(**self.content)
                formatted_values.append(formatted)
            del self.content[self.VALUE_TAG]

        elif operation.format_method is self.FormatMethod.STRFTIME:
            template = one(args)
            for value in values:
                formatted = value.strftime(template)
                formatted_values.append(formatted)

        return formatted_values

    @async_debug()
    async def _transform_values(self, operation, values):
        transformed_values = []
        args = (self._render_references(a) for a in operation.transform_args)

        if operation.transform_method is self.TransformMethod.JOIN:
            delimiter = one(args)
            joined_values = delimiter.join(values)
            transformed_values.append(joined_values)

        elif operation.transform_method is self.TransformMethod.SPLIT:
            delimiter = one(args)
            for value in values:
                split_values = value.split(delimiter)
                transformed_values.extend(split_values)

        return transformed_values

    def _select_targets(self, config, latest, prior, parent):
        operation_scope = self._derive_operation_scope(config)
        if operation_scope is self.OperationScope.LATEST:
            return enlist(latest)
        if operation_scope is self.OperationScope.PRIOR:
            return enlist(prior)
        if operation_scope is self.OperationScope.PARENT:
            return [parent]
        assert operation_scope is self.OperationScope.PAGE
        return [self.web_driver]  # Selenium WebDriver instance

    def _derive_operation_scope(self, config):
        if self.SCOPE_TAG in config:
            operation_scope = config[self.SCOPE_TAG]
            return self.OperationScope[operation_scope.upper()]

        return self.DEFAULT_OPERATION_SCOPE

    def _configure_operation(self, config):
        is_multiple = config.get(self.IS_MULTIPLE_TAG, False)
        find_method, find_args = self._configure_method(
            config, self.FindMethod)
        wait_method, wait_args = self._configure_method(
            config, self.WaitMethod)
        wait = config.get(self.WAIT_TAG, 0)
        click = config.get(self.CLICK_TAG, False)
        extract_method, extract_args = self._configure_method(
            config, self.ExtractMethod)
        get_method, get_args = self._configure_method(
            config, self.GetMethod)
        parse_method, parse_args = self._configure_method(
            config, self.ParseMethod)
        format_method, format_args = self._configure_method(
            config, self.FormatMethod)
        transform_method, transform_args = self._configure_method(
            config, self.TransformMethod)
        return self.ExtractOperation(is_multiple, find_method, find_args,
                                     wait_method, wait_args,
                                     wait, click,
                                     extract_method, extract_args,
                                     get_method, get_args,
                                     parse_method, parse_args,
                                     format_method, format_args,
                                     transform_method, transform_args)

    @staticmethod
    def _configure_method(config, method_enum):
        method_keys = method_enum.set(str.lower)
        method_key = one_max(k for k in config if k in method_keys)
        if not method_key:
            return None, None
        method_type = method_enum[method_key.upper()]
        method_args = enlist(config[method_key])
        return method_type, method_args

    def _derive_find_method(self, operation, element):
        element_tag = (self.ELEMENTS_TAG if operation.is_multiple
                       else self.ELEMENT_TAG)
        method_tag = operation.find_method.name.lower()
        find_method_name = f'find_{element_tag}_by_{method_tag}'
        find_method = getattr(element, find_method_name)
        find_by = getattr(By, operation.find_method.name)
        return find_method, find_by

    def _render_references(self, template):
        while (self.LEFT_REFERENCE_TOKEN in template and
               self.RIGHT_REFERENCE_TOKEN in template):
            reference = (template.split(self.RIGHT_REFERENCE_TOKEN)[0]
                                 .split(self.LEFT_REFERENCE_TOKEN)[-1])
            if not reference:
                return template
            reference_tag = self.REFERENCE_TEMPLATE.format(reference)
            value = self._get_by_reference(reference) or ''
            template = template.replace(reference_tag, str(value))

        return template

    def _get_by_reference_tag(self, reference_tag):
        parsed = parse(self.REFERENCE_TEMPLATE, reference_tag)
        if not parsed:
            raise ValueError(f"parse('{self.REFERENCE_TEMPLATE}', "
                             f"'{reference_tag}') failed to find match")
        reference = parsed.fixed[0]
        return self._get_by_reference(reference)

    def _get_by_reference(self, reference):
        components = reference.split(self.REFERENCE_DELIMITER)
        field_name = components[0]
        value = self.content[field_name]
        if value:
            for component in components[1:]:
                value = getattr(value, component)
        return value

    def _validate_element(self, value):
        if not isinstance(value, (self.web_driver_class, WebElement)):
            raise TypeError(f'Expected driver or element. Received: {value}')

    def _execute_in_future(self, func, *args, **kwds):
        """Run in executor with kwds support & default loop/executor"""
        return run_in_executor(self.loop, None, func, *args, **kwds)

    def _form_file_path(self, directory):
        return os.path.join(self.FILE_PATH_BASE, directory, self.FILE_NAME)

    @lru_cache(maxsize=None, typed=False)
    def _marshall_configuration(self, file_path):
        with open(file_path) as stream:
            return yaml.safe_load(stream)

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

    def __repr__(self):
        class_name = self.__class__.__name__
        return (f'<{class_name}: {self.directory}, {self.created_timestamp}>')

    def __init__(self, directory, web_driver_type=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.created_timestamp = self.loop.time()

        self.directory = directory
        self.file_path = self._form_file_path(self.directory)
        self.configuration = self._marshall_configuration(self.file_path)
        self.is_enabled = self.configuration.get(self.IS_ENABLED_TAG, True)

        self.web_driver_type = web_driver_type or self.DEFAULT_WEB_DRIVER_TYPE
        self.web_driver_class = self._derive_web_driver_class(self.web_driver_type)
        self.web_driver_kwargs = self._derive_web_driver_kwargs(self.web_driver_type)
        self.web_driver = None

        self.content = None  # Temporary storage for content being extracted


class SourceExtractor(BaseExtractor):

    FILE_NAME = 'source.yaml'

    HTTPS_TAG = 'https://'
    HTTP_TAG = 'http://'
    WWW_DOT_TAG = 'www.'
    DOMAIN_DELIMITER = '.'
    DIRECTORY_NAME_DELIMITER = '_'
    PATH_DELIMITER = '/'
    QUERY_STRING_DELIMITER = '?'

    MINIMUM_WAIT = 1
    MAXIMUM_WAIT = 3

    @async_debug()
    async def extract(self):
        if self.initial_wait:
            await asyncio.sleep(self.initial_wait)
        return await super().extract()

    @async_debug()
    async def _extract_page(self):
        content_config = self.configuration[self.CONTENT_TAG]
        try:
            content = await self._extract_content(self.web_driver, content_config)
            if not content:
                raise ValueError('No content extracted')

        except Exception as e:
            PP.pprint(dict(
                msg='Extract content failure',
                type='extract_content_failure', error=str(e),
                extractor=repr(self), config=content_config))
            raise
        else:
            content[self.SOURCE_URL_TAG] = self.page_url
            return content

    @classmethod
    def provision_extractors(cls, urls=None):
        """
        Provision Extractors

        Instantiate and yield source extractors for the given urls.

        I/O:
        urls=None:  List of url strings for source content
        yield:      Fully configured source extractor instances
        """
        human_selection_shuffle(urls)
        initial_wait = 0
        for url in urls:
            try:
                initial_wait += random.uniform(cls.MINIMUM_WAIT, cls.MAXIMUM_WAIT)
                extractor = cls(page_url=url, initial_wait=initial_wait)
                if extractor.is_enabled:
                    yield extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

    def _derive_directory(self, page_url):
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
            path = os.path.join(self.FILE_PATH_BASE, sub_directory)
            if not Path(path).is_dir():
                deepest_index = i
                break

        # Look for source configuration directory, starting with deepest
        for i in range(deepest_index, 0, -1):
            sub_directory = self.PATH_DELIMITER.join(path_components[:i])
            path = os.path.join(self.FILE_PATH_BASE, sub_directory, self.FILE_NAME)
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

    def __init__(self, page_url, initial_wait=0, web_driver_type=None, loop=None):
        self.page_url = url_normalize(page_url)
        self.initial_wait = initial_wait
        directory = self._derive_directory(self.page_url)
        super().__init__(directory, web_driver_type, loop)


class MultiExtractor(BaseExtractor):

    FILE_NAME = 'multi.yaml'

    @async_debug()
    async def _extract_page(self):
        return await self._extract_multiple()

    @async_debug()
    async def _extract_multiple(self):
        content_config = self.configuration[self.CONTENT_TAG]
        items_config = content_config[self.ITEMS_TAG]
        self.item_results = OrderedDict()

        elements = await self._perform_operation(self.web_driver, items_config)

        if elements is not None:
            for index, element in enumerate(elements, start=1):
                try:
                    content = await self._extract_content(element, content_config, index)
                    if not content:
                        raise ValueError('No content extracted')
                    source_url = content.get(self.SOURCE_URL_TAG)
                    if not source_url:
                        raise ValueError('Content missing source URL')
                    self.item_results[source_url] = content

                except Exception as e:
                    PP.pprint(dict(
                        msg='Extract content failure',
                        type='extract_content_failure', error=str(e),
                        extractor=repr(self), config=content_config))

        else:
            PP.pprint(dict(
                msg='Extract item results failure',
                type='extract_item_results_failure',
                extractor=repr(self), config=items_config))

        # TODO: Store preliminary results in redis
        source_results = await self._extract_sources(self.item_results)
        await self._combine_results(self.item_results, source_results)
        return self.item_results

    @async_debug()
    async def _extract_sources(self, item_results):
        source_urls = [content[self.SOURCE_URL_TAG] for content in item_results.values()]
        source_extractors = SourceExtractor.provision_extractors(source_urls)
        futures = {extractor.extract() for extractor in source_extractors}
        if not futures:
            return []
        done, pending = await asyncio.wait(futures)
        source_results = [task.result() for task in done]
        return source_results

    @async_debug()
    async def _combine_results(self, item_results, source_results):
        for source_result in source_results:
            source_url = source_result[self.SOURCE_URL_TAG]
            item_result = item_results[source_url]
            source_overrides = ((k, v) for k, v in source_result.items() if v is not None)
            for field, source_value in source_overrides:
                item_value = item_result.get(field)
                if item_value is not None and item_value != source_value:
                    PP.pprint(dict(
                        msg='Overwriting item value from source',
                        type='overwriting_item_value_from_source',
                        extractor=repr(self), field=field,
                        item_value=item_value, source_value=source_value))

                item_result[field] = source_value

    @classmethod
    def provision_extractors(cls, problem_name=None, org_name=None, geo_name=None):
        """
        Provision Extractors

        Instantiate and yield all multi extractors configured with the
        given problem name, org name, and geo name.

        I/O:
        problem_name=None:  Name of problem to be used as search term
        org_name=None:      Name of organization to be used as search term
        geo_name=None:      Name of geo to be used as search term
        yield:              Fully configured search extractor instances
        """
        dir_nodes = os.walk(cls.FILE_PATH_BASE)
        multi_directories = (cls._debase_directory(dn[0]) for dn in dir_nodes
                             if cls.FILE_NAME in dn[2])

        for directory in multi_directories:
            try:
                extractor = cls(directory, problem_name, org_name, geo_name)
                if extractor.is_enabled:
                    yield extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

    @classmethod
    def _debase_directory(cls, directory_path):
        base = f'{cls.FILE_PATH_BASE}/'
        if not directory_path.startswith(base):
            raise ValueError(f"'{directory_path}' must start with '{base}'")
        directory = directory_path.replace(base, '', 1)
        return directory

    def _form_page_url(self, configuration):
        problem_name = self.problem_name
        org_name = self.org_name
        geo_name = self.geo_name

        page_url = self.configuration[self.PAGE_URL_TAG]
        clause_delimiter = self.configuration.get(self.CLAUSE_DELIMITER_TAG)
        clause_count = 0

        local_dict = locals()
        terms = ((component, local_dict[f'{component}_name'])
                 for component in self.SearchComponent.names(str.lower))

        for index, (component, term) in enumerate(terms, start=1):
            clause_tag = f'{component}_clause'
            clause_template = self.configuration.get(clause_tag)
            rendered_clause = self._form_url_clause(
                clause_template, component, term, index)
            if rendered_clause:
                if clause_delimiter and clause_count:
                    rendered_clause = f'{clause_delimiter}{rendered_clause}'
                clause_count += 1
            page_url = page_url.replace(f'{{{clause_tag}}}', rendered_clause)

        if not clause_count:
            raise ValueError('At least one search term is required')

        return page_url

    def _form_url_clause(self, clause_template, component, term, index):
        if not clause_template or not term:
            return ''
        encoded_term = urllib.parse.quote(term)
        return (clause_template.replace(f'{{{component}}}', encoded_term)
                               .replace(f'{{{self.INDEX_TAG}}}', str(index)))

    def __repr__(self):
        class_name = self.__class__.__name__
        org_clause = f' at {self.org_name}' if self.org_name else ''
        geo_clause = f' in {self.geo_name}' if self.geo_name else ''
        community_name = f'{self.problem_name}{org_clause}{geo_clause}'
        return (f'<{class_name}: {self.directory}, {community_name}, '
                f'{self.created_timestamp}>')

    def __init__(self, directory, problem_name, org_name=None, geo_name=None,
                 web_driver_type=None, loop=None):
        self.problem_name = problem_name
        self.org_name = org_name
        self.geo_name = geo_name

        super().__init__(directory, web_driver_type, loop)

        self.page_url = self._form_page_url(self.configuration)
        self.item_results = None  # Store results after extraction
