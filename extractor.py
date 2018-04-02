#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import os
import urllib
from collections import OrderedDict, namedtuple
from datetime import datetime
from functools import lru_cache, partial

from parse import parse
from pprint import PrettyPrinter
from ruamel import yaml
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.async import run_in_executor
from utils.debug import async_debug
from utils.structures import FlexEnum
from utils.tools import delist, enlist, one, one_max

INDENT = 4
WIDTH = 160


class BaseExtractor:

    FILE_PATH_BASE = 'extractors'

    OPTIONS_TAG = 'options'

    IS_ENABLED_TAG = 'is_enabled'
    CONTENT_TAG = 'content'
    PAGE_URL_TAG = 'page_url'
    CLAUSE_DELIMITER_TAG = 'clause_delimiter'
    SEARCH_RESULTS_TAG = 'search_results'

    ELEMENT_TAG = 'element'
    ELEMENTS_TAG = 'elements'
    TEXT_TAG = 'text'
    KEEP_TAG = 'keep'

    # TODO: Make Content a class/model
    CONTENT_FIELDS = [
        'source_url', 'title', 'author_names', 'publication',
        'volume', 'issue', 'issue_date', 'pages', 'doi',
        'published_timestamp', 'granularity_published', 'tzinfo_published',
        'publisher', 'summary', 'full_text']

    INDEX_TAG = 'index'
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

    ParseMethod = FlexEnum('ParseMethod', 'PARSE STRPTIME')

    ExtractOperation = namedtuple('ExtractOperation',
                                  'is_multiple find_method find_term '
                                  'wait click attribute '
                                  'parse_method parse_template')

    ####################################################################


class SourceExtractor(BaseExtractor):

    FILE_NAME = 'source.yaml'


class RegistryExtractor(BaseExtractor):

    FILE_NAME = 'registry.yaml'

    @async_debug()
    async def extract(self):
        if not self.is_enabled:
            self.pp.pprint(dict(
                msg='Extractor disabled warning',
                type='extract_disabled_warning', extractor=repr(self)))
        try:
            await self._provision_web_driver()
            await self._fetch_page()
            return await self._extract_search_results()
        finally:
            await self._dispose_web_driver()

    @async_debug(offset=1)
    async def _provision_web_driver(self):
        future_web_driver = self._execute_in_future(self.web_driver_class,
                                                    **self.web_driver_kwargs)
        self.web_driver = await future_web_driver
        max_wait = self.configuration.get(self.WAIT_TAG, self.DEFAULT_MAX_WAIT)
        # Configure web driver to allow waiting on each operation
        self.web_driver.implicitly_wait(max_wait)

    @async_debug(offset=1)
    async def _fetch_page(self):
        future_page = self._execute_in_future(self.web_driver.get, self.page_url)
        await future_page

    @async_debug(offset=1)
    async def _extract_search_results(self):
        content_config = self.configuration[self.CONTENT_TAG]
        search_results_config = content_config[self.SEARCH_RESULTS_TAG]

        elements = await self._perform_operation(search_results_config,
                                                 self.web_driver)

        search_results = []

        if elements is not None:
            for index, element in enumerate(elements, start=1):
                try:
                    content = await self._extract_content(content_config,
                                                          element, index)
                    if not content:
                        raise ValueError('No content extracted')
                    search_results.append(content)

                except Exception as e:
                    self.pp.pprint(dict(
                        msg='Extract content failure',
                        type='extract_content_failure', error=str(e),
                        extractor=repr(self), config=content_config))
        else:
            self.pp.pprint(dict(
                msg='Extract search results failure',
                type='extract_search_results_failure',
                extractor=repr(self), config=search_results_config))

        return search_results

    @async_debug(offset=1)
    async def _dispose_web_driver(self):
        if self.web_driver:
            future_web_driver_quit = self._execute_in_future(self.web_driver.quit)
            await future_web_driver_quit

    @async_debug(offset=2)
    async def _extract_content(self, config, element, index):
        content = OrderedDict()
        for field in self.CONTENT_FIELDS:
            field_config = config[field]
            if not isinstance(field_config, (list, dict)):
                content[field] = field_config
                continue

            try:
                content[field] = await self._perform_operation(field_config,
                                                               element, index)
            # e.g. NoSuchElementException
            except Exception as e:
                self.pp.pprint(dict(
                    msg='Extract field failure',
                    type='extract_field_failure',
                    error=str(e), field=field, content=content,
                    extractor=repr(self)))

        return content

    @async_debug(offset=3)
    async def _perform_operation(self, config, target, index=1):
        if isinstance(config, list):
            latest = prior = parent = target
            for operation_config in config:
                # self._validate_element(latest)
                new_targets = self._select_targets(operation_config, latest, prior, parent)
                prior = latest
                for new_target in new_targets:
                    latest = await self._perform_operation(operation_config, new_target, index)
            return latest

        if isinstance(config, dict):
            operation = self._configure_operation(config)
            max_wait_seconds = operation.wait

            if operation.find_method:
                find_method, find_by = self._derive_find_method(operation, target)
                find_term = operation.find_term.format(index=index)
                if max_wait_seconds:
                    wait = WebDriverWait(self.web_driver, max_wait_seconds,
                                         poll_frequency=self.WAIT_POLL_INTERVAL)
                    # TODO: Make wait condition configurable rather than max wait
                    wait_condition = EC.presence_of_element_located((find_by, find_term))
                    future_elements = wait.until(wait_condition)
                    future_elements = self._execute_in_future(wait.until, wait_condition)
                else:
                    future_elements = self._execute_in_future(find_method, find_term)
                new_targets = enlist(await future_elements)
            else:
                new_targets = enlist(target)
                if max_wait_seconds:
                    self.loop.sleep(max_wait_seconds)

            if operation.click:
                await self._perform_clicks(new_targets)

            if operation.attribute:
                values = await self._extract_attributes(operation, new_targets)
            else:
                values = new_targets

            if not operation.parse_method:
                return delist(values)

            parsed_values = await self._parse_values(operation, values)

            return delist(parsed_values)

    @async_debug(offset=4)
    async def _perform_clicks(self, elements):
        """
        Perform clicks on given elements (one click on each)

        Subsequent operations may require web driver to wait via either
        implicit or explicit waits.
        """
        for element in elements:
            future_dom = self._execute_in_future(element.click)
            await future_dom
            # await asyncio.sleep(0.2) # TODO: replace once explicit waits work

    @async_debug(offset=4)
    async def _extract_attributes(self, operation, elements):
        values = []
        for element in elements:
            func = (partial(getattr, element) if operation.attribute == self.TEXT_TAG
                    else element.get_attribute)

            future_value = self._execute_in_future(func, operation.attribute)
            value = await future_value
            values.append(value)

        return values

    @async_debug(offset=4)
    async def _parse_values(self, operation, values):
        parsed_values = []
        if operation.parse_method is self.ParseMethod.PARSE:
            for value in values:
                parsed = parse(operation.parse_template, value)
                if parsed and parsed.named and self.KEEP_TAG in parsed.named:
                    parsed_values.append(parsed.named[self.KEEP_TAG])
                else:
                    self.pp.pprint(dict(
                        msg='Extractor parse failure',
                        type='extractor_parse_failure',
                        template=operation.parse_template, value=value,
                        parsed=parsed, extractor=repr(self)))

        elif operation.parse_method is self.ParseMethod.STRPTIME:
            for value in values:
                parsed_value = datetime.strptime(value, operation.parse_template)
                parsed_values.append(parsed_value)

        return parsed_values

    def _validate_element(self, value):
        if not isinstance(value, (self.web_driver_class, WebElement, list)):
            raise TypeError('Expected driver, element, or list. '
                            f'Received: {value}')

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
        find_method, find_term = self._configure_action(config, self.FindMethod)
        wait = config.get(self.WAIT_TAG, 0)
        click = config.get(self.CLICK_TAG, False)
        attribute = config.get(self.ATTRIBUTE_TAG)
        parse_method, parse_template = self._configure_action(config,
                                                              self.ParseMethod)
        return self.ExtractOperation(is_multiple, find_method, find_term,
                                     wait, click, attribute,
                                     parse_method, parse_template)

    @staticmethod
    def _configure_action(config, action_enum):
        action_keys = action_enum.set(str.lower)
        action_key = one_max(k for k in config if k in action_keys)
        if not action_key:
            return None, None
        action_method = action_enum[action_key.upper()]
        action_arg = config[action_key]
        return action_method, action_arg

    def _derive_find_method(self, operation, element):
        element_tag = (self.ELEMENTS_TAG if operation.is_multiple
                       else self.ELEMENT_TAG)
        method_tag = operation.find_method.name.lower()
        find_method_name = f'find_{element_tag}_by_{method_tag}'
        find_method = getattr(element, find_method_name)
        find_by = getattr(By, operation.find_method.name)
        return find_method, find_by

    def _execute_in_future(self, func, *args, **kwds):
        return run_in_executor(self.loop, None, func, *args, **kwds)

    def _form_file_path(self, directory):
        return os.path.join(self.FILE_PATH_BASE, directory, self.FILE_NAME)

    @lru_cache(maxsize=None, typed=False)
    def _marshall_configuration(self, file_path):
        with open(file_path) as stream:
            return yaml.safe_load(stream)

    def _form_page_url(self, configuration, problem_name, org_name, geo_name):
        page_url = self.configuration[self.PAGE_URL_TAG]
        clause_delimiter = self.configuration.get(self.CLAUSE_DELIMITER_TAG)
        clause_count = 0

        local_dict = locals()
        search_terms = ((component, local_dict[f'{component}_name'])
                        for component in self.SearchComponent.names(str.lower))

        for index, (component, search_term) in enumerate(search_terms, start=1):
            clause_tag = f'{component}_clause'
            clause_template = self.configuration.get(clause_tag)
            rendered_clause = self._form_url_clause(
                clause_template, component, search_term, index)
            if rendered_clause:
                if clause_delimiter and clause_count:
                    rendered_clause = f'{clause_delimiter}{rendered_clause}'
                clause_count += 1
            page_url = page_url.replace(f'{{{clause_tag}}}', rendered_clause)

        if not clause_count:
            raise ValueError('At least one search term is required')

        return page_url

    def _form_url_clause(self, clause_template, component, search_term, index):
        if not clause_template or not search_term:
            return ''
        encoded_term = urllib.parse.quote(search_term)
        return (clause_template.replace(f'{{{component}}}', encoded_term)
                               .replace(f'{{{self.INDEX_TAG}}}', str(index)))

    def _derive_web_driver_class(self, web_driver_type):
        return getattr(webdriver, web_driver_type.name.capitalize())

    def _derive_web_driver_kwargs(self, web_driver_type):
        if web_driver_type is self.WebDriverType.CHROME:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--disable-extensions')
            return dict(chrome_options=chrome_options)
        elif web_driver_type is self.WebDriverType.FIREFOX:
            raise NotImplementedError('Firefox not yet supported')

    def __repr__(self):
        class_name = self.__class__.__name__
        org_clause = f' at {self.org_name}' if self.org_name else ''
        geo_clause = f' in {self.geo_name}' if self.geo_name else ''
        community_name = f'{self.problem_name}{org_clause}{geo_clause}'
        return (f'<{class_name}: {self.directory}, {community_name}, '
                f'{self.created_timestamp}>')

    def __init__(self, directory, problem_name, org_name=None, geo_name=None,
                 web_driver_type=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.created_timestamp = self.loop.time()
        self.pp = PrettyPrinter(indent=INDENT, width=WIDTH)

        self.directory = directory
        self.file_path = self._form_file_path(self.directory)
        self.configuration = self._marshall_configuration(self.file_path)

        self.is_enabled = self.configuration.get(self.IS_ENABLED_TAG, True)

        self.problem_name = problem_name
        self.org_name = org_name
        self.geo_name = geo_name
        self.page_url = self._form_page_url(
            self.configuration, self.problem_name, self.org_name, self.geo_name)

        self.web_driver_type = web_driver_type or self.DEFAULT_WEB_DRIVER_TYPE
        self.web_driver_class = self._derive_web_driver_class(self.web_driver_type)
        self.web_driver_kwargs = self._derive_web_driver_kwargs(self.web_driver_type)
        self.web_driver = None
