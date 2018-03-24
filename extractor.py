#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import os
from collections import OrderedDict, namedtuple
from datetime import datetime
from functools import lru_cache, partial

from parse import parse
from ruamel import yaml
from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement

from utils.async import run_in_executor
from utils.structures import FlexEnum
from utils.tools import delist, enlist, one, one_max


class BaseExtractor:

    FILE_PATH_BASE = 'extractors'

    OPTIONS_TAG = 'options'
    MAX_WAIT_SECONDS_TAG = 'max_wait_seconds'

    CONTENT_TAG = 'content'
    PAGE_URL_TAG = 'page_url'
    SEARCH_RESULTS_TAG = 'search_results'

    ELEMENT_TAG = 'element'
    ELEMENTS_TAG = 'elements'
    TEXT_TAG = 'text'
    KEEP_TAG = 'keep'

    # TODO: Make Content a class/model
    CONTENT_FIELDS = [
        'source_url', 'title', 'author_names', 'publication',
        'published_timestamp', 'granularity_published', 'tzinfo_published',
        'publisher', 'summary', 'full_text']

    WebDriverType = FlexEnum('WebDriverType', 'CHROME FIREFOX')
    DEFAULT_WEB_DRIVER_TYPE = WebDriverType.CHROME
    DEFAULT_MAX_WAIT_SECONDS = 10

    ####################################################################
    # TODO: Make ExtractOperation a class & encapsulate relevant methods
    ####################################################################

    SCOPE_TAG = 'scope'
    IS_MULTIPLE_TAG = 'is_multiple'
    CLICK_TAG = 'click'
    ATTRIBUTE_TAG = 'attribute'

    OperationScope = FlexEnum('OperationScope', 'PAGE PARENT PRIOR LATEST')
    DEFAULT_OPERATION_SCOPE = OperationScope.LATEST

    FindMethod = FlexEnum('FindMethod',
                          'CLASS_NAME CSS_SELECTOR ID LINK_TEXT NAME '
                          'PARTIAL_LINK_TEXT TAG_NAME XPATH')

    ParseMethod = FlexEnum('ParseMethod', 'PARSE STRPTIME')

    ExtractOperation = namedtuple('ExtractOperation',
                                  'is_multiple find_method find_term click '
                                  'attribute parse_method parse_template')

    ####################################################################


class SourceExtractor(BaseExtractor):

    FILE_NAME = 'source.yaml'


class RegistryExtractor(BaseExtractor):

    FILE_NAME = 'registry.yaml'

    async def extract(self):
        try:
            await self._provision_web_driver()
            await self._fetch_page()
            return await self._extract_search_results()
        finally:
            await self._dispose_web_driver()

    async def _provision_web_driver(self):
        future_web_driver = self._execute_in_future(self.web_driver_class,
                                                    **self.web_driver_kwargs)
        self.web_driver = await future_web_driver
        wait_seconds = self.configuration.get(self.MAX_WAIT_SECONDS_TAG,
                                              self.DEFAULT_MAX_WAIT_SECONDS)
        # Configure web driver to allow waiting on each operation
        self.web_driver.implicitly_wait(wait_seconds)

    async def _fetch_page(self):
        future_page = self._execute_in_future(self.web_driver.get, self.page_url)
        await future_page

    async def _extract_search_results(self):
        content_config = self.configuration[self.CONTENT_TAG]
        search_results_config = content_config[self.SEARCH_RESULTS_TAG]

        elements = await self._perform_operation(search_results_config,
                                                 self.web_driver)
        search_results = []
        for element in elements:
            try:
                content = await self._extract_content(content_config, element)
                if content:
                    search_results.append(content)
            except Exception as e:
                print(e)

        return search_results

    async def _dispose_web_driver(self):
        if self.web_driver:
            future_web_driver_quit = self._execute_in_future(self.web_driver.quit)
            await future_web_driver_quit

    async def _extract_content(self, config, element):
        content = OrderedDict()
        for field in self.CONTENT_FIELDS:
            field_config = config[field]
            if not isinstance(field_config, (list, dict)):
                content[field] = field_config
                continue

            content[field] = await self._perform_operation(field_config, element)

        return content

    async def _perform_operation(self, config, element):
        if isinstance(config, list):
            latest = prior = parent = element
            for operation_config in config:
                self._validate_element(latest)
                targets = self._select_targets(operation_config, latest, prior, parent)
                prior = latest
                for target in targets:
                    latest = await self._perform_operation(operation_config, target)
            return latest

        if isinstance(config, dict):
            operation = self._configure_operation(config)

            if operation.find_method:
                find_method = self._derive_find_method(operation, element)
                future_elements = self._execute_in_future(find_method, operation.find_term)
                new_elements = enlist(await future_elements)
            else:
                new_elements = enlist(element)

            if operation.click:
                await self._perform_clicks(new_elements)

            if not operation.attribute:
                return delist(new_elements)

            values = await self._extract_attributes(operation, new_elements)

            if not operation.parse_method:
                return delist(values)

            parsed_values = self._parse_values(operation, values)

            return delist(parsed_values)

    async def _perform_clicks(self, elements):
        """
        Perform clicks on given elements (one click on each)

        Subsequent operations may require web driver to wait via either
        implicit or explicit waits.
        """
        for element in elements:
            future_dom = self._execute_in_future(element.click)
            await future_dom

    async def _extract_attributes(self, operation, elements):
        values = []
        for element in elements:
            func = (partial(getattr, element) if operation.attribute == self.TEXT_TAG
                    else element.get_attribute)

            future_value = self._execute_in_future(func, operation.attribute)
            value = await future_value
            values.append(value)

        return values

    def _parse_values(self, operation, values):
        parsed_values = []
        if operation.parse_method is self.ParseMethod.PARSE:
            for value in values:
                parsed = parse(operation.parse_template, value)
                if parsed and parsed.named and self.KEEP_TAG in parsed.named:
                    parsed_values.append(parsed.named[self.KEEP_TAG])
                else:
                    print(dict(msg='Extractor Parse Failure',
                               type='extractor_parse_failure',
                               template=operation.parse_template,
                               value=value, parsed=parsed))

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
        click = config.get(self.CLICK_TAG, False)
        attribute = config.get(self.ATTRIBUTE_TAG)
        parse_method, parse_template = self._configure_action(config,
                                                              self.ParseMethod)
        return self.ExtractOperation(is_multiple, find_method, find_term, click,
                                     attribute, parse_method, parse_template)

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
        return getattr(element, find_method_name)

    def _execute_in_future(self, func, *args, **kwds):
        return run_in_executor(self.loop, None, func, *args, **kwds)

    def _form_file_path(self, directory):
        return os.path.join(self.FILE_PATH_BASE, directory, self.FILE_NAME)

    @lru_cache(maxsize=None, typed=False)
    def _marshall_configuration(self):
        with open(self.file_path) as stream:
            return yaml.safe_load(stream)

    def _form_page_url(self, configuration, problem_name, org_name, geo_name):
        url_template = self.configuration[self.PAGE_URL_TAG]
        url_template = url_template.replace('{problem}', problem_name)
        # TODO: Add support for optional org/geo names
        url_template = url_template.replace('{org}', org_name or '')
        url_template = url_template.replace('{geo}', geo_name or '')
        return url_template

    def _derive_web_driver_class(self, web_driver_type):
        return getattr(webdriver, web_driver_type.name.capitalize())

    def _derive_web_driver_kwargs(self, web_driver_type):
        if web_driver_type is self.WebDriverType.CHROME:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--disable-extensions')
            return dict(chrome_options=chrome_options)
        elif web_driver_type is self.WebDriverType.FIREFOX:
            raise NotImplementedError('Firefox not yet supported')

    def __init__(self, directory, problem_name, org_name=None, geo_name=None,
                 web_driver_type=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()

        self.directory = directory
        self.file_path = self._form_file_path(self.directory)
        self.configuration = self._marshall_configuration()
        self.problem_name = problem_name
        self.org_name = org_name
        self.geo_name = geo_name
        self.page_url = self._form_page_url(
            self.configuration, self.problem_name, self.org_name, self.geo_name)

        self.web_driver_type = web_driver_type or self.DEFAULT_WEB_DRIVER_TYPE
        self.web_driver_class = self._derive_web_driver_class(self.web_driver_type)
        self.web_driver_kwargs = self._derive_web_driver_kwargs(self.web_driver_type)
        self.web_driver = None
