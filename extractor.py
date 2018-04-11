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
# from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.async import run_in_executor
from utils.debug import async_debug
from utils.structures import FlexEnum
from utils.time import flex_strptime
from utils.tools import delist, enlist, multi_parse, one, one_max, one_min

INDENT = 4
WIDTH = 160


class BaseExtractor:

    FILE_PATH_BASE = 'extractors'
    FILE_NAME = NotImplementedError

    OPTIONS_TAG = 'options'

    IS_ENABLED_TAG = 'is_enabled'
    CONTENT_TAG = 'content'
    PAGE_URL_TAG = 'page_url'
    CLAUSE_DELIMITER_TAG = 'clause_delimiter'
    SEARCH_RESULTS_TAG = 'search_results'

    ELEMENT_TAG = 'element'
    ELEMENTS_TAG = 'elements'
    INDEX_TAG = 'index'
    TEXT_TAG = 'text'
    VALUE_TAG = 'value'

    REFERENCE_TEMPLATE = '<{}>'

    # TODO: Make Content a class/model
    CONTENT_FIELDS = [
        'source_url', 'title', 'author_names', 'publication', 'volume',
        'issue', 'issue_date', 'issue_date_granularity',
        'first_page', 'last_page', 'doi',
        'published_timestamp', 'granularity_published', 'tzinfo_published',
        'publisher', 'summary', 'full_text']

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

    ExtractMethod = FlexEnum('ExtractMethod', 'ATTRIBUTE PROPERTY')
    GetMethod = FlexEnum('GetMethod', 'GET')
    ParseMethod = FlexEnum('ParseMethod', 'PARSE STRPTIME')
    FormatMethod = FlexEnum('FormatMethod', 'FORMAT STRFTIME')
    TransformMethod = FlexEnum('TransformMethod', 'JOIN SPLIT')
    # SetMethod = FlexEnum('SetMethod', 'SET')

    ExtractOperation = namedtuple('ExtractOperation',
                                  'is_multiple find_method find_args '
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
            self.pp.pprint(dict(
                msg='Extractor disabled warning',
                type='extractor_disabled_warning', extractor=repr(self)))
        try:
            await self._provision_web_driver()
            await self._fetch_page()
            return await self._extract_page()
        finally:
            await self._dispose_web_driver()

    @async_debug(offset=1)
    async def _provision_web_driver(self):
        # TODO: retrieve from web driver pool
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

    async def _extract_page(self):
        raise NotImplementedError

    @async_debug(offset=1)
    async def _dispose_web_driver(self):
        if self.web_driver:
            future_web_driver_quit = self._execute_in_future(self.web_driver.quit)
            await future_web_driver_quit

    @async_debug(offset=2)
    async def _extract_content(self, config, element, index):
        self.content = content = OrderedDict()
        for field in self.CONTENT_FIELDS:
            field_config = config[field]
            # Allow field to be set by other field without overwriting
            if field_config is None and content.get(field):
                continue
            elif not isinstance(field_config, (list, dict)):
                content[field] = field_config
                continue

            try:
                content[field] = await self._perform_operation(
                    field_config, element, index)
            # e.g. NoSuchElementException
            except Exception as e:
                self.pp.pprint(dict(
                    msg='Extract field failure',
                    type='extract_field_failure',
                    error=str(e), field=field, content=content,
                    extractor=repr(self)))

        self.content = None
        return content

    @async_debug(offset=3)
    async def _perform_operation(self, config, target, index=1):
        if isinstance(config, list):
            latest = prior = parent = target
            for operation_config in config:
                new_targets = self._select_targets(operation_config, latest, prior, parent)
                prior = latest
                for new_target in new_targets:
                    latest = await self._perform_operation(operation_config, new_target, index)
            return latest

        if isinstance(config, dict):
            operation = self._configure_operation(config)

            if operation.find_method:
                new_targets = await self._find_elements(operation, target, index)
            else:
                new_targets = enlist(target)
                if operation.wait:
                    self.loop.sleep(operation.wait)

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

    @async_debug(offset=4)
    async def _find_elements(self, operation, element, index=1):
        self._validate_element(element)
        find_method, find_by = self._derive_find_method(operation, element)
        args = (self._render_references(a) for a in operation.find_args)
        template = one(args)
        selector = template.format(index=index)

        if operation.wait:
            wait = WebDriverWait(self.web_driver, operation.wait,
                                 poll_frequency=self.WAIT_POLL_INTERVAL)
            # TODO: Make wait condition configurable rather than max wait
            wait_condition = EC.presence_of_element_located((find_by, selector))
            future_elements = self._execute_in_future(wait.until, wait_condition)
        else:
            future_elements = self._execute_in_future(find_method, selector)

        new_elements = await future_elements
        return enlist(new_elements)

    @async_debug(offset=4)
    async def _click_elements(self, elements):
        """
        Click elements sequentially

        Subsequent operations may require web driver to wait for DOM
        changes via either implicit or explicit waits.
        """
        for element in elements:
            future_dom = self._execute_in_future(element.click)
            await future_dom

    @async_debug(offset=4)
    async def _extract_values(self, operation, elements):
        extracted_values = []
        extract_method_name = f'get_{operation.extract_method.name.lower()}'
        args = (self._render_references(a) for a in operation.extract_args)
        field = one(args)

        for element in elements:
            if field == self.TEXT_TAG:
                func = partial(getattr, element)
                future_value = self._execute_in_future(func, field)
                value = await future_value
                if value:
                    extracted_values.append(value)
                    continue

            func = getattr(element, extract_method_name)
            future_value = self._execute_in_future(func, field)
            value = await future_value
            extracted_values.append(value)

        return extracted_values

    @async_debug(offset=4)
    async def _get_values(self, operation, values):
        retrieved_values = []
        args = operation.get_args

        if operation.get_method is self.GetMethod.GET:
            # Ignore current values; get new ones based on get_args
            tags = one_min(args)
            retrieved_values = [self._get_by_reference_tag(t) for t in tags]

        return retrieved_values

    @async_debug(offset=4)
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
                    self.pp.pprint(dict(
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

    @async_debug(offset=4)
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

    @async_debug(offset=4)
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
        while '<' in template and '>' in template:
            reference = template.split('>')[0].split('<')[-1]
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
        components = reference.split('.')
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
        return run_in_executor(self.loop, None, func, *args, **kwds)

    def _form_file_path(self, directory):
        return os.path.join(self.FILE_PATH_BASE, directory, self.FILE_NAME)

    @lru_cache(maxsize=None, typed=False)
    def _marshall_configuration(self, file_path):
        with open(file_path) as stream:
            return yaml.safe_load(stream)

    def _form_page_url(self, configuration):
        raise NotImplementedError

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
        return (f'<{class_name}: {self.directory}, {self.created_timestamp}>')

    def __init__(self, directory, web_driver_type=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.created_timestamp = self.loop.time()
        self.pp = PrettyPrinter(indent=INDENT, width=WIDTH)

        self.directory = directory
        self.file_path = self._form_file_path(self.directory)
        self.configuration = self._marshall_configuration(self.file_path)
        self.is_enabled = self.configuration.get(self.IS_ENABLED_TAG, True)

        self.page_url = self._form_page_url(self.configuration)

        self.web_driver_type = web_driver_type or self.DEFAULT_WEB_DRIVER_TYPE
        self.web_driver_class = self._derive_web_driver_class(self.web_driver_type)
        self.web_driver_kwargs = self._derive_web_driver_kwargs(self.web_driver_type)
        self.web_driver = None

        self.content = None  # Temporary storage for content being extracted


class SourceExtractor(BaseExtractor):

    FILE_NAME = 'source.yaml'


class SearchExtractor(BaseExtractor):

    FILE_NAME = 'search.yaml'

    async def _extract_page(self):
        search_results = await self._extract_search_results()
        return search_results

    @async_debug(offset=1)
    async def _extract_search_results(self):
        content_config = self.configuration[self.CONTENT_TAG]
        search_results_config = content_config[self.SEARCH_RESULTS_TAG]

        elements = await self._perform_operation(search_results_config,
                                                 self.web_driver)

        self.search_results = []

        if elements is not None:
            for index, element in enumerate(elements, start=1):
                try:
                    content = await self._extract_content(content_config,
                                                          element, index)
                    if not content:
                        raise ValueError('No content extracted')
                    self.search_results.append(content)

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

        return self.search_results

    def _form_page_url(self, configuration):
        problem_name = self.problem_name
        org_name = self.org_name
        geo_name = self.geo_name

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

        self.search_results = None  # Store results after extraction

        super().__init__(directory, web_driver_type, loop)
