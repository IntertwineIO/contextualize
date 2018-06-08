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
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from url_normalize import url_normalize

from secret_service.agency import SecretService
from utils.async import run_in_executor
from utils.cache import AsyncCache
from utils.debug import async_debug, sync_debug
from utils.structures import FlexEnum
from utils.time import flex_strptime
from utils.statistics import HumanDwellTime, human_dwell_time, human_selection_shuffle
from utils.tools import (
    PP, delist, enlist, multi_parse, one, one_max, one_min, xor_constrain
)


class BaseExtractor:

    FILE_NAME = NotImplementedError

    OPTIONS_TAG = 'options'

    IS_ENABLED_TAG = 'is_enabled'
    SOURCE_URL_TAG = 'source_url'
    CONTENT_TAG = 'content'
    ELEMENT_TAG = 'element'
    ELEMENTS_TAG = 'elements'
    VALUE_TAG = 'value'

    REFERENCE_TEMPLATE = '<{}>'
    LEFT_REFERENCE_SYMBOL = REFERENCE_TEMPLATE[0]
    RIGHT_REFERENCE_SYMBOL = REFERENCE_TEMPLATE[-1]
    REFERENCE_DELIMITER = '.'

    WebDriverType = FlexEnum('WebDriverType', 'CHROME FIREFOX')
    WEB_DRIVER_TYPE_DEFAULT = WebDriverType.CHROME
    WAIT_MAXIMUM_DEFAULT = 10
    WAIT_POLL_INTERVAL = 0.2

    DELAY_TAG = 'delay'
    DELAY_DEFAULTS = HumanDwellTime(
        mu=0, sigma=0.5, base=1, multiplier=1, minimum=1, maximum=3)

    ####################################################################
    # TODO: Make ExtractOperation a class & encapsulate relevant methods
    ####################################################################

    SCOPE_TAG = 'scope'
    IS_MULTIPLE_TAG = 'is_multiple'
    WAIT_TAG = 'wait'
    CLICK_TAG = 'click'
    ATTRIBUTE_TAG = 'attribute'

    OperationScope = FlexEnum('OperationScope', 'PAGE PARENT PRIOR LATEST')
    OPERATION_SCOPE_DEFAULT = OperationScope.LATEST

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
    TransformMethod = FlexEnum('TransformMethod', 'EXCISE JOIN SPLIT')
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
            return await self._perform_extraction(self.page_url)
        finally:
            await self._dispose_web_driver()

    @async_debug()
    async def _provision_web_driver(self):
        # TODO: retrieve from web driver pool
        future_web_driver = self._execute_in_future(self.web_driver_class,
                                                    **self.web_driver_kwargs)
        self.web_driver = await future_web_driver
        max_implicit_wait = self.configuration.get(self.WAIT_TAG, self.WAIT_MAXIMUM_DEFAULT)
        # Configure web driver to allow waiting on each operation
        self.web_driver.implicitly_wait(max_implicit_wait)

    @async_debug()
    async def _perform_extraction(self, url=None):
        if url:
            await self._fetch_page(url)
        return await self._extract_page()

    @async_debug()
    async def _fetch_page(self, url):
        future_page = self._execute_in_future(self.web_driver.get, url)
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
    async def _extract_content(self, element, config, index=1, **kwds):
        self.content_map = content_map = OrderedDict(kwds)
        for field in self.model.fields():
            try:
                field_config = config[field]

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

    @async_debug(context="self.content_map.get('source_url')")
    async def _extract_field(self, field, element, config, index=1):
        if isinstance(config, list):
            return await self._perform_operation_series(element, config, index)
        if isinstance(config, dict):
            return await self._perform_operation(element, config, index)
        return config

    @async_debug(context="self.content_map.get('source_url')")
    async def _perform_operation_series(self, target, config, index=1):
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

    @async_debug(context="self.content_map.get('source_url')")
    async def _perform_operation(self, target, config, index=1):
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

    @async_debug(context="self.content_map.get('source_url')")
    async def _find_elements(self, operation, element, index=1):
        element = delist(element)
        self._validate_element(element)
        find_method, find_by = self._derive_find_method(operation, element)
        args = (self._render_references(a) for a in operation.find_args)
        template = one(args)
        selector = template.format(index=index)

        if operation.wait_method:
            explicit_wait = operation.wait or self.WAIT_MAXIMUM_DEFAULT
            wait = WebDriverWait(self.web_driver, explicit_wait,
                                 poll_frequency=self.WAIT_POLL_INTERVAL)
            wait_method_name = operation.wait_method.name.lower()
            wait_condition_method = getattr(expected_conditions, wait_method_name)
            locator = (find_by, selector)
            wait_condition = wait_condition_method(locator)
            future_elements = self._execute_in_future(wait.until, wait_condition)
        else:
            future_elements = self._execute_in_future(find_method, selector)

        new_elements = await future_elements
        return enlist(new_elements)

    @async_debug(context="self.content_map.get('source_url')")
    async def _click_elements(self, elements):
        """
        Click elements sequentially

        Subsequent operations may require web driver to wait for DOM
        changes via either implicit or explicit waits.
        """
        for element in elements:
            future_dom = self._execute_in_future(element.click)
            await future_dom

    @async_debug(context="self.content_map.get('source_url')")
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

    @async_debug(context="self.content_map.get('source_url')")
    async def _get_values(self, operation, values):
        retrieved_values = []
        args = operation.get_args

        if operation.get_method is self.GetMethod.GET:
            # Ignore current values; get new ones based on get_args
            tags = one_min(args)
            retrieved_values = [self._get_by_reference_tag(t) for t in tags]

        return retrieved_values

    @async_debug(context="self.content_map.get('source_url')")
    async def _parse_values(self, operation, values):
        parsed_values = []
        args = (self._render_references(a) for a in operation.parse_args)

        if operation.parse_method is self.ParseMethod.PARSE:
            templates = one_min(args)
            for value in values:
                if not value:
                    parsed_values.append(value)
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

    @async_debug(context="self.content_map.get('source_url')")
    async def _format_values(self, operation, values):
        formatted_values = []
        args = (self._render_references(a) for a in operation.format_args)

        if operation.format_method is self.FormatMethod.FORMAT:
            template = one(args)
            fields = {} if self.content_map is None else self.content_map
            if self.VALUE_TAG in fields:
                raise ValueError(
                    "Reserved word '{self.VALUE_TAG}' cannot be content field")
            fields[self.VALUE_TAG] = None
            for value in values:
                fields[self.VALUE_TAG] = value
                formatted = template.format(**fields)
                formatted_values.append(formatted)
            del fields[self.VALUE_TAG]

        elif operation.format_method is self.FormatMethod.STRFTIME:
            template = one(args)
            for value in values:
                formatted = value.strftime(template)
                formatted_values.append(formatted)

        return formatted_values

    @async_debug(context="self.content_map.get('source_url')")
    async def _transform_values(self, operation, values):
        transformed_values = []
        args = (self._render_references(a) for a in operation.transform_args)

        if operation.transform_method is self.TransformMethod.EXCISE:
            snippets = one_min(args)
            for value in values:
                cleansed_value = value
                for snippet in snippets:
                    cleansed_value = cleansed_value.replace(snippet, '')
                transformed_values.append(cleansed_value)

        elif operation.transform_method is self.TransformMethod.JOIN:
            delimiter = one(args)
            joined_values = delimiter.join(values)
            transformed_values.append(joined_values)

        elif operation.transform_method is self.TransformMethod.SPLIT:
            delimiter = one(args)
            for value in values:
                split_values = value.split(delimiter)
                transformed_values.extend(split_values)

        return transformed_values

    @async_debug(context="self.content_map.get('source_url')")
    async def _cache_content(self, unique_key, content):
        redis = self.cache.client
        await redis.hmset_dict(unique_key, content.jsonify())

    @sync_debug(context="self.content_map.get('source_url')")
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

    @sync_debug(context="self.content_map.get('source_url')")
    def _derive_operation_scope(self, config):
        if self.SCOPE_TAG in config:
            operation_scope = config[self.SCOPE_TAG]
            return self.OperationScope[operation_scope.upper()]

        return self.OPERATION_SCOPE_DEFAULT

    @sync_debug(context="self.content_map.get('source_url')")
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

    def _configure_method(self, config, method_enum):
        method_keys = method_enum.set(str.lower)
        method_key = one_max(k for k in config if k in method_keys)
        if not method_key:
            return None, None
        method_type = method_enum[method_key.upper()]
        method_args = enlist(config[method_key])
        return method_type, method_args

    @sync_debug(context="self.content_map.get('source_url')")
    def _derive_find_method(self, operation, element):
        element_tag = (self.ELEMENTS_TAG if operation.is_multiple
                       else self.ELEMENT_TAG)
        method_tag = operation.find_method.name.lower()
        find_method_name = f'find_{element_tag}_by_{method_tag}'
        find_method = getattr(element, find_method_name)
        find_by = getattr(By, operation.find_method.name)
        return find_method, find_by

    @sync_debug(context="self.content_map.get('source_url')")
    def _render_references(self, template):
        while (self.LEFT_REFERENCE_SYMBOL in template and
               self.RIGHT_REFERENCE_SYMBOL in template):
            reference = (template.split(self.RIGHT_REFERENCE_SYMBOL)[0]
                                 .split(self.LEFT_REFERENCE_SYMBOL)[-1])
            if not reference:
                return template
            reference_tag = self.REFERENCE_TEMPLATE.format(reference)
            value = self._get_by_reference(reference) or ''
            template = template.replace(reference_tag, str(value))

        return template

    @sync_debug(context="self.content_map.get('source_url')")
    def _get_by_reference_tag(self, reference_tag):
        parsed = parse(self.REFERENCE_TEMPLATE, reference_tag)
        if not parsed:
            raise ValueError(f"parse('{self.REFERENCE_TEMPLATE}', "
                             f"'{reference_tag}') failed to find match")
        reference = parsed.fixed[0]
        return self._get_by_reference(reference)

    @sync_debug(context="self.content_map.get('source_url')")
    def _get_by_reference(self, reference):
        components = reference.split(self.REFERENCE_DELIMITER)
        field_name = components[0]
        value = self.content_map[field_name]
        if value:
            for component in components[1:]:
                value = getattr(value, component)
        return value

    def _validate_element(self, value):
        if not isinstance(value, (self.web_driver_class, WebElement)):
            raise TypeError(f'Expected driver or element. Received: {value}')

    @sync_debug(context="self.content_map.get('source_url')")
    def _execute_in_future(self, func, *args, **kwds):
        """Run in executor with kwds support & default loop/executor"""
        return run_in_executor(self.loop, None, func, *args, **kwds)

    # Initialization Methods

    @sync_debug()
    def _form_file_path(self, base, directory):
        return os.path.join(base, directory, self.FILE_NAME)

    @lru_cache(maxsize=None, typed=False)
    def _marshall_configuration(self, file_path):
        with open(file_path) as stream:
            return yaml.safe_load(stream)

    @sync_debug()
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
                config=delay_overrides, unsupported=unsupported, extractor=repr(self)))

        return delay_config

    @sync_debug()
    def _derive_web_driver_class(self, web_driver_type):
        return getattr(webdriver, web_driver_type.name.capitalize())

    @sync_debug()
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

    def __init__(self, model, directory, web_driver_type=None, cache=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.cache = cache or AsyncCache(self.loop)
        self.created_timestamp = self.loop.time()

        self.model = model
        self.base_directory = model.BASE_DIRECTORY
        self.directory = directory
        self.file_path = self._form_file_path(self.base_directory, self.directory)
        self.configuration = self._marshall_configuration(self.file_path)
        self.is_enabled = self.configuration.get(self.IS_ENABLED_TAG, True)
        self.delay_configuration = self._configure_delay(self.configuration)

        self.web_driver_type = web_driver_type or self.WEB_DRIVER_TYPE_DEFAULT
        self.web_driver_class = self._derive_web_driver_class(self.web_driver_type)
        self.web_driver_kwargs = self._derive_web_driver_kwargs(self.web_driver_type)
        self.web_driver = None

        self.content_map = None  # Temporary storage for content being extracted

    def __repr__(self):
        class_name = self.__class__.__name__
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

    @async_debug()
    async def _extract_page(self):
        content_config = self.configuration[self.CONTENT_TAG]
        try:
            content = await self._extract_content(self.web_driver, content_config,
                                                  source_url=self.page_url)
        except Exception as e:
            PP.pprint(dict(
                msg='Extract content failure', type='extract_content_failure',
                error=e, extractor=repr(self), config=content_config))
            raise
        else:
            await self._cache_content(self.page_url, content)
            return content

    @sync_debug()
    @classmethod
    def provision_extractors(cls, model, urls=None, delay_configuration=None,
                             web_driver_type=None, cache=None, loop=None):
        """
        Provision Extractors

        Instantiate and yield source extractors for the given urls.

        I/O:
        model:                      Extractable content class
        urls=None:                  List of content URL strings
        delay_configuration=None:   Configuration to stagger extractions
        web_driver_type=None:       WebDriverType, e.g. CHROME (default)
        cache=None:                 AsyncCache singleton (optional)
        loop=None:                  Event loop (optional)
        yield:                      Fully configured source extractors
        """
        loop = loop or asyncio.get_event_loop()
        cache = cache or AsyncCache()
        web_driver_type = web_driver_type or cls.WEB_DRIVER_TYPE_DEFAULT

        human_selection_shuffle(urls)
        delay_config = delay_configuration or cls.DELAY_DEFAULTS._asdict()
        initial_delay = 0
        for url in urls:
            try:
                initial_delay += human_dwell_time(**delay_config)
                extractor = cls(model, page_url=url, initial_delay=initial_delay,
                                web_driver_type=web_driver_type, cache=cache, loop=loop)
                if extractor.is_enabled:
                    yield extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

    @sync_debug()
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

    @sync_debug()
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

    def __init__(self, model, page_url, initial_delay=0, web_driver_type=None,
                 cache=None, loop=None):
        self.initial_delay = initial_delay
        self.page_url = url_normalize(page_url)
        directory = self._derive_directory(model, self.page_url)
        super().__init__(model, directory, web_driver_type, cache, loop)


class MultiExtractor(BaseExtractor):

    FILE_NAME = 'multi.yaml'

    # URL keys
    URL_TAG = 'url'
    URL_TEMPLATE_TAG = 'url_template'
    CLAUSE_SERIES_TAG = 'clause_series'
    CLAUSE_SERIES_TOKEN = f'{{{CLAUSE_SERIES_TAG}}}'
    CLAUSE_DELIMITER_TAG = 'clause_delimiter'
    CLAUSE_INDEX_TAG = 'clause_index'
    # PAGE_INDEX_TAG = 'page_index'

    # Pagination keys
    PAGINATION_TAG = 'pagination'
    PAGES_TAG = 'pages'
    NEXT_PAGE_TAG = 'next_page'
    NEXT_PAGE_CLICK_TAG = 'next_page_click'
    NEXT_PAGE_URL_TAG = 'next_page_url'

    EXTRACT_SOURCES_TAG = 'extract_sources'
    ITEMS_TAG = 'items'

    @async_debug()
    async def _perform_extraction(self, url=None):
        url = url or self.page_url
        results = await super()._perform_extraction(url)

        pagination_config = self.configuration.get(self.PAGINATION_TAG)
        if not pagination_config:
            return results

        pages = pagination_config.get(self.PAGES_TAG, float('Inf'))
        if pages < 2:
            return results

        click_config = pagination_config.get(self.NEXT_PAGE_CLICK_TAG)
        url_config = pagination_config.get(self.NEXT_PAGE_URL_TAG)
        next_page_operation_config = xor_constrain(click_config, url_config)
        via_url = bool(url_config)
        updated_results = await self._extract_following_pages(
            next_page_operation_config, pages, via_url)

        return updated_results if updated_results else results

    @async_debug()
    async def _extract_following_pages(self, config, pages, via_url=False):
        updated_results = None
        page = 1
        while page < pages:
            try:
                next_page_result = await self._extract_field(
                    self.NEXT_PAGE_TAG, self.web_driver, config, page)
            except NoSuchElementException:
                break  # Last page always fails to find next element

            page += 1
            delay = human_dwell_time(**self.delay_configuration)
            await asyncio.sleep(delay)
            url = next_page_result if via_url else None
            updated_results = await super()._perform_extraction(url)

        return updated_results

    @async_debug()
    async def _extract_page(self):
        return await self._extract_multiple()

    @async_debug()
    async def _extract_multiple(self):
        content_config = self.configuration[self.CONTENT_TAG]
        items_config = content_config[self.ITEMS_TAG]
        unique_field = self.model.UNIQUE_FIELD

        elements = await self._extract_field(self.ELEMENTS_TAG, self.web_driver, items_config)

        if elements is not None:
            for index, element in enumerate(elements, start=1):
                try:
                    content = await self._extract_content(element, content_config, index)

                    unique_key = getattr(content, unique_field)
                    if not unique_key:
                        raise ValueError(f"Content missing value for '{unique_field}'")

                except Exception as e:
                    PP.pprint(dict(
                        msg='Extract content failure', type='extract_content_failure',
                        error=e, index=index, extractor=repr(self), config=content_config))

                if unique_key in self.item_results:
                    PP.pprint(dict(
                        msg='Unique key collision', type='unique_key_collision',
                        field=unique_field, unique_key=unique_key,
                        old_content=self.item_results[unique_key], new_content=content,
                        index=index, extractor=repr(self), config=content_config))

                await self._cache_content(unique_key, content)
                self.item_results[unique_key] = content

        else:
            PP.pprint(dict(
                msg='Extract item results failure', type='extract_item_results_failure',
                extractor=repr(self), config=items_config))

        if self.configuration.get(self.EXTRACT_SOURCES_TAG, True):
            if unique_field != self.SOURCE_URL_TAG:
                raise ValueError('Unique field must be '
                                 f"'{self.SOURCE_URL_TAG}' to extract sources")
            # TODO: Store preliminary results in redis
            source_results = await self._extract_sources(self.item_results)
            await self._combine_results(self.item_results, source_results)

        return self.item_results

    @async_debug()
    async def _extract_sources(self, item_results):
        source_urls = [content.source_url for content in item_results.values()]
        source_extractors = SourceExtractor.provision_extractors(
            self.model, source_urls, self.delay_configuration,
            self.web_driver_type, self.cache, self.loop)
        futures = [extractor.extract() for extractor in source_extractors]
        if not futures:
            return []
        done, pending = await asyncio.wait(futures)
        source_results = [task.result() for task in done]
        return source_results

    @async_debug()
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

    @sync_debug()
    @classmethod
    def provision_extractors(cls, model, url_fragments=None, web_driver_type=None,
                             cache=None, loop=None):
        """
        Provision Extractors

        Instantiate and yield all multi extractors configured with the
        given model and url fragments.

        I/O:
        model:                  Any Extractable content class (via
                                Extractable mixin); defines base config
                                directory and fields to extract.

        url_fragments=None:     Dictionary of all url fragments required
                                to render a complete url, as defined by
                                each url configuration.

                                Example:
                                    dict(problem='Homelessness',
                                         org=None,
                                         geo='Texas')

        web_driver_type=None:   WebDriverType, e.g. CHROME (default)
        cache=None:             AsyncCache singleton (optional)
        loop=None:              Event loop (optional)
        yield:                  Fully configured search extractors
        """
        loop = loop or asyncio.get_event_loop()
        cache = cache or AsyncCache()
        web_driver_type = web_driver_type or cls.WEB_DRIVER_TYPE_DEFAULT

        base = model.BASE_DIRECTORY
        dir_nodes = os.walk(base)
        directories = (cls._debase_directory(base, dn[0]) for dn in dir_nodes
                       if cls.FILE_NAME in dn[2])

        for directory in directories:
            try:
                extractor = cls(model, directory, url_fragments, web_driver_type, cache, loop)
                if extractor.is_enabled:
                    yield extractor
            # FileNotFoundError, ruamel.yaml.scanner.ScannerError, ValueError
            except Exception as e:
                print(e)  # TODO: Replace with logging

    @classmethod
    def _debase_directory(cls, base, path):
        base = os.path.join(base, '')  # Add slash
        if not path.startswith(base):
            raise ValueError(f"'{path}' must start with '{base}'")
        directory = path.replace(base, '', 1)
        return directory

    @sync_debug()
    def _encode_url_fragments(self, url_fragments):
        return {k: urllib.parse.quote(v) for k, v in url_fragments.items()
                if v is not None}

    @sync_debug()
    def _form_page_url(self, configuration, url_fragments):
        url_config = self.configuration[self.URL_TAG]
        # Support shorthand form for hard-coded urls
        if isinstance(url_config, str):
            return url_config

        encoded_fragments = self._encode_url_fragments(url_fragments)
        url_template = url_config[self.URL_TEMPLATE_TAG]
        if self.CLAUSE_SERIES_TOKEN in url_template:
            encoded_fragments[self.CLAUSE_SERIES_TAG] = self._form_clause_series(
                url_config, encoded_fragments)

        return url_template.format(**encoded_fragments)

    @sync_debug()
    def _form_clause_series(self, url_config, encoded_fragments):
        clauses = []
        clause_keys = url_config[self.CLAUSE_SERIES_TAG]
        clause_index = 1
        for clause_key in clause_keys:
            clause_template = url_config[clause_key]
            encoded_fragments[self.CLAUSE_INDEX_TAG] = str(clause_index)
            try:
                rendered_clause = clause_template.format(**encoded_fragments)
            except KeyError:
                continue
            else:
                clauses.append(rendered_clause)
                clause_index += 1

        if not clauses:
            raise ValueError(f'No clauses rendered in series: {encoded_fragments}')

        del encoded_fragments[self.CLAUSE_INDEX_TAG]
        clause_delimiter = url_config[self.CLAUSE_DELIMITER_TAG]
        return clause_delimiter.join(clauses)

    def __init__(self, model, directory, url_fragments=None,
                 web_driver_type=None, cache=None, loop=None):
        self.url_fragments = {k: v for k, v in url_fragments.items()
                              if v is not None} if url_fragments else {}
        super().__init__(model, directory, web_driver_type, cache, loop)
        self.page_url = self._form_page_url(self.configuration, self.url_fragments)
        self.item_results = OrderedDict()  # Store results after extraction

    def __repr__(self):
        class_name = self.__class__.__name__
        directory = getattr(self, 'directory', None)
        url_fragments = getattr(self, 'url_fragments', None)
        created_timestamp = getattr(self, 'created_timestamp', None)
        return (f'<{class_name} | {directory} | {url_fragments!r} | {created_timestamp}>')
