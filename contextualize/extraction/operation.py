#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
from contextlib import suppress
from functools import partial

from parse import parse
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait

from contextualize.exceptions import TooManyValuesError
from contextualize.utils.asynchronous import run_in_executor
from contextualize.utils.debug import debug
from contextualize.utils.enum import FlexEnum
from contextualize.utils.iterable import one, one_max, one_min
from contextualize.utils.time import GranularDateTime
from contextualize.utils.tools import PP, delist, enlist, multi_parse


class ExtractionOperation:

    ATTRIBUTE_TAG = 'attribute'
    CLICK_TAG = 'click'
    ELEMENT_TAG = 'element'
    ELEMENTS_TAG = 'elements'
    IS_MULTIPLE_TAG = 'is_multiple'
    UNDEFINED_TAG = '(undefined)'
    SCOPE_TAG = 'scope'
    VALUE_TAG = 'value'
    WAIT_TAG = 'wait'

    REFERENCE_TEMPLATE = '<{}>'
    LEFT_REFERENCE_SYMBOL = REFERENCE_TEMPLATE[0]
    RIGHT_REFERENCE_SYMBOL = REFERENCE_TEMPLATE[-1]
    REFERENCE_DELIMITER = '.'

    Scope = FlexEnum('ExtractionOperationScope', 'PAGE PARENT PRIOR LATEST')
    SCOPE_DEFAULT = Scope.LATEST

    FindMethod = FlexEnum('FindMethod',
                          'CLASS_NAME CSS_SELECTOR ID LINK_TEXT NAME '
                          'PARTIAL_LINK_TEXT TAG_NAME XPATH')

    # Wait times in seconds
    WAIT_EXPLICIT_DEFAULT = 10
    WAIT_POLL_INTERVAL = 0.2

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

    ExtractionMethod = FlexEnum('ExtractionMethod', 'GETATTR ATTRIBUTE PROPERTY')
    GetMethod = FlexEnum('GetMethod', 'GET')
    ParseMethod = FlexEnum('ParseMethod', 'PARSE STRPTIME')
    FormatMethod = FlexEnum('FormatMethod', 'FORMAT STRFTIME')
    TransformMethod = FlexEnum('TransformMethod', 'EXCISE JOIN SPLIT')
    # SetMethod = FlexEnum('SetMethod', 'SET')

    async def execute(self, target, index=1):
        """
        Execute

        Execute operation on the given target and index. Each operation
        may consist of the following steps:

        1. Find element(s) via configured FindMethod and selector;
           plural elements if operation is_multiple, otherwise element
        2. Wait for the configured duration
        3. Click element(s) passed or selected
        4. Extract element value(s) via configured ExtractionMethod
        5. Get value(s) by configured reference; ignore current values
        6. Parse value(s) via configured ParseMethod and templates
        7. Format value(s) via configured FormatMethod and templates
        8. Transform value(s) via configured TransformMethod and args

        I/O:
        target:   Driver or element if lone operation or 1st in series
                  Driver, element, or other value if 2nd+ in series
        index=1:  Element number on page, 1-indexed
        return:   Single value after performing all configured steps
        """
        if self.find_method:
            new_targets = await self._find_elements(target, index)
        else:
            new_targets = enlist(target)
            if self.wait:
                await asyncio.sleep(self.wait)

        if self.click:
            await self._click_elements(new_targets)

        if self.extract_method:
            values = await self._extract_values(new_targets)
        else:
            values = new_targets

        if self.get_method:
            values = await self._get_values(values)

        if self.parse_method:
            values = await self._parse_values(values)

        if self.format_method:
            values = await self._format_values(values)

        if self.transform_method:
            values = await self._transform_values(values)

        with suppress(TooManyValuesError):
            values = delist(values)

        return values

    async def _find_elements(self, element, index=1):
        """
        Find elements

        Find element(s) via operation's find method/args, is_multiple,
        and wait method/args.

        The find arg specifies a Selenium selector template that may be
        rendered with the element index.

        If is_multiple is True, the find method returns a list of
        elements matching the selector.

        If a wait method is configured, the find method is constrained
        to wait until the wait condition is satisfied.

        I/O:
        element:  Driver or element to perform the find; note that all
                  wait operation selectors use absolute paths.
        index=1:  Element number on page, 1-indexed
        return:   Element(s) found
        """
        element = delist(element)
        self._validate_element(element)
        find_method, find_by = self._derive_find_method(element)
        arguments = (self._render_references(a) for a in self.find_args)
        template = one(arguments)
        selector = template.format(index=index)

        if self.wait_method:
            explicit_wait = self.wait or self.WAIT_EXPLICIT_DEFAULT
            wait = WebDriverWait(self.web_driver, explicit_wait,
                                 poll_frequency=self.WAIT_POLL_INTERVAL)
            wait_method_name = self.wait_method.name.lower()
            wait_condition_method = getattr(expected_conditions, wait_method_name)
            locator = (find_by, selector)
            wait_condition = wait_condition_method(locator)
            future_elements = self._execute_in_future(wait.until, wait_condition)
        else:
            future_elements = self._execute_in_future(find_method, selector)

        new_elements = await future_elements
        return enlist(new_elements)

    def _validate_element(self, value):
        """Validate that value is web driver or element"""
        if not isinstance(value, (type(self.web_driver), WebElement)):
            raise TypeError(f'Expected driver or element. Received: {value}')

    def _derive_find_method(self, element):
        """Derive find (method, by) from operation and given element"""
        element_tag = self.ELEMENTS_TAG if self.is_multiple else self.ELEMENT_TAG
        method_tag = self.find_method.name.lower()
        find_method_name = f'find_{element_tag}_by_{method_tag}'
        find_method = getattr(element, find_method_name)
        find_by = getattr(By, self.find_method.name)
        return find_method, find_by

    async def _click_elements(self, elements):
        """
        Click elements sequentially

        Subsequent operations may require web driver to wait for DOM
        changes via either implicit or explicit waits.
        """
        for element in elements:
            future_dom = self._execute_in_future(element.click)
            await future_dom

    async def _extract_values(self, elements):
        """Extract values via the operation's extract method/args"""
        extracted_values = []
        arguments = (self._render_references(a) for a in self.extract_args)
        field = one(arguments)
        extract_method = self.extract_method

        if extract_method is self.ExtractionMethod.GETATTR:
            for element in elements:
                func = partial(getattr, element)
                future_value = self._execute_in_future(func, field)
                value = await future_value
                extracted_values.append(value)

        elif (extract_method is self.ExtractionMethod.ATTRIBUTE or
              extract_method is self.ExtractionMethod.PROPERTY):
            extract_method_name = f'get_{extract_method.name.lower()}'
            for element in elements:
                func = getattr(element, extract_method_name)
                future_value = self._execute_in_future(func, field)
                value = await future_value
                extracted_values.append(value)

        return extracted_values

    async def _get_values(self, values):
        """Get values from another field of the same item"""
        retrieved_values = []
        arguments = self.get_args

        if self.get_method is self.GetMethod.GET:
            # Ignore current values; get new ones based on get_args
            tags = one_min(arguments)
            retrieved_values = [self._get_by_reference_tag(t) for t in tags]

        return retrieved_values

    async def _parse_values(self, values):
        """Parse values via the operation's parse method/args"""
        parsed_values = []
        arguments = (self._render_references(a) for a in self.parse_args)

        if self.parse_method is self.ParseMethod.PARSE:
            templates = one_min(arguments)
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
                        msg='Extractor parse failure', type='extractor_parse_failure',
                        templates=templates, value=value, parsed=parsed,
                        operation=repr(self), extractor=repr(self.extractor), error=e))

        elif self.parse_method is self.ParseMethod.STRPTIME:
            templates = one_min(arguments)
            for value in values:
                if value is None:
                    parsed_values.append(None)
                    continue
                parsed = GranularDateTime.strptime(value, *templates)
                parsed_values.append(parsed)

        return parsed_values

    async def _format_values(self, values):
        """Format values via the operation's format method/args"""
        formatted_values = []
        arguments = (self._render_references(a) for a in self.format_args)

        if self.format_method is self.FormatMethod.FORMAT:
            template = one(arguments)
            content_map = self.extractor.content_map
            fields = {} if content_map is None else content_map
            if self.VALUE_TAG in fields:
                raise ValueError(
                    "Reserved word '{self.VALUE_TAG}' cannot be content field")
            fields[self.VALUE_TAG] = None
            for value in values:
                fields[self.VALUE_TAG] = value
                formatted = template.format(**fields)
                formatted_values.append(formatted)
            del fields[self.VALUE_TAG]

        elif self.format_method is self.FormatMethod.STRFTIME:
            template = one(arguments)
            for value in values:
                formatted = value.strftime(template)
                formatted_values.append(formatted)

        return formatted_values

    async def _transform_values(self, values):
        """Transform values via the operation's transform method/args"""
        transformed_values = []
        arguments = (self._render_references(a) for a in self.transform_args)

        if self.transform_method is self.TransformMethod.EXCISE:
            snippets = one_min(arguments)
            for value in values:
                cleansed_value = value
                for snippet in snippets:
                    cleansed_value = cleansed_value.replace(snippet, '')
                transformed_values.append(cleansed_value)

        elif self.transform_method is self.TransformMethod.JOIN:
            delimiter = one(arguments)
            joined_values = delimiter.join(values)
            transformed_values.append(joined_values)

        elif self.transform_method is self.TransformMethod.SPLIT:
            delimiter = one(arguments)
            for value in values:
                split_values = value.split(delimiter)
                transformed_values.extend(split_values)

        return transformed_values

    def _render_references(self, template):
        """Render references within a template"""
        while (self.LEFT_REFERENCE_SYMBOL in template and
               self.RIGHT_REFERENCE_SYMBOL in template):
            reference = (template.split(self.RIGHT_REFERENCE_SYMBOL)[0]
                                 .split(self.LEFT_REFERENCE_SYMBOL)[-1])
            if not reference:
                return template
            reference_tag = self.REFERENCE_TEMPLATE.format(reference)
            value = self._get_by_reference(reference)
            value_string = '' if value is None else str(value)
            template = template.replace(reference_tag, value_string)

        return template

    def _get_by_reference_tag(self, reference_tag):
        """Get (value) by reference tag using angle brackets"""
        parsed = parse(self.REFERENCE_TEMPLATE, reference_tag)
        if not parsed:
            raise ValueError(f"parse('{self.REFERENCE_TEMPLATE}', "
                             f"'{reference_tag}') failed to find match")
        reference = parsed.fixed[0]
        return self._get_by_reference(reference)

    def _get_by_reference(self, reference):
        """Get (value) by reference using dot notation from the field"""
        components = reference.split(self.REFERENCE_DELIMITER)
        field_name = components[0]
        value = self.extractor.content_map[field_name]
        if value:
            for component in components[1:]:
                value = getattr(value, component)
        return value

    def _select_targets(self, latest, prior, parent):
        """Select targets based on scope and latest/prior/parent"""
        if self.scope is self.Scope.LATEST:
            return enlist(latest)
        if self.scope is self.Scope.PRIOR:
            return enlist(prior)
        if self.scope is self.Scope.PARENT:
            return [parent]
        assert self.scope is self.Scope.PAGE
        return [self.web_driver]  # Selenium webdriver instance

    def _execute_in_future(self, func, *args, **kwds):
        """Run in executor with kwds support & default loop/executor"""
        return run_in_executor(self.loop, None, func, *args, **kwds)

    # Initialization Methods

    @classmethod
    def _configure_scope(cls, configuration):
        """Configure scope for the current operation – see Scope"""
        if cls.SCOPE_TAG in configuration:
            scope = configuration[cls.SCOPE_TAG]
            return cls.Scope[scope.upper()]

        return cls.SCOPE_DEFAULT

    @staticmethod
    def _configure_method(configuration, method_enum):
        """Configure method based on the given method enum"""
        method_keys = set(method_enum.names(transform=str.lower))
        method_key = one_max(k for k in configuration if k in method_keys)
        if not method_key:
            return None, None
        method_type = method_enum[method_key.upper()]
        method_args = enlist(configuration[method_key])
        return method_type, method_args

    @classmethod
    def from_dict(cls, configuration, field=None, extractor=None):
        """
        From dict

        Construct operation to extract a field based on the given
        configuration dictionary and extractor.

        I/O:
        configuration:   dictionary representation of an operation
        field=None:      content field name being extracted
        extractor=None:  extractor instance performing the operation
        return:          ExtractionOperation instance
        """
        scope = cls._configure_scope(configuration)
        is_multiple = configuration.get(cls.IS_MULTIPLE_TAG, False)
        find_method, find_args = cls._configure_method(configuration, cls.FindMethod)
        wait_method, wait_args = cls._configure_method(configuration, cls.WaitMethod)
        wait = configuration.get(cls.WAIT_TAG, 0)
        click = configuration.get(cls.CLICK_TAG, False)
        extract_method, extract_args = cls._configure_method(configuration, cls.ExtractionMethod)
        get_method, get_args = cls._configure_method(configuration, cls.GetMethod)
        parse_method, parse_args = cls._configure_method(configuration, cls.ParseMethod)
        format_method, format_args = cls._configure_method(configuration, cls.FormatMethod)
        transform_method, transform_args = cls._configure_method(configuration, cls.TransformMethod)

        return cls(scope=scope, is_multiple=is_multiple,
                   find_method=find_method, find_args=find_args,
                   wait_method=wait_method, wait_args=wait_args,
                   wait=wait, click=click,
                   extract_method=extract_method, extract_args=extract_args,
                   get_method=get_method, get_args=get_args,
                   parse_method=parse_method, parse_args=parse_args,
                   format_method=format_method, format_args=format_args,
                   transform_method=transform_method, transform_args=transform_args,
                   field=field, extractor=extractor)

    def __init__(self, scope, is_multiple,
                 find_method, find_args,
                 wait_method, wait_args,
                 wait, click,
                 extract_method, extract_args,
                 get_method, get_args,
                 parse_method, parse_args,
                 format_method, format_args,
                 transform_method, transform_args,
                 field=None, extractor=None):

        self.scope = scope
        self.is_multiple = is_multiple
        self.find_method = find_method
        self.find_args = find_args
        self.wait_method = wait_method
        self.wait_args = wait_args
        self.wait = wait
        self.click = click
        self.extract_method = extract_method
        self.extract_args = extract_args
        self.get_method = get_method
        self.get_args = get_args
        self.parse_method = parse_method
        self.parse_args = parse_args
        self.format_method = format_method
        self.format_args = format_args
        self.transform_method = transform_method
        self.transform_args = transform_args

        self.field = field or self.UNDEFINED_TAG
        self.extractor = extractor

    @property
    def web_driver(self):
        return self.extractor.web_driver

    @property
    def loop(self):
        return self.extractor.loop

    def __repr__(self):
        class_name = self.__class__.__name__
        field = getattr(self, 'field', None)
        extractor = getattr(self, 'extractor', None)
        return (f'<{class_name} | {field} | {extractor}>')
