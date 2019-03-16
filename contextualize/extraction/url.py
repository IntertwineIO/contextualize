#!/usr/bin/env python
# -*- coding: utf-8 -*-
import urllib
from collections import OrderedDict
from itertools import zip_longest

from contextualize.exceptions import NoneValueError
from contextualize.utils.enum import FlexEnum
from contextualize.utils.iterable import one
from contextualize.utils.structures import DotNotatableOrderedDict
from contextualize.utils.tools import is_nonstring_sequence, load_class


class BaseURLConstructor(DotNotatableOrderedDict):

    INDEX_TAG = 'index'
    TERM_TAG = 'term'

    TOKEN_TEMPLATE = '{{{}}}'

    def _construct_clause(self, template, search_data, topic=None, term=None, index=1):
        """
        Construct clause

        Construct URL clause by recursing depth-first to replace
        template tokens via search topic, term, and/or index.

        I/O:
        template:     string with 1+ tokens specified via {}
        search_data:  encoded search data, an ordered dict
        topic=None:   search data key to specify any terms
        term=None:    search data term
        index=1:      index specified by a URL clause series
        return:       rendered URL template
        raise:        NoneValueError if token value is None term
                      ValueError if unknown token
                      TypeError if unexpected type in configuration
        """
        definitions = self.definitions
        rendered = template
        tokens = self._find_tokens(template)

        for token in tokens:
            if token == self.INDEX_TAG:
                value = str(index)

            elif token in search_data or token == self.TERM_TAG:
                terms = self._get_relevant_search_terms(token, topic, search_data)
                term_index = index - 1 if term else 0
                value = terms[term_index]

            elif token in definitions:
                token_value = definitions[token]

                if isinstance(token_value, str):
                    value = self._construct_clause(template=token_value,
                                                   search_data=search_data,
                                                   topic=topic,
                                                   term=term,
                                                   index=index)

                elif isinstance(token_value, URLClauseSeries):
                    value = token_value.construct(search_data, topic)

                else:
                    raise TypeError(f"Expected str or dict for '{token}' value; "
                                    f"received '{type(token_value)}': {token_value}")
            else:
                raise ValueError(f'Unknown token: {token}')

            rendered = rendered.replace(self.TOKEN_TEMPLATE.format(token), value)

        return rendered

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


class URLConstructor(BaseURLConstructor):

    URL_TAG = 'url'
    URL_TEMPLATE_TAG = 'url_template'

    def __init__(self, url_template, **kwargs):
        super().__init__()
        self.url_template = url_template
        self.definitions = {k: self.configure_field(k, v) for k, v in kwargs.items()}

    @property
    def is_shorthand(self):
        return not(self.definitions)

    @classmethod
    def from_dict(cls, configuration):
        # Support shorthand form for hard-coded urls
        if isinstance(configuration, str):
            return cls(url_template=configuration)

        return cls(**configuration)

    def configure_field(self, field, value):
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            return URLClauseSeries.from_dict(value, self)
        raise TypeError(f"Expected str or dict for '{field}'. Received '{type(value)}': {value}")

    def construct(self, search_data):
        """Construct URL from search data"""
        if self.is_shorthand:
            return self.url_template

        encoded_search_data = OrderedDict(self._encode_search_data(k, v)
                                          for k, v in search_data.items())

        return self._construct_clause(template=self.url_template, search_data=encoded_search_data)

    def _encode_search_data(self, key, value):
        """Return key & URL-encoded value, a list of strings or None"""
        if value is None:
            return key, None
        if isinstance(value, str):
            return key, [urllib.parse.quote(value)]
        if is_nonstring_sequence(value):
            return key, [urllib.parse.quote(v) for v in value]
        raise TypeError(f"Expected string or list/tuple for '{key}'; "
                        f"received '{type(value)}': {value}")


class URLClauseSeries(BaseURLConstructor):

    SERIES_TAG = 'series'
    TEMPLATES_TAG = 'templates'
    DELIMITER_TAG = 'delimiter'

    SERIES_CLASS_NAME_TEMPLATE = 'URL{}Series'
    SeriesType = FlexEnum('URLClauseSeriesType', 'TOPIC TERM')

    def __init__(self, templates, delimiter, constructor):
        self.templates = templates
        self.delimiter = delimiter
        self.constructor = constructor

    @property
    def definitions(self):
        return self.constructor.definitions

    @classmethod
    def from_dict(cls, configuration, constructor):
        series_type = cls.SeriesType[configuration[cls.SERIES_TAG]]
        series_class = cls._get_series_class(series_type)

        return series_class(templates=configuration[cls.TEMPLATES_TAG],
                            delimiter=configuration[cls.DELIMITER_TAG],
                            constructor=constructor)

    @classmethod
    def _get_series_class(cls, series_type):
        series_type_name = series_type.name.capitalize()
        series_class_name = cls.SERIES_CLASS_NAME_TEMPLATE.format(series_type_name)
        return load_class(f'{cls.__module__}.{series_class_name}')


class URLTopicSeries(URLClauseSeries):

    def construct(self, search_data, topic=None):
        """Construct URL topic series based on search data and topic"""
        templates = self.templates
        clauses = []
        index = 1
        for template, topic in zip_longest(templates, search_data, fillvalue=templates[-1]):
            try:
                clause = self._construct_clause(template=template,
                                                search_data=search_data,
                                                topic=topic,
                                                index=index)
            except NoneValueError:
                continue  # skip clauses with null search topics
            else:
                clauses.append(clause)
                index += 1

        return self.delimiter.join(clauses)


class URLTermSeries(URLClauseSeries):

    def construct(self, search_data, topic=None):
        """Construct URL term series based on search data and topic"""
        templates = self.templates
        token = one(token for token in self._find_tokens(self.templates[0])
                    if token in search_data or token == self.TERM_TAG)
        terms = self._get_relevant_search_terms(token, topic, search_data)

        clauses = []
        index = 1
        for template, term in zip_longest(templates, terms, fillvalue=templates[-1]):
            try:
                clause = self._construct_clause(template=template,
                                                search_data=search_data,
                                                topic=topic,
                                                term=term,
                                                index=index)
            except NoneValueError:
                continue  # skip clauses with null search topics
            else:
                clauses.append(clause)
                index += 1

        return self.delimiter.join(clauses)
