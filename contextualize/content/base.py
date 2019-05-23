#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from collections import OrderedDict
from dataclasses import dataclass, fields as dataclass_fields
from functools import lru_cache
from itertools import chain

from contextualize.utils.enum import FlexEnum
from contextualize.utils.serialization import NULL, serialize_nonstandard, serialize
from contextualize.utils.time import GranularDateTime
from contextualize.utils.tools import PP, load_class, represent


class FieldMixin:

    @classmethod
    @lru_cache(maxsize=None)
    def fields(cls):
        """Return tuple of all fields defined on the dataclass"""
        return dataclass_fields(cls)

    @classmethod
    def field_names(cls, include_private=False):
        """Return generator that emits field names"""
        if include_private:
            return (field.name for field in cls.fields())
        return (field.name for field in cls.fields() if field.name[0] != '_')

    def field_values(self, include_private=False):
        """Return generator that emits field values"""
        return (getattr(self, name) for name in self.field_names(include_private))

    def field_items(self, include_private=False):
        """Return generator that emits field name/value 2-tuples"""
        return ((name, getattr(self, name)) for name in self.field_names(include_private))

    def as_dict(self, include_private=False):
        """Return ordered dict field name/value pairs"""
        return OrderedDict(self.field_items(include_private))

    def __repr__(self):
        return represent(self, **self.as_dict(include_private=True))

    def __str__(self):
        return PP.pformat(self.as_dict(include_private=True))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return all(self_value == other_value for self_value, other_value
                       in zip(self.field_values(include_private=True),
                              other.field_values(include_private=True)))
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return any(self_value != other_value for self_value, other_value
                       in zip(self.field_values(include_private=True),
                              other.field_values(include_private=True)))
        return NotImplemented


class Hashable(FieldMixin):
    ENCODING_DEFAULT = 'utf-8'
    MODEL_KEY = '__model__'
    DESERIALIZE_METHOD_PREFIX = 'deserialize_'

    @classmethod
    def from_hash(cls, hashed, encoding=None):
        """Instantiate from hashed object, optionally decoding too"""
        is_encoded = isinstance(next(iter(hashed.values())), bytes)
        if encoding and not is_encoded:
            raise TypeError('Encoding provided, but content not encoded')
        encoding = (encoding or cls.ENCODING_DEFAULT) if is_encoded else None

        model_key = cls.MODEL_KEY.encode(encoding) if is_encoded else cls.MODEL_KEY
        try:
            model_specifier = hashed[model_key]
            model_specifier = str(model_specifier, encoding) if is_encoded else model_specifier
        except (KeyError, TypeError):
            raise TypeError('Model key is improperly encoded or missing')

        if cls.get_specifier() != model_specifier:
            model = load_class(model_specifier)
            return model._instantiate_from_hash(hashed, model_key, is_encoded, encoding)

        return cls._instantiate_from_hash(hashed, model_key, is_encoded, encoding)

    @classmethod
    def _instantiate_from_hash(cls, hashed, model_key, is_encoded, encoding):
        """Instantiate from hashed object, decoding as necessary"""
        field_data = ((k, v) for k, v in hashed.items() if k != model_key)

        if is_encoded:
            field_data = ((k.decode(encoding), v.decode(encoding)) for k, v in field_data)

        field_hash = dict(field_data)
        init_kwds = OrderedDict()

        for field in cls.field_names(include_private=True):
            if field in field_hash:
                value = field_hash[field]
                if value == NULL or value is None:
                    init_kwds[field] = None
                    continue
                custom_method_name = f'{cls.DESERIALIZE_METHOD_PREFIX}{field}'
                if hasattr(cls, custom_method_name):
                    custom_method = getattr(cls, custom_method_name)
                    init_kwds[field] = custom_method(value, **field_hash)
                else:
                    init_kwds[field] = value

        return cls(**init_kwds)

    def to_hash(self, encoding=None):
        """Convert to ordered dict, optionally encoding as well"""
        cls = self.__class__
        model_data = ((self.MODEL_KEY, cls.get_specifier()),)
        field_data = ((k, serialize(v)) for k, v in self.field_items(include_private=True)
                      if v is not None)
        serialized = chain(model_data, field_data)
        if encoding:
            serialized = ((k.encode(encoding), v.encode(encoding)) for k, v in serialized)
        return OrderedDict(serialized)

    def to_json(self, encoding=None):
        """Convert object to JSON, optionally encoding as well"""
        od = self.as_dict(include_private=True)
        rendered_json = json.dumps(od, ensure_ascii=False, default=serialize_nonstandard)
        if encoding:
            rendered_json = rendered_json.encode(encoding)
        return rendered_json

    @classmethod
    def get_specifier(cls):
        return f'{cls.__module__}.{cls.__qualname__}'

    @classmethod
    def datetime_from_string(cls, dt_string):
        return GranularDateTime.deserialize(dt_string)

    @classmethod
    def enum_from_string(cls, enum_specifier):
        return FlexEnum.deserialize(enum_specifier)

    def __str__(self):
        return PP.pformat(self.to_hash())


@dataclass
class Extractable(Hashable):
    """Extractable mixin to allow class to be extracted from websites"""

    source_url: str
    rank: int = None
    _cache_version: str = None
    _last_extracted: str = None

    @classmethod
    def deserialize_rank(cls, rank_string, **field_hash):
        return int(rank_string)

    @property
    def cache_version(self):
        return self._cache_version

    @cache_version.setter
    def cache_version(self, value):
        self._cache_version = value

    @property
    def last_extracted(self):
        return self._last_extracted

    @last_extracted.setter
    def last_extracted(self, value):
        self._last_extracted = value

    @classmethod
    def deserialize__last_extracted(cls, dt_string, **field_hash):
        return cls.datetime_from_string(dt_string)
