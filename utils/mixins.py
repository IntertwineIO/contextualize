#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from collections import OrderedDict
from itertools import chain

from utils.serialization import safe_decode, safe_encode, serialize_nonstandard, serialize
from utils.tools import PP, derive_attributes, load_class


class FieldMixin:

    _fields = {}

    def items(self):
        """Return generator that emits all field/value tuples"""
        return ((f, getattr(self, f)) for f in self.fields())

    def values(self):
        """Return generator that emits all values"""
        return (getattr(self, f) for f in self.fields())

    def quoted_values(self):
        """Return generator that emits all values, quoted unless None"""
        return (f"'{v}'" if v is not None else 'None' for v in self.values())

    @classmethod
    def fields(cls):
        """Return list of all fields defined on the class"""
        try:
            return cls._fields[cls.__name__]
        except KeyError:
            cls._fields[cls.__name__] = derive_attributes(cls)
            return cls._fields[cls.__name__]

    def __repr__(self):
        arg_string = ', '.join(self.quoted_values())
        return f'{self.__class__.__name__}({arg_string})'

    def __str__(self):
        return PP.pformat(self.to_hash())


class Hashable(FieldMixin):
    ENCODING_DEFAULT = 'utf-8'

    @classmethod
    def from_hash(cls, content_hash, encoding=None):
        is_encoded = isinstance(next(iter(content_hash.values())), bytes)
        model_key = b'__model__' if is_encoded else '__model__'

        if cls is Hashable:
            model_specifier = content_hash[model_key]
            model = load_class(model_specifier)
            return model.from_hash(content_hash, encoding)

        field_info = ((k, v) for k, v in content_hash.items() if k != model_key)

        if is_encoded:
            encoding = encoding or cls.ENCODING_DEFAULT
            field_info = ((k.decode(encoding), safe_decode(v, encoding)) for k, v in field_info)

        field_info_map = dict(field_info)
        kwds = OrderedDict()

        for field in cls.fields():
            if field in field_info_map:
                raw_value = field_info_map[field]
                custom_method_name = f'deserialize_{field}'
                if hasattr(cls, custom_method_name):
                    custom_method = getattr(cls, custom_method_name)
                    kwds[field] = custom_method(raw_value)
                else:
                    kwds[field] = raw_value

        return cls(**kwds)

    def to_hash(self, encoding=None):
        """Convert to ordered dict, optionally encoding as well"""
        cls = self.__class__
        model_info = (('__model__', f'{cls.__module__}.{cls.__qualname__}'),)
        field_info = ((k, serialize(v)) for k, v in self.items())
        serialized = chain(model_info, field_info)
        if encoding:
            serialized = ((k.encode(encoding), safe_encode(v, encoding)) for k, v in serialized)
        return OrderedDict(serialized)

    def to_json(self, encoding=None):
        """Convert object to JSON, optionally encoding as well"""
        od = OrderedDict(self.items())
        rendered_json = json.dumps(od, ensure_ascii=False, default=serialize_nonstandard)
        if encoding:
            rendered_json = rendered_json.encode(encoding)
        return rendered_json


# TODO: Convert to Py3.7 Data Class and generalize unique field
class Extractable(Hashable):
    """Extractable mixin to allow class to be extracted from websites"""
    UNIQUE_FIELD = 'source_url'

    def __init__(self, source_url=None, *args, **kwds):
        super().__init__(*args, **kwds)
        if not source_url:
            raise ValueError(f"Content missing unique key: '{source_url}'")

        self.source_url = source_url
