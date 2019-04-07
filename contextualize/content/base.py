#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from collections import OrderedDict
from itertools import chain

from contextualize.utils.enum import FlexEnum
from contextualize.utils.serialization import NULL, serialize_nonstandard, serialize
from contextualize.utils.time import GranularDateTime
from contextualize.utils.tools import PP, derive_attributes, load_class


class FieldMixin:

    _fields = {}

    @classmethod
    def fields(cls):
        """Return list of all fields defined on the class"""
        try:
            return cls._fields[cls.__name__]
        except KeyError:
            cls._fields[cls.__name__] = derive_attributes(cls)
            return cls._fields[cls.__name__]

    def items(self):
        """Return generator that emits all field/value tuples"""
        return ((f, getattr(self, f)) for f in self.fields())

    def values(self):
        """Return generator that emits all values"""
        return (getattr(self, f) for f in self.fields())

    def repr_values(self):
        """Return generator that emits the repr of each value"""
        return (f"{v!r}" for v in self.values())

    def __repr__(self):
        arg_string = ', '.join(self.repr_values())
        return f'{self.__class__.__name__}({arg_string})'

    def __str__(self):
        return PP.pformat(OrderedDict(self.items()))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return all(self_value == other_value for self_value, other_value
                       in zip(self.values(), other.values()))
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return any(self_value != other_value for self_value, other_value
                       in zip(self.values(), other.values()))
        return NotImplemented


class Hashable(FieldMixin):
    ENCODING_DEFAULT = 'utf-8'

    @classmethod
    def from_hash(cls, hashed, encoding=None):
        """Instantiate from hashed object, optionally decoding too"""
        is_encoded = isinstance(next(iter(hashed.values())), bytes)
        if encoding and not is_encoded:
            raise TypeError('Encoding provided, but content not encoded')
        encoding = (encoding or cls.ENCODING_DEFAULT) if is_encoded else None

        model_key = '__model__'.encode(encoding) if is_encoded else '__model__'
        try:
            model_specifier = hashed[model_key]
            model_specifier = str(model_specifier, encoding) if is_encoded else model_specifier
        except (KeyError, TypeError):
            raise TypeError('Model key is improperly encoded or missing')

        if cls.get_specifier() != model_specifier:
            model = load_class(model_specifier)
            return model.from_hash(hashed, encoding)

        field_data = ((k, v) for k, v in hashed.items() if k != model_key)

        if is_encoded:
            field_data = ((k.decode(encoding), v.decode(encoding)) for k, v in field_data)

        field_hash = dict(field_data)
        init_kwds = OrderedDict()

        for field in cls.fields():
            if field in field_hash:
                value = field_hash[field]
                if value == NULL or value is None:
                    init_kwds[field] = None
                    continue
                custom_method_name = f'deserialize_{field}'
                if hasattr(cls, custom_method_name):
                    custom_method = getattr(cls, custom_method_name)
                    init_kwds[field] = custom_method(value, **field_hash)
                else:
                    init_kwds[field] = value

        return cls(**init_kwds)

    def to_hash(self, encoding=None):
        """Convert to ordered dict, optionally encoding as well"""
        cls = self.__class__
        model_data = (('__model__', cls.get_specifier()),)
        field_data = ((k, serialize(v)) for k, v in self.items() if v is not None)
        serialized = chain(model_data, field_data)
        if encoding:
            serialized = ((k.encode(encoding), v.encode(encoding)) for k, v in serialized)
        return OrderedDict(serialized)

    def to_json(self, encoding=None):
        """Convert object to JSON, optionally encoding as well"""
        od = OrderedDict(self.items())
        rendered_json = json.dumps(od, ensure_ascii=False, default=serialize_nonstandard)
        if encoding:
            rendered_json = rendered_json.encode(encoding)
        return rendered_json

    @classmethod
    def get_specifier(cls):
        return f'{cls.__module__}.{cls.__qualname__}'

    @classmethod
    def deserialize_datetime(cls, dt_string):
        return GranularDateTime.deserialize(dt_string)

    @classmethod
    def deserialize_enum(cls, enum_specifier):
        return FlexEnum.deserialize(enum_specifier)

    def __str__(self):
        return PP.pformat(self.to_hash())


# TODO: Convert to Py3.7 Data Class and generalize unique field
class Extractable(Hashable):
    """Extractable mixin to allow class to be extracted from websites"""
    SOURCE_KEY_FIELD = 'source_url'

    def __init__(self, source_url=None, *args, **kwds):
        super().__init__(*args, **kwds)
        if not source_url:
            raise ValueError(f"Content missing unique key: '{source_url}'")

        self.source_url = source_url
