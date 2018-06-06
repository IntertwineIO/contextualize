#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict

from utils.tools import PP, derive_attributes, json_serialize

ENCODING_DEFAULT = 'utf-8'


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

    def to_json(self, encoding=ENCODING_DEFAULT):
        """Convert and encode object to JSON bytes (default UTF-8)"""
        od = OrderedDict(self.items())
        json_unicode = json.dumps(od, ensure_ascii=False, default=json_serialize)
        return json_unicode.encode(encoding)

    def __repr__(self):
        arg_string = ', '.join(self.quoted_values())
        return f'{self.__class__.__name__}({arg_string})'

    def __str__(self):
        return PP.pformat(OrderedDict(self.items()))


# TODO: Convert to Py3.7 Data Class and generalize unique field
class Extractable(FieldMixin):
    """Extractable mixin to allow class to be extracted from websites"""
    UNIQUE_FIELD = 'source_url'

    def __init__(self, source_url=None, *args, **kwds):
        super().__init__(*args, **kwds)
        if not source_url:
            raise ValueError(f"Content missing unique key: '{source_url}'")

        self.source_url = source_url
