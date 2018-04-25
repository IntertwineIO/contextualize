#!/usr/bin/env python
# -*- coding: utf-8 -*-
from utils.tools import derive_attributes


class FieldMixin:

    VALUE_TAG = 'value'
    _FIELDS = None

    def items(self):
        return ((f, getattr(self, f)) for f in self.fields())

    def values(self):
        return (getattr(self, f) for f in self.fields())

    def quoted_values(self):
        return (f"'{v}'" for v in self.values())

    @classmethod
    def fields(cls):
        if cls._FIELDS is not None:
            return cls._FIELDS
        cls._FIELDS = list(derive_attributes(cls.__init__))
        return cls._FIELDS

    def __repr__(self):
        arg_string = ', '.join(self.quoted_values())
        return f'{self.__class__.__name__}({arg_string})'
