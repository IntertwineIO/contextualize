#!/usr/bin/env python
# -*- coding: utf-8 -*-
from utils.tools import derive_attributes


class FieldMixin:

    _fields = {}

    def items(self):
        return ((f, getattr(self, f)) for f in self.fields())

    def values(self):
        return (getattr(self, f) for f in self.fields())

    def quoted_values(self):
        return (f"'{v}'" for v in self.values())

    @classmethod
    def fields(cls):
        try:
            return cls._fields[cls.__name__]
        except KeyError:
            cls._fields[cls.__name__] = derive_attributes(cls)
            return cls._fields[cls.__name__]

    def __repr__(self):
        arg_string = ', '.join(self.quoted_values())
        return f'{self.__class__.__name__}({arg_string})'


class Extractable(FieldMixin):
    """Extractable mixin to allow class to be extracted from websites"""
    def __init__(self, source_url=None, *args, **kwds):
        super().__init__(*args, **kwds)
        self.source_url = source_url
