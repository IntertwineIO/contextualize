#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import OrderedDict


class DotNotatable:
    """
    DotNotatable

    A mixin providing dot notation capability to classes supporting
    __getitem__, such as dict.
    """
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as e:
            cls = self.__class__
            class_path = f'{cls.__module__}.{cls.__qualname__}'
            raise AttributeError(f"'{class_path}' object has no attribute '{name}'") from e

    def __setattr__(self, name, value):
        self[name] =  value

    def __delattr__(self, name):
        del self[name]


class DotNotatableDict(DotNotatable, dict):
    pass


class DotNotatableOrderedDict(DotNotatable, OrderedDict):
    pass
