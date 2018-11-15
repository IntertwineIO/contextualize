#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Sentinel:
    """Sentinels are unique objects for special comparison use cases"""
    _count = 0
    _registry = {}
    _default_key_template = 'sentinel_{id}'

    @classmethod
    def by_key(cls, key):
        return cls._registry[key]

    def __init__(self, key=None):
        cls = self.__class__
        self.id = cls._count
        cls._count += 1
        key = self._default_key_template.format(id=self.id) if key is None else key
        if key in cls._registry:
            raise KeyError(f"Sentinel key '{key}' already in use!")
        self.key = key
        cls._registry[key] = self

    def __repr__(self):
        class_name = self.__class__.__name__
        return f"{class_name}.by_key('{self.key}')"

    def __bool__(self):
        """Sentinels evaluate to False like None or empty string"""
        return False
