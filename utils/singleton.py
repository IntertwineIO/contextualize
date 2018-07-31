#!/usr/bin/env python
# -*- coding: utf-8 -*-
from itertools import chain


class Singleton:
    """
    Singleton

    A base class to ensure only a single instance is created. Several
    measures are taken to encourage responsible usage:
    - The instance is only initialized once upon initial creation and
      arguments are permitted but not required
    - The constructor prohibits arguments on subsequent calls unless
      they match the initial ones as state must not change
    - Modifying __new__ in subclasses is not permitted to guard against
      side-stepping the aforementioned restrictions

    Adapted from Guido van Rossum's Singleton:
    https://www.python.org/download/releases/2.2/descrintro/#__new__
    """
    _instance_ = None
    _arguments_ = None

    def __new__(cls, *args, **kwds):

        if cls.__new__ is not Singleton.__new__:
            raise ValueError('Singletons may not modify __new__')

        if cls._instance_ is not None:
            if args or kwds:
                if (args != cls._arguments_['args'] or kwds != cls._arguments_['kwds']):
                    raise ValueError('Singleton initialization may not change')
            return cls._instance_

        cls._instance_ = instance = object.__new__(cls)
        cls._arguments_ = {'args': args, 'kwds': kwds}
        instance.initialize(*args, **kwds)
        return instance

    def initialize(self, *args, **kwds):
        pass

    def __repr__(self):
        class_name = self.__class__.__name__
        args, kwds = self._arguments_['args'], self._arguments_['kwds']
        arg_strings = (str(arg) for arg in args)
        kwd_strings = (f'{k}={v}' for k, v in kwds.items()) if kwds else ()
        full_arg_string = ', '.join(chain(arg_strings, kwd_strings))
        return f'{class_name}({full_arg_string})'
