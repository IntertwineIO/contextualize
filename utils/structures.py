#!/usr/bin/env python
# -*- coding: utf-8 -*-
from itertools import chain


class InfinIterator:
    """
    InfinIterator, the infinite iterator

    Useful for testing functions that work on iterators, since unlike
    most other iterators, this one can be used any number of times.
    """
    def __iter__(self):
        return self

    def __next__(self):
        try:
            value = self.values[self.index]
        except IndexError:
            self.index = 0  # Reset to 0 so it can be used again
            raise StopIteration()
        self.index += 1
        return value

    def __init__(self, iterable):
        self.values = iterable if isinstance(iterable, (list, tuple)) else list(iterable)
        self.index = 0


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
    __instance = None
    __arguments = None

    def __new__(cls, *args, **kwds):

        if cls.__new__ is not Singleton.__new__:
            raise ValueError('Singletons may not modify __new__')

        if cls.__instance is not None:
            if args or kwds:
                if (args != cls.__arguments['args'] or kwds != cls.__arguments['kwds']):
                    raise ValueError('Singleton initialization may not change')
            return cls.__instance

        cls.__instance = instance = object.__new__(cls)
        cls.__arguments = {'args': args, 'kwds': kwds}
        instance.initialize(*args, **kwds)
        return instance

    def initialize(self, *args, **kwds):
        pass

    def __repr__(self):
        class_name = self.__class__.__name__
        args, kwds = self.__arguments['args'], self.__arguments['kwds']
        arg_strings = (str(arg) for arg in args)
        kwd_strings = (f'{k}={v}' for k, v in kwds.items()) if kwds else ()
        full_arg_string = ', '.join(chain(arg_strings, kwd_strings))
        return f'{class_name}({full_arg_string})'
