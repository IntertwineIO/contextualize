#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import wrapt

from utils.decor import factory_direct


@wrapt.decorator
def argh(func, instance, args, kwargs):
    print(f'args: {args}')
    print(f'kwargs: {kwargs}')
    return func(*args, **kwargs)


def print_locals(local_dict, names):
    for arg_name in names.split():
        arg_value = local_dict[arg_name]
        print(f'{arg_name}: {arg_value}')


def funcy(a, c=13, *argz, e=15, g, **kwargz):
    print_locals(locals(), 'a c argz e g kwargz')
    return a, c, argz, e, g, kwargz


class C:
    PROPS = 'kudos!'

    def instance(self, a, c=13, *argz, e=15, g, **kwargz):
        print_locals(locals(), 'a c argz e g kwargz')
        return a, c, argz, e, g, kwargz

    @classmethod
    def classy(cls, a, c=13, *argz, e=15, g, **kwargz):
        print_locals(locals(), 'a c argz e g kwargz')
        return a, c, argz, e, g, kwargz

    @staticmethod
    def staticy(a, c=13, *argz, e=15, g, **kwargz):
        print_locals(locals(), 'a c argz e g kwargz')
        return a, c, argz, e, g, kwargz

    @property
    def propsy(self):
        return self.PROPS


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'decorator',    'args',                         'error'),
    [
     (0,     argh,          [funcy],                         None),
     (1,     argh,          [C.instance],                    None),
     (2,     argh,          [C.classy],                      None),
     (3,     argh,          [C.staticy],                     None),
     (4,     argh,          [C.propsy],                      TypeError),
     (5,     argh,          [funcy, 1],                      TypeError),
     (6,     argh,          [1],                             TypeError),
     ])
def test_factory_direct_application(idx, decorator, args, error):
    """Test factory direct application"""
    assert factory_direct(decorator) is decorator

    if error:
        with pytest.raises(error):
            factory_direct(decorator, *args)

    else:
        decorated = factory_direct(decorator, *args)
        assert decorated.__wrapped__ is args[0]


def decorator_factory(*args, **kwargs):
    print(f'entering decorator factory with {args} and {kwargs}')

    def decorator(func):
        print(f'entering decorator with {kwargs}')

        @wrapt.decorator
        def wrapper(func, instance, args, kwargs):
            print(f'entering wrapper for {func}, {instance} with {args} and {kwargs}')
            value = func(*args, **kwargs)
            print('exiting wrapper')
            return value

        enwrapped = wrapper(func)
        print('exiting decorator')
        return enwrapped

    print('entering factory direct')
    decorated = factory_direct(decorator, *args)
    print('exiting factory direct')
    return decorated


class D:
    PROPS = 'kudos!'

    @decorator_factory
    def instance(self, a, c=13, *argz, e=15, g, **kwargz):
        print_locals(locals(), 'a c argz e g kwargz')
        return a, c, argz, e, g, kwargz

    @decorator_factory
    @classmethod
    def classy(cls, a, c=13, *argz, e=15, g, **kwargz):
        print_locals(locals(), 'a c argz e g kwargz')
        return a, c, argz, e, g, kwargz

    @decorator_factory
    @staticmethod
    def staticy(a, c=13, *argz, e=15, g, **kwargz):
        print_locals(locals(), 'a c argz e g kwargz')
        return a, c, argz, e, g, kwargz

    @property
    @decorator_factory
    def propsy(self):
        return self.PROPS


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'func',         'args',         'kwargs',       'check_func'),
    [
     (0,     D().instance,  [1],            {'g': 7},        C().instance),
     (1,     D.classy,      [1],            {'g': 7},        C.classy),
     (2,     D.staticy,     [1],            {'g': 7},        C.staticy),
     (3,     D.propsy,      None,           None,            C.propsy),
     ])
def test_factory_direct_calls(idx, func, args, kwargs, check_func):
    """Test factory direct calls"""
    if isinstance(func, property):
        value = func.fget(D())
        check_value = check_func.fget(C())
    else:
        value = func(*args, **kwargs)
        check_value = check_func(*args, **kwargs)

    assert value == check_value
