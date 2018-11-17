#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import os
import pytest
from collections import OrderedDict as OD
from itertools import chain

import wrapt

from utils.signature import CallSign, normalize
from utils.tools import ischildclass


def validate_call_sign(idx, func, args_and_kwargs, check):
    call_sign = CallSign(func)
    args, kwargs = args_and_kwargs

    if ischildclass(check, Exception):
        with pytest.raises(check):
            call_sign.signify(*args, **kwargs)
        with pytest.raises(check):
            call_sign.normalize(*args, **kwargs)
        with pytest.raises(check):
            call_sign.normalize_via_bind(*args, **kwargs)
        return

    args_check, kwargs_check = check

    normalized = call_sign.normalize(*args, **kwargs)
    assert normalized.args == args_check
    assert normalized.kwargs == kwargs_check

    normalized_via_bind = call_sign.normalize_via_bind(*args, **kwargs)
    assert normalized_via_bind.args == args_check
    assert normalized_via_bind.kwargs == kwargs_check

    call_sign.enhance_sort = True
    signified = call_sign.signify(*args, **kwargs)

    positional_only = signified.positional_only or {}
    positional_or_keyword = signified.positional_or_keyword or {}
    var_positional = signified.var_positional

    args = tuple(chain(positional_only.values(),
                       positional_or_keyword.values(),
                       var_positional.values if var_positional else ()))
    assert args == args_check

    kwargs = signified.keyword_only or {}
    var_keyword = signified.var_keyword
    kwargs.update(var_keyword.values if var_keyword else {})
    assert kwargs == kwargs_check

    argspec = inspect.getfullargspec(func)
    if var_positional:
        assert var_positional.name == argspec.varargs
    if var_keyword:
        assert var_keyword.name == argspec.varkw

    normalize_wrapper = normalize()
    normalized_func = normalize_wrapper(func)
    normalized_results = normalized_func(*args, **kwargs)
    results = func(*args, **kwargs)
    try:
        normalized_results.__eq__(results)
        assert normalized_results == results
    except Exception:
        # _io.TextIOWrapper comparison with other instance not supported
        assert func is open


@wrapt.decorator
def argh(func, instance, args, kwargs):
    print(f'args: {args}')
    print(f'kwargs: {kwargs}')
    return func(*args, **kwargs)


@argh
def full_signature(a, c=13, *argz, e=15, g, **kwargz):
    local_dict = locals()
    for arg_name in 'a c argz e g kwargz'.split():
        arg_value = local_dict[arg_name]
        print(f'{arg_name}: {arg_value}')
    return a, c, argz, e, g, kwargz


class F:
    @argh
    def imethod(self, a, c=13, *argz, e=15, g, **kwargz):
        local_dict = locals()
        for arg_name in 'a c argz e g kwargz'.split():
            arg_value = local_dict[arg_name]
            print(f'{arg_name}: {arg_value}')
        return a, c, argz, e, g, kwargz

    @argh
    @classmethod
    def cmethod(cls, a, c=13, *argz, e=15, g, **kwargz):
        local_dict = locals()
        for arg_name in 'a c argz e g kwargz'.split():
            arg_value = local_dict[arg_name]
            print(f'{arg_name}: {arg_value}')
        return a, c, argz, e, g, kwargz

    @argh
    @staticmethod
    def smethod(a, c=13, *argz, e=15, g, **kwargz):
        local_dict = locals()
        for arg_name in 'a c argz e g kwargz'.split():
            arg_value = local_dict[arg_name]
            print(f'{arg_name}: {arg_value}')
        return a, c, argz, e, g, kwargz


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'args_and_kwargs',                          'check'),
    [
     (0,     ((), dict(g=7, a=1)),                       ((1, 13), OD(e=15, g=7))),
     (1,     ((), dict(g=7, c=3, a=1)),                  ((1, 3), OD(e=15, g=7))),
     (2,     ((), dict(g=7, e=5, a=1)),                  ((1, 13), OD(e=5, g=7))),
     (3,     ((), dict(g=7, b=2, a=1)),                  ((1, 13), OD(e=15, g=7, b=2))),
     (4,     ((), dict(c=3, d=4, g=7, b=2, a=1, e=5)),   ((1, 3), OD(e=5, g=7, b=2, d=4))),
     (5,     ((1,), dict(g=7)),                          ((1, 13), OD(e=15, g=7))),
     (6,     ((1,), dict(g=7, c=3)),                     ((1, 3), OD(e=15, g=7))),
     (7,     ((1,), dict(g=7, e=5)),                     ((1, 13), OD(e=5, g=7))),
     (8,     ((1,), dict(g=7, b=2)),                     ((1, 13), OD(e=15, g=7, b=2))),
     (9,     ((1,), dict(c=3, d=4, g=7, b=2, e=5)),      ((1, 3), OD(e=5, g=7, b=2, d=4))),
     (10,    ((1, 3), dict(g=7)),                        ((1, 3), OD(e=15, g=7))),
     (11,    ((1, 3), dict(g=7, e=5)),                   ((1, 3), OD(e=5, g=7))),
     (12,    ((1, 3), dict(g=7, b=2)),                   ((1, 3), OD(e=15, g=7, b=2))),
     (13,    ((1, 3), dict(d=4, g=7, b=2, h=8, e=5)),    ((1, 3), OD(e=5, g=7, b=2, d=4, h=8))),
     (14,    ((1, 3, 2), dict(g=7)),                     ((1, 3, 2), OD(e=15, g=7))),
     (15,    ((1, 3, 5), dict(d=4, g=7, b=2, h=8, e=5)), ((1, 3, 5), OD(e=5, g=7, b=2, d=4, h=8))),
     (16,    ((1, 3, 2, 4), dict(g=7)),                  ((1, 3, 2, 4), OD(e=15, g=7))),
     (17,    ((1, 3, 2, 5), dict(d=4, g=7, h=8, e=5)),   ((1, 3, 2, 5), OD(e=5, g=7, d=4, h=8))),
     (18,    ((), dict()),                                TypeError),
     (19,    ((), dict(g=7)),                             TypeError),
     (20,    ((1,), dict()),                              TypeError),
     (21,    ((1, 3), dict()),                            TypeError),
     (22,    ((1, 3), dict(a=11, g=7)),                   TypeError),
     (23,    ((1, 3), dict(c=13, g=7)),                   TypeError),
     (24,    ((1, 3, 4, 5), dict(d=14, e=5, h=8)),        TypeError),
     ])
@pytest.mark.parametrize(('func'), (full_signature, F().imethod, F().cmethod, F().smethod))
def test_call_sign_on_full_signature_functions(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)


def named_signature(a, c=13, *, e=15, g):
    return a, c, e, g


class N:
    def imethod(self, a, c=13, *, e=15, g):
        return a, c, e, g

    @classmethod
    def cmethod(cls, a, c=13, *, e=15, g):
        return a, c, e, g

    @staticmethod
    def smethod(a, c=13, *, e=15, g):
        return a, c, e, g


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'args_and_kwargs',                           'check'),
    [
     (0,     ((), dict(a=1, g=7)),                       ((1, 13), OD(e=15, g=7))),
     (1,     ((), dict(g=7, a=1)),                       ((1, 13), OD(e=15, g=7))),
     (2,     ((), dict(a=1, c=3, g=7)),                  ((1, 3), OD(e=15, g=7))),
     (3,     ((), dict(g=7, c=3, a=1)),                  ((1, 3), OD(e=15, g=7))),
     (4,     ((), dict(e=5, g=7, a=1)),                  ((1, 13), OD(e=5, g=7))),
     (5,     ((), dict(c=3, g=7, e=5, a=1)),             ((1, 3), OD(e=5, g=7))),
     (6,     ((1,), dict(g=7)),                          ((1, 13), OD(e=15, g=7))),
     (7,     ((1,), dict(g=7, c=3)),                     ((1, 3), OD(e=15, g=7))),
     (8,     ((1,), dict(g=7, e=5)),                     ((1, 13), OD(e=5, g=7))),
     (9,     ((1,), dict(g=7, c=3, e=5)),                ((1, 3), OD(e=5, g=7))),
     (10,    ((1, 3), dict(g=7)),                        ((1, 3), OD(e=15, g=7))),
     (11,    ((1, 3), dict(g=7, e=5)),                   ((1, 3), OD(e=5, g=7))),
     (12,    ((), dict()),                                TypeError),
     (13,    ((), dict(g=7)),                             TypeError),
     (14,    ((1,), dict()),                              TypeError),
     (15,    ((1, 3), dict(g=7, a=1)),                    TypeError),
     (16,    ((1, 3), dict(g=7, c=3)),                    TypeError),
     (17,    ((), dict(g=7, b=2, a=1)),                   TypeError),
     (18,    ((1,), dict(g=7, b=2)),                      TypeError),
     (19,    ((1, 3, 2), dict(g=7)),                      TypeError),
     ])
@pytest.mark.parametrize(('func'), (named_signature, N().imethod, N().cmethod, N().smethod))
def test_call_sign_on_named_signature_functions(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)


def var_signature(*argz, **kwargz):
    return argz, kwargz


class V:
    def imethod(self, *argz, **kwargz):
        return argz, kwargz

    @classmethod
    def cmethod(cls, *argz, **kwargz):
        return argz, kwargz

    @staticmethod
    def smethod(*argz, **kwargz):
        return argz, kwargz


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'args_and_kwargs',                          'check'),
    [
     (0,     ((), {}),                                  ((), {})),
     (1,     ((1,), {}),                                ((1,), {})),
     (2,     ((1, 2), {}),                              ((1, 2), {})),
     (3,     ((1, 2, 3), {}),                           ((1, 2, 3), {})),
     (4,     ((), dict(a=1)),                           ((), OD(a=1))),
     (5,     ((), dict(a=1, b=2)),                      ((), OD(a=1, b=2))),
     (6,     ((), dict(a=1, b=2, c=3)),                 ((), OD(a=1, b=2, c=3))),
     (7,     ((1,), dict(a=1)),                         ((1,), OD(a=1))),
     (8,     ((1, 2), dict(a=1, b=2)),                  ((1, 2), OD(a=1, b=2))),
     (9,     ((1, 2, 3), dict(a=1, b=2, c=3)),          ((1, 2, 3), OD(a=1, b=2, c=3))),
     ])
@pytest.mark.parametrize(('func'), (var_signature, V().imethod, V().cmethod, V().smethod))
def test_call_sign_on_var_signature_functions(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)


def positional_or_keyword_signature(a, c=13):
    return a, c


class PK:
    def imethod(self, a, c=13):
        return a, c

    @classmethod
    def cmethod(cls, a, c=13):
        return a, c

    @staticmethod
    def smethod(a, c=13):
        return a, c


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'args_and_kwargs',                          'check'),
    [
     (0,     ((), dict(a=1)),                           ((1, 13), {})),
     (1,     ((), dict(a=1, c=3)),                      ((1, 3), {})),
     (2,     ((), dict(c=3, a=1)),                      ((1, 3), {})),
     (3,     ((1,), {}),                                ((1, 13), {})),
     (4,     ((1, 3), {}),                              ((1, 3), {})),
     (5,     ((1,), dict(c=3)),                         ((1, 3), {})),
     (6,     ((), dict(c=3)),                            TypeError),
     (7,     ((1, 2, 3), {}),                            TypeError),
     (8,     ((1,), dict(a=3)),                          TypeError),
     (9,     ((1, 3), dict(c=3)),                        TypeError),
     (10,    ((1, 3), dict(b=2)),                        TypeError),
     (11,    ((), dict(a=1, b=2)),                       TypeError),
     (12,    ((), dict(c=3, b=2)),                       TypeError),
     (13,    ((), dict(c=3, b=2, a=1)),                  TypeError),
     (14,    ((), {}),                                   TypeError),
     ])
@pytest.mark.parametrize(
    ('func'), (positional_or_keyword_signature, PK().imethod, PK().cmethod, PK().smethod))
def test_call_sign_on_positional_or_keyword_signature_functions(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)


def var_positional_signature(*argz):
    return argz


class VP:
    def imethod(self, *argz):
        return argz

    @classmethod
    def cmethod(cls, *argz):
        return argz

    @staticmethod
    def smethod(*argz):
        return argz


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'args_and_kwargs',                          'check'),
    [
     (0,     ((), {}),                                  ((), {})),
     (1,     ((1,), {}),                                ((1,), {})),
     (2,     ((1, 2), {}),                              ((1, 2), {})),
     (3,     ((1, 2, 3), {}),                           ((1, 2, 3), {})),
     (4,     ((), dict(a=1)),                            TypeError),
     (7,     ((1,), dict(a=1)),                          TypeError),
     (8,     ((1, 2), dict(b=2)),                        TypeError),
     (9,     ((1, 2, 3), dict(c=3)),                     TypeError),
     ])
@pytest.mark.parametrize(
    ('func'), (var_positional_signature, VP().imethod, VP().cmethod, VP().smethod))
def test_call_sign_on_var_positional_signature_functions(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)


def keyword_only_signature(*, e, f=16, g):
    return e, f, g


class KO:
    def imethod(self, *, e, f=16, g):
        return e, f, g

    @classmethod
    def cmethod(cls, *, e, f=16, g):
        return e, f, g

    @staticmethod
    def smethod(*, e, f=16, g):
        return e, f, g


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'args_and_kwargs',                          'check'),
    [
     (0,     ((), dict(e=5, g=7)),                      ((), OD(e=5, f=16, g=7))),
     (1,     ((), dict(g=7, e=5)),                      ((), OD(e=5, f=16, g=7))),
     (2,     ((), dict(e=5, f=6, g=7)),                 ((), OD(e=5, f=6, g=7))),
     (3,     ((), dict(g=7, e=5, f=6)),                 ((), OD(e=5, f=6, g=7))),
     (4,     ((), dict(f=6, g=7, e=5)),                 ((), OD(e=5, f=6, g=7))),
     (5,     ((), dict()),                               TypeError),
     (6,     ((), dict(g=7)),                            TypeError),
     (7,     ((), dict(e=5)),                            TypeError),
     (8,     ((), dict(g=7, e=5, h=8)),                  TypeError),
     (9,     ((1,), dict(e=5, g=7)),                     TypeError),
     ])
@pytest.mark.parametrize(
    ('func'), (keyword_only_signature, KO().imethod, KO().cmethod, KO().smethod))
def test_call_sign_on_keyword_only_signature_functions(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)


def var_keyword_signature(**kwargz):
    return kwargz


class VK:
    def imethod(self, **kwargz):
        return kwargz

    @classmethod
    def cmethod(cls, **kwargz):
        return kwargz

    @staticmethod
    def smethod(**kwargz):
        return kwargz


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'args_and_kwargs',                          'check'),
    [
     (0,     ((), {}),                                  ((), {})),
     (1,     ((), dict(a=1)),                           ((), OD(a=1))),
     (2,     ((), dict(a=1, b=2)),                      ((), OD(a=1, b=2))),
     (3,     ((), dict(a=1, b=2, c=3)),                 ((), OD(a=1, b=2, c=3))),
     (4,     ((1,), {}),                                TypeError),
     (5,     ((1, 2), {}),                              TypeError),
     (6,     ((1,), dict(a=1)),                         TypeError),
     (7,     ((1, 2), dict(a=1, b=2)),                  TypeError),
     (8,     ((1,), dict(a=1, b=2, c=3)),               TypeError),
     (9,     ((1, 2), dict(a=1, b=2, c=3)),             TypeError),
     ])
@pytest.mark.parametrize(
    ('func'), (var_keyword_signature, VK().imethod, VK().cmethod, VK().smethod))
def test_call_sign_on_var_keyword_signature_functions(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
FILE = 'a_file.txt'
PATH = os.path.join(CURRENT_DIR, 'a_file.txt')


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'func', 'args_and_kwargs',          'check'),
    [
     (0,     ord,   (('A',), {}),              (('A',), {})),
     (1,     ord,   ((), {}),                   TypeError),
     (2,     ord,   (('A', 'B'), {}),           TypeError),
     (3,     ord,   ((), dict(c='C')),          TypeError),
     (4,     ord,   (('A'), dict(c='C')),       TypeError),
     (5,     pow,   ((2, 3, 4), {}),           ((2, 3, 4), {})),
     (6,     pow,   ((2, 3), {}),              ((2, 3, None), {})),
     (7,     pow,   ((2,), {}),                 TypeError),
     (8,     pow,   ((), {}),                   TypeError),
     (9,     pow,   ((2, 3, 4, 5), {}),         TypeError),
     (10,    pow,   ((2, 3), dict(z=5)),        TypeError),
     (11,    pow,   ((2, 3), dict(y=5)),        TypeError),
     (12,    pow,   ((2, 3), dict(x=5)),        TypeError),
     (13,    pow,   ((2, 3), dict(w=5)),        TypeError),
     (14,    open,  ((PATH,), {}),             ((PATH, 'r', -1, None, None, None, True, None), {})),
     (15,    open,  ((), dict(file=PATH)),     ((PATH, 'r', -1, None, None, None, True, None), {})),
     (16,    open,  ((PATH,), dict(mode='w')), ((PATH, 'w', -1, None, None, None, True, None), {})),
     (17,    open,  ((), {}),                   TypeError),
     (18,    open,  ((PATH), dict(file=PATH)),  TypeError),
     (19,    open,  ((PATH), dict(x=0)),        TypeError),
     ])
def test_call_sign_on_builtins(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)
