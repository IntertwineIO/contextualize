#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import pytest
from collections import OrderedDict as OD
from itertools import chain

import wrapt

from utils.signature import CallSign
from utils.tools import ischildclass


def validate_call_sign(idx, func, args_and_kwargs, check):
    call_sign = CallSign(func)
    args, kwargs = args_and_kwargs

    if ischildclass(check, Exception):
        with pytest.raises(check):
            call_sign.normalize(*args, **kwargs)
    else:
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


@wrapt.decorator
def argh(func, instance, args, kwargs):
    print(f'args: {args}')
    print(f'kwargs: {kwargs}')
    return func(*args, **kwargs)


@argh
def func(a, c=13, *argz, e=15, g, **kwargz):
    local_dict = locals()
    for arg_name in 'a c argz e g kwargz'.split():
        arg_value = local_dict[arg_name]
        print(f'{arg_name}: {arg_value}')


class C:
    @argh
    def imethod(self, a, c=13, *argz, e=15, g, **kwargz):
        local_dict = locals()
        for arg_name in 'a c argz e g kwargz'.split():
            arg_value = local_dict[arg_name]
            print(f'{arg_name}: {arg_value}')

    @argh
    @classmethod
    def cmethod(cls, a, c=13, *argz, e=15, g, **kwargz):
        local_dict = locals()
        for arg_name in 'a c argz e g kwargz'.split():
            arg_value = local_dict[arg_name]
            print(f'{arg_name}: {arg_value}')

    @argh
    @staticmethod
    def smethod(a, c=13, *argz, e=15, g, **kwargz):
        local_dict = locals()
        for arg_name in 'a c argz e g kwargz'.split():
            arg_value = local_dict[arg_name]
            print(f'{arg_name}: {arg_value}')


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
@pytest.mark.parametrize(('func'), (func, C().imethod, C().cmethod, C().smethod))
def test_call_sign_on_python_functions(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'func', 'args_and_kwargs',          'check'),
    [
     (0,     ord,   (('A',), {}),               (('A',), {})),
     (1,     ord,   ((), {}),                    TypeError),
     (2,     ord,   (('A', 'B'), {}),            TypeError),
     (3,     ord,   ((), dict(c='C')),           TypeError),
     (4,     ord,   (('A'), dict(c='C')),        TypeError),
     (5,     pow,   ((2, 3, 4), {}),            ((2, 3, 4), {})),
     (6,     pow,   ((2, 3), {}),               ((2, 3, None), {})),
     (7,     pow,   ((2,), {}),                  TypeError),
     (8,     pow,   ((), {}),                    TypeError),
     (9,     pow,   ((2, 3, 4, 5), {}),          TypeError),
     (10,    pow,   ((2, 3), dict(z=5)),         TypeError),
     (11,    pow,   ((2, 3), dict(y=5)),         TypeError),
     (12,    pow,   ((2, 3), dict(x=5)),         TypeError),
     (13,    pow,   ((2, 3), dict(w=5)),         TypeError),
     (14,    open,  (('a',), {}),               (('a', 'r', -1, None, None, None, True, None), {})),
     (15,    open,  ((), dict(file='b')),       (('b', 'r', -1, None, None, None, True, None), {})),
     (16,    open,  (('a',), dict(mode='w')),   (('a', 'w', -1, None, None, None, True, None), {})),
     (17,    open,  ((), {}),                    TypeError),
     (18,    open,  (('a'), dict(file='b')),     TypeError),
     (19,    open,  (('a'), dict(x=0)),          TypeError),
     ])
def test_call_sign_on_builtins(idx, func, args_and_kwargs, check):
    validate_call_sign(idx, func, args_and_kwargs, check)
