#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import inspect
from collections import OrderedDict, namedtuple
from itertools import chain

from utils.tools import isnamedtuple, is_selfish


class CallSign:
    """
    Call Sign

    Call Sign allows the arguments with which a function is CALLed to be
    viewed through the lens of the function's SIGNature.

    I/O:
    func:               A function or method
    enhance_sort=False: By default, signature() imposes no order on
                        varkwargs and normalize() sorts only varkwargs.
                        If enhanced_sort is True, signature() sorts
                        varkwargs and normalize() sorts all kwargs.
    """

    SignatureArguments = namedtuple('SignatureArguments',
        'arg_map varargs kwargs_only varkwargs varargs_name varkwargs_name')

    NormalizedArguments = namedtuple('NormalizedArguments', 'args kwargs')

    def signature(self, *args, **kwargs):
        """
        Signature

        Align calling arguments with function's signature categories,
        as defined by the CallSign.SignatureArguments namedtuple:

        arg_map:        Ordered dict of signature's positional args
        varargs:        List of signature's variable args values
        kwargs_only:    Ordered dict of all keyword only args
        varkwargs:      Ordered dict of signature's variable kwargs
        varargs_name:   Name of signature's variable args parameter
        varkwargs_name: Name of signature's variable kwargs parameter

        I/O:
        *args:                  Calling args
        **kwargs:               Calling kwargs
        return:                 CallSign.SignatureArguments namedtuple
        """
        argspec = self.argspec
        varargs = kwargs_only = varkwargs = None
        named_kwargs = set()

        arg_map = OrderedDict(self._emit_positional_args(args, kwargs))

        if len(argspec.args) - int(self.is_selfish) > len(arg_map):
            remaining_args = self._emit_remaining_args(arg_map, kwargs, named_kwargs)
            arg_map.update(remaining_args)

        if argspec.varargs:
            varargs = self._extract_varargs(args)
            varargs_name = argspec.varargs

        if argspec.kwonlyargs:
            kwargs_only = OrderedDict(self._emit_keyword_only_args(kwargs, named_kwargs))

        if argspec.varkw:
            varkwargs = self._emit_varkwargs(kwargs, named_kwargs)
            if self.enhance_sort:
                varkwargs = sorted(varkwargs)
            varkwargs = OrderedDict(varkwargs)
            varkwargs_name = argspec.varkw

        return self.SignatureArguments(arg_map=arg_map,
                                       varargs=varargs,
                                       kwargs_only=kwargs_only,
                                       varkwargs=varkwargs,
                                       varargs_name=varargs_name,
                                       varkwargs_name=varkwargs_name)

    def normalize(self, *args, **kwargs):
        """
        Normalize

        Normalize calling arguments to a signature-based standard form
        as defined by the CallSign.NormalizedArguments named 2-tuple:

        args:   List of signature's positional arg values followed
                by varargs, where the positional args may be specified
                positionally, via keyword, or by default.

        kwargs: Ordered dict of keyword-only args followed by varkwargs
                sorted by key and without any positional arg overlaps.

        Use in conjunction with memoization to increase cache hits when
        calls are made with a diversity of argument forms: positional,
        keyword, varargs, varkwargs, and defaults.

        I/O:
        *args:                  Calling args
        **kwargs:               Calling kwargs
        return:                 CallSign.NormalizedArguments namedtuple
        """
        argspec = self.argspec
        named_kwargs = set()

        normalized_args = [v for k, v in self._emit_positional_args(args, kwargs)]

        if len(argspec.args) - int(self.is_selfish) > len(normalized_args):
            remaining = self._emit_remaining_args(normalized_args, kwargs, named_kwargs)
            normalized_args.extend(v for k, v in remaining)

        if argspec.varargs:
            normalized_args.extend(self._extract_varargs(args))

        normalized_kwargs = OrderedDict()

        if argspec.kwonlyargs:
            normalized_kwargs.update(self._emit_keyword_only_args(kwargs, named_kwargs))

        if argspec.varkw:
            varkwargs = self._emit_varkwargs(kwargs, named_kwargs)
            if self.enhance_sort:
                all_kwargs = chain(normalized_kwargs.items(), varkwargs)
                normalized_kwargs = OrderedDict(sorted(all_kwargs))
            else:
                normalized_kwargs.update(sorted(varkwargs))

        return self.NormalizedArguments(args=normalized_args, kwargs=normalized_kwargs)

    def _emit_positional_args(self, args, kwargs):
        """Emit positional args given calling args and kwargs"""
        arg_names_iter = iter(self.argspec.args)
        if self.is_selfish:
            next(arg_names_iter)

        for arg_name, arg in zip(arg_names_iter, args):
            if arg_name in kwargs:
                raise TypeError(f"{self.class_name} for {self.function_name}() "
                                f"got multiple values for argument '{arg_name}'")
            yield arg_name, arg

    def _emit_remaining_args(self, positional_args, kwargs, named_kwargs):
        """Emit remaining args and update named kwargs as side effect"""
        argspec = self.argspec
        default_values = argspec.defaults
        num_no_default_args = len(argspec.args) - len(default_values)
        start_index = len(positional_args) + int(self.is_selfish)

        for i, arg_name in enumerate(argspec.args[start_index:], start=start_index):
            if arg_name in kwargs:
                named_kwargs.add(arg_name)
                yield arg_name, kwargs[arg_name]
            else:
                default_index = i - num_no_default_args
                if default_index < 0:
                    raise TypeError(f"{self.class_name} for {self.function_name}() "
                                    f"missing required positional argument '{arg_name}'")
                yield arg_name, default_values[default_index]

    def _extract_varargs(self, args):
        """Extract varargs (if any) from args based on signature"""
        num_arg_names = len(self.argspec.args) - int(self.is_selfish)
        # Use slicing here as emit version slows normalize() by ~5%
        return args[num_arg_names:] if len(args) > num_arg_names else []

    def _emit_keyword_only_args(self, kwargs, named_kwargs):
        """Emit keyword-only args & update named kwargs as side effect"""
        argspec = self.argspec
        kwarg_only_defaults = argspec.kwonlydefaults

        for kwarg_name in argspec.kwonlyargs:
            if kwarg_name in kwargs:
                named_kwargs.add(kwarg_name)
                yield kwarg_name, kwargs[kwarg_name]
            else:
                try:
                    yield kwarg_name, kwarg_only_defaults[kwarg_name]
                except KeyError:
                    raise TypeError(f"{self.class_name} for {self.function_name}() "
                                    f"missing required keyword-only argument '{kwarg_name}'")

    def _emit_varkwargs(self, kwargs, named_kwargs):
        """Emit varkwargs: all kwargs not in named kwargs as generator"""
        return ((k, v) for k, v in kwargs.items() if k not in named_kwargs)

    @property
    def class_name(self):
        return self.__class__.__qualname__

    @property
    def function_name(self):
        return self.func.__qualname__

    @property
    def is_selfish(self):
        """True if function requires a self/cls/meta parameter"""
        try:
            return self._is_selfish
        except AttributeError:
            self._is_selfish = is_selfish(self.func)
            return self._is_selfish

    @classmethod
    def make_generic(cls, func):
        """Make generic call sign by wrapping function"""
        def generic(*args, **kwargs):
            return func(*args, **kwargs)
        return cls(generic)

    @classmethod
    def manifest(cls, func):
        """Manifest ensures call sign creation via a generic backup"""
        try:
            return cls(func)
        except TypeError:
            return cls.make_generic(func)

    def __init__(self, func, *, enhance_sort=False):
        self.func = func
        # getfullargspec is expensive, so perform just once
        self.argspec = inspect.getfullargspec(self.func)
        self.enhance_sort = enhance_sort


def normalize(enhance_sort=False):
    """Normalize decorator for standardizing call arguments"""
    @wrapt.decorator
    def normalize_wrapper(wrapped, instance, args, kwargs):
        if asyncio.iscoroutinefunction(wrapped):
            raise TypeError('Function decorated with normalize must not be async.')

        try:
            call_sign = wrapped.call_sign
        except AttributeError:
            # Cache call_sign as CallSign's getfullargspec is expensive
            call_sign = CallSign(wrapped, enhance_sort=enhance_sort)
            wrapped.call_sign = call_sign

        normalized = call_sign.normalize(*args, **kwargs)
        return wrapped(*normalized.args, **normalized.kwargs)

    return normalize_wrapper
