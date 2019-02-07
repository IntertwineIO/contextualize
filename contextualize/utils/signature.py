#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import inspect
from collections import OrderedDict, namedtuple
from itertools import chain, groupby, zip_longest

import wrapt

from contextualize.utils.decor import factory_direct
from contextualize.utils.iterable import consume
from contextualize.utils.sentinel import Sentinel
from contextualize.utils.tools import is_selfish


class CallSign:
    """
    Call Sign

    Call Sign allows the arguments with which a function is CALLed to be
    viewed through the lens of the function's SIGNature.

    I/O:
    func:               A function or method
    enhance_sort=False: By default, signature() imposes no order on
                        varkwargs and normalize() sorts only varkwargs.
                        If enhance_sort is True, signature() sorts
                        varkwargs and normalize() sorts all kwargs.
    """
    SIGNATURE_TAG = inspect.Signature.__qualname__

    SENTINEL = Sentinel()

    VAR_KINDS = {'var_positional', 'var_keyword'}

    NamedValues = namedtuple('NamedValues', 'name values')

    SignatureArguments = namedtuple('SignatureArguments', [
        'positional_only',
        'positional_or_keyword',
        'var_positional',
        'keyword_only',
        'var_keyword'
        ])

    NormalizedArguments = namedtuple('NormalizedArguments', 'args kwargs')

    def signify(self, *args, **kwargs):
        """
        Signify

        Align calling arguments with function's signature categories,
        as defined by the SignatureArguments namedtuple:

        positional_only:        List of positional only parameters
        positional_or_keyword:  Ordered dict of positional/keyword args
        var_positional:         NamedValues namedtuple of varargs
        keyword_only:           ordered dict of parameters keyed by name
        var_keyword:            NamedValues namedtuple of varkwargs

        Values are None if there are no parameters of the given kind.

        I/O:
        *args:                  Calling args
        **kwargs:               Calling kwargs
        return:                 SignatureArguments namedtuple
        """
        parameters = self.parameters
        positional_only = positional_or_keyword = var_positional = keyword_only = var_keyword = None
        named_kwargs = set()

        if parameters.positional_only:
            positional_only = OrderedDict(self._emit_positional_only_args(args, kwargs))

        if parameters.positional_or_keyword:
            positional_or_keyword = OrderedDict(
                self._emit_positional_or_keyword_args(args, kwargs, named_kwargs))

        var_positional_values = self._derive_var_positional_args(args)
        if parameters.var_positional:
            var_positional_name = parameters.var_positional.name
            var_positional = self.NamedValues(name=var_positional_name,
                                              values=var_positional_values)

        if parameters.keyword_only:
            keyword_only = OrderedDict(self._emit_keyword_only_args(kwargs, named_kwargs))

        var_keyword_values_iter = self._emit_var_keyword_args(kwargs, named_kwargs)
        if parameters.var_keyword:
            if self.enhance_sort:
                var_keyword_values_iter = sorted(var_keyword_values_iter)
            var_keyword_values = OrderedDict(var_keyword_values_iter)
            var_keyword_name = parameters.var_keyword.name
            var_keyword = self.NamedValues(name=var_keyword_name, values=var_keyword_values)
        else:
            consume(var_keyword_values_iter)  # confirm there are no var keywords

        return self.SignatureArguments(positional_only=positional_only,
                                       positional_or_keyword=positional_or_keyword,
                                       var_positional=var_positional,
                                       keyword_only=keyword_only,
                                       var_keyword=var_keyword)

    def normalize(self, *args, **kwargs):
        """
        Normalize

        Normalize calling arguments to a signature-based standard form
        as defined by the NormalizedArguments namedtuple:

        args:   List of signature's positional-only args followed by
                positional/keyword args and then var positional args,
                with any defaults applied as necessary.

        kwargs: Ordered dict of keyword-only args followed by var
                keyword args, sorted by key and without any conflicts.

        Use in conjunction with memoization to increase cache hits when
        calls are made with a diversity of argument forms: positional,
        keyword, var positional, var keyword, and defaults.

        I/O:
        *args:                  Calling args
        **kwargs:               Calling kwargs
        return:                 NormalizedArguments namedtuple
        """
        parameters = self.parameters
        named_kwargs = set()
        normalized_args_iter = ()

        if parameters.positional_only:
            normalized_args_iter = (v for k, v in self._emit_positional_only_args(args, kwargs))

        if parameters.positional_or_keyword:
            positional_or_keyword_iter = (
                v for k, v in self._emit_positional_or_keyword_args(args, kwargs, named_kwargs))
            normalized_args_iter = chain(normalized_args_iter, positional_or_keyword_iter)

        var_positional = self._derive_var_positional_args(args)
        if var_positional:
            normalized_args_iter = chain(normalized_args_iter, var_positional)

        normalized_args = tuple(normalized_args_iter)

        keyword_only_items = (
            self._emit_keyword_only_args(kwargs, named_kwargs) if parameters.keyword_only else ())

        var_keyword_items = self._emit_var_keyword_args(kwargs, named_kwargs)

        if self.enhance_sort:
            normalized_kwargs = OrderedDict(sorted(chain(keyword_only_items, var_keyword_items)))
        else:
            normalized_kwargs = OrderedDict(keyword_only_items)  # evaluate keyword only args...
            normalized_kwargs.update(sorted(var_keyword_items))  # ...before var_keywords

        return self.NormalizedArguments(args=normalized_args, kwargs=normalized_kwargs)

    def _emit_positional_only_args(self, args, kwargs):
        """Emit positional only args given calling args and kwargs"""
        positional_only_params = self.parameters.positional_only.values()
        num_positional_only = len(positional_only_params)
        eligible_args = args[:num_positional_only]

        for param, arg in zip_longest(positional_only_params, eligible_args,
                                      fillvalue=self.SENTINEL):
            param_name = param.name
            if param_name in kwargs:
                raise TypeError(f"{self.class_name} for {self.function_name}() "
                                f"received kwarg for positional-only argument '{param_name}'")
            elif arg is self.SENTINEL:
                default = param.default
                if default is param.empty:
                    raise TypeError(f"{self.class_name} for {self.function_name}() "
                                    f"missing required positional-only argument '{param_name}'")
                yield param_name, default
            else:
                yield param_name, arg

    def _emit_positional_or_keyword_args(self, args, kwargs, named_kwargs):
        """Emit positional/keyword args with named kwargs side effect"""
        parameters = self.parameters
        num_positional_only = len(parameters.positional_only)
        positional_or_keyword_params = parameters.positional_or_keyword.values()
        num_positional_or_keyword = len(positional_or_keyword_params)
        eligible_args = args[num_positional_only:num_positional_only + num_positional_or_keyword]

        for param, arg in zip_longest(positional_or_keyword_params, eligible_args,
                                      fillvalue=self.SENTINEL):
            param_name = param.name
            if param_name in kwargs:
                if arg is not self.SENTINEL:
                    raise TypeError(f"{self.class_name} for {self.function_name}() "
                                    f"received multiple values for argument '{param_name}'")
                named_kwargs.add(param_name)
                yield param_name, kwargs[param_name]
            elif arg is self.SENTINEL:
                default = param.default
                if default is param.empty:
                    raise TypeError(f"{self.class_name} for {self.function_name}() "
                                    f"missing required positional/keyword argument '{param_name}'")
                yield param_name, default
            else:
                yield param_name, arg

    def _derive_var_positional_args(self, args):
        """Derive varargs (if any) from args based on signature"""
        parameters = self.parameters
        num_params = len(parameters.positional_only) + len(parameters.positional_or_keyword)

        if len(args) > num_params:
            if not parameters.var_positional:
                raise TypeError(f"{self.class_name} for {self.function_name}() "
                                f"expected at most {num_params} arguments, received {len(args)}")
            # Use slicing here as emit version slows normalize() by ~5%
            return args[num_params:]
        return ()

    def _emit_keyword_only_args(self, kwargs, named_kwargs):
        """Emit keyword-only args & update named kwargs as side effect"""
        for param in self.parameters.keyword_only.values():
            param_name = param.name
            if param_name in kwargs:
                named_kwargs.add(param_name)
                yield param_name, kwargs[param_name]
            else:
                default = param.default
                if default is param.empty:
                    raise TypeError(f"{self.class_name} for {self.function_name}() "
                                    f"missing required keyword-only argument '{param_name}'")
                yield param_name, default

    def _emit_var_keyword_args(self, kwargs, named_kwargs):
        """Emit var keyword args: all kwargs not in named kwargs"""
        if len(kwargs) > len(named_kwargs) and not self.parameters.var_keyword:
            extras = kwargs.keys() - named_kwargs
            raise TypeError(f"{self.class_name} for {self.function_name}() "
                            f"does not accept var keyword arguments, extras: {extras}")

        var_keyword_items = ((k, v) for k, v in kwargs.items() if k not in named_kwargs)

        for k, v in var_keyword_items:
            yield (k, v)

    def normalize_via_bind(self, *args, **kwargs):
        """
        Normalize via bind

        Same as normalize(), but use builtin signature.bind().

        Unfortunately, this is twice as slow due to poor performance and
        lack of sorting in signature.bind() and bound.apply_defaults().
        """
        bound = self.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        normalized_args = bound.args
        normalized_kwargs = self._normalize_kwargs(bound)
        return self.NormalizedArguments(args=normalized_args, kwargs=normalized_kwargs)

    def _normalize_kwargs(self, bound):
        bound_kwargs = bound.kwargs
        if not bound_kwargs:
            return OrderedDict()

        if not self.parameters.keyword_only or self.enhance_sort:
            return OrderedDict(sorted(bound.kwargs.items()))

        var_keyword_param = self.parameters.var_keyword
        if not var_keyword_param or var_keyword_param.name not in bound.arguments:
            return OrderedDict(bound.kwargs.items())

        keyword_only_params = self.parameters.keyword_only
        kwargs_only_items = ((name, bound_kwargs[name]) for name in keyword_only_params.keys()
                             if name in bound_kwargs)

        varkwargs_items = sorted(bound.arguments[var_keyword_param.name].items())

        normalized_kwargs = OrderedDict(chain(kwargs_only_items, varkwargs_items))
        return normalized_kwargs

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

    def _derive_parameters(self):
        """
        Derive parameters

        Return SignatureArguments namedtuple of parameters:

        positional_only:        ordered dict of parameters keyed by name
        positional_or_keyword:  ordered dict of parameters keyed by name
        var_positional:         parameter or None
        keyword_only:           ordered dict of parameters keyed by name
        var_keyword:            parameter or None
        """
        parameters = self.SignatureArguments(None, None, None, None, None)._asdict()
        params_iter = iter(self.signature.parameters.items())
        for kind, params in groupby(params_iter, key=lambda item: item[1].kind):
            key = kind.name.lower()
            value = next(params)[1] if key in self.VAR_KINDS else OrderedDict(params)
            parameters[key] = value

        default_keys = (k for k, v in parameters.items() if k not in self.VAR_KINDS and v is None)
        for key in default_keys:
            parameters[key] = OrderedDict()

        return self.SignatureArguments(**parameters)

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
        except ValueError:
            return cls.make_generic(func)

    def __init__(self, func, *, enhance_sort=False):
        self.func = func
        self.enhance_sort = enhance_sort
        # inspection is expensive, so perform just once
        self.signature = inspect.signature(self.func)
        self.parameters = self._derive_parameters()


    def __repr__(self):
        return repr(self.signature).replace(self.SIGNATURE_TAG, self.class_name)


def normalize(*args, enhance_sort=False):
    """Normalize decorator (factory) for standardizing call arguments"""
    def normalize_decorator(func):
        # Cache call_sign as CallSign's getfullargspec is expensive
        call_sign = CallSign(func, enhance_sort=enhance_sort)

        if asyncio.iscoroutinefunction(func):
            @wrapt.decorator
            async def async_normalize_wrapper(func, instance, args, kwargs):
                normalized = call_sign.normalize(*args, **kwargs)
                return await func(*normalized.args, **normalized.kwargs)

            return async_normalize_wrapper(func)

        @wrapt.decorator
        def normalize_wrapper(func, instance, args, kwargs):
            normalized = call_sign.normalize(*args, **kwargs)
            return func(*normalized.args, **normalized.kwargs)

        return normalize_wrapper(func)

    return factory_direct(normalize_decorator, *args)
