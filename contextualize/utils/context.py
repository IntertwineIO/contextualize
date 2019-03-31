#!/usr/bin/env python
# -*- coding: utf-8 -*-
import contextvars
from contextlib import suppress
from contextvars import ContextVar

from contextualize.exceptions import ContextManagerReentryError
from contextualize.utils.tools import represent


FLEX_CONTEXT = ContextVar('flex_context', default={})


class FlexContext:
    """
    FlexContext

    FlexContext is a context manager for context variables. Unlike
    regular context variables that must be declared individually at the
    module level, flex context variables all share a single FLEX_CONTEXT
    variable by utilizing a dictionary.

    A copy of the current FLEX_CONTEXT is retrieved via get_context().

    A FlexContext instance is initialized with a `delta` dictionary of
    flex context variables and values to be applied upon context entry.
    As a context manager, a FlexContext instance is entered via `with`.

    Upon context entry, FLEX_CONTEXT is updated with a new `context` in
    which `delta` has been applied to the `baseline` FLEX_CONTEXT. At
    this point, `context` and `baseline` are established and stored on
    the FlexContext instance for reference.

    Upon context exit, the FLEX_CONTEXT is reset to the `baseline`. In
    addition, the `context` and `baseline` fields are reset to None.

    Multiple FlexContext instances may be applied by nesting `with`
    statements. A single FlexContext instance may be re-entered multiple
    times, but only after exiting after each use.

    Usage:
    >>> print(FlexContext.get_context())
    {}

    >>> with FlexContext(color='blue', number=42, obj=object()) as context_a:
            print(FlexContext.get_context())
            assert context_a.context == FlexContext.get_context()
            assert context_a.baseline == {}
            with FlexContext(color='yellow', obj=object()) as context_b:
                print(FlexContext.get_context())
                assert context_b.context == FlexContext.get_context()
                assert context_b.baseline == context_a.context
            print(FlexContext.get_context())
            assert context_b.context is None and context_b.baseline is None
    {'color': 'blue', 'number': 42, 'obj': <object object at 0x107b4ca60>}
    {'color': 'yellow', 'number': 42, 'obj': <object object at 0x107b4caa0>}
    {'color': 'blue', 'number': 42, 'obj': <object object at 0x107b4ca60>}

    >>> print(FlexContext.get_context())
    {}
    """

    @staticmethod
    def get_context():
        """Get context from flex_context ContextVar; always current state"""
        context = FLEX_CONTEXT.get()
        return context.copy()

    @property
    def delta(self):
        """Delta context vars; dict to apply to baseline upon entry"""
        return self._delta.copy()

    @property
    def baseline(self):
        """Baseline (old) context dict upon entry; None outside context"""
        return None if self._baseline is None else self._baseline.copy()

    @property
    def context(self):
        """Context (new) dict upon entry; None outside context"""
        return None if self._context is None else self._context.copy()

    def __enter__(self):
        """Enter context, applying delta to baseline to form context"""
        if self._token is not None:
            raise ContextManagerReentryError(id=self._token)
        self._baseline = self.get_context()
        self._context = {**self._baseline, **self._delta}
        self._token = FLEX_CONTEXT.set(self._context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context, resetting baseline and context to None"""
        FLEX_CONTEXT.reset(self._token)
        self._baseline = None
        self._context = None
        self._token = None

    def __getattr__(self, name):
        """Get delta vars and within context, context vars"""
        with suppress(KeyError):
            return self._delta[name]
        try:
            return self._context[name]
        except TypeError as e:
            raise AttributeError(
                f"'{self!r}' context vars are only available within context") from e
        except KeyError as e:
            raise AttributeError(f"'{name}' not found in '{self!r}' context") from e

    def __setattr__(self, name, value):
        """Setattr is disabled for context vars; contexts are immutable"""
        if name in self.__dict__:
            return super().__setattr__(name, value)
        raise AttributeError(f"'{self!r}' vars are immutable")

    def __delattr__(self, name):
        """Delattr is disabled for context vars; contexts are immutable"""
        if name in self.__dict__:
            return super().__delattr__(name)
        raise AttributeError(f"'{self!r}' vars cannot be deleted")

    def __contains__(self, item):
        """Contains item in delta vars or, within context, context vars"""
        try:
            return item in self._delta or item in self._context
        except TypeError:
            return False

    def __repr__(self):
        return represent(self, **self._delta)

    def __init__(self, **delta):
        """Initialize instance with `delta` dict of context var names/values"""
        self._initialize_attributes(_delta=delta, _baseline=None, _context=None, _token=None)

    def _initialize_attributes(self, **attributes):
        """Initialize attributes on instance given dict of attribute names/values"""
        for attribute, value in attributes.items():
            super().__setattr__(attribute, value)
