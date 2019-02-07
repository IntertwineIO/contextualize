#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import partial


def run_in_executor(loop, executor, func, *args, **kwds):
    """Run in executor with support for keyword arguments"""
    partial_fn = partial(func, **kwds)
    future = loop.run_in_executor(executor, partial_fn, *args)
    return future
