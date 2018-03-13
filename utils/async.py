#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import partial


def execute_future(loop, executor, func, *args, **kwds):
    partial_fn = partial(func, **kwds)
    future = loop.run_in_executor(executor, partial_fn, *args)
    return future
