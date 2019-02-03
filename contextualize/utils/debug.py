#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import inspect

import wrapt
from pprint import PrettyPrinter

from contextualize.utils.decor import factory_direct
from contextualize.utils.tools import WIDTH

DELIMITER = '–'  # chr(8211)
SEPARATOR = DELIMITER * WIDTH
DEBUG_WRAPPERS = {'async_debug_wrapper', 'sync_debug_wrapper'}


def offset_text(text, offset_space):
    """Offset text by given space with proper newline handling"""
    lines = text.split('\n')
    offset_lines = (offset_space + line for line in lines)
    offset_text = '\n'.join(offset_lines)
    return offset_text


def format_text(label, text, offset_space):
    """Format text with given label and space with newline handling"""
    if '\n' in text:
        print(f'{offset_space}{label}:')
        print(f'{offset_text(text, offset_space)}')
    else:
        print(f'{offset_space}{label}: {text}')


def derive_offset_space(offset=None, indent=4):
    """Derive offset space by counting debug wrapper stack frames"""
    if offset is None:
        frame_records = inspect.stack()
        new_offset = sum(1 for f in frame_records
                         if f.function in DEBUG_WRAPPERS) - 1
    else:
        new_offset = offset
    return ' ' * new_offset * indent


def evaluate_context(self, context, func, *args, **kwargs):
    """Evaluate context, which may reference self/func/args/kwargs"""
    try:
        evaluated_context = str(eval(context))
    except Exception as e:
        evaluated_context = f'Exception encountered in evaluating context: {e}'
    return evaluated_context


def loop_repr(loop):
    class_name, running, closed, debug = repr(loop)[1:-1].split()
    module = loop.__class__.__module__
    hex_id = hex(id(loop))
    return f'<{module}.{class_name} object at {hex_id} {running} {closed} {debug}>'


def print_enter_info(func, context, instance, args, kwargs,
                     printer, offset_space, loop=None, is_async=False):
    """Print enter info for function to be called/awaited"""
    print(SEPARATOR)
    async_ = 'async ' if is_async else ''
    print(f'{offset_space}Entering {async_}{func.__qualname__} ({func.__module__})')
    if context is not None:
        context_text = evaluate_context(instance, context, func, *args, **kwargs)
        format_text('context', context_text, offset_space)
    if instance is not None:
        format_text('instance', repr(instance), offset_space)
    format_text('args', printer.pformat(args), offset_space)
    format_text('kwargs', printer.pformat(kwargs), offset_space)
    loop = loop or asyncio.get_event_loop()
    format_text('loop', loop_repr(loop), offset_space)
    start_time = loop.time()
    format_text('start', str(start_time), offset_space)
    print(SEPARATOR)


def print_exit_info(func, context, instance, args, kwargs,
                    result, end_time, elapsed_time,
                    printer, offset_space, loop=None, is_async=False):
    """Print exit info for function upon its return"""
    print(SEPARATOR)
    async_ = 'async ' if is_async else ''
    print(f'{offset_space}Returning from {async_}{func.__qualname__} ({func.__module__})')
    if context is not None:
        context_text = evaluate_context(instance, context, func, *args, **kwargs)
        format_text('context', context_text, offset_space)
    if instance is not None:
        format_text('instance', repr(instance), offset_space)
    format_text('args', printer.pformat(args), offset_space)
    format_text('kwargs', printer.pformat(kwargs), offset_space)
    format_text('return', printer.pformat(result), offset_space)
    loop = loop or asyncio.get_event_loop()
    format_text('loop', loop_repr(loop), offset_space)
    format_text('end', str(end_time), offset_space)
    format_text('elapsed', str(elapsed_time), offset_space)
    print(SEPARATOR)


def debug(*args, offset=None, indent=4, context=None):
    """
    Debug

    Debug decorator factory for regular and async functions. May be used
    on functions and methods. For class methods, static methods, and
    properties, @debug must be the inner wrapper.

    Debugging information:
    - Upon entering: args/kwargs, instance repr (if method) & start time
    - Upon exiting: args/kwargs, instance repr (if method), return value
      & end/elapsed time (args/kwargs/instance are repeated since the
      return message can be far removed from the enter message)
    - Horizontal separators visually delineate each message
    - Each message is offset by the degree to which the function follows
      other @debug decorated functions since the last gather/wait

    I/O:
    offset=None:    By default, offset increases automatically with each
                    level of decorated debug call, but this parameter
                    allows it to be overridden
    indent=4:       Integer specifying the number of spaces to be used
                    for each level of offset
    context=None:   String to be evaluated & printed as enter/exit info;
                    May reference `self`, `func`, `args`, or `kwargs`.
    """
    def debug_decorator(func):

        if asyncio.iscoroutinefunction(func):
            @wrapt.decorator
            async def async_debug_wrapper(func, instance, args, kwargs):
                loop = asyncio.get_event_loop()
                offset_space = derive_offset_space(offset, indent)
                printer = PrettyPrinter(indent=indent, width=WIDTH - len(offset_space))

                print_enter_info(func, context, instance, args, kwargs,
                                 printer, offset_space, loop, is_async=True)

                true_start_time = loop.time()
                result = await func(*args, **kwargs)
                end_time = loop.time()
                elapsed_time = end_time - true_start_time

                print_exit_info(func, context, instance, args, kwargs,
                                result, end_time, elapsed_time,
                                printer, offset_space, loop, is_async=True)
                return result

            return async_debug_wrapper(func)

        @wrapt.decorator
        def sync_debug_wrapper(func, instance, args, kwargs):
            loop = asyncio.get_event_loop()
            offset_space = derive_offset_space(offset, indent)
            printer = PrettyPrinter(indent=indent, width=WIDTH - len(offset_space))

            print_enter_info(func, context, instance, args, kwargs,
                             printer, offset_space, loop)

            true_start_time = loop.time()
            result = func(*args, **kwargs)
            end_time = loop.time()
            elapsed_time = end_time - true_start_time

            print_exit_info(func, context, instance, args, kwargs,
                            result, end_time, elapsed_time,
                            printer, offset_space, loop)
            return result

        return sync_debug_wrapper(func)

    return factory_direct(debug_decorator, *args)
