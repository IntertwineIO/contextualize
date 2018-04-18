#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import inspect

import wrapt
from pprint import PrettyPrinter

DELIMITER = '–'
WIDTH = 160
SEPARATOR = DELIMITER * WIDTH


def offset_text(text, offset_space):
    lines = text.split('\n')
    offset_lines = (offset_space + line for line in lines)
    offset_text = '\n'.join(offset_lines)
    return offset_text


def format_text(label, text, offset_space):
    if '\n' in text:
        print(f'{offset_space}{label}:')
        print(f'{offset_text(text, offset_space)}')
    else:
        print(f'{offset_space}{label}: {text}')


def async_debug(offset=None, indent=4):

    @wrapt.decorator
    async def async_debug_wrapper(wrapped, instance, args, kwargs):
        nonlocal offset
        if offset is None:
            frame_records = inspect.stack()
            offset = sum(1 for f in frame_records
                         if f.function == 'async_debug_wrapper') - 1

        offset_space = ' ' * offset * indent
        pp = PrettyPrinter(indent=indent, width=WIDTH)
        print(SEPARATOR)
        print(f'{offset_space}Awaiting {wrapped.__name__}')
        if instance is not None:
            format_text('instance', repr(instance), offset_space)

        format_text('args', pp.pformat(args), offset_space)
        format_text('kwargs', pp.pformat(kwargs), offset_space)

        loop = asyncio.get_event_loop()
        start_time = loop.time()
        format_text('start', str(start_time), offset_space)
        print(SEPARATOR)

        true_start_time = loop.time()
        result = await wrapped(*args, **kwargs)
        end_time = loop.time()
        elapsed_time = end_time - true_start_time

        print(SEPARATOR)
        print(f'{offset_space}Result returned from {wrapped.__name__}')
        if instance is not None:
            format_text('instance', repr(instance), offset_space)

        format_text('end', str(end_time), offset_space)
        format_text('elapsed', str(elapsed), offset_space)

        format_text('args', pp.pformat(args), offset_space)
        format_text('kwargs', pp.pformat(kwargs), offset_space)
        format_text('result', pp.pformat(result), offset_space)
        print(SEPARATOR)

        return result

    return async_debug_wrapper
