#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

import wrapt
from pprint import PrettyPrinter

DELIMITER = '–'
WIDTH = 160
SEPARATOR = DELIMITER * WIDTH


def offset_text(text, offset_):
    lines = text.split('\n')
    offset_lines = (offset_ + line for line in lines)
    offset_text = '\n'.join(offset_lines)
    return offset_text


def format_text(label, text, offset_):
    if '\n' in text:
        print(f'{offset_}{label}:')
        print(f'{offset_text(text, offset_)}')
    else:
        print(f'{offset_}{label}: {text}')


def async_debug(offset=0, indent=4):
    @wrapt.decorator
    async def wrapper(wrapped, instance, args, kwargs):
        pp = PrettyPrinter(indent=indent, width=WIDTH)
        print(SEPARATOR)
        offset_ = ' ' * offset
        print(f'{offset_}Awaiting {wrapped.__name__}')
        if instance is not None:
            format_text('instance', repr(instance), offset_)

        format_text('args', pp.pformat(args), offset_)
        format_text('kwargs', pp.pformat(kwargs), offset_)

        loop = asyncio.get_event_loop()
        start_time = loop.time()
        format_text('start', str(start_time), offset_)
        print(SEPARATOR)

        result = await wrapped(*args, **kwargs)

        print(SEPARATOR)
        end_time = loop.time()
        elapsed = end_time - start_time

        print(f'{offset_}Result returned from {wrapped.__name__}')
        if instance is not None:
            format_text('instance', repr(instance), offset_)

        format_text('end', str(end_time), offset_)
        format_text('elapsed', str(elapsed), offset_)

        format_text('args', pp.pformat(args), offset_)
        format_text('kwargs', pp.pformat(kwargs), offset_)
        format_text('result', pp.pformat(result), offset_)
        print(SEPARATOR)

        return result
    return wrapper
