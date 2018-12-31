#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

logger = logging.getLogger(__name__)


class BaseException(Exception):
    """Base exception class for any custom exceptions"""

    def __init__(self, message=None, *args, **kwds):
        template = message if message else ' '.join(self.__doc__.split())
        message = template.format(**kwds) if kwds else (
            template.format(*args) if args else template)
        message = message.strip('\"\'')
        logger.debug(message)
        super().__init__(self, message)


class NoneValueError(BaseException, ValueError):
    """Expected non-null value; received: None"""


class TooFewValuesError(BaseException, ValueError):
    """Expected number: {expected}; received: {received}"""


class TooManyValuesError(BaseException, ValueError):
    """Expected number: {expected}; received: {received}"""
