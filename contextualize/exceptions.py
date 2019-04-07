#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

logger = logging.getLogger(__name__)


class ErrorDocstringMixin:
    """Mixin allowing custom exceptions to be defined via docstrings"""

    def __init__(self, message=None, *args, **kwds):
        template = message if message else ' '.join(self.__doc__.split())
        message = template.format(**kwds) if kwds else (
            template.format(*args) if args else template)
        message = message.strip('\"\'')
        logger.debug(message)
        super().__init__(self, message)


class BaseCustomError(ErrorDocstringMixin, Exception):
    """An error has occurred"""


class ContextManagerReentryError(BaseCustomError):
    """The same context cannot be re-entered until exiting; id: <id>"""


class BaseCustomValueError(ErrorDocstringMixin, ValueError):
    """A ValueError has occurred"""


class NoneValueError(BaseCustomValueError):
    """Expected non-null value; received: None"""


class TooFewValuesError(BaseCustomValueError):
    """Expected at least: {expected}; received: {received}"""


class TooManyValuesError(BaseCustomValueError):
    """Expected at most: {expected}; received: {received}"""
