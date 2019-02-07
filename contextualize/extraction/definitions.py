#!/usr/bin/env python
# -*- coding: utf-8 -*-
from contextualize.utils.enum import FlexEnum


class ExtractionStatus(FlexEnum):
    """Extraction status"""
    INITIALIZED = 1
    STARTED = 2
    PRELIMINARY = 3
    COMPLETED = 4

    def indicates_results(self):
        """Return True iff status indicates there are results"""
        cls = self.__class__
        if not hasattr(cls, '_indicates_results_set'):
            cls._indicates_results_set = {self.PRELIMINARY, self.COMPLETED}
        return self in cls._indicates_results_set
