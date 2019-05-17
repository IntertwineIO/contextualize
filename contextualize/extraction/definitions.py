#!/usr/bin/env python
# -*- coding: utf-8 -*-
from contextualize.utils.enum import IncreasingEnum


class ExtractionStatus(IncreasingEnum):
    """
    Extraction status

    State changes:
    FAILURE     -> INITIATED (after threshold)
    EMPTY       -> INITIATED (after threshold)
    INITIATED   -> PRELIMINARY, FAILURE, EMPTY
    PRELIMINARY -> COMPLETED
    COMPLETED   -> PRELIMINARY (after threshold)
    """
    FAILURE = 1      # Extraction failed within threshold
    EMPTY = 2        # Extraction returned no results within threshold
    INITIATED = 3    # Extraction in process; check back
    PRELIMINARY = 4  # Preliminary content available; check back
    COMPLETED = 5    # Extraction completed

    @classmethod
    def aggregate(cls, *statuses):
        """
        Aggregate statuses to produce an overall extraction status:

        FAILURE     : all extractions FAILURE
        EMPTY       : 1+ extractions EMPTY and rest are FAILURE
        INITIATED   : 1+ extractions INITIATED and none PRELIMINARY
        PRELIMINARY : 1+ extractions PRELIMINARY
        COMPLETED   : all extractions COMPLETED
        """
        minimum = cls.minimum(*statuses, nullable=True)
        if minimum is None:
            return None

        if minimum is cls.COMPLETED:
            return cls.COMPLETED

        maximum = cls.maximum(*statuses, nullable=True)
        if maximum >= cls.PRELIMINARY:
            return cls.PRELIMINARY

        return maximum
