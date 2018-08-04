#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import random
from datetime import datetime


@pytest.fixture(scope='function')
def seed():
    """Create consistently rotating seed for tests"""
    now = datetime.now()
    seed = int(now.strftime('%Y%m%d'))  # YYYYMMDD
    print('random seed:', seed)
    random.seed(seed)
    return seed
