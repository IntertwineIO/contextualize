#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple

WebAddress = namedtuple('WebAddress', 'address port')

REDIS_ADDRESS = 'redis://localhost'

TEST_WEB_SERVER_SCHEME = 'http://'
TEST_WEB_SERVER_ADDRESS = WebAddress('localhost', 8888)
TEST_WEB_SERVER_BASE_URL = (f'{TEST_WEB_SERVER_SCHEME}'
                            f'{TEST_WEB_SERVER_ADDRESS.address}:'
                            f'{TEST_WEB_SERVER_ADDRESS.port}')
