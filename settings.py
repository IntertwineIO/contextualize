#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple

WebAddress = namedtuple('WebAddress', 'host port')

REDIS_ADDRESS = 'redis://localhost'

TEST_WEB_SERVER_HOST = 'localhost'
TEST_WEB_SERVER_PORT = 8888
TEST_WEB_SERVER_ADDRESS = WebAddress(TEST_WEB_SERVER_HOST, TEST_WEB_SERVER_PORT)
TEST_WEB_SERVER_BASE_URL = f'http://{TEST_WEB_SERVER_HOST}:{TEST_WEB_SERVER_PORT}'
