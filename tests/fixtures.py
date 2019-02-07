#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import random
import threading
from datetime import datetime

import aiohttp
import pytest
import uvloop

from contextualize.utils.testing.servers.web_server import ScopedHTTPServer
from settings import TEST_WEB_SERVER_ADDRESS

TEST_WEB_SERVER_BASE_PATH = '/tests'
EVENT_LOOP_SCOPE = 'session'

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@pytest.fixture(scope='function')
def seed():
    """Create consistently rotating seed for tests"""
    now = datetime.now()
    seed = int(now.strftime('%Y%m%d'))  # YYYYMMDD
    print('random seed:', seed)
    random.seed(seed)
    return seed


@pytest.fixture(scope='session')
def web_server():
    """Pytest fixture to serve up a directory via HTTP"""
    with ScopedHTTPServer(TEST_WEB_SERVER_ADDRESS, base_path=TEST_WEB_SERVER_BASE_PATH) as httpd:
        thread = threading.Thread(target=httpd.serve_forever,
                                  name='test_web_server_thread',
                                  daemon=True)
        thread.start()
        yield httpd


@pytest.fixture(scope=EVENT_LOOP_SCOPE)
def event_loop():
    """Override pytest_asyncio event_loop fixture for uvloop & session scope"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope=EVENT_LOOP_SCOPE)  # aiohttp scope must be <= loop scope
async def web_client_session(event_loop):
    """Pytest fixture for aiohttp client session"""
    async with aiohttp.ClientSession() as session:
        yield session
