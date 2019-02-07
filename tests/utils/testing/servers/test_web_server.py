#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple
from urllib.parse import urljoin

import aiohttp
import pytest

from settings import TEST_WEB_SERVER_BASE_URL

TEST_RELATIVE_URL = '/utils/testing/servers/hello_world.html'
TEST_URL = urljoin(TEST_WEB_SERVER_BASE_URL, TEST_RELATIVE_URL)

Response = namedtuple('Response', 'text status')


async def fetch(url, session):
    async with session.get(url) as response:
        text = await response.text()
        return Response(text=text, status=response.status)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_web_server_fixture(web_server):
    async with aiohttp.ClientSession() as session:
        response = await fetch(TEST_URL, session)
        assert response.text == 'Hello World!'
        assert response.status == 200


@pytest.mark.unit
@pytest.mark.asyncio
async def test_web_server_via_web_client_fixture(web_server, web_client_session):
    response = await fetch(web_client_session, TEST_URL)
    assert response.text == 'Hello World!'
    assert response.status == 200
