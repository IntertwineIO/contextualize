#!/usr/bin/env python
# -*- coding: utf-8 -*-
from sanic import Sanic

from contextualize.api.community import bp as community
from contextualize.utils.cache import AsyncCache

APPLICATION = 'contextualize'

app = Sanic(APPLICATION)
app.blueprint(community)


@app.listener('before_server_start')
async def initialize_cache(app, loop):
    app.cache = AsyncCache(loop)


@app.listener('after_server_stop')
async def terminate_cache(app, loop):
    await app.cache.disconnect()
