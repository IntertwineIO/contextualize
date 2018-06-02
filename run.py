#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

import uvloop

from service import Service
from utils.cache import AsyncCache


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    cache = AsyncCache(loop)

    problem_name = 'Homelessness'
    org_name = None
    geo_name = 'Texas'

    service = Service(problem_name, org_name, geo_name, cache, loop)

    try:
        loop.run_until_complete(service.contextualize())

    except asyncio.CancelledError:
        print('One or more tasks have been canceled.')

    finally:
        cache.shutdown(loop)
        loop.close()


if __name__ == '__main__':
    main()
