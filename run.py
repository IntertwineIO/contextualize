#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

import uvloop

from utils.cache import AsyncCache
from service import Service


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    problem_name = 'Homelessness'
    org_name = None
    geo_name = 'Texas'

    service = Service(loop, problem_name, org_name, geo_name)

    try:
        loop.run_until_complete(service.contextualize())

    except asyncio.CancelledError:
        print('One or more tasks have been canceled.')

    finally:
        loop.run_until_complete(AsyncCache().disconnect())
        loop.close()


if __name__ == '__main__':
    main()
