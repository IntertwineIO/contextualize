#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

import uvloop

from service import Service


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    problem_name = 'Homelessness'
    org_name = None
    geo_name = 'California'

    service = Service(loop, problem_name, org_name, geo_name)

    service.contextualize()

    loop.close()


if __name__ == '__main__':
    main()
