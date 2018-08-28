#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from collections import OrderedDict

from sanic import Blueprint
from sanic import response

from services.community_service import CommunityService
from utils.api import HTTPMethod as HTTP
from utils.cache import AsyncCache

BLUEPRINT = 'community'
bp = Blueprint(BLUEPRINT)


@bp.route(f'/{BLUEPRINT}', methods=HTTP.list('GET'))
async def test(request):
    return response.json({'hello': 'world'})


@bp.route(f'/{BLUEPRINT}/content/extraction', methods=HTTP.list('POST'))
async def extract(request):
    '''
    Extract community content

    Usage:
    curl -H "Content-Type: application/json" -X POST -d '{
        "/communities/homelessness/us/tx?org=None": {
            "name": "Homelessness in Texas, U.S.",
            "problem": "/problems/homelessness",
            "org": null,
            "geo": "/geos/us/tx"
        },
        "/problems/homelessness": {
            "name": "Homelessness"
        },
        "/geos/us/tx": {
            "name": "Texas",
            "abbrev": "TX",
            "display": "Texas, U.S.",
            "path_parent": "/geos/us"
        },
        "root": "/communities/homelessness/us/tx?org=None"
    }' 'http://127.0.0.1:5001/community/content/extraction'
    '''
    app = request.app
    loop = app.loop
    cache = AsyncCache()
    payload = json.loads(request.body, object_pairs_hook=OrderedDict)

    community_service = CommunityService(payload)
    extract_community_content = community_service.extract_content()

    loop.create_task(extract_community_content)

    return response.json({"search_data": search_data})
