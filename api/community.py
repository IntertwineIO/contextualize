#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from collections import OrderedDict

from sanic import Blueprint
from sanic import response

from service import Service
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

    community_key = payload['root']
    community = payload[community_key]
    problem_key = community['problem']
    problem_terms = payload[problem_key]['name'] if problem_key else None
    org_key = community['org']
    org_terms = payload[org_key]['name'] if org_key else None
    geo_key = community['geo']
    if geo_key:
        geo = payload[geo_key]
        geo_terms = [geo['name'], geo['abbrev']] if geo['abbrev'] else geo['name']
    else:
        geo_terms = None

    search_data = OrderedDict(problem=problem_terms, org=org_terms, geo=geo_terms)
    service = Service(cache, loop)
    extract_community_content = service.extract_content(**search_data)

    loop.create_task(extract_community_content)

    return response.json({"search_data": search_data})
