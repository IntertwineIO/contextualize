#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from collections import OrderedDict

from sanic import Blueprint
from sanic import response

from contextualize.extraction.definitions import ExtractionStatus
from contextualize.services.community_service import CommunityService
from contextualize.utils.api import HTTPMethod as HTTP

BLUEPRINT = 'community'
bp = Blueprint(BLUEPRINT)


@bp.route(f'/{BLUEPRINT}', methods=list(HTTP.names('GET')))
async def test(request):
    return response.json({'hello': 'world'})


@bp.route(f'/{BLUEPRINT}/content', methods=list(HTTP.names('POST')))
async def contextualize(request):
    '''
    Contextualize community problem by furnishing related content:
    - Related content includes the following:
        - Community content
        - Geo ancestor content (future)
          (e.g. Homelessness in Texas, Homelessness in US, Homelessness)
        - Highly-rated content in strongly related communities (future)
    - Look for existing content in cache
    - Extract additional content for a community if...
        - No extraction has occurred
        - The most recent extraction was long enough ago (future)

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
    }' 'http://127.0.0.1:5001/community/content'
    '''
    app = request.app
    loop = app.loop
    payload = json.loads(request.body, object_pairs_hook=OrderedDict)

    community_service = CommunityService.from_payload(payload)
    response_value = dict(search_data=community_service.search_data)

    status = await community_service.determine_status()

    response_value['status'] = status.name
    # TODO: COMPLETED -> SUCCESS; NO_RESULTS; FAILURE
    if status >= ExtractionStatus.PRELIMINARY:
        community_content = await community_service.cache.retrieve_search_results()
        if community_content:
            response_value['content'] = community_content

    if status is not ExtractionStatus.COMPLETED:
        extract_community_content = community_service.extract_content()
        loop.create_task(extract_community_content)

    return response.json(response_value)


@bp.route(f'/{BLUEPRINT}/content/extraction', methods=list(HTTP.names('POST')))
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
    payload = json.loads(request.body, object_pairs_hook=OrderedDict)

    community_service = CommunityService.from_payload(payload)
    extract_community_content = community_service.extract_content()

    loop.create_task(extract_community_content)

    return response.json({'status': ExtractionStatus.INITIALIZED.name})
