#!/usr/bin/env python
# -*- coding: utf-8 -*-
from sanic import Blueprint
from sanic.response import json

BLUEPRINT_NAME = 'community'
bp = Blueprint(BLUEPRINT_NAME)

@bp.route(f'/{BLUEPRINT_NAME}')
async def test(request):
    return json({"hello": "world"})
