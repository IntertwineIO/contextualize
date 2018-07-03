#!/usr/bin/env python
# -*- coding: utf-8 -*-
from sanic import Sanic

from api.community import bp as community

APPLICATION_NAME = 'contextualize'

app = Sanic(APPLICATION_NAME)
app.blueprint(community)
