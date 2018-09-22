#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from utils.tools import get_related_json, ischildclass

sun = dict(name='Sun')

moon = dict(name='Moon', planet='earth')
phobos = dict(name='Phobos', planet='mars')
deimos = dict(name='Deimos', planet='mars')
mars_moons = [phobos, deimos]

mercury = dict(name='Mercury', star=sun, inner=None, outer='venus', moons=[])
venus = dict(name='Venus', star=sun, inner='mercury', outer='earth', moons=[])
earth = dict(name='Earth', star=sun, inner='venus', outer='mars', moons=['moon'])
mars = dict(name='Mars', star=sun, inner='earth', outer='jupiter', moons=['phobos', 'deimos'])

payload = dict(venus=venus, earth=earth, mars=mars, moon=moon, phobos=phobos, deimos=deimos)
paybug1 = dict(venus=venus, earth=earth, mars=mars, moon=moon, phobos=phobos)
paybug2 = dict(venus=venus, earth=earth, mars=mars, moon=moon)

@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'base',  'field',   'payload', 'strict', 'check'), [
    (0,     earth,   'outer',   payload,   True,     mars),        # Key
    (1,     earth,   'outer',   None,      True,     TypeError),   # Key/no payload/strict
    (2,     earth,   'outer',   None,      False,    None),        # Key/no payload
    (3,     earth,   'star',    payload,   True,     sun),         # Nested dict
    (4,     earth,   'star',    None,      True,     sun),         # Nested dict/no payload
    (5,     mercury, 'inner',   payload,   True,     None),        # None
    (6,     mars,    'outer',   payload,   True,     KeyError),    # Missing key/strict
    (7,     mars,    'outer',   payload,   False,    None),        # Missing key
    (8,     venus,   'planet',  payload,   True,     KeyError),    # Missing field/strict
    (9,     venus,   'planet',  payload,   False,    None),        # Missing field
    (10,    venus,   'moons',   payload,   True,     []),          # List: empty
    (11,    venus,   'moons',   None,      True,     []),          # List: empty/no payload
    (12,    earth,   'moons',   payload,   True,     [moon]),      # List: 1 key
    (13,    earth,   'moons',   None,      True,     TypeError),   # List: 1 key/no payload/strict
    (14,    earth,   'moons',   None,      False,    []),          # List: 1 key/no payload
    (15,    mars,    'moons',   payload,   True,     mars_moons),  # List: 2 keys
    (16,    mars,    'moons',   paybug1,   True,     KeyError),    # List: 2 keys/1 missing/strict
    (17,    mars,    'moons',   paybug1,   False,    [phobos]),    # List: 2 keys/1 missing
    (18,    mars,    'moons',   paybug2,   True,     KeyError),    # List: 2 keys/2 missing/strict
    (19,    mars,    'moons',   paybug2,   False,    []),          # List: 2 keys/2 missing
])
def test_get_related_json(idx, base, field, payload, strict, check):
    """Test get related JSON under different scenarios"""
    if ischildclass(check, Exception):
        with pytest.raises(check):
            value = get_related_json(base, field, payload, strict)

    else:
        value = get_related_json(base, field, payload, strict)
        if check is None or isinstance(check, dict):
            assert value is check
        else:
            assert value == check
