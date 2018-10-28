#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from utils.tools import derive_domain, get_related_json, ischildclass, logical_xor, xor_constrain


FULL_URL = 'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5452388/'
CHECK_DOMAIN = 'www.ncbi.nlm.nih.gov'


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'url',                                                  'base',       'check'),
    [(0,    'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5452388/', None,         CHECK_DOMAIN),
     (1,          '//www.ncbi.nlm.nih.gov/pmc/articles/PMC5452388/', None,         CHECK_DOMAIN),
     (2,            'www.ncbi.nlm.nih.gov/pmc/articles/PMC5452388/', None,         CHECK_DOMAIN),
     (3,    'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5452388',  None,         CHECK_DOMAIN),
     (4,    'https://www.ncbi.nlm.nih.gov/',                         None,         CHECK_DOMAIN),
     (5,    'https://www.ncbi.nlm.nih.gov',                          None,         CHECK_DOMAIN),
     (6,            'www.ncbi.nlm.nih.gov/pmc/articles/PMC5452388',  None,         CHECK_DOMAIN),
     (7,            'www.ncbi.nlm.nih.gov/',                         None,         CHECK_DOMAIN),
     (8,            'www.ncbi.nlm.nih.gov',                          None,         CHECK_DOMAIN),
     (9,                                '/pmc/articles/PMC5452388/', CHECK_DOMAIN, CHECK_DOMAIN),
     (10,                               '/pmc/articles/PMC5452388/', FULL_URL,     CHECK_DOMAIN),
     (11,                               '/pmc/articles/PMC5452388/', None,         ValueError),
     (12,                               '/pmc/articles/PMC5452388/', '',           ValueError),
     (13,                               '/pmc/articles/PMC5452388/', '/pmc',       ValueError),
     (14,                               None,                        None,         ValueError),
     (15,                               '',                          None,         ValueError),
     ])
def test_derive_domain(idx, url, base, check):
    """Test derive domain under different scenarios"""
    if ischildclass(check, Exception):
        with pytest.raises(check):
            value = derive_domain(url, base)

    else:
        value = derive_domain(url, base)
        if check is None or isinstance(check, dict):
            assert value is check
        else:
            assert value == check


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
    ('idx', 'base',   'field',  'payload', 'strict', 'check'),
    [(0,     earth,   'outer',   payload,   True,     mars),        # Key
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


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'a',    'b',    'check'),
    [
     (0,      False,  False,  False),
     (1,      True,   False,  True),
     (2,      False,  True,   True),
     (3,      True,   True,   False),
     (4,      0,      0,      False),
     (5,     'a',     0,      True),
     (6,      0,     'b',     True),
     (7,     'a',    'b',     False),
     (8,      None,   None,   False),
     (9,     'a',     None,   True),
     (10,     None,  'b',     True),
     (11,    'a',    'b',     False),
     ])
def test_logical_xor(idx, a, b, check):
    assert logical_xor(a, b) is check


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx',  'a',    'b',    'check'),
    [
     (0,      False,  False,  ValueError),
     (1,      True,   False,  True),
     (2,      False,  True,   True),
     (3,      True,   True,   ValueError),
     (4,      0,      0,      ValueError),
     (5,     'a',     0,      'a'),
     (6,      0,     'b',     'b'),
     (7,     'a',    'b',     ValueError),
     (8,      None,   None,   ValueError),
     (9,     'a',     None,   'a'),
     (10,     None,  'b',     'b'),
     (11,    'a',    'b',     ValueError),
     ])
def test_xor_constrain(idx, a, b, check):
    if ischildclass(check, Exception):
        with pytest.raises(check):
            xor_constrain(a, b)
    else:
        assert xor_constrain(a, b) is check
