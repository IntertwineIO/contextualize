#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from itertools import permutations

from utils.tools import numify
from utils.verbiage import Plurality, a, an


@pytest.mark.unit
@pytest.mark.parametrize(
    ('article', 'phrase'),
    [
     ('an',     'alligator'),
     ('a',      'baboon'),
     ('a',      'cat'),
     ('a',      'dolphin'),
     ('an',     'elephant'),
     ('a',      'European swallow'),
     ('a',      'fox'),
     ('a',      'gazelle'),
     ('a',      'hippopotamus'),
     ('an',     'herb-loving dinosaur'),
     ('a',      'herbivorous dinosaur'),  # American English is gloriously inconsistent
     ('an',     'iguana'),
     ('a',      'jellyfish'),
     ('a',      'koala'),
     ('a',      'lobster'),
     ('a',      'manatee'),
     ('a',      'newt'),
     ('an',     'octopus'),
     ('a',      'one-eyed minion'),
     ('a',      'panther'),
     ('a',      'quail'),
     ('a',      'red panda'),
     ('an',     'R.O.U.S.'),
     ('a',      'salamander'),
     ('a',      'tiger'),
     ('a',      'unicorn'),
     ('an',     'urchin'),
     ('a',      'vulture'),
     ('a',      'whale'),
     ('a',      'xeme'),  # missing from cmudict
     ('an',     'x-ray tetra'),
     ('a',      'yak'),
     ('a',      'zebra'),
     ('a',      '0-headed hydra'),
     ('a',      '1-headed hydra'),
     ('a',      '2-headed hydra'),
     ('a',      '3-headed hydra'),
     ('a',      '4-headed hydra'),
     ('a',      '5-headed hydra'),
     ('a',      '6-headed hydra'),
     ('a',      '7-headed hydra'),
     ('an',     '8-headed hydra'),
     ('a',      '9-headed hydra'),
     ('a',      '10-headed hydra'),
     ('an',     '11-headed hydra'),
     ('a',      '12-headed hydra'),
     ('a',      '13-headed hydra'),
     ('a',      '14-headed hydra'),
     ('a',      '15-headed hydra'),
     ('a',      '16-headed hydra'),
     ('a',      '17-headed hydra'),
     ('an',     '18-headed hydra'),
     ('a',      '19-headed hydra'),
     ('a',      '20-headed hydra'),
     ('a',      '30-headed hydra'),
     ('a',      '40-headed hydra'),
     ('a',      '50-headed hydra'),
     ('a',      '60-headed hydra'),
     ('a',      '70-headed hydra'),
     ('an',     '80-headed hydra'),
     ('a',      '90-headed hydra'),
     ('a',      '100-headed hydra'),
     ('a',      '200-headed hydra'),
     ('a',      '300-headed hydra'),
     ('a',      '400-headed hydra'),
     ('a',      '500-headed hydra'),
     ('a',      '600-headed hydra'),
     ('a',      '700-headed hydra'),
     ('an',     '800-headed hydra'),
     ('a',      '900-headed hydra'),
     ('a',      '1000-headed hydra'),
     ('an',     '1100-headed hydra'),
     ('a',      '1200-headed hydra'),
     ('a',      '1300-headed hydra'),
     ('a',      '1400-headed hydra'),
     ('a',      '1500-headed hydra'),
     ('a',      '1600-headed hydra'),
     ('a',      '1700-headed hydra'),
     ('an',     '1800-headed hydra'),
     ('a',      '1900-headed hydra'),
     ('a',      '2000-headed hydra'),
     ('a',      '3000-headed hydra'),
     ('a',      '4000-headed hydra'),
     ('a',      '5000-headed hydra'),
     ('a',      '6000-headed hydra'),
     ('a',      '7000-headed hydra'),
     ('an',     '8000-headed hydra'),
     ('a',      '9000-headed hydra'),
     ('a',      '10,000-headed hydra'),
     ('an',     '11,000-headed hydra'),
     ('a',      '12,000-headed hydra'),
     ('a',      '13,000-headed hydra'),
     ('a',      '14,000-headed hydra'),
     ('a',      '15,000-headed hydra'),
     ('a',      '16,000-headed hydra'),
     ('a',      '17,000-headed hydra'),
     ('an',     '18,000-headed hydra'),
     ('a',      '19,000-headed hydra'),
     ('a',      '20,000-headed hydra'),
     ('a',      '30,000-headed hydra'),
     ('a',      '40,000-headed hydra'),
     ('a',      '50,000-headed hydra'),
     ('a',      '60,000-headed hydra'),
     ('a',      '70,000-headed hydra'),
     ('an',     '80,000-headed hydra'),
     ('a',      '90,000-headed hydra'),
     ('a',      '100,000-headed hydra'),
     ('a',      '110,000-headed hydra'),
     ('a',      '120,000-headed hydra'),
     ('a',      '130,000-headed hydra'),
     ('a',      '140,000-headed hydra'),
     ('a',      '150,000-headed hydra'),
     ('a',      '160,000-headed hydra'),
     ('a',      '170,000-headed hydra'),
     ('a',      '180,000-headed hydra'),
     ('a',      '190,000-headed hydra'),
     ('a',      '200,000-headed hydra'),
     ('a',      '300,000-headed hydra'),
     ('a',      '400,000-headed hydra'),
     ('a',      '500,000-headed hydra'),
     ('a',      '600,000-headed hydra'),
     ('a',      '700,000-headed hydra'),
     ('an',     '800,000-headed hydra'),
     ('a',      '900,000-headed hydra'),
     ('a',      '1,000,000-headed hydra'),
     ('a',      '1,100,000-headed hydra'),
     ('a',      '1,200,000-headed hydra'),
     ('a',      '1,300,000-headed hydra'),
     ('a',      '1,400,000-headed hydra'),
     ('a',      '1,500,000-headed hydra'),
     ('a',      '1,600,000-headed hydra'),
     ('a',      '1,700,000-headed hydra'),
     ('a',      '1,800,000-headed hydra'),
     ('a',      '1,900,000-headed hydra'),
     ('a',      '2,000,000-headed hydra'),
     ('a',      '3,000,000-headed hydra'),
     ('a',      '4,000,000-headed hydra'),
     ('a',      '5,000,000-headed hydra'),
     ('a',      '6,000,000-headed hydra'),
     ('a',      '7,000,000-headed hydra'),
     ('an',     '8,000,000-headed hydra'),
     ('a',      '9,000,000-headed hydra'),
     ('a',      '10,000,000-headed hydra'),
     ('an',     '11,000,000-headed hydra'),
     ('a',      '12,000,000-headed hydra'),
     ('a',      '13,000,000-headed hydra'),
     ('a',      '14,000,000-headed hydra'),
     ('a',      '15,000,000-headed hydra'),
     ('a',      '16,000,000-headed hydra'),
     ('a',      '17,000,000-headed hydra'),
     ('an',     '18,000,000-headed hydra'),
     ('a',      '19,000,000-headed hydra'),
     ('a',      '20,000,000-headed hydra'),
     ])
def test_indefinite_article(article, phrase):
    check = f'{article} {phrase}'
    articled1 = a(phrase)
    assert articled1 == check
    articled2 = an(phrase)
    assert articled2 == check


@pytest.mark.unit
@pytest.mark.parametrize(
    ('idx', 'forms',                'article',  'thing',            'things'),
    [
     (0,    'g/oose/eese',          'a',        'goose',            'geese'),
     (1,    'ox/en',                'an',       'ox',               'oxen'),
     (2,    '/cow/kine',            'a',        'cow',              'kine'),
     (3,    'octop/us/odes',        'an',       'octopus',          'octopodes'),
     (4,    'unicorn/s',            'a',        'unicorn',          'unicorns'),
     (5,    "R.O.U.S./'s",          'an',       'R.O.U.S.',         "R.O.U.S.'s"),
     (6,    '11-headed hydra/s',    'an',       '11-headed hydra',  '11-headed hydras'),
     ])
@pytest.mark.parametrize(
    'number',
    [
        0,
        0.5,
        1,
        2,
        3,
        1000000
    ])
@pytest.mark.parametrize(
    'templates',
    [
        None,
        '0=no $things;1=$a $thing;2=a couple $things',
    ])
def test_plurality(idx, number, forms, templates, article, thing, things):
    """Test Plurality functionality"""
    plurality = Plurality(number, forms, templates) if templates else Plurality(number, forms)
    validate_instantiation(plurality, number, forms, templates, thing, things)
    validate_str(plurality, number, templates, article, thing, things)
    validate_repr(plurality)
    validate_clone(plurality)
    validate_partials(number, forms, templates, article, plurality)


def validate_instantiation(plurality, number, forms, templates, thing, things):
    assert plurality.number == number
    assert plurality.singular == thing
    assert plurality.plural == things
    assert plurality.forms == forms
    if templates:
        assert plurality.custom_templates == templates


def validate_str(plurality, number, templates, article, thing, things):
    if templates is None:
        template_map = Plurality.TEMPLATE_DEFAULTS
        template = template_map.get(number, template_map[Plurality.NUMBER_TOKEN])
        str_check = template.safe_substitute(n=number, thing=thing, things=things)
    else:
        template_map = Plurality.TEMPLATE_DEFAULTS.copy()
        template_formatters = templates.split(Plurality.FORMATTER_DELIMITER)
        template_strings = (f.split(Plurality.TEMPLATE_ASSIGNER) for f in template_formatters)
        custom_templates = (
            (numify(k, k), Plurality.TEMPLATE_CLASS(v)) for k, v in template_strings)
        template_map.update(custom_templates)
        template = template_map.get(number, template_map[Plurality.NUMBER_TOKEN])
        str_check = template.safe_substitute(a=article, n=number, thing=thing, things=things)

    assert str(plurality) == str_check
    assert plurality + '' == str_check
    assert '' + plurality == str_check


def validate_repr(plurality):
    plurality_repr = repr(plurality)
    evalled_plurality_repr = eval(plurality_repr)
    assert evalled_plurality_repr == plurality
    assert str(evalled_plurality_repr) == str(plurality)


def validate_clone(plurality):
    p1 = plurality.clone()
    assert p1 == plurality
    assert str(p1) == str(plurality)
    assert p1.template_map is plurality.template_map

    p2 = plurality.clone_with(42)
    assert p2 != plurality
    assert str(p2) != str(plurality)
    assert p2.template_map is plurality.template_map

    p3 = plurality.clone(deep=True)
    assert p3 == p1
    assert str(p3) == str(p1)
    assert p3.template_map == p1.template_map
    assert p3.template_map is not p1.template_map

    p4 = plurality.clone_with(42, deep=True)
    assert p4 != plurality
    assert p4 == p2
    assert str(p4) == str(p2)
    assert p4.template_map == p2.template_map
    assert p4.template_map is not p2.template_map
    assert p4.template_map is not p3.template_map


def validate_partials(number, forms, templates, article, plurality_check):
    all_args = [number, forms]
    if templates:
        all_args.append(templates)
    num_args = len(all_args)

    for arg_gen in permutations(all_args):
        args = list(arg_gen)

        for num_init_args in range(num_args):
            init_args, remaining_args = args[:num_init_args], args[num_init_args:]
            print(init_args, remaining_args)
            p1 = Plurality(*init_args)
            validate_partial_str(p1, article)

            # Validate calling plurality instance on remaining args
            p2 = p1(*remaining_args)
            assert p2 is not p1
            assert p2 == plurality_check
            assert str(p2) == str(plurality_check)

            # Validate formatting plurality instance with remaining args
            stringified_remaining_args = [str(arg) for arg in remaining_args]
            formatter = Plurality.FORMATTER_DELIMITER.join(stringified_remaining_args)
            formatable = f'{{plurality:{formatter}}}'
            formatted = formatable.format(plurality=p1)
            assert formatted == str(plurality_check)


def validate_partial_str(plurality, article):
    n = plurality.number
    thing, things = plurality.singular, plurality.plural
    template = plurality.get_template()
    template_str = template.template
    plurality_str = str(plurality)

    validate_number_in_partial_str(n, template_str, plurality_str)
    validate_forms_in_partial_str(thing, things, article, template_str, plurality_str)


def validate_number_in_partial_str(number, template_str, plurality_str):
    number_token = f'${Plurality.NUMBER_TOKEN}'
    if number_token in template_str:
        if number is None:
            assert number_token in plurality_str
        else:
            assert str(number) in plurality_str


def validate_forms_in_partial_str(thing, things, article, template_str, plurality_str):
    thing_token, things_token = f'${Plurality.SINGULAR_TOKEN}', f'${Plurality.PLURAL_TOKEN}'

    if things_token in template_str:
        validate_things_in_partial_str(things, things_token, plurality_str)

    elif thing_token in template_str:
        validate_thing_in_partial_str(thing, thing_token, article, template_str, plurality_str)


def validate_things_in_partial_str(things, things_token, plurality_str):
    if things is None:
        assert things_token in plurality_str
    else:
        assert things in plurality_str


def validate_thing_in_partial_str(thing, thing_token, article, template_str, plurality_str):
    article_token = f'${Plurality.ARTICLE_TOKEN}'
    if thing is None:
        assert thing_token in plurality_str

        if article_token in template_str:
            article_thing_token = a(thing_token)
            assert article_thing_token in plurality_str
    else:
        assert thing in plurality_str
        # if thing and $a template and number == 1, it's not partial
        assert article_token not in template_str
