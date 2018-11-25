#!/usr/bin/env python
# -*- coding: utf-8 -*-
import importlib
import inspect
import re
from collections import OrderedDict
from past.builtins import basestring
from pprint import PrettyPrinter
from urllib.parse import urlparse

from parse import parse

from exceptions import TooFewValuesError, TooManyValuesError

INDENT = 4
WIDTH = 200

PP = PrettyPrinter(indent=INDENT, width=WIDTH)

CLASS_NAME_PATTERN = re.compile(r'[A-Z][a-zA-Z0-9]*$')

def is_class_name(name):
    return CLASS_NAME_PATTERN.match(name)

INTERPRETER_MODULE = '__main__'
MODULE_NAME_PATTERN = re.compile(r'[a-z][a-z_0-9]*[a-z0-9]$')

def is_module_name(name):
    if name == INTERPRETER_MODULE:
        return True
    return MODULE_NAME_PATTERN.match(name)

SELF_REFERENTIAL_PARAMS = {'self', 'cls', 'meta'}

def derive_args(func):
    """Derive args from the given function"""
    args = inspect.getfullargspec(func).args
    if args and args[0] in SELF_REFERENTIAL_PARAMS:
        del args[0]
    return args


def derive_attributes(cls, _mro=None):
    """
    Derive attributes

    Given a class, derive all instance attributes the class declares or
    inherits by inspecting __init__ methods. Attributes are listed
    by initial declaration order, taking into account super calls.

    Does NOT detect the following:
    - attributes set in methods other than __init__
    - attributes set via setattr
    - attributes only conditionally set (they will always be included)
    """
    mro = cls.mro() if _mro is None else _mro
    len_mro = len(mro)
    attributes = OrderedDict()

    try:
        lines = inspect.getsource(cls.__init__).split('\n')

    except TypeError:  # class definition does not contain __init__
        if len_mro > 2:
            super_attributes = derive_attributes(mro[1], mro[1:])
            attributes.update(super_attributes)

        return attributes

    for line in lines:
        line = line.strip()
        parsed = parse('self.{field} = {}', line)

        if parsed is not None:
            field = parsed.named['field']
            attributes[field] = None
            continue

        if len_mro > 2:  # No attributes on object class
            parsed = (parse('super().__init__({}', line) or
                      parse(f'super({cls.__name__}, self).__init__({{}}', line))
            if parsed is not None:
                super_attributes = derive_attributes(mro[1], mro[1:])
                attributes.update(super_attributes)

    return [k for k in attributes] if _mro is None else attributes


def derive_domain(url, base=None):
    """Derive domain, even if no scheme; use base if relative url"""
    parsed = urlparse(url)
    if parsed.netloc:
        return parsed.netloc

    try:
        url_start = url[0]
    except (TypeError, IndexError):
        raise ValueError(f'Invalid URL: {url}')

    if url_start == '/':
        try:
            base_start = base[0]
        except (TypeError, IndexError):
            raise ValueError('Base is required for relative URL')

        if base_start == '/':
            raise ValueError('Base may not be relative')

        return derive_domain(url=base)

    first_slash_index = url.find('/')
    if first_slash_index > 0:
        return url[:first_slash_index]

    return url


def delist(obj):
    """
    Delist

    Unpack the given obj as follows:
    If obj is not a list, return the obj.
    If obj is a list with 2 or more items, return the list.
    If obj is a list with a single item, return the item.
    If obj is an empty list, return None.
    """
    if isinstance(obj, list):
        if not obj:
            return None
        if len(obj) == 1:
            return obj[0]

    return obj


def enlist(obj):
    """
    Enlist

    Package the given obj as a list as follows:
    If obj is already a list, return the obj.
    If obj is not a list, return list containing obj.
    If obj is None, return an empty list.
    """
    if obj is None:
        return []
    return obj if isinstance(obj, list) else [obj]


def derive_qualname(obj):
    """Derive __qualname__; must be called on self from __init__"""
    classes = []
    stack = frame = None
    is_eligible = False
    try:
        stack = inspect.stack()
        for frame in stack:
            if frame.function == '__call__':
                is_eligible = True
            elif frame.function == '<module>':
                break
            elif is_eligible:
                if not is_class_name(frame.function):
                    continue
                classes.append(frame.function)
    finally:
        del frame
        del stack

    outer = '.'.join(reversed(classes))
    qualname = obj.__class__.__qualname__

    if classes and not qualname.startswith(f'{outer}.'):
        qualname = '.'.join((outer, qualname))

    return qualname


def get_related_json(base, field, payload=None, strict=False):
    """
    Get related JSON

    Return the related JSON specified by the base dictionary, field, and
    encompassing JSON payload. Supports both nested and non-nested JSON.

    If the field value is a string, return the payload item keyed by it.
    If the field value is a list and its first element is a string,
        treat elements as payload keys and return a list of objects.
    Otherwise, assume the value is nested JSON and return it.

    I/O:
    base:          Base JSON dictionary in which to look up field
    field:         Field name for related JSON on base dictionary
    payload=None:  Encompassing JSON payload
    strict=True:   If True, raise on missing field or payload key;
                   If False, return None or exclude it if inside a list
    return:        Related JSON dictionary, list or value
    raise:         KeyError if strict and base is missing field
                   KeyError if strict and payload is missing the key
                   TypeError if strict and invalid payload in key lookup
    """
    try:
        value = base[field]
    except KeyError:
        if strict:
            raise
        return

    if isinstance(value, basestring):
        try:
            return payload[value]
        except (KeyError, TypeError):
            if strict:
                raise
            return

    if isinstance(value, list) and value and isinstance(value[0], basestring):
        values = []
        for element in value:
            try:
                values.append(payload[element])
            except (KeyError, TypeError):
                if strict:
                    raise
        return values

    return value


def ischildclass(obj, classinfo):
    """Check if obj extends classinfo; return None if invalid params"""
    try:
        return issubclass(obj, classinfo)
    except TypeError:
        return None


def isclassmethod(func):
    return inspect.ismethod(func) and inspect.isclass(func.__self__)


def isinstancemethod(func):
    return inspect.ismethod(func) and not inspect.isclass(func.__self__)


def isiterator(obj):
    """Check if object is an iterator (not just iterable)"""
    cls = obj.__class__
    return hasattr(cls, '__next__') and not hasattr(cls, '__len__')


def isnamedtuple(obj):
    """Check if object is a namedtuple"""
    return isinstance(obj, tuple) and hasattr(obj, '_asdict')


def isnonstringsequence(obj):
    """Check if object is non-string sequence: list, tuple, range..."""
    if (isinstance(obj, basestring) or hasattr(obj, 'items') or not hasattr(obj, '__getitem__')):
        return False
    try:
        iter(obj)
        return True
    except TypeError:
        return False


def is_selfish(func):
    """Check if function requires a self/cls/meta parameter"""
    if inspect.isbuiltin(func):
        return False
    try:
        return func.__self__ is not None
    except AttributeError:
        return False


def load_class(specifier):
    """
    Load Class

    Load class based on the given specifier.

    I/O:
    specifier:  absolute path to class, where an inner class can be
                specified via dot notation:
                module.path.to.OuterClass.InnerClass
    return:     class object
    raise:      ValueError if invalid specifier
    """
    module_names, class_names = [], []
    components = specifier.split('.')

    for component in components:
        if is_module_name(component):
            module_names.append(component)
        elif is_class_name(component):
            class_names.append(component)
        else:
            raise ValueError(f'Invalid class specifier component: {component}')

    if not module_names:
        raise ValueError(f'Class specifier missing module: {specifier}')
    if not class_names:
        raise ValueError(f'Class specifier missing class: {specifier}')

    module_path = '.'.join(module_names)
    first_class = class_names[0]

    importlib.invalidate_caches()
    module = importlib.import_module(module_path)

    cls = module
    for class_name in class_names:
        try:
            cls = getattr(cls, class_name)
        except AttributeError:
            raise ValueError(f'Class not found: {class_name}')

    return cls


def multi_parse(templates, string):
    """
    Multi parse attempts to parse the string with each of the templates
    until successful and returns the parse result.

    I/O:
    templates:  sequence of templates
    string:     string to be parsed
    return:     first successful parse result
    """
    for template in templates:
        parsed = parse(template, string)
        if parsed:
            return parsed

    raise ValueError(
        f"'{string}' does not match any template: {templates}")


def logical_xor(a, b):
    """Logical xor of a and b, returning bool"""
    return bool(a) ^ bool(b)


def xor_constrain(a, b):
    """
    Xor Constrain

    Return truthy value between a and b, raising ValueError if either
    both are truthy or both are falsy.
    """
    if a and not b:
        return a
    if b and not a:
        return b
    if a and b:
        raise ValueError('xor error: both values cannot be True')
    raise ValueError('xor error: both values cannot be False')
