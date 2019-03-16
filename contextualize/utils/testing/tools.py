#!/usr/bin/env python
# -*- coding: utf-8 -*-
from contextlib import contextmanager


def read_file(path):
    """Read file at given path"""
    with open(path) as file:
        return file.read()


def write_file(path, content):
    """Write file at given path with given content"""
    with open(path, 'w') as file:
        file.write(content)


@contextmanager
def reset_files(*paths):
    """Context manager that resets contents of specified file paths"""
    initial_content = {path: None for path in paths}
    for path in initial_content.keys():
        initial_content[path] = read_file(path)

    try:
        yield initial_content.items()

    finally:
        for path, content in initial_content.items():
            write_file(path, content)
