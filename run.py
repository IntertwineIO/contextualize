#!/usr/bin/env python
# -*- coding: utf-8 -*-
from api import app


def main():
    app.run(host='127.0.0.1', port=5001)


if __name__ == '__main__':
    main()
