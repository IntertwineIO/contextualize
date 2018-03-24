#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import os

from ruamel.yaml.scanner import ScannerError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from utils.async import run_in_executor

EXTRACTOR_PATH_BASE = 'extractors'
PARSER_FILE_NAME = 'parser.yaml'


class Service:

    def contextualize(self):

        parsers = self.provision_parsers()

        url_a = 'http://www.jurn.org/#gsc.tab=0&gsc.q=Austin%2C%20TX%20intitle%3Ahomelessness&gsc.sort='
        url_b = 'http://www.jurn.org/#gsc.tab=0&gsc.q=intitle%3Ahomelessness&gsc.sort='

        futures = {
            self.fetch_results(url_a, 'a'),
            self.fetch_results(url_b, 'b'),
        }

        done, pending = self.loop.run_until_complete(asyncio.wait(futures))

        print([f'{task.result()}\n' for task in done])

        # TODO: split fetch_page from parse_content

    def provision_parsers(self):

        parser_paths = [os.path.join(EXTRACTOR_PATH_BASE, d, PARSER_FILE_NAME)
                        for d in os.listdir(EXTRACTOR_PATH_BASE)]

        parsers = {}
        for path in parser_paths:
            try:
                with open(path) as stream:
                    data = yaml.load(stream)
                    # parsers.add(Parser(**data))
                    parsers.add(data)

            except FileNotFoundError as e:
                print(e)  # TODO: Replace with logging
            except ScannerError as e:
                print(e)  # TODO: Replace with logging

        return parsers

    async def fetch_results(self, url, tag):
        start_time = self.loop.time()
        print(f'{tag}: start time: {start_time}')

        chrome_options = Options()
        chrome_options.add_argument('--disable-extensions')

        future_driver = run_in_executor(self.loop, None, webdriver.Chrome,
                                        chrome_options=chrome_options)
        driver = await future_driver
        print(f'{tag}: ready driver {tag}')

        future_page = self.loop.run_in_executor(None, driver.get, url)
        await future_page

        print(f'{tag}: the future is now')
        elements = driver.find_elements_by_xpath('//td[2]/div[1]/a[@class="gs-title"]')

        rv = [(element.text, element.get_attribute('href')) for element in elements]

        end_time = self.loop.time()
        print(f'{tag}: end time: {end_time} elapsed: {end_time-start_time}')

        return rv

    def __init__(self, loop=None, problem_name=None, org_name=None, geo_name=None):
        self.loop = loop or asyncio.get_event_loop()
        self.problem_name = problem_name
        self.org_name = org_name
        self.geo_name = geo_name
