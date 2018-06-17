#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import csv
import os
import random
from collections import OrderedDict
from functools import lru_cache
from itertools import chain

from utils.mixins import Extractable
from utils.structures import FlexEnum
from utils.tools import PP

Browser = FlexEnum('Browser', 'CHROME FIREFOX')


class SecretAgent(Extractable):
    """SecretAgent, a user agent class"""
    BASE_DIRECTORY = 'secret_service/extractors'
    # UNIQUE_FIELD = 'user_agent'

    @classmethod
    def default(cls):
        return cls(source_url=('https://developers.whatismybrowser.com/'
                               'useragents/parse/627832-chrome-windows-blink'),
                   user_agent=('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                               'AppleWebKit/537.36 (KHTML, like Gecko) '
                               'Chrome/60.0.3112.113 Safari/537.36'),
                   browser='Chrome',
                   browser_version='60.0.3112.113',
                   operating_system='Windows',
                   hardware_type='Computer',
                   popularity='Very common')

    def __str__(self):
        return self.user_agent

    def __init__(self, source_url=None, user_agent=None, browser=None, browser_version=None,
                 operating_system=None, hardware_type=None, popularity=None,
                 *args, **kwds):
        super().__init__(source_url=source_url, *args, **kwds)
        self.user_agent = user_agent
        self.browser = browser
        self.browser_version = browser_version
        self.operating_system = operating_system
        self.hardware_type = hardware_type
        self.popularity = popularity


class SecretService:
    """Secret Service, a service class for managing Secret Agents"""
    DEFAULT_BROWSER = Browser.CHROME
    BASE_DIRECTORY = 'secret_service'
    FILE_IDENTIFIER = 'agents'
    FILE_TYPE = 'csv'
    CSV_FORMAT = dict(delimiter='|', quotechar='"')

    _data = {}

    @property
    def random(self):
        """Random user agent string for the current browser"""
        return self.random_agent.user_agent

    @property
    def random_agent(self):
        """Random secret agent instance for the current browser"""
        user_agents = self._data.get(self.browser)

        try:
            selected_agent = SecretAgent(*random.choice(user_agents))
        except Exception as e:
            PP.pprint(dict(
                msg='Unable to generate random agent; using default',
                type='unable_to_generate_random_agent', error=e,
                file_path=self.file_path, browser=self.browser, secret_service=repr(self)))
            return SecretAgent.default()

        return selected_agent

    def acquire_data(self):
        """Acquire user agent data by extracting and saving it"""
        self.extract_data()
        self.save_data()

    def extract_data(self):
        """Extract user agent data"""
        from extractor import MultiExtractor

        loop = asyncio.get_event_loop()
        search_terms = OrderedDict(browser=self.browser.name.lower())
        extractors = MultiExtractor.provision_extractors(SecretAgent, search_terms)
        futures = {extractor.extract() for extractor in extractors}
        done, pending = loop.run_until_complete(asyncio.wait(futures))
        agent_dicts = chain(*(task.result().values() for task in done))
        self._data[self.browser] = [list(d.values()) for d in agent_dicts]

        loop.close()

    def save_data(self, file_path=None):
        """Save data to the given file path and clear the cache"""
        file_path = file_path or self.file_path
        data = self._data.get(self.browser)
        if not data:
            raise ValueError('No data to save')
        with open(file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, **self.CSV_FORMAT)
            csv_writer.writerows(data)

        self.get_saved_data.cache_clear()

    def load_data(self, file_path=None):
        """Load data from the given file and store it on the service"""
        file_path = file_path or self.file_path
        try:
            self._data[self.browser] = self.get_saved_data(self.file_path)
        except FileNotFoundError as e:
            PP.pprint(dict(
                msg='User agent data file missing; use acquire_data()',
                type='user_agent_data_file_missing', error=e,
                file_path=self.file_path, browser=self.browser,
                secret_service=repr(self)))

    @classmethod
    @lru_cache(maxsize=None)
    def get_saved_data(cls, file_path=None):
        """Get saved data from the given file and cache it"""
        file_path = file_path or cls._form_file_path(cls.DEFAULT_BROWSER)
        with open(file_path, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file, **cls.CSV_FORMAT)
            return list(csv_reader)

    @classmethod
    def _form_file_path(cls, browser):
        """Form file path for given browser"""
        browser_name = browser.name.lower()
        file_name = f'{browser_name}_{cls.FILE_IDENTIFIER}.{cls.FILE_TYPE}'
        return os.path.join(cls.BASE_DIRECTORY, file_name)

    def __init__(self, browser=None, file_path=None):
        self.browser = Browser.cast(browser) if browser else self.DEFAULT_BROWSER
        self.file_path = file_path or self._form_file_path(self.browser)
        if self.browser not in self._data:
            self.load_data()
