#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import csv
import os
import random
from collections import OrderedDict
from dataclasses import dataclass, field
from functools import lru_cache
from itertools import chain

from contextualize.content.base import Extractable
from contextualize.utils.cache import AsyncCache
from contextualize.utils.enum import FlexEnum
from contextualize.utils.tools import PP

BASE_DIRECTORY = '/'.join(__name__.split('.')[:-1])

Browser = FlexEnum('Browser', 'CHROME FIREFOX')


@dataclass
class SecretAgent(Extractable):
    """SecretAgent, a user agent class"""
    PROVIDER_DIRECTORY = f'{BASE_DIRECTORY}/providers'

    user_agent: str = field(default=None, repr=True)
    browser: str = field(default=None, repr=False)
    browser_version: str = field(default=None, repr=False)
    operating_system: str = field(default=None, repr=False)
    hardware_type: str = field(default=None, repr=False)
    popularity: str = field(default=None, repr=False)

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


class SecretService:
    """Secret Service, a service class for managing Secret Agents"""
    DEFAULT_BROWSER = Browser.CHROME
    AGENT_FILE_DIRECTORY = f'{BASE_DIRECTORY}'
    FILE_IDENTIFIER = 'agents'
    FILE_TYPE = 'csv'
    CSV_FORMAT = dict(delimiter='|', quotechar='"')

    @property
    def random(self):
        """Random user agent string for the current browser"""
        return self.random_agent.user_agent

    @property
    def random_agent(self):
        """Random secret agent instance for the current browser"""
        user_agents = self.data.get(self.browser)
        agent_data = random.choice(user_agents)
        agent_kwargs = dict(zip(self.headers, agent_data))
        try:
            return SecretAgent(**agent_kwargs)
        except Exception as e:
            PP.pprint(dict(
                msg='Unable to generate random agent; using default',
                type='unable_to_generate_random_agent', error=e,
                file_path=self.file_path, browser=self.browser, secret_service=repr(self)))
            return SecretAgent.default()

    def acquire_data(self):
        """Acquire user agent data by extracting and saving it"""
        self.extract_data()
        self.save_data()

    def extract_data(self):
        """Extract user agent data"""
        from contextualize.extraction.extractor import MultiExtractor

        loop = asyncio.get_event_loop()
        search_terms = OrderedDict(browser=self.browser.name.lower())
        extractors = MultiExtractor.provision_extractors(
            SecretAgent, search_terms, use_cache=False, loop=loop)

        futures = {extractor.extract() for extractor in extractors}
        done, pending = loop.run_until_complete(asyncio.wait(futures))
        agent_dicts = chain(*(task.result().values() for task in done))
        self.data[self.browser] = [list(agent_dict.field_values()) for agent_dict in agent_dicts]

        cache = AsyncCache()
        cache.terminate(loop)
        loop.close()

    def save_data(self, file_path=None):
        """Save data to the given file path and clear the cache"""
        file_path = file_path or self.file_path
        data = self.data.get(self.browser)
        if not self.headers:
            raise ValueError('Headers are missing')
        if not data:
            raise ValueError('No data to save')
        with open(file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, **self.CSV_FORMAT)
            csv_writer.writerow(self.headers)
            csv_writer.writerows(data)

        self.read_data_file.cache_clear()

    def load_data(self, file_path=None):
        """Load data from the given file and store it on the service"""
        file_path = file_path or self.file_path
        try:
            headers, data = self.read_data_file(self.file_path)
            self.headers = headers
            self.data[self.browser] = data
        except FileNotFoundError as e:
            agent = SecretAgent.default()
            self.headers = list(agent.field_names())
            self.data[self.browser] = [list(agent.field_values())]
            self.read_data_file.cache_clear()
            PP.pprint(dict(
                msg='User agent data file missing, so using default; try acquire_data()',
                type='user_agent_data_file_missing', error=e,
                file_path=self.file_path, browser=self.browser,
                secret_service=repr(self)))

    @classmethod
    @lru_cache(maxsize=None)
    def read_data_file(cls, file_path=None):
        """Get saved data from the given file and cache it"""
        file_path = file_path or cls._form_file_path(cls.DEFAULT_BROWSER)
        with open(file_path, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file, **cls.CSV_FORMAT)
            agent_fields = {field.name: field for field in SecretAgent.fields()}
            headers = [agent_fields[header].name for header in next(csv_reader)]
            column_types = [(i, agent_fields[header].type) for i, header in enumerate(headers)
                            if agent_fields[header].type != str]
            rows = []
            for row in csv_reader:
                for column, field_type in column_types:
                    row[column] = field_type(row[column])
                rows.append(row)
            return headers, rows

    @classmethod
    def _form_file_path(cls, browser):
        """Form file path for given browser"""
        browser_name = browser.name.lower()
        file_name = f'{browser_name}_{cls.FILE_IDENTIFIER}.{cls.FILE_TYPE}'
        return os.path.join(cls.AGENT_FILE_DIRECTORY, file_name)

    def __init__(self, browser=None, file_path=None):
        self.browser = Browser.cast(browser) if browser else self.DEFAULT_BROWSER
        self.file_path = file_path or self._form_file_path(self.browser)
        self.headers = None
        self.data = {}
        self.load_data(self.file_path)
