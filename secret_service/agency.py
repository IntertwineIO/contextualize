#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import os
import random
from collections import OrderedDict
from functools import lru_cache

from utils.mixins import FieldMixin
from utils.structures import FlexEnum


Browser = FlexEnum('Browser', 'CHROME FIREFOX')


class SecretAgent(FieldMixin):
    """SecretAgent, a user agent class"""

    def __str__(self):
        return self.user_agent

    def __init__(self, user_agent=None, browser=None, browser_version=None,
                 operating_system=None, hardware_type=None, popularity=None,
                 *args, **kwds):
        super().__init__(*args, **kwds)
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
    _DATA = {}

    @property
    def random(self):
        """Random user agent string for the current browser"""
        return self.random_agent.user_agent

    @property
    def random_agent(self):
        """Random secret agent instance for the current browser"""
        if self.browser not in self._DATA:
            try:
                self._DATA[self.browser] = self.load_data(self.file_path)
            except FileNotFoundError as e:
                raise FileNotFoundError(
                    f'{e}; use {self.__class__.__name__}.extract() to create')

        user_agents = self._DATA.get(self.browser)
        if not user_agents:
            raise ValueError('No user agents found')

        selected_agent = SecretAgent(*random.choice(user_agents))
        return selected_agent

    @classmethod
    @lru_cache(maxsize=None)
    def load_data(cls, file_path=None):
        """Load data from the given file path and cache it"""
        file_path = file_path or self._form_file_path(cls.DEFAULT_BROWSER)
        with open(file_path, newline='') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='|', quotechar='"')
            return list(csv_reader)

    @classmethod
    async def extract_data(cls, browser):
        """Extract user agent data and save to file"""
        # TODO
        self.load_data.cache_clear()

    @classmethod
    def _form_file_path(cls, browser):
        browser_name = browser.name.lower()
        file_name = f'{browser_name}_{cls.FILE_IDENTIFIER}.{cls.FILE_TYPE}'
        return os.path.join(cls.BASE_DIRECTORY, file_name)

    def __init__(self, browser=None, file_path=None):
        self.browser = Browser.cast(browser) if browser else self.DEFAULT_BROWSER
        self.file_path = file_path or self._form_file_path(self.browser)
