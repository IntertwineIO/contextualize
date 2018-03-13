#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import datetime

import uvloop
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from utils.async import execute_future


async def fetch_page(loop, url, tag):
    start_time = loop.time()
    print(f'{tag}: start time: {start_time}')

    chrome_options = Options()
    chrome_options.add_argument('--disable-extensions')

    future_driver = execute_future(loop, None, webdriver.Chrome,
                                   chrome_options=chrome_options)
    driver = await future_driver
    print(f'{tag}: ready driver {tag}')

    future_page = loop.run_in_executor(None, driver.get, url)
    await future_page

    print(f'{tag}: the future is now')
    elements = driver.find_elements_by_xpath('//td[2]/div[1]/a[@class="gs-title"]')

    rv = [(element.text, element.get_attribute('href')) for element in elements]

    end_time = loop.time()
    print(f'{tag}: end time: {end_time} elapsed: {end_time-start_time}')

    return rv


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    url_a = 'http://www.jurn.org/#gsc.tab=0&gsc.q=Austin%2C%20TX%20intitle%3Ahomelessness&gsc.sort='
    url_b = 'http://www.jurn.org/#gsc.tab=0&gsc.q=intitle%3Ahomelessness&gsc.sort='

    futures = {
        fetch_page(loop, url_a, 'a'),
        fetch_page(loop, url_b, 'b'),
    }

    done, pending = loop.run_until_complete(asyncio.wait(futures))

    print([f'{task.result()}\n' for task in done])

    loop.close()


if __name__ == '__main__':
    main()
