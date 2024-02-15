import reactivex
from reactivex.scheduler import ThreadPoolScheduler
from reactivex import create, of, operators as ops

import scrapy
from scrapy.crawler import CrawlerProcess

from HyveeSpider import HyveeSpider
from IowaFoodHubSpider import IowaFoodHubSpider
from datetime import datetime
import pandas as pd

from DSPG_Products import Products

import logging
import sys
import multiprocessing
import os

def scrapeHyvee():
    def _scrapeHyvee(source):
        def subscribe(observer, scheduler = None):
            optimal_thread_count = multiprocessing.cpu_count()
            pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    if value['store'] == "Hyvee":
                        spider = HyveeSpider()
                        data = spider.start_requests()
                        value['data'] = data
                    observer.on_next(value)
                except:
                    logging.error(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeHyvee


def scrapeIowaFoodHub():
    def _scrapeIowaFoodHub(source):
        def subscribe(observer, scheduler = None):
            optimal_thread_count = multiprocessing.cpu_count()
            pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    if value['store'] == "IFPA":
                        iowaFoodHubSpider = IowaFoodHubSpider
                        process = CrawlerProcess()
                        process.crawl(iowaFoodHubSpider)
                        process.start()
                        process.stop()
                        if value['data'] and iowaFoodHubSpider.DataFrame.size > 0:
                            value['data'] = pd.concat([value['data'], iowaFoodHubSpider.DataFrame], ignore_index=True)

                    observer.on_next(value)
                except:
                    logging.error(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeIowaFoodHub


def uploadData():
    def _uploadData(source):
        def subscribe(observer, scheduler = None):
            optimal_thread_count = multiprocessing.cpu_count()
            pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    currentDate = str(datetime(datetime.today().year, datetime.today().month, datetime.today().day))[:-9]
                    filePath = "data/{0}.csv".format(currentDate)
                    if os.path.isfile(filePath):
                        df = pd.read_csv(filePath)
                        value['data'] = pd.concat([value['data'], df], ignore_index=True)
                    if 'data' in value:
                        value['data'].to_csv(os.path.join("data", "{0}.csv".format(currentDate)), index=False)
                    observer.on_next(value)
                except:
                    logging.error(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _uploadData

source = of({'store': "Hyvee"}, {'store': "IFPA"})
source.pipe(
    scrapeHyvee(),
    scrapeIowaFoodHub(),
    uploadData()
).subscribe(on_next = lambda e: print("on Next: {0}".format(e)),
    on_error = lambda e: logging.error("Error Occurred: {0}".format(sys.exc_info())),
    on_completed = lambda: logging.info("Done!"))