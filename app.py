import reactivex
from reactivex.scheduler import ThreadPoolScheduler
from reactivex import create, of, operators as ops

import scrapy
from scrapy.crawler import CrawlerProcess

from HyveeSpider import HyveeSpider
from IowaFoodHubSpider import IowaFoodHubSpider
from RussMarket import RussMarketSpider
from FreshThymeSpider import FreshThymeSpider
from GatewaySpider import GatewaySpider

from datetime import datetime
import pandas as pd

from DSPG_Products import Products
from crochet import setup

import logging
import sys
import multiprocessing
import os

# setup()

def scrapeHyvee():
    def _scrapeHyvee(source):
        def subscribe(observer, scheduler = None):
            # optimal_thread_count = multiprocessing.cpu_count()
            # pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
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
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeHyvee


def scrapeIowaFoodHub():
    def _scrapeIowaFoodHub(source):
        def subscribe(observer, scheduler = None):
            # optimal_thread_count = multiprocessing.cpu_count()
            # pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    if value['store'] == "IFPA":
                        iowaFoodHubSpider = IowaFoodHubSpider
                        process = CrawlerProcess()
                        process.crawl(iowaFoodHubSpider)
                        process.start()
                        process.stop()
                        if 'data' in value:
                            if iowaFoodHubSpider.DataFrame.size > 0:
                                value['data'] = pd.concat([value['data'], iowaFoodHubSpider.DataFrame], ignore_index=True)

                    observer.on_next(value)
                except:
                    logging.error(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeIowaFoodHub

def scrapeRussMarket():
    def _scrapeRussMarket(source):
        def subscribe(observer, scheduler = None):
            # optimal_thread_count = multiprocessing.cpu_count()
            # pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    if value['store'] == "RUSS":
                        spider = RussMarketSpider()
                        #Running the spider
                        spider.start_requests()
                        if spider.dataFrames.size > 0:
                            if 'data' in value:
                                value['data'] = pd.concat([value['data'], spider.dataFrames], ignore_index=True)
                            else:
                                 value['data'] = spider.dataFrames
                    observer.on_next(value)
                except:
                    logging.error(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeRussMarket

def scrapeFreshThyme():
    def _scrapeFreshThyme(source):
        def subscribe(observer, scheduler = None):
            # optimal_thread_count = multiprocessing.cpu_count()
            # pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    if value['store'] == "FreshThyme":
                        setProducts = Products()
                        # Products = setProducts.ProductList
                        # DataFrame = setProducts.ProductDataFrames

                        freshThymeSpider = FreshThymeSpider
                        process = CrawlerProcess()
                        process.crawl(freshThymeSpider)
                        process.start(stop_after_crawl=False)
                        process.stop()
                        if freshThymeSpider.DataFrame.size > 0:
                            if 'data' in value:
                                value['data'] = pd.concat([value['data'], freshThymeSpider.DataFrame], ignore_index=True)
                            else:
                                 value['data'] = freshThymeSpider.DataFrame
                    observer.on_next(value)
                except:
                    logging.error(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeFreshThyme

def scrapeGateway():
    def _scrapeGateway(source):
        def subscribe(observer, scheduler = None):
            # optimal_thread_count = multiprocessing.cpu_count()
            # pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    if value['store'] == "Gateway":
                        spider = GatewaySpider()
                        spider.start_requests(stop_after_crawl=False)

                        if spider.dataFrames.size > 0:
                            if 'data' in value:
                                value['data'] = pd.concat([value['data'], spider.dataFrames], ignore_index=True)
                            else:
                                value['data'] = spider.dataFrames
                    observer.on_next(value)
                except:
                    logging.error(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeGateway


def uploadData():
    def _uploadData(source):
        def subscribe(observer, scheduler = None):
            # optimal_thread_count = multiprocessing.cpu_count()
            # pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    currentDate = str(datetime(datetime.today().year, datetime.today().month, datetime.today().day))[:-9]
                    filePath = "data/{0}.csv".format(currentDate)
                    if 'data' in value:
                        if os.path.isfile(filePath):
                            df = pd.read_csv(filePath)
                            if len(value['data']) > 0:
                                value['data'] = pd.concat([value['data'], df], ignore_index=True)
                            else:
                                value['data'] = df

                        vendorList = pd.read_csv("vendorList.csv")
                        if not 'Local' in value['data']:
                                value['data']['Local'] = False
                        if not 'Vendor' in value['data']:
                                value['data']['Vendor'] = ""

                        products = value['data']['Product'].apply(lambda x: x.lower() if isinstance(x, str) else x)
                        for row in products.keys():
                            df = vendorList.filter(like=products[row], axis = 0)
                            if df.columns.size > 0:
                                value['data'].loc[row, 'Local'] = True
                                value['data'].loc[row, 'Vendor'] = df.columns[0]
                        
                        value['data'].to_csv(os.path.join("data", "{0}.csv".format(currentDate)), index=False)
                    observer.on_next(value)
                except:
                    logging.error(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _uploadData

source = of({'store': "Hyvee", 'data': []}, {'store': "IFPA"}, {'store': "RUSS"}, {'store': "FreshThyme"}, {'store': "Gateway"})
source.pipe(
    scrapeHyvee(),
    scrapeIowaFoodHub(),
    scrapeRussMarket(),
    scrapeFreshThyme(),
    # scrapeGateway(),
    uploadData()
).subscribe(on_next = lambda e: print("on Next: {0}".format(e)),
    on_error = lambda e: logging.error("Error Occurred: {0}".format(sys.exc_info())),
    on_completed = lambda: logging.info("Done!"))