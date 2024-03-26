import reactivex
from reactivex.scheduler import ThreadPoolScheduler
from reactivex import create, of, operators as ops


from HyveeSpider import HyveeSpider
from RussMarket import RussMarketSpider
from GatewaySpider import GatewaySpider

from datetime import datetime
import pandas as pd

import logging
import sys
import requests
import os
import traceback

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
                    print(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeHyvee

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
                    print(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _scrapeRussMarket

def scrapeGateway():
    def _scrapeGateway(source):
        def subscribe(observer, scheduler = None):
            # optimal_thread_count = multiprocessing.cpu_count()
            # pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
            def on_next(value):
                try:
                    if value['store'] == "Gateway":
                        spider = GatewaySpider()
                        spider.start_requests()

                        if spider.dataFrames.size > 0:
                            if 'data' in value:
                                value['data'] = pd.concat([value['data'], spider.dataFrames], ignore_index=True)
                            else:
                                value['data'] = spider.dataFrames
                    observer.on_next(value)
                except:
                    print(sys.exc_info())
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
                    if 'data' in value and isinstance(value['data'], pd.DataFrame):
                        vendorList = pd.read_csv("vendorList.csv", header=None, index_col=0)
                        if not 'Local' in value['data']:
                                value['data']['Local'] = False
                        if not 'Vendor' in value['data']:
                                value['data']['Vendor'] = value['data']['Brand']
                        if not 'VendorAddress' in value['data']:
                                value['data']['VendorAddress'] = ""

                        products = value['data']['Product'].apply(lambda x: x.lower() if isinstance(x, str) else x)
                        vendors = value['data']['Vendor'].apply(lambda x: x.lower() if isinstance(x, str) else x)
                        for row in products.keys():
                            # value['data'][0] = getVendorData(value['data'][0], vendorList=vendorList)
                            product = products[row] if not vendors[row] else vendors[row]
                            (localToIowa, vendorAddress) = getVendorData(product, vendorList=vendorList)
                            value['data'].loc[row, 'Local'] = localToIowa
                            value['data'].loc[row, 'VendorAddress'] = vendorAddress
                        
                        filePath = "data/{0}.csv".format(currentDate)
                        if os.path.isfile(filePath):
                            df = pd.read_csv(filePath)
                            if len(value['data']) > 0:
                                value['data'] = pd.concat([value['data'], df], ignore_index=True)
                                
                        value['data'].to_csv(os.path.join(filePath), index=False)
                    observer.on_next(value)
                except:
                    print(sys.exc_info())
                    observer.on_error(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed
                # scheduler=pool_scheduler
                )
        return reactivex.create(subscribe)
    return _uploadData

def getVendorData(item, vendorList):
    localToIowa = False
    vendor = ''
    vendorAddress = ''
    searchSuccess = False
    
    df = vendorList.filter(like=item, axis = 0)
    for row in vendorList[1].keys():
        if row in item:
            localToIowa = True
            searchSuccess = True
            break
    if not searchSuccess:
        url = 'https://services-here.aws.mapquest.com/v1/search?query={0}&count=5&prefetch=5&client=yogi&clip=none'.format(item)
        headers = {'Host': 'services-here.aws.mapquest.com', 'User-Agent': 'PostmanRuntime/7.37.0'}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            results = data['results']
            if results:
                address = results[0]['address']
                region, locality, address1, postalCode = ('', '', '', '')
                if 'region' in address:
                    region = address['region']
                    if region == 'IA':
                        localToIowa = True
                if 'locality' in address:
                    locality = address['locality']
                if 'address1' in address:
                    address1 = address['address1']
                if 'postalCode' in address:
                    postalCode = address['postalCode']
                vendorAddress = "{0} {1} {2} {3}".format(address1, locality, region, postalCode)
    return (localToIowa, vendorAddress)

source = of({'store': "Hyvee", 'data': []}, {'store': "RUSS"}, {'store': "Gateway"})
source.pipe(
    scrapeHyvee(),
    scrapeRussMarket(),
    # scrapeGateway(),
    uploadData()
).subscribe(on_next = lambda e: print("on Next: {0}".format(e)),
    on_error = lambda e: logging.error("Error Occurred: {0}".format(traceback.print_exc(file=sys.stdout))),
    on_completed = lambda: logging.info("Done!"))