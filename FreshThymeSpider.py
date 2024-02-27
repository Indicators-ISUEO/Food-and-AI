from datetime import datetime
import pandas as pd
import os
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from DSPG_Products import Products #Imports the products to be processed
from DSPG_Cleaner import DataCleaner # This is to handle the cleaning of data
from DSPG_SpiderErrors import DataFormatingError
import pandas as pd

class FreshThymeSpider(scrapy.Spider):
    name = 'Fresh Thyme Market Spider'
    Products = []
    DataFrame = []
    def __init__(self):
        setProducts = Products()
        self.Products = setProducts.ProductList
        self.DataFrame = setProducts.ProductDataFrames

    def start_requests( self ):
        #Bacon Scraper part
        bacon_urls = ['https://ww2.freshthyme.com/sm/planning/rsid/951/results?q=Bacon&take=48&f=Category%3AHot+Dogs%2C+Bacon+%26+Sausage'
                    #   'https://ww2.freshthyme.com/sm/planning/rsid/952/results?q=Bacon&take=48&f=Category%3AHot+Dogs%2C+Bacon+%26+Sausage'
                      ]
        for url in bacon_urls:
            yield scrapy.Request( url = url, callback = self.cardsParse, meta={'index': 0, 'url': url})

        # #Egg Scraper part
        # egg_urls = ['https://ww2.freshthyme.com/sm/planning/rsid/951/results?q=Eggs&take=48&f=Category%3AEggs',
        #             'https://ww2.freshthyme.com/sm/planning/rsid/952/results?q=Eggs&take=48&f=Category%3AEggs']
        # for url in egg_urls:
        #     yield scrapy.Request( url = url, callback = self.cardsParse, meta={'index': 1, 'url': url})

        # #Heirloom Tomatoes part
        # heirloomTomato_urls = ['https://ww2.freshthyme.com/sm/planning/rsid/951/results?q=heirloom%20Tomatoes',
        #                        'https://ww2.freshthyme.com/sm/planning/rsid/952/results?q=heirloom%20Tomatoes']
        # for url in heirloomTomato_urls:
        #     yield scrapy.Request( url = url, callback = self.cardsParse, meta={'index': 2, 'url': url})

        # # All Tomatoes
        # allTomatoesUrl =['https://ww2.freshthyme.com/sm/planning/rsid/951/results?q=Tomatoes&take=48&f=Category%3AFresh+Vegetables',
        #                  'https://ww2.freshthyme.com/sm/planning/rsid/952/results?q=Tomatoes&take=48&f=Category%3AFresh+Vegetables']
        # for url in allTomatoesUrl:
        #     yield scrapy.Request( url = url, callback = self.cardsParse, meta={'index': 3, 'url': url})

    def cardsParse(self, response):
        #Failsafe for links
        try:
            #grabs the store location
            storeXpath = '//*[contains(@class,"HeaderSubtitle")]/text()'
            store = response.xpath(storeXpath).extract_first()
            #grabs all cards from list and saves the link to follow
            xpath = '//*[contains(@class,"Listing")]/div/a/@href'
            listCards = response.xpath(xpath)
            for url in listCards:
                yield response.follow( url = url, callback = self.itemParse, meta={'store': store, 'index': response.meta.get('index'), 'url': response.meta.get('url')} )
        except AttributeError:
           pass
    
    def itemParse(self, response):
        #xpaths to extract 
        nameXpath = '//*[contains(@class, "PdpInfoTitle")]/text()'
        priceXpath = '//*[contains(@class, "PdpMainPrice")]/text()'
        prevPriceXpath = '//*[contains(@class, "PdpPreviousPrice")]/text()'
        name = response.xpath(nameXpath).extract_first()
        price = response.xpath(priceXpath).extract_first()
        sale = response.xpath(prevPriceXpath).extract_first()
        url = response.meta.get('url')
        clean = DataCleaner()
        #Adding the data to data frame
        indexFrame = response.meta.get('index')
        if(indexFrame == 0):
            #We only want bacon
            if 'bacon' not in name.lower().replace(' ', ''):
                return
            clean.LoadDataSet(0, url)
            clean.Data['Product'] = name
            clean.Data['Current Price'] = price
            clean.Data['Orignal Price'] = sale
            clean.baconModifications()
        elif(indexFrame == 1):
            string = name.lower().replace(' ', '')
            string = string.split('-')
            checkString = string[len(string)-1]
            #We only want eggs
            if 'each' not in checkString or 'cooked' in name.lower().replace(' ', '') or 'boiled' in name.lower().replace(' ', ''):
                return
            clean.LoadDataSet(1, url)
            if '1each' in checkString or '12each' in checkString:
                clean.Data['True Amount'] = f"{1} dz"
                clean.Data['Amount in dz'] = 1.0
            elif '1.5each' in checkString:
                clean.Data['True Amount'] = f"{1.5} dz"
                clean.Data['Amount in dz'] = 1.5
            clean.Data['Product'] = name
            clean.Data['Current Price'] = price
            clean.Data['Orignal Price'] = sale
        elif(indexFrame == 2 or indexFrame == 3):
            clean.LoadDataSet(indexFrame, url)
            clean.Data['Product'] = name
            clean.Data['Current Price'] = price
            clean.Data['Orignal Price'] = sale
            clean.tomatoesModifications(None)
        #Add more products here
        else:
            raise DataFormatingError(indexFrame)
        clean = self.setLocationalData(clean, response.meta.get('store'))
        clean.cleanPricing()
        # if(indexFrame < len(self.DataFrame)):
        self.DataFrame = pd.concat([self.DataFrame, pd.DataFrame(clean.Data, index=[0])], ignore_index=True)
            # DataFrame[indexFrame].loc[len(DataFrame[indexFrame])] = list(clean.Data.values())
        # else:
        #     raise DataFormatingError(indexFrame)
        # yield self.DataFrame
        return None
        
    def setLocationalData(self, clean, storeLocation):
        store = storeLocation.lower().replace(' ', '')
        if 'westdesmoines' in store:
            clean.Data['Address'] = '2900 University Ave. Suite 240'
            clean.Data['State'] = 'IA'
            clean.Data['City'] = 'West Des Moines'
            clean.Data['Zip Code'] = '50266'    
        elif 'davenport' in store:
            clean.Data['Address'] = '2130 E. Kimberly Rd.'
            clean.Data['State'] = 'IA'
            clean.Data['City'] = 'Davenport'
            clean.Data['Zip Code'] = '52807'    
        return clean
