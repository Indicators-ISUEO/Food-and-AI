from datetime import datetime
import pandas as pd
import os
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from DSPG_Products import Products #Imports the products to be processed
from DSPG_Cleaner import DataCleaner # This is to handle the cleaning of data
from DSPG_SpiderErrors import ProductFinderError


class IowaFoodHubSpider(scrapy.Spider):
    name = 'Iowa Food Hub'
    currentDate = str(datetime(datetime.today().year, datetime.today().month, datetime.today().day))[:-8]
    product = Products()
    DataFrame = product.ProductDataFrames

    def start_requests( self ):

        iowaFoodHubBaconUrl = 'https://iowa-food-hub.myshopify.com/search?q=bacon'
        yield scrapy.Request( url = iowaFoodHubBaconUrl, callback = self.iowaFoodHubSearch, meta={'type': 'Bacon'})

        iowaFoodHubEggsUrl = 'https://iowa-food-hub.myshopify.com/search?q=Egg'
        yield scrapy.Request( url = iowaFoodHubEggsUrl, callback = self.iowaFoodHubSearch, meta={'type': 'Eggs'})

    def iowaFoodHubSearch(self, response):
        #Failsafe for links
        try:
            #grabs all cards from list and saves the link to follow
            xpath = '//*[@id="MainContent"]//a[contains(@class,"list-view-item")]/@href'
            linkList = response.xpath(xpath)
            productType = response.meta.get('type')
            itemIndex = self.dataFrameItemIndex(productType)
            if productType == 'Bacon':
                for url in linkList:
                    yield response.follow( url = url, callback = self.iowaFoodHubBacon, meta={'DataFrameIndex': itemIndex}, dont_filter=True )
            elif productType == 'Eggs':
                for url in linkList:
                    yield response.follow( url = url, callback = self.iowaFoodHubEggs, meta={'DataFrameIndex': itemIndex}, dont_filter=True )
            return self.DataFrame
        except AttributeError:
           pass

    def iowaFoodHubBacon(self, response):
        #validating the name. 
        #We want to validate the name first before we load the cleaner for speed
        nameXpath = '//*[@id="ProductSection-product-template"]//*[contains(@class, "product-single__title")]/text()'
        name = response.xpath(nameXpath).extract_first()       
        if "bacon" not in name.lower():
            return
        
        #load cleaner template
        clean = DataCleaner()
        clean.LoadDataSet(0, response.url)
        clean.Data['Product'] = name
        #The other areas we are interested in
        venderXpath = '//*[@id="ProductSection-product-template"]//*[contains(@class, "product-single__vendor")]/text()'
        priceXpath = '//*[@id="ProductPrice-product-template"]/text()'
        clean.Data['Current Price'] = response.xpath(priceXpath).extract_first()
        clean.Data['Brand'] = response.xpath(venderXpath).extract_first()
        #getting the product discription
        discXpath = '//*[@id="ProductSection-product-template"]//*[contains(@class, "product-single__description") and @itemprop="description"]/descendant-or-self::text()'
        description = response.xpath(discXpath).getall()
        # remove leading and trailing whitespace from each string
        description = [text.strip() for text in description]
        # remove empty strings
        description = list(filter(None, description))
        # join the strings into a single string
        descriptionText = " ".join(description)
        unit = clean.findWeightUnit(descriptionText)
        if not unit:
            unit = clean.findWeightUnit(clean.Data['Product'])
        clean.Data['True Weight'] = unit
        clean.Data['Weight in lbs'] = clean.ozToLb(clean.Data['True Weight'])
        clean.cleanPricing()
        clean = self.setLocationalData(clean)
        clean.Data['Source'] = 'Iowa Food Hub'
        #Adding product to data frame   
        indexFrame = response.meta.get('DataFrameIndex')
        self.DataFrame = pd.concat([self.DataFrame, pd.DataFrame(clean.Data, index=[0])], ignore_index=True)
        # self.DataFrame[indexFrame].loc[len(self.DataFrame[indexFrame])] = list(clean.Data.values())
                
    def iowaFoodHubEggs(self, response):
        #validating the name
        nameXpath = '//*[@id="ProductSection-product-template"]//*[contains(@class, "product-single__title") and @itemprop="name"]/text()'       
        name = response.xpath(nameXpath).extract_first()
        if "egg" not in name.lower():
            return
       
        #load cleaner template
        clean = DataCleaner()
        clean.LoadDataSet(1, response.url)
        clean.Data['Product'] = name
        
        #The other areas we are interested in
        venderXpath = '//*[@id="ProductSection-product-template"]//*[contains(@class, "product-single__vendor") and @itemprop="brand"]/text()'
        priceXpath = '//*[@id="ProductPrice-product-template" and @itemprop="price"]/text()'
        clean.Data['Current Price'] = response.xpath(priceXpath).extract_first()
        clean.Data['Brand'] = response.xpath(venderXpath).extract_first()
        
        #getting the product discription
        discXpath = '//*[@id="ProductSection-product-template"]//*[contains(@class, "product-single__description") and @itemprop="description"]/descendant-or-self::text()'
        description = response.xpath(discXpath).getall()
        # remove leading and trailing whitespace from each string
        description = [text.strip() for text in description]
        # remove empty strings
        description = list(filter(None, description))
        # join the strings into a single string
        descriptionText = " ".join(description)
        if not clean.EggFinder(descriptionText):
            clean.EggFinder(clean.Data['Product Type'])
        clean.cleanPricing()
        clean = self.setLocationalData(clean)   
        #Adding product to data frame
        indexFrame = response.meta.get('DataFrameIndex')
        self.DataFrame = pd.concat([self.DataFrame, pd.DataFrame(clean.Data, index=[0])], ignore_index=True)
        # self.DataFrame[indexFrame].loc[len(self.DataFrame[indexFrame])] = list(clean.Data.values())
        test = ""
        
    def dataFrameItemIndex(self, string):
        for item in self.product.ProductList:
            if item[1] == string:
                return item[0]
        raise ProductFinderError(string)
            
    def setLocationalData(self, clean):
        clean.Data['Address'] = '200 Railroad Street'
        clean.Data['State'] = 'IA'
        clean.Data['City'] = 'Decorah'
        clean.Data['Zip Code'] = '52101'    
        return clean 
    