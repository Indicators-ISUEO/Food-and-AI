#Imports 
import time
import sys
#DSPG imports to assist the spider
from DSPG_SeleniumSpider import SeleniumSpider #Important to look at since it provides the framework
from DSPG_Cleaner import DataCleaner # This is to handle the cleaning of data
from DSPG_Products import Products #Imports the products to be processed
from DSPG_SpiderErrors import DataFormatingError #Very Important
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Using Products class. We only need to add the xpaths and urls since thats 
# all that really changes from spider to spider
class ProductsLoader():
    #index iteration, Name, Urls, Xpaths
    Products = []
    DataFrames = []

    def __init__(self):
        setProducts = Products()
        self.Products = setProducts.ProductList
        self.DataFrames = setProducts.ProductDataFrames
        self.urlsAdder()
        self.xpathMaker()

    #Adding Urls to products
    def urlsAdder(self):
        products = ["carrots", "green onions", "potatoes", "organic spinach", "fresh spinach", "lettuce", "tomato", "strawberries",
                    "raspberries", "mushrooms", "egg large", "egg medium", "chicken whole", "beef steak", "beef whole", "pork bacon"]
        # products = ["carrots", "green onions", "potatoes"]
        links = []
        for product in products: 
            response = requests.get("https://api.freshop.com/1/products?app_key=russ_market&limit=24&q={0}&relevance_sort=asc&sort=relevance&store_id=6158".format(product))
            if response.status_code == 200:
                data = response.json()
                if 'items' in data:
                    for item in data['items']:
                        link_url = item['canonical_url']
                        if product in link_url:
                            links.append(link_url)

        
        self.Products[0].append(links)

    #This handles the xpaths by adding to the Products class
    #most websites have simular xpaths for each item. You might need to make differnet xpaths for each item 
    #if that is the case
    #For assigning xpaths mark them if they are optional meaning it could or could not be present on the page 
    #we do this for speed up if you mark it as non optional and its not pressent it will skip the value 
    #and hurt the preformence
    #best practice is to render the optional last so it reduces the chances of skipping 
    #Note spiecal cases do happen but they are extremely rare a good indiaction of finding one 
    #is by using skipHandler method and tracking/watching the logs  
    #IMPORTANT < -!- NOT ALL XPATHS ARE THE SAME FOR EACH PRODUCT -!->
    def xpathMaker(self):
        #Add the xpaths here and mark if they are optional
        #Format [xpath, optional, speical]
        #xpath, Optional
        
        nameXpath = '//*[@id="page-title"]//h1[contains(@class,"fp-page-header fp-page-title")]'
        priceXpath = '//*[@id="page-title"]//*[contains(@class,"fp-item-price")]/span[contains(@class,"fp-item-base-price")]'
        weightXpath = '//*[@id="page-title"]//*[contains(@class,"fp-item-price")]/span[contains(@class,"fp-item-size")]' 
        saleXpath = '//*[@id="page-title"]//*[contains(@class,"fp-item-sale")]/span[contains(@class,"fp-item-sale-date")]/strong' #optional
        
        xpathList = [(nameXpath, False, True),
                     (priceXpath, False),
                     (weightXpath, False),
                     (saleXpath, True)]  
                           
        self.Products[0].append(xpathList)
        self.Products[1].append(xpathList)
        self.Products[2].append(xpathList)
        self.Products[3].append(xpathList)
        
        

#We handle most of the cleaning is done in the DataCleaner class in DSPG_Cleaner
#There are suttle differences on what is inputed from the different spiders
class DataFormater():
    def __init__(self):
        self.Clean = DataCleaner()

    def cleanUp(self, input, inputIndex, url):
        #Loading a clean dictionary
        self.Clean.LoadDataSet(inputIndex, url)
        #Sales
        self.Clean.Data['Source'] = 'Russ Market'
        if input[3] != None:
            self.Clean.Data['Current Price'] = input[3]
            self.Clean.Data['Orignal Price'] = input[1]
        else:
            self.Clean.Data['Current Price'] = input[1]
        #Common inputs
        self.Clean.Data['Product'] = input[0]
        if(inputIndex == 0): #Bacon
            self.Clean.Data['True Weight'] = input[2]
            self.Clean.baconModifications()
        elif(inputIndex == 1): #Eggs
            self.Clean.Data['True Amount'] = input[2]
            self.Clean.eggModifications()
        elif(inputIndex == 2 or inputIndex == 3): #Tomatoes
            if input[2] == 'lb':
                string = self.Clean.Data['Current Price'] + "/lb"
                self.Clean.tomatoesModifications(string)
            else:
                self.Clean.tomatoesModifications(input[2])
        #Add products here
        else:
            raise DataFormatingError(inputIndex)
        self.setLocationalData()
        self.Clean.cleanPricing()
        # return list(self.Clean.Data.values())
        return self.Clean.Data

    def outOfStock(self, input, inputIndex, url):
        self.Clean.LoadDataSet(inputIndex, url)
        keys = list(self.Clean.Data.keys())
        for key in keys[:-2]:
            self.Clean.Data[key] = "Out of Stock"
        self.Clean.Data['Product Type'] = input
        self.setLocationalData()
        return list(self.Clean.Data.values())

    def setLocationalData(self):
        self.Clean.Data['Address'] = '900 South Locust Street'
        self.Clean.Data['State'] = 'IA'
        self.Clean.Data['City'] = 'Glenwood'
        self.Clean.Data['Zip Code'] = '51534'


class RussMarketSpider(SeleniumSpider):
    name = "Russ Market"    #The store name 
    skipped = []            #Skipped data 

    def __init__(self):
        super().__init__()
        self.format = DataFormater() #Loads the Formater for cleanup and formating
        # vvv --- If you need to change default values add them below --- vvv
        
    
    #This handles the restart in case we run into an error
    def restart(self):
        super().restart()
        self.setStoreLocation()

    #Some stores need to have a location set
    def setStoreLocation(self):
        storeLocationUrl = 'https://www.russmarket.com/shop#!/?store_id=6158'
        self.setThisUrl(storeLocationUrl)
        time.sleep(5) #Wait for the page to set
        self.log("Store location set")

    #This starts the spider 
    def start_requests( self ):
        self.runTime = time.time()
        self.setStoreLocation()
        self.log("Loading from ProductsLoader Class")
        load = ProductsLoader() #Loads all products
        self.dataFrames = load.DataFrames #Adds all dataframes
        self.debug("Products Loaded and Data Frames Added")
        self.debug('\n < --- Setup runtime is %s seconds --- >' % (time.time() - self.runTime))
        totalRecoveries = 0  #Number of recoveries made while running
        #Sweeps through all products
        for product in load.Products:
            totalRecoveries += self.requestExtraction(product)
        self.log("Exporting files")
        #Dataframes to CSV files
        # for df, product in zip(self.dataFrames, load.Products):
            # self.saveDataFrame(df, self.currentDate + self.name + " " + product[1] + ".csv")
        # self.debug('\n < --- Total runtime took %s seconds with %d recoveries --- >' % (time.time() - self.runTime, totalRecoveries))
        # if len(self.skipped) != 0:
            # self.debug('\n < -!- WARNING SKIPPED (' + str(len(self.skipped)) + ') DATA FOUND --->')
        # self.Logs_to_file(self.currentDate + self.name + ' Spider Logs.txt')
        if len(self.skipped) != 0:
            # self.debug(self.skipped)
            self.skipHandler()      
        self.driver.quit()

    #Speical case handled here
    def handleSpeical(self, product, url):
        notFoundXpath = '//*[@id="products"]//*[contains(@class,"fp-text-center fp-not-found")]/h1'
        data = self.javascriptXpath(notFoundXpath)
        if data in {'empty', 'skip'}:
            self.debug("Missing item skipping")
            return ['SKIPPED']
        else:
            self.debug("An Item not in stock for: ", url) 
            return self.format.outOfStock(data, product[0], url)

    #This handles the reqests for each url and adds the data to the dataframe
    def handleRequests(self, product):
        productUrls = product[2]
        total = len(productUrls)
        while self.count < total:
            url = productUrls[self.count]
            self.driver.get(url)
            self.log("Making a request for: ", url)
            items = []
            time.sleep(1) # marionette Error Fix
            breakout = False
            for xpath in product[3]:
                data = self.makeRequest(xpath, url)
                if data == 'speical':
                    #Speical case handled here
                    items = self.handleSpeical(product, url)
                    breakout = True
                    break
                elif data == 'skip':  
                    #To help clean the data we skip the item with gaps of data 
                    self.debug("An Item has been skipped for: ", url)  
                    #Taking the product and index added as well as the url to retry for later 
                    #This could take time to do so we do this at the very end after we made the cvs files
                    self.skipped.append([product, self.count, url])
                    items = ['SKIPPED']
                    break
                else:
                    #data added to item
                    items.append(data)          
            if 'SKIPPED' in items:
                #No point in cleaning skipped items
                items = ['SKIPPED']*(self.dataFrames[product[0]].shape[1] - 1)
                items.append(url)
            elif breakout: 
                breakout = False
            else:
                #We call the DataFormater class to handle the cleaning of the data
                #Its best to clean the data before we add it to the data frame
                self.debug('Formating Data Started: ', items)
                items = self.format.cleanUp(items, product[0], url)
                self.debug('Formating Data Finished: ', items)
                if items == None:
                    self.printer("Data Formater not configured to ", product[1])
            self.debug('Extracted: ', items)
            self.dataFrames = pd.concat([self.dataFrames, pd.DataFrame(items, index=[0])], ignore_index=True)
            # self.dataFrames[product[0]].loc[len(self.dataFrames[product[0]])] = items                    
            self.count += 1
            self.printer(product[1] + " item added ", self.count, " of ", total, ":  ", items)

    #This is here to hopefully fix skipped data
    #Best case sinarios this will never be used
    def skipHandler(self):
        
        # skipped format
        # [product, DataFrame index, url]
        while len(self.skipped) != 0:
            #each skip 
            for index, dataSkip in enumerate(self.skipped):
                product = dataSkip[0]
                url = dataSkip[2]
                #Limiting the Attempts to fix while avoiding bottlenecking the problem
                for attempt in range(self.attempts*2):
                    self.driver.get(url)
                    self.log("Making a request for: ", url)
                    items = []
                    breakout = True
                    speicalBreak = False
                    for xpath in product[3]:
                        data = self.makeRequest(xpath, url)
                        if data == 'speical':
                            #Speical case handled here
                            items = self.handleSpeical(product, url)
                            speicalBreak = True
                            if 'SKIPPED' in items:
                                breakout = False
                            break
                        elif data == 'skip':  
                            break
                        else:
                            #data added to item
                            items.append(data)
                    if breakout:
                        if not speicalBreak:
                            items = self.format.cleanUp(items, product[0], url)
                            if items == None:
                                self.printer("Data Formater not configured to ", product[1])
                                break
                        self.dataFrames[product[0]].loc[dataSkip[1]] = items                    
                        self.printer("Fixed " + product[1] + " item: ", items)
                        #To avoid infinite loops and never saving our data we save the file now
                        self.saveDataFrame(self.dataFrames[product[0]], self.currentDate + "REPAIRED " + self.name + " " + product[1] + ".csv")
                        self.debug('\n < --- Total runtime with saving of repairs took %s seconds --- >' % (time.time() - self.runTime))
                        self.Logs_to_file(self.currentDate + self.name + ' Spider REPAIR Logs.txt')
                        #To avoid fixing fixed items we pop, mark, and break
                        self.skipped.pop(index)
                        break
                self.debug("Item still missing attempting other skipped for now") 
        self.debug('\n < --- Total runtime with all repairs took %s seconds --- >' % (time.time() - self.runTime))
        self.Logs_to_file(self.currentDate + self.name + ' spider COMPLETED REPAIR Logs.txt')