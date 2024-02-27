#Imports 
import time
#DSPG imports to assist the spider
from DSPG_SeleniumSpider import SeleniumSpider #Important to look at since it provides the framework
from DSPG_Cleaner import DataCleaner # This is to handle the cleaning of data
from DSPG_Products import Products #Imports the products to be processed
from DSPG_SpiderErrors import DataFormatingError #Very Important
import pandas as pd

# Using Products class. We only need to add the xpaths and urls since thats 
# all that really changes from spider to spider
class ProductsLoader():
    #index iteration, Name, Urls, Xpaths
    Products = []
    dataFrames = []

    def __init__(self):
        setProducts = Products()
        self.Products = setProducts.ProductList
        self.dataFrames = setProducts.ProductDataFrames
        self.urlsAdder()
        self.xpathMaker()

    #Adding Urls to products
    def urlsAdder(self):
        BaconUrls = [
                     'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-18483'
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-18485',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-24190',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-18553',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-33732',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-18521',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-18548',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-18469',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-33734',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-33736',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-33731',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-29349',
                    #  'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-18524'
                    ]
        EggUrls = [
                #    'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22775',
                #    'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22776',
                #    'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-12603'
                  ]
        HeirloomTomatoesUrls = [
                                # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11820',
                                # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22455',
                                # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11896',
                                # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11973',
                                # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22343'
                               ]
        TomatoesUrls = [
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-28349',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22501',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11861',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-26358',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22515',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22617',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11820',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11756',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22341',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-22343',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-28492',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-29803',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11787',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-35543',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11972',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11973',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-11975',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-12014',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-43100',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-35365',
                        # 'https://gatewaymarket.storebyweb.com/s/1000-1/i/INV-1000-35542'
                       ]
        self.Products[0].append(BaconUrls)
        self.Products[1].append(EggUrls)
        self.Products[2].append(HeirloomTomatoesUrls)
        self.Products[3].append(TomatoesUrls)
        
        

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
        nameXpath = '//*[@id="item-details"]/h1[contains(@class, "name")]'
        priceXpath = '//*[@id="item-details"]//*[contains(@class, "wc-pricing")]//*[contains(@aria-describedby, "priceDesc")]'
        prevPriceXpath = '//*[@id="item-details"]//*[contains(@class, "wc-pricing")]/div[contains(@class, "text-muted")]/s' # optional
        brandXpath = '//*[@id="item-details"]/div[1]' # optional 
        sizeXpath = '//*[@id="details"]//*[@class="size"]' # optional 
        weightXpath = '//*[@id="item-details"]//*[contains(@class, "wc-sold-by-avg-weight")]'# optional 
        xpathList = [(nameXpath, False),
                     (priceXpath, False),
                     (prevPriceXpath, True),
                     (brandXpath, True),
                     (sizeXpath,True, True)]
        self.Products[0].append(xpathList)
        self.Products[1].append(xpathList)
        xpathList = [(nameXpath, False),
                    (priceXpath, False),
                    (prevPriceXpath, True),
                    (brandXpath, True, True),
                    (weightXpath, True),
                    (sizeXpath,True, True)]
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
        #Common inputs
        self.Clean.Data['Source'] = 'Gateway'
        self.Clean.Data['Product'] = input[0]
        self.Clean.Data['Current Price'] = input[1]
        self.Clean.Data['Orignal Price'] = input[2]
        self.Clean.Data['Brand'] = input[3]
        if(inputIndex == 0): #Bacon
            self.Clean.Data['True Weight'] = input[4]
            self.Clean.baconModifications()
        elif(inputIndex == 1): #Eggs
            self.Clean.Data['True Amount'] = input[4]
            self.Clean.eggModifications()
        elif(inputIndex == 2 or inputIndex == 3): #Tomatoes
            #This part is for Weight the rest is handled with in the function 
            if input[4] != None:
                self.Clean.tomatoesModifications(input[4])
            elif input[5] != None:
                self.Clean.tomatoesModifications(input[5])
            else:
                self.Clean.tomatoesModifications(None)
        #Add products here
        else:
            raise DataFormatingError(inputIndex)
        self.setLocationalData()
        self.Clean.cleanPricing()
        return self.Clean.Data
        
        
    def setLocationalData(self):
        self.Clean.Data['Address'] = '2002 Woodland Avenue'
        self.Clean.Data['State'] = 'IA'
        self.Clean.Data['City'] = 'Des Moines'
        self.Clean.Data['Zip Code'] = '50312'    


class GatewaySpider(SeleniumSpider):
    name = "Gateway Market" #The store name 
    skipped = []            #Skipped data 

    def __init__(self):
        super().__init__()
        self.format = DataFormater() #Loads the Formater for cleanup and formating
        # vvv --- If you need to change default values add them below --- vvv
        
    #This starts the spider 
    def start_requests( self ):
        self.runTime = time.time()
        self.log("Loading from ProductsLoader Class")
        load = ProductsLoader() #Loads all products
        self.dataFrames = load.dataFrames #Adds all dataframes
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
        self.debug('\n < --- Total runtime took %s seconds with %d recoveries --- >' % (time.time() - self.runTime, totalRecoveries))
        if len(self.skipped) != 0:
            self.debug('\n < -!- WARNING SKIPPED (' + str(len(self.skipped)) + ') DATA FOUND --->')
        self.Logs_to_file(self.currentDate + self.name + ' Spider Logs.txt')
        if len(self.skipped) > 0:
            self.debug(self.skipped)
            self.skipHandler()      
        self.driver.quit()

    #This handles the reqests for each url and adds the data to the dataframe
    def handleRequests(self, product):
        productUrls = product[2]
        total = len(productUrls)
        while self.count < total:
            url = productUrls[self.count]
            self.driver.get(url)
            self.log("Making a request for: ", url)
            item = []
            time.sleep(1) # marionette Error Fix
            for xpath in product[3]:
                data = self.makeRequest(xpath, url)
                if data == 'speical':
                    #Speical case handled here
                    item.append(None)
                elif data == 'skip':  
                    #To help clean the data we skip the item with gaps of data 
                    self.debug("An Item has been skipped for: ", url)  
                    item = ['SKIPPED']
                    #Taking the product and index added as well as the url to retry for later 
                    #This could take time to do so we do this at the very end after we made the cvs files
                    self.skipped.append([product, self.count, url])
                    break
                else:
                    #data added to item
                    item.append(data)
            if 'SKIPPED' in item:
                #No point in cleaning skipped items
                items = ['SKIPPED']*(self.dataFrames[product[0]].shape[1] - 1)
                items.append(url)
            else:
                #We call the DataFormater class to handle the cleaning of the data
                #Its best to clean the data before we add it to the data frame
                self.debug('Formating Data Started: ', item)
                items = self.format.cleanUp(item, product[0], url)
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
        corrections = 0
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
                    item = []
                    breaker = True
                    for xpath in product[3]:
                        data = self.makeRequest(xpath, url)
                        if data == 'speical':
                            #Speical case handled here
                            item.append(None)
                        elif data == 'skip':  
                            breaker = False
                            break
                        else:
                            #data added to item
                            item.append(data)
                    if breaker:
                        items = self.format.cleanUp(item, product[0], url)
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
                        corrections += 1
                        break
                self.debug("Item still missing attempting other skipped for now") 
        self.debug('\n < --- Total runtime with all repairs took %s seconds --- >' % (time.time() - self.runTime))
        self.Logs_to_file(self.currentDate + self.name + ' spider COMPLETED REPAIR Logs.txt')