#Imports 
import time
import sys
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
    DataFrames = []

    def __init__(self):
        setProducts = Products()
        self.Products = setProducts.ProductList
        self.DataFrames = setProducts.ProductDataFrames
        self.urlsAdder()
        self.xpathMaker()

    #Adding Urls to products
    def urlsAdder(self):
        BaconUrls = [
                     'https://www.russmarket.com/shop/meat/bacon/sliced/hormel_black_label_thick_cut_bacon_1_lb/p/229335'
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/hormel_black_label_original_bacon_16_ounce/p/2349369',
                    #  'https://www.russmarket.com/shop/meat/bacon/mullan_road_bacon/p/5220311',
                    #  'https://www.russmarket.com/shop/meat/bacon/prairie_fresh_signature_applewood_smoked_bacon_pork_loin_filet_27_2_oz/p/6828650',
                    #  'https://www.russmarket.com/shop/meat/bacon/farmland_bacon_thick_cut_naturally_hickory_smoked/p/571658',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced_slab_bacon/p/1564405684712590572',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/smithfield_naturally_hickory_smoked_hometown_original_bacon/p/3142755',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/jamestown_economy_sliced_bacon_16_oz/p/180026',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/smithfield_naturally_hickory_smoked_thick_cut_bacon_12_oz_pack/p/3142757',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/smithfield_bacon_thick_cut_12_oz/p/2101085',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/farmland_naturally_hickory_smoked_classic_cut_bacon_16_oz/p/2376191',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/farmland_naturally_hickory_smoked_thick_cut_bacon_16_oz/p/95721',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/farmland_naturally_applewood_smoked_bacon_16_oz/p/585815',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/hormel_black_label_microwave_ready_bacon/p/26151',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/oscar_mayer_original_bacon_16_oz/p/32303',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/big_buy_hardwood_smoked_bacon/p/2101073',
                    #  'https://www.russmarket.com/shop/meat/bacon/turkey/oscar_mayer_turkey_bacon_original_12_oz/p/39809',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/farmland_bacon_low_sodium_hickory_smoked_16_oz/p/2376192',
                    #  'https://www.russmarket.com/shop/meat/bacon/canadian/farmland_bacon_double_smoked_double_thick_cut_12_oz/p/1564405684713224952',
                    #  'https://www.russmarket.com/shop/meat/bacon/canadian/hormel_black_label_naturally_hardwood_smoked_97_fat_free_canadian_bacon_6_oz_zip_pak/p/26168',
                    #  'https://www.russmarket.com/shop/meat/bacon/sliced/oscar_mayer_bacon_original_8_oz/p/32302',
                    ]

        EggUrls = [
                #    'https://www.russmarket.com/shop/dairy/eggs/everyday/best_choice_grade_a_large_eggs/p/3139172'
                #    'https://www.russmarket.com/shop/dairy/eggs/everyday/best_choice_large_eggs/p/3139176',
                #    'https://www.russmarket.com/shop/dairy/eggs/everyday/best_choice_jumbo_eggs/p/3139173',
                #    'https://www.russmarket.com/shop/dairy/eggs/everyday/best_choice_extra_large_eggs/p/3139174',
                #    'https://www.russmarket.com/shop/dairy/eggs/everyday/best_choice_large_eggs/p/3139192',
                #    'https://www.russmarket.com/shop/dairy/eggs/everyday/eggland_s_best_large_eggs_12_ea/p/54283',
                #    'https://www.russmarket.com/shop/dairy/eggs/everyday/eggland_s_best_large_eggs_18_ea/p/54279'
                  ]

        HeirloomTomatoesUrls = [
                                # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/heirloom_tomatoes/p/12412'
                               ]

        TomatoesUrls = [
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/roma_tomatoes/p/12447#!/?department_id=22508184'
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/tomatoes_on_the_vine/p/2311660#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/tomatoes_grape/p/1564405684709718017#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/nature_sweet_cherubs_heavenly_salad_tomatoes_10_oz/p/1564405684704878585#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/on_vine_cherry_tomatoes/p/645688#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/org_cherry_tomatoes/p/151480#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/red_sun_farms_sweet_pops_tomatoes_10_oz/p/1564405684704089139#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/nature_sweet_tomatoes_fresh_ingredient_10_oz/p/1564405684704878584#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/naturesweet_constellation_tomatoes/p/1564405684705657728#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/regular_red_tomato_large/p/12409#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/tomato_on_the_vine_organic/p/2313397#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/beefsteak_tomato/p/111605#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/red_cocktail_intermediate_tomatoes/p/78471#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/yellow_tomatoes/p/2311723#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/nature_sweet_cherry_tomatoes_on_the_vine_d_vines/p/1564405684704300013#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/tomatoes_hot_house/p/12571#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/organic_grape_tomatoes/p/6830116#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/signature_brand_organics_grape_tomatoes_organic/p/23476#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/real_grape_tomatoes/p/2456990#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/organic_grape_tomatoes/p/8036367#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/sweet_pops_red_chry_snacking_tomato/p/1564405684705309980#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/redsun_tomatoes_cocktail/p/645689#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/heirloom_tomatoes/p/12412#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/heritage_tomatoes/p/1564405684712593332#!/?department_id=22508184',
                        # 'https://www.russmarket.com/shop/produce/fresh_vegetables/tomatoes/tomatillos/p/12573#!/?department_id=22508184'
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