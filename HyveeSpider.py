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
                     'https://www.hy-vee.com/aisles-online/p/11315/Hormel-Black-Label-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/47128/Hormel-Black-Label-Fully-Cooked-Original-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/41626/Applegate-Naturals-Uncured-Sunday-Bacon-Hickory-Smoked',
                    #  'https://www.hy-vee.com/aisles-online/p/57278/HyVee-Double-Smoked-Thick-Sliced-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2405550/Applegate-Naturals-No-Sugar-Uncured-Bacon-Hickory-Smoked',
                    #  'https://www.hy-vee.com/aisles-online/p/57279/HyVee-Sweet-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/11366/Hormel-Black-Label-Original-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2455081/Jimmy-Dean-Premium-Hickory-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/3595492/Farmland-Bacon-Double-Smoked-Double-Thick-Cut',
                    #  'https://www.hy-vee.com/aisles-online/p/47117/Hormel-Black-Label-Center-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/57277/HyVee-Center-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2199424/Country-Smokehouse-Thick-Applewood-Slab-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/77228/Hormel-Black-Label-Original-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/21239/Farmland-Naturally-Hickory-Smoked-Classic-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2456254/Jimmy-Dean-Premium-Applewood-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/21240/Farmland-Naturally-Hickory-Smoked-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/47159/Hormel-Black-Label-Original-Bacon-4Pk',
                    #  'https://www.hy-vee.com/aisles-online/p/50315/Oscar-Mayer-Naturally-Hardwood-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/50321/Oscar-Mayer-Center-Cut-Original-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/50316/Oscar-Mayer-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2199421/Country-Smokehouse-Thick-Hickory-Smoked-Slab-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/104466/Hickory-Country-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/23975/HyVee-Hickory-House-Applewood-Naturally-Smoked-Thick-Sliced-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/23949/HyVee-Sweet-Smoked-Thick-Sliced-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/23963/HyVee-Fully-Cooked-Hickory-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/11173/Hormel-Black-Label-Applewood-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/21317/Farmland-Naturally-Applewood-Smoked-Classic-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/21238/Farmland-Naturally-Hickory-Smoked-Thick-Cut-Bacon-Package',
                    #  'https://www.hy-vee.com/aisles-online/p/23948/HyVee-Lower-Sodium-Sweet-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/458259/Wright-Naturally-Hickory-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/11384/Hormel-Natural-Choice-Uncured-Original-Bacon-12-oz',
                    #  'https://www.hy-vee.com/aisles-online/p/2476490/Jimmy-Dean-FC-Hickory-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/1646677/Smithfield-Hometown-Original-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/53849/Farmland-Naturally-Hickory-Smoked-Lower-Sodium-Classic-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/47121/Hormel-Black-Label-Maple-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/164627/Oscar-Mayer-Fully-Cooked-Original-Bacon-252-oz-Box',
                    #  'https://www.hy-vee.com/aisles-online/p/23974/HyVee-Hickory-House-Hickory-Smoked-Thick-Sliced-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/50319/Oscar-Mayer-Selects-Smoked-Uncured-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2471760/Jimmy-Dean-FC-Applewood-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/16239/Oscar-Mayer-Center-Cut-Thick-Sliced-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2214511/Hormel-Black-Label-Original-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/1008152/Wright-Naturally-Smoked-Applewood-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/1813260/Smithfield-Naturally-Hickory-Smoked-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/23976/HyVee-Hickory-House-Peppered-Naturally-Smoked-Thick-Sliced-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/21320/Farmland-Naturally-Applewood-Smoked-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/21253/Farmland-Naturally-Hickory-Smoked-Extra-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/1255920/Hormel-Black-Label-Cherrywood-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/57304/HyVee-Blue-Ribbon-Maple-Naturally-Smoked-Thick-Sliced-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/21252/Farmland-Naturally-Hickory-Smoked-30-Less-Fat-Center-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2501872/Bourbon-And-Brown-Sugar-Slab-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/2516586/Hormel-Natural-ChoiceOriginal-Thick-Cut-Uncured-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/21319/Farmland-Naturally-Hickory-Smoked-Double-Smoked-Classic-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/317829/Des-Moines-Bacon-And-Meat-Company-Hardwood-Smoked-Uncured-Country-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/1255919/Hormel-Black-Label-Jalapeno-Thick-Cut-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/3538865/Oscar-Mayer-Bacon-Thick-Cut-Applewood',
                    #  'https://www.hy-vee.com/aisles-online/p/317830/Des-Moines-Bacon-And-Meat-Company-Applewood-Smoked-Bacon',
                    #  'https://www.hy-vee.com/aisles-online/p/3308731/Oscar-Mayer-Natural-Fully-Cooked-Uncured-Bacon'
                    ]
        EggUrls = [
                    'https://www.hy-vee.com/aisles-online/p/57236/HyVee-Grade-A-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/23899/HyVee-Grade-A-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/715446/Farmers-Hen-House-Free-Range-Organic-Large-Brown-Grade-A-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/2849570/Thats-Smart-Large-Shell-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/31351/Farmers-Hen-House-Free-Range-Grade-A-Large-Brown-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/23900/HyVee-Grade-A-Extra-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/71297/Egglands-Best-Farm-Fresh-Grade-A-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/36345/Egglands-Best-Grade-A-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/3192325/HyVee-Free-Range-Large-Brown-Egg-Grade-A',
                    # 'https://www.hy-vee.com/aisles-online/p/23903/HyVee-Grade-A-Jumbo-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/3192323/HyVee-Cage-Free-Large-Brown-Egg-Grade-A',
                    # 'https://www.hy-vee.com/aisles-online/p/36346/Egglands-Best-Cage-Free-Brown-Grade-A-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/3192322/HyVee-Cage-Free-Large-Brown-Egg-Grade-A',
                    # 'https://www.hy-vee.com/aisles-online/p/858343/HyVee-Cage-Free-Omega3-Grade-A-Large-Brown-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/1901565/Farmers-Hen-House-Pasture-Raised-Organic-Grade-A-Large-Brown-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/60364/HyVee-HealthMarket-Organic-Grade-A-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/71298/Egglands-Best-Extra-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/23902/HyVee-Grade-A-Extra-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/453006/Egglands-Best-XL-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/2668550/HyVee-One-Step-Pasture-Raised-Large-Brown-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/66622/Farmers-Hen-House-Jumbo-Brown-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/3274825/Nellies-Eggs-Brown-Free-Range-Large',
                    # 'https://www.hy-vee.com/aisles-online/p/57235/HyVee-Grade-A-Medium-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/2437128/Pete-And-Gerrys-Eggs-Organic-Brown-Free-Range-Large',
                    # 'https://www.hy-vee.com/aisles-online/p/36347/Egglands-Best-Organic-Cage-Free-Grade-A-Large-Brown-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/2698224/Nellies-Free-Range-Eggs-Large-Fresh-Brown-Grade-A',
                    # 'https://www.hy-vee.com/aisles-online/p/57237/HyVee-Grade-A-Large-Eggs',
                    # 'https://www.hy-vee.com/aisles-online/p/190508/Farmers-Hen-House-Organic-Large-Brown-Eggs'
                   ]
        HeirloomTomatoesUrls = [
                                'https://www.hy-vee.com/aisles-online/p/37174/'
                               ]
        
        TomatoesUrls = [
                        'https://www.hy-vee.com/aisles-online/p/8657/Roma-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/8824/Tomatoes-on-the-Vine',
                        # 'https://www.hy-vee.com/aisles-online/p/3538538/Basket-And-Bushel-Grape-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/3538540/Basket-And-Bushel-Gourmet-Medley-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/2973805/NatureSweet-Cherub-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/8854/Greenhouse-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/3758077/Angel-Sweet-Tomatoes-Snack-Cup',
                        # 'https://www.hy-vee.com/aisles-online/p/793206/Nature-Sweet-Cherubs',
                        # 'https://www.hy-vee.com/aisles-online/p/3128235/Full-Circle-Market-Organic-Grape-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/410515/Sunset-Campari-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/3009875/Sunset-Flavor-Bombs-Cherry-Tomatoes-on-Vine',
                        # 'https://www.hy-vee.com/aisles-online/p/3538539/Basket-And-Bushel-One-Sweet-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/3538537/Basket-And-Bushel-Grape-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/37174/Heirloom-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/2850121/Sugar-Bomb-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/3538541/Basket-And-Bushel-Zima-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/3538542/Basket-And-Bushel-Mini-Kumato-Tomatoes',
                        # 'https://www.hy-vee.com/aisles-online/p/491714/Sunset-Tomatoes-Organic-Campari',
                        # 'https://www.hy-vee.com/aisles-online/p/37463/Cherry-Tomatoes'
                       ]
        
        self.Products[0].append(BaconUrls)
        self.Products[1].append(EggUrls)
        # self.Products[2].append(HeirloomTomatoesUrls)
        # self.Products[3].append(TomatoesUrls)

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
        nameXpath = '//*[contains(@class, "product-details_detailsContainer")]/h1'
        priceXpath = '//*[contains(@class, "product-details_detailsContainer")]/p[1]'
        prevPriceXpath = '//*[contains(@class, "product-details_detailsContainer")]/p[2]'
        weightXpath = '//*[contains(@class, "product-details_detailsContainer")]/p[3]' # optional
        #xpath, Optional
        xpathList = [(nameXpath, False),
                     (priceXpath, False),
                     (prevPriceXpath, False),
                     (weightXpath, True)]
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
        #Because of how the page is layed out we need to swap elements for the cleaner to work 
        if input[3] == None:
            input = self.swap_elements(input, 2, 3)
        #Common inputs
        self.Clean.Data['Product'] = input[0]
        self.Clean.Data['Current Price'] = input[1]
        self.Clean.Data['Orignal Price'] = input[2]
        self.Clean.Data['Source'] = 'Hyvee'
        if(inputIndex == 0): #Bacon
            self.Clean.Data['True Weight'] = input[3]
            self.Clean.baconModifications()
        elif(inputIndex == 1): #Eggs
            self.Clean.Data['True Amount'] = input[3]
            self.Clean.eggModifications()
        elif(inputIndex == 2 or inputIndex == 3): #Tomatoes
            self.Clean.tomatoesModifications(input[3])
        #Add products here
        else:
            raise DataFormatingError(inputIndex)
        # self.setLocationalData()
        self.Clean.cleanPricing()
        return self.Clean.Data
        # return list(self.Clean.Data.values())
        
    def swap_elements(self, input, idx1, idx2):
        # Make a copy of the input list to avoid modifying it
        new_lst = input.copy()
        # Swap the elements at the two indices
        new_lst[idx1], new_lst[idx2] = new_lst[idx2], new_lst[idx1]
        return new_lst

    #For Hyvee there are multiple stores however I could not find a way to check each store
    #This is something to be developed (improved upon) in the future
    # def setLocationalData(self):
        # self.Clean.Data['Address'] = 'NA'
        # self.Clean.Data['State'] = 'NA'
        # self.Clean.Data['City'] = 'NA'
        # self.Clean.Data['Zip Code'] = 'NA'

class HyveeSpider(SeleniumSpider):
    name = "Hyvee"  #The store name 
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
        #     self.saveDataFrame(df, self.currentDate + self.name + " " + product[1] + ".csv")
        self.debug('\n < --- Total runtime took %s seconds with %d recoveries --- >' % (time.time() - self.runTime, totalRecoveries))
        # if len(self.skipped) != 0:
        #     self.debug('\n < -!- WARNING SKIPPED (' + str(len(self.skipped)) + ') DATA FOUND --->')
        # self.Logs_to_file(self.currentDate + self.name + ' Spider Logs.txt')
        if len(self.skipped) > 0:
            self.debug(self.skipped)
            self.skipHandler()      
        self.driver.quit()
        return self.dataFrames

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
            # self.dataFrames.append(pd.DataFrame(items, index=[0]), ignore_index=True)
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
