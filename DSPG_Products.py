import pandas as pd
from DSPG_SpiderErrors import ProductsError

#This class is here so that we can expand to differnet products easier making the spider more dynamic and expandable
class Products():
    ProductDataFrames = []
    #index iteration , Urls, Xpaths
    ProductList = []

    def __init__(self):
        self.loadProducts()
        self.dataFrameAdder()
        self.loadValidation()


    def loadProducts(self):
        #index iteration, Name, Urls, Xpaths
        self.ProductList = [[0, 'Bacon'],
                            [1, 'Eggs'],
                            [2, 'Heirloom Tomatoes'],
                            [3, 'Tomatoes']
                           ]
    
    #This adds the dataframe to the spider on load
    def dataFrameAdder(self):
        #Dataframes (You can add more here)
        self.ProductDataFrames = [pd.DataFrame(columns=['Bacon', 'Current Price', 'Orignal Price', 'Weight in lbs', 'True Weight', 'Brand', 'Address', 'State', 'City', 'Zip Code', 'Date Collected', 'Url']), #Bacon Frame
                                  pd.DataFrame(columns=['Eggs', 'Current Price', 'Orignal Price', 'Amount in dz', 'True Amount', 'Brand', 'Address', 'State', 'City', 'Zip Code', 'Date Collected', 'Url']), #Egg Frame
                                  pd.DataFrame(columns=['Heirloom Tomatoes', 'Current Price', 'Orignal Price', 'Weight in lbs', 'True Weight', 'Organic', 'Brand', 'Address', 'State', 'City', 'Zip Code', 'Date Collected', 'Url']), #Heirloom Tomato Frame
                                  pd.DataFrame(columns=['Tomatoes', 'Current Price', 'Orignal Price', 'Weight in lbs', 'True Weight', 'Organic', 'Brand', 'Address', 'State', 'City', 'Zip Code', 'Date Collected', 'Url']) #Tomato Frame
                                 ]
    
    # Note: This is a fail safe for lots of products.
    # DataFrame == Product index
    def loadValidation(self):
        if len(self.ProductDataFrames) != len(self.ProductList):
            raise ProductsError()