from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from datetime import datetime
import os
import time

# This class is a super class.
# We inherent common functions in order to reduces common code through out each Selenium Spider.
# Best practice is to only add functions that are both common and know wont change from spider to spider.
# This is ONLY here for the common frame works. 
# Creators Note: In the future it can be VERY dangerous if we make this class too powerful 

class SeleniumSpider():
    spiderLogs = [] #The logs 
    # self.log("selenium spider class")
    #These are methods that are available for your convences
    def log(self, *args):
        self.spiderLogs.append(('Logger:', args))
        if self.LOGGER:
            print('Logger:', *args)

    def debug(self, *args):
        self.spiderLogs.append(('Debug:', args))
        if self.DEBUGGER:
            print('Debug:', *args)
    
    def printer(self, *args):
        self.spiderLogs.append(('Printer:', args))
        print(*args)
    
    def printLogs(self):
        print("\n< --- Printing Logs --- >\n")
        for entry in self.spiderLogs:
            print(*entry)

    def Logs_to_file(self, filename):
        # Create the folder if it does not exist
        if not os.path.exists(self.folderPath):
            os.makedirs(self.folderPath)
        file_path = os.path.join(self.folderPath, filename)
        with open(file_path, 'w') as file:
            for log_entry in self.spiderLogs:
                file.write('{} {}\n'.format(log_entry[0], log_entry[1]))
        
        # with open(filename, 'w') as file:
        #     for log_entry in self.spiderLogs:
        #         file.write('{} {}\n'.format(log_entry[0], log_entry[1]))
    
    def __init__(self):
        # If you need to change anything add it to the spider's __init__ and it will overwrite these
        # Note these are all the default values and should be set in the inherited spider class
        # DON'T CHANGE THESE 3 IT MIGHT BREAK
        # self.log("selenium spider init")
        self.count = 0              #This saves the location of the url we are going through
        self.runTime = 0            #Total time of extractions
        self.totalRecoveries = 0    #Number of recoveries made while running
        
        # Debuging/Testing
        self.retry = True           #This is when it run's into an error and you want it to retry running from where it left off.
        self.DEBUGGER = False       #The debugger switch to see whats going on. 
        self.LOGGER = False         #When you need to see everything that happends. 
        
        # Values that will increase run time
        # WARNING: High number longer time but less chance of a skip or fails when extraction 
        self.attempts = 3           #The number of attempts the spider can retry if an error occurs.    Recommended range 2 - 50
        self.waitTime = 10          #The number of seconds WebDriver will wait for the page to load.    Recommended range 5 - 30
        self.maxRetryCount = 50     #Number of retrys the javascript can make.                          Recommended range 10 - 100
        
        #Formating the dates recoreded
        self.currentDate = str(datetime(datetime.today().year, datetime.today().month, datetime.today().day))[:-8]
        #The name of the folder that it will save to
        self.folderPath = self.currentDate + "Data"
        
        #Selenium needs a webdriver to work. I chose Firefox however you can do another if you need too
        options = Options()
        # options.headless = True
        options.add_argument("--headless")
        self.driver = webdriver.Firefox(options=options, service=FirefoxService(GeckoDriverManager().install(), log_path=os.path.devnull))
        # self.driver = webdriver.Firefox(options=options)
        # self.log("Driver started")

    #This handles the restart in case we run into an error
    def restart(self):
        self.driver.quit()
        options = Options()
        # options.headless = True
        options.add_argument("--headless")
        self.driver = webdriver.Firefox(options=options, service=FirefoxService(GeckoDriverManager().install(), log_path=os.path.devnull))
        self.log("Driver restarted")

    def saveDataFrame(self, dataFrame, fileName):
        if not os.path.exists(self.folderPath):
            os.makedirs(self.folderPath)
            self.log('New folder made')
        dataFrame.to_csv(os.path.join(self.folderPath, fileName), index=False)
        self.log('\n', dataFrame.to_string())
    
    #This is for testing the code without it trying to recover
    def testingRequestExtraction(self, product):
        self.count = 0
        errors = 0
        start = time.time()
        self.debug("Starting "+ product[1]) 
        self.handleRequests(product)
        self.debug(product[1] + " Finished")    
        self.log('\n< --- ' + product[1] + ' scrape took %s seconds with %d recoveries --- >\n' % ((time.time() - start), errors))
        return 0

    #This handles the extraction request for the inputed product 
    #Note: It wont be any differet unless we are taking in different store. 
    def requestExtraction(self, product):
        self.count = 0
        errors = 0
        start = time.time()
        self.debug("Starting "+ product[1])  
        if self.retry:  
            for trying in range(self.attempts):
                try:
                    self.handleRequests(product)
                    self.debug(product[1] + " Finished")    
                    self.log('\n< --- ' + product[1] + ' scrape took %s seconds with %d recoveries --- >\n' % ((time.time() - start), errors))
                    return errors
                except Exception as e:
                    #Note sometimes the browser will closed unexpectedly and theres not we can do but restart the driver
                    errors += 1
                    self.debug("An error occurred:", e)
                    self.debug("Recovering extraction and continueing")
                    self.restart() 
            self.debug(product[1] + " Did not Finished after " + str(self.attempts) + " Time wasted: %s seconds" % (time.time() - start))
            return errors
        self.handleRequests(product)
        self.debug(product[1] + " Finished")    
        self.log('\n< --- ' + product[1] + ' scrape took %s seconds --- >\n' % (time.time() - start))
        return 0

    #This is to be build upon in the spider and is the only required function to be passed into and handled by the inherited spider
    def handleRequests(self):
        pass

    #Sometimes a URL must be set
    def setThisUrl(self, url):
        self.driver.get(url)
        return

    #Sometimes a button must be pushed
    def clickThis(self, xpath):
        ignored_exceptions=(NoSuchElementException,StaleElementReferenceException)
        elements = WebDriverWait(self.driver, self.waitTime, ignored_exceptions=ignored_exceptions).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
        elements[0].click()
        return

    #This returns data from the request 
    def makeRequest(self, xpath, url):
        #Retrying the xpath given the number of attempts
        for attempt in range(self.attempts):
            data = self.javascriptXpath(xpath[0])
            if data in {'empty', 'skip'}:
                #speical case in case you need it
                if len(xpath) == 3:
                    if xpath[2]:
                        if attempt == 0:
                           self.debug("Found a speical case double checking")
                           continue
                        #example would be when there is actually is a '' in the xpath
                        #or a product is not in stock
                        self.debug("xpath marked as speical")
                        return 'speical'
                if xpath[1] and data == 'empty':    
                    #this is where setting the xpath to optional comes in
                    self.debug("xpath wasnt avaliable")
                    return None 
                self.debug("Missing item retrying")
            else:  #Data found
                self.log(data + ' was added to the list for: ', url)
                return data
        return 'skip'
    
    #Collecting the data from the xpath in JavaScript is faster and results in fewer errors than doing it in python
    #This is where selenium shines because we can both use JavaScript and render JavaScript websites
    #and is the only reason why we use it instead of scrapy
    def javascriptXpath(self, xpath):
        # if the time expires it assumes xpath wasnt found in the page
        try: 
            #Waits for page to load 
            ignored_exceptions=(NoSuchElementException,StaleElementReferenceException)
            elements = WebDriverWait(self.driver, self.waitTime, ignored_exceptions=ignored_exceptions).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))

            # Runs the javascript and collects the text data from the inputed xpath
            # We want to keep repeating if we get any of these outputs becasue the page is still 
            # loading and we dont want to skip or waste time. (for fast computers)
            retrycount = 0
            invalidOutputs = {'error', 'skip' '$nan', 'nan', 'notfound', ''}
            while retrycount < self.maxRetryCount :
                text = self.driver.execute_script("""
                    const element = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                    if (!element) {
                        return 'skip';
                    }
                    return element.textContent.trim();
                """, 
                xpath)
                if text == None:
                    retrycount+=1
                    continue
                checkText = text.replace(" ", "").lower()
                if checkText in invalidOutputs:
                    retrycount+=1
                else:
                    self.log(retrycount, "xpath attempts for (", text, ")")
                    return text
            self.log("xpath attempts count met. Problematic text (" + text + ") for ", xpath)
            return 'skip'
        except TimeoutException:
            self.log('Could not find xpath for: ', xpath)
            return 'empty'

