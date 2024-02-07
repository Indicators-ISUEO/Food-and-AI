import requests
import json
import sys
import datetime
from datetime import date, timedelta
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.proxy import ProxyType
from selenium.webdriver.common.proxy import Proxy

import pandas as pd


if __name__ == "__main__":
    product_list = pd.DataFrame({'price': [], 'product': [], 'quantity':[]})
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    # chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument(f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36')
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    # 
    # driver = webdriver.Chrome(executable_path=DRIVER_PATH, options=chrome_options)
    driver = webdriver.Chrome(options=chrome_options)
    crop = "green onion"
    url = "https://www.hy-vee.com/aisles-online/search?search={0}".format(crop)
    driver.get(url)
    try:
        wait = WebDriverWait(driver, 120)
        productCards = wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, '.styles__SecondaryProductInfoContainer-fsf453-17')))
        for card in productCards:
            try:
                card_details = card.text.split("\n")
                new_row = {'price': card_details[0], 'product': card_details[1], 'quantity': card_details[2]}
                product_list = product_list.append(new_row, ignore_index=True)
                print(card_details)
            except: 
                continue
        # select = Select(driver.find_element(By.ID,'search-input',))
        # print("select"+select)
    except:
        print(sys.exc_info())


# styles__ProductTitle-fsf453-4 gLMfrZ product-title
# styles__ProductTitle-fsf453-4 gLMfrZ product-title
