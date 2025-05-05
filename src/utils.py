import os
from bs4 import BeautifulSoup
import time
import random

from selenium import webdriver 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc ## use py 3.11 or lesser for this to work.

import re


class ScraperUtils:
    def __init__(self):
        # Setup Chrome options
        self.options = uc.ChromeOptions()
        self.options.add_argument('--no-sandbox')
        self.driver = uc.Chrome(options=self.options)
        pass

    ## General Functions
    # Function to simulate human-like waiting (random sleep time)
    @staticmethod
    def human_sleep(min_sleep:float=2, max_sleep:float=5):
        time.sleep(random.uniform(min_sleep, max_sleep))

    #function to extract phone number and format them a bit.
    @staticmethod
    def format_number(number:str):
        # If the number contains '31' and doesn't start with '+', format it as '+31'
        number = re.sub(r"[^\d]", "", number)
        if number.startswith('31'): ##'31' in number and not
            return '+' + re.sub(r"[^\d]", "", number)
        # If the number starts with '06', leave it as '06'
        elif number.startswith('06'):
            return re.sub(r"[^\d]", "", number)
        else:
            print('ERROR: Weird phone number.')
            return number
            
    def retrieve_html(self, url:str, driver):
        self.driver.get(url)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        return soup
    
    