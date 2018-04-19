# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 17:28:01 2018

@author: E
"""

from selenium import webdriver
import os
from parsel import Selector

os.environ['MOZ_HEADLESS'] = '1'  

ccass_url = 'http://www.hkexnews.hk/sdw/search/searchsdw.aspx'
  
# Open Selenium 
driver = webdriver.Firefox() #PhantomJS()
driver.get(ccass_url) 
#driver.get_screenshot_as_file('1.png')   

# Input Initial StockID
stockid_input = driver.find_element_by_xpath('//*[@id="txtStockCode"]')
stockid_input.click()
stockid_input.send_keys('00001')      

# Click Search
search_button = driver.find_element_by_xpath('//input[@id="btnSearch"]')
search_button.click()
#driver.save_screenshot('2.png')

# Load the page source
sel = Selector(text=driver.page_source) 

# Close Selenium
driver.quit()  

# Xpath
view_stat = sel.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
print(view_stat)