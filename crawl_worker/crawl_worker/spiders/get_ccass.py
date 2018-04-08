# -*- coding: utf-8 -*-
#-----------------------------------------------------------
# scrapy CCASS Data and save to MySQL 
# Version 2.01
# @2018-03-13
#-----------------------------------------------------------
# scrapy crawl get_ccass
#-----------------------------------------------------------

from scrapy import Spider
from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from scrapy.conf import settings

from crawl_worker.items import BrokerInfoItem

from selenium import webdriver
from selenium.webdriver.support.ui import Select as Selenium_Selector

import json
from bs4 import BeautifulSoup
import re

from datetime import date

import os
os.environ['MOZ_HEADLESS'] = '1'

class CCASS_Selenium_Spider(Spider):
    name = 'get_ccass'
    allowed_domains = ['www.hkexnews.hk']       
    
    def start_requests(self):            
        ccass_url = 'http://www.hkexnews.hk/sdw/search/searchsdw.aspx'
        sel = self.open_selenium(ccass_url)  
        
        queue_file = self.queue_file
        
        try:
            # read data from queue file
            with open(queue_file, 'r') as infile:
                lines = infile.readlines()            
                
            # delete queue file
            os.remove(queue_file)
            
            # start scrapy ...
            if len(lines) > 0:
                for line in lines:
                    msg_dict = json.loads(line)
                    #print(str(msg_dict['stockid']), str(msg_dict['sdate']))
                    stockid = str(msg_dict['stockid'])
                    sdate = str(msg_dict['sdate'])
                    form_data = self.get_formdata(sel, stockid, sdate)
                    yield FormRequest(url=ccass_url, formdata=form_data, callback=self.parse_ccass)
                
        except KeyboardInterrupt:
            exit
        
    def parse_ccass(self, response):
        try:
            sdate = self.crawl_date(response)
            stockid = self.crawl_stockid(response)
            broker_info = self.crawl_brokerinfo(response)
            self.logger.info('parse %s on the date %s', stockid, sdate)
            br_item = BrokerInfoItem()
            br_item['stockid'] = stockid
            br_item['sdate'] = sdate
            br_item['broker_info'] = broker_info
            return br_item
        except:
            self.logger.info('fail to parse data')

    # -------------
    # Functions
    # -------------
    
    def open_selenium(self, ccass_url): # Selenium to get the 1st Form Data     
        # Open Selenium 
        driver = webdriver.Firefox() #PhantomJS()
        driver.get(ccass_url)        
        # Input Initial StockID
        stockid_input = driver.find_element_by_xpath('//*[@id="txtStockCode"]')
        stockid_input.click()
        stockid_input.send_keys('00001')      
        # Click Search
        search_button = driver.find_element_by_xpath('//input[@id="btnSearch"]')
        search_button.click()
        # Load Page to Scrapy
        sel = Selector(text=driver.page_source) 
        # Close Selenium
        driver.quit()
        # Return Selector Handle
        return sel

    def get_formdata(self, sel, stockid, sdate):  # Fill Form and Return for FormRequest
        view_stat = sel.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
        view_generator = sel.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first()
        event_valid = sel.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first()
        sdate = sdate.split('-')
        syear = sdate[0]
        smonth = sdate[1]
        sday = sdate[2]
        formdata = {'__VIEWSTATE': view_stat,
                    '__VIEWSTATEGENERATOR': view_generator,
                    '__EVENTVALIDATION': event_valid,
                    'today':str(date.today()).replace('-',''),
                    'sortBy':'',
                    'selPartID':'',
                    'alertMsg':'',
                    'ddlShareholdingDay':sday,
                    'ddlShareholdingMonth':smonth,
                    'ddlShareholdingYear':syear,
                    'txtStockCode':stockid,
                    'txtStockName':'',
                    'txtParticipantID':'',
                    'txtParticipantName':'',
                    'btnSearch.x':'40',
                    'btnSearch.y':'14'}
        return formdata

    def crawl_date(self, response):   # Crawl Date String from Web Page
        sdate = re.findall(r'\d{2}/\d{2}/\d{4}',Selector(response).extract())[0]     
        sdate = sdate.split('/')
        syear = sdate[2]
        smonth = sdate[1]
        sday = sdate[0]
        sdate = syear + '-' + smonth + '-' + sday
        return sdate

    def crawl_stockid(self, response):  # Crawl StockID from Web Page
        stockid = response.xpath('//span[contains(@class,"mobilezoom")]/text()').extract_first().strip()
        return stockid

    def crawl_brokerinfo(self, response):
        bs4_ccass_results = BeautifulSoup(response.body, "html.parser")
        bs4_broker_info = bs4_ccass_results.find_all('tr', {'class': ['row0','row1']})
        return bs4_broker_info