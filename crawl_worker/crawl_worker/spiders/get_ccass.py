# -*- coding: utf-8 -*-
#-----------------------------------------------------------
# scrapy CCASS Data and save to MySQL 
# Version 2.01
# @2018-03-13
#-----------------------------------------------------------
# scrapy crawl get_ccass
#-----------------------------------------------------------

from scrapy import Spider
from scrapy.http import FormRequest
from scrapy.selector import Selector

from bs4 import BeautifulSoup
import re

import json
import os

from crawl_worker.items import BrokerInfoItem

class CCASS_Spider(Spider):
    name = 'get_ccass'
    allowed_domains = ['www.hkexnews.hk']            
        
    def start_requests(self):     
        ccass_url = 'http://www.hkexnews.hk/sdw/search/searchsdw.aspx'
        queue_file = self.queue_file
        
        try:
            # read data from queue file
            with open(queue_file, 'r') as infile:
                formdata = json.load(infile)            
                
            # delete queue file
            os.remove(queue_file)
            
            # start scrapy ...
            if formdata is not None:
                for f in formdata:
                    yield FormRequest(url=ccass_url, formdata=f, callback=self.parse_ccass)
                
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
            self.logger.error('fail to parse data')

    # -------------
    # Functions
    # -------------
 
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