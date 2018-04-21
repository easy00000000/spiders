# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 15:24:22 2018

@author: Easy
"""

import sys
import getopt

from tools import data
from crawl_worker.crawl_worker import settings
from tasks.scrapy_task import scrapy_ccass

def main(argv):  
    # Read Parameters
    try:
        opts, args = getopt.getopt(argv,"hi:d:s:e:",["stockid=","days=","start_date","end_date"])
    except getopt.GetoptError:
        data.print_param_examples()
        sys.exit(2)
    
    # Read Existing Data    
    existing_data = []
    existing_data = data.read_existing_data(
                            settings.MYSQL_HOST,
                            settings.MYSQL_PORT,
                            settings.MYSQL_USER,
                            settings.MYSQL_PASSWD,
                            settings.CCASS_DB,
                            settings.StockID_Index_Table)
    
    # Generate Celery Task Data
    execute_data = data.get_data_list(opts, existing_data)
    if execute_data is not None:        
        form_data = []
        form_data = data.get_formdata(execute_data)
        if form_data is not None:
            distribute_tasks(form_data)
        
def distribute_tasks(execute_data, n=100):
    msg = []
    c = 0
    print('--- ---')
    for e in execute_data:
        print(e['txtStockCode'],':',e['ddlShareholdingYear'],'-',e['ddlShareholdingMonth'],'-',e['ddlShareholdingDay']) 
        msg.append(e)
        c = c+1
        if c>n:  
            print('--- ---')
            # Celery Task Distribution        
            scrapy_ccass.delay(msg) # execute_data is tuple format            
            msg = []
            c = 0
            
    if len(msg)>0:
        print('--- ---')
        scrapy_ccass.delay(msg)
        
if __name__ == "__main__":
    main(sys.argv[1:])