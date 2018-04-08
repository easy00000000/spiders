# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 15:24:22 2018

@author: Easy
"""

import sys
import getopt

from tools import readMySQL
from tools import structure_data
from tools import stocklist

from tasks.scrapy_task import scrapy_ccass

def main(argv):   
    try:
        opts, args = getopt.getopt(argv,"hi:d:s:e:",["stockid=","days=","start_date","end_date"])
    except getopt.GetoptError:
        print_param_examples()
        sys.exit(2)
 
    execute_data = get_data_list(opts)
    
    if execute_data is not None:
        distribute_tasks(execute_data)        
        
def distribute_tasks(execute_data, n=10):
    msg = []
    c = 0
    print('--- ---')
    for e in execute_data:
        print(e) 
        msg.append(structure_data.tuple2json(e))
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
        
def get_data_list(opts):
    start_date = ''
    end_date = ''
    days = -1
    stockid = ''
    
    for opt, arg in opts:
        if opt == '-h':
            print_param_examples()
            sys.exit()
        elif opt in ("-i", "--stockid"):
            stockid = arg
        elif opt in ("-d", "--days"):
            days = int(arg)
        elif opt in ("-s", "--start_date"):
            start_date = arg
        elif opt in ("-e", "--end_date"):
            end_date = arg
        
    existing_data = readMySQL.read_existing_data()
    execute_data = []
    
    if len(stockid) == 5:
        # specifc stockid
        if days > 0:
            # crawl specific stockid in period:
            execute_data = structure_data.crawl_1(stockid, days, existing_data) # execute_data is a tuple value
        elif days < 0:
            if len(start_date) > 0 and len(end_date) > 0 :
                # crawl specific stockid between start_date and end_date:
                execute_data = structure_data.crawl_2(stockid, start_date, end_date, existing_data)
        elif days == 0:
            # re-crawl missing stockid by comparing MySQL:
            execute_data = structure_data.crawl_3(stockid, existing_data)
            
    elif len(stockid) == 0:
        slist = stocklist.read()
        print(slist)
        # from stocklist
        if days > 0:
            # crawl in period:
            execute_data = structure_data.crawl_n1(slist, days, existing_data)
        elif days < 0:
            if len(start_date) > 0 and len(end_date) > 0 :
                # crawl between start_date and end_date:
                execute_data = structure_data.crawl_n2(slist, start_date, end_date, existing_data)
        elif days == 0:
            # re-crawl missing stockid by comparing MySQL: 
            execute_data = structure_data.crawl_n3(slist, existing_data)
    else:
        print_param_examples()
        sys.exit()
        
    #print_params(stockid, days, start_date, end_date)    
    return execute_data
    
def print_param_examples():
    crawl_manager = 'python crawl_manager.py'
    crawl_command = '-i <stockid> -d <days> -s <start_date> -e <end_date>'
    print(crawl_manager, crawl_command)
    # crawl specific stockid in period:
    crawl_1 = '-i "00001" -d 5'
    print(crawl_manager, crawl_1)
    # crawl specific stockid between start_date and end_date:
    crawl_2 = '-i "00001" -s "2018-01-02" -e "2018-02-28"'
    print(crawl_manager, crawl_2)
    # crawl specific stockid missing data
    crawl_3 = '-i "00001" -d 0'
    print(crawl_manager, crawl_3)
    # crawl list between start_date and end_date:
    crawl_n1 = '-s "2018-01-02" -e "2018-02-28"'
    print(crawl_manager, crawl_n1)
    # crawl list in period:
    crawl_n2 = '-d 5'
    print(crawl_manager, crawl_n2)
    # re-crawl missing stockid by comparing MySQL:
    crawl_n3 = '-d 0'
    print(crawl_manager, crawl_n3)
    
def print_params(stockid, days, start_date, end_date): 
    print('StockID', stockid)
    print('Days', days)
    print('Start_Date', start_date)
    print('End_Date', end_date)

if __name__ == "__main__":
    main(sys.argv[1:])