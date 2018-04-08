# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 15:24:22 2018

@author: Easy
"""

import sys
import getopt

import readMySQL
import structure_data
import stocklist
import mq

def main(argv):
    start_date = ''
    end_date = ''
    days = -1
    stockid = ''
    
    try:
        opts, args = getopt.getopt(argv,"hi:d:s:e:",["stockid=","days=","start_date","end_date"])
    except getopt.GetoptError:
        print_param_examples()
        sys.exit(2)
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
    
    print_params(stockid, days, start_date, end_date)
    
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
        
    if execute_data is not None:
        for e in execute_data:            
            print(e)    
        mq.rabbit_producer(execute_data)
    
def print_param_examples():
    print('crawl-manager.py -i <stockid> -d <days> -s <start_date> -e <end_date>')
    # crawl specific stockid in period:
    print('crawl-manager.py -i "00001" -d 5')
    # crawl specific stockid between start_date and end_date:
    print('crawl-manager.py -i "00001" -s "2018-01-02" -e "2018-02-28"')
    # crawl list between start_date and end_date:
    print('crawl-manager.py -s "2018-01-02" -e "2018-02-28"')
    # crawl list in period:
    print('crawl-manager.py -d 5')
    # re-crawl missing stockid by comparing MySQL:
    print('crawl-manager.py -d 0')
    
def print_params(stockid, days, start_date, end_date): 
    print('StockID', stockid)
    print('Days', days)
    print('Start_Date', start_date)
    print('End_Date', end_date)

if __name__ == "__main__":
    main(sys.argv[1:])