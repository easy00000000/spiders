# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 18:06:08 2018

@author: Easy
"""

from datetime import date, timedelta
import json
import MySQLdb
from selenium import webdriver
from parsel import Selector
import os

def read_stocklist(d='tools/', json_file='stocklist.json'):
    stocklist = []
    with open(d+json_file, 'r') as infile:  
        stocklists = json.load(infile)
    for stock in stocklists:
        stocklist.append(stock['Code'])
    return stocklist

def open_selenium(ccass_url): # Selenium to get the 1st Form Data 
    # Use Headless Firefox
    os.environ['MOZ_HEADLESS'] = '1'    
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

def generate_formdata(view_stat, view_generator, event_valid, stockid, sdate):  # Fill Form and Return for FormRequest
    syear = str(sdate.year)
    if (sdate.month<10):
        smonth = '0' + str(sdate.month)
    else:
        smonth = str(sdate.month)
    if (sdate.day<10):
        sday = '0' + str(sdate.day)
    else:
        sday = str(sdate.day)
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

def get_formdata(execute_data):
    ccass_url = 'http://www.hkexnews.hk/sdw/search/searchsdw.aspx'
    form_data = []
    try:
        sel = open_selenium(ccass_url)
        view_stat = sel.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
        view_generator = sel.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first()
        event_valid = sel.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first()
        for d in execute_data:
            stockid, sdate = d
            f = generate_formdata(view_stat, view_generator, event_valid, stockid, sdate)
            form_data.append(f)
        return form_data
    except Exception as err:
        print(err)
        return None

def read_existing_data(h,p,ur,pw,db,st):
     # Connect to MySQL
    conn = MySQLdb.connect(host = h,
                           port = p,
                           user = ur, 
                           passwd = pw,
                           db = db,
                           charset = 'utf8',
                           use_unicode = True
                           )
    cursor = conn.cursor()
    sql_command = 'SELECT StockID, Date FROM ' + st + ' GROUP BY StockID, Date'
    cursor.execute(sql_command)
    results = cursor.fetchall()
    conn.close()
    # Load Tuple to List
    existing_data = []
    if results is not None:
        for result in results:
            # result: ('00001', datetime.date(2018, 3, 2))
            existing_data.append(result)
    return existing_data

def get_data_list(opts, existing_data):
    start_date = ''
    end_date = ''
    days = -1
    stockid = ''
    
    for opt, arg in opts:
        if opt == '-h':
            print_param_examples()
            return None
        elif opt in ("-i", "--stockid"):
            stockid = arg
        elif opt in ("-d", "--days"):
            days = int(arg)
        elif opt in ("-s", "--start_date"):
            start_date = arg
        elif opt in ("-e", "--end_date"):
            end_date = arg
           
    execute_data = []
    
    if len(stockid) == 5:
        # specifc stockid
        if days > 0:
            # crawl specific stockid in period:
            execute_data = crawl_1(stockid, days, existing_data) # execute_data is a tuple value
        elif days < 0:
            if len(start_date) > 0 and len(end_date) > 0 :
                # crawl specific stockid between start_date and end_date:
                execute_data = crawl_2(stockid, start_date, end_date, existing_data)
        elif days == 0:
            # re-crawl missing stockid by comparing MySQL:
            execute_data = crawl_3(stockid, existing_data)
            
    elif len(stockid) == 0:
        slist = read_stocklist()
        print(slist)
        # from stocklist
        if days > 0:
            # crawl in period:
            execute_data = crawl_n1(slist, days, existing_data)
        elif days < 0:
            if len(start_date) > 0 and len(end_date) > 0 :
                # crawl between start_date and end_date:
                execute_data = crawl_n2(slist, start_date, end_date, existing_data)
        elif days == 0:
            # re-crawl missing stockid by comparing MySQL: 
            execute_data = crawl_n3(slist, existing_data)
    else:
        print_param_examples()
        return None
        
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

def crawl_1(stockid, period, existing_data):
    execute_data = []
    n = 1
    while (n < period):
        if is_sunday(n):
            pass
        else:
            sdate_str = format_date(n)
            check_value = get_tuple_value(stockid, sdate_str)
            if (check_value in existing_data):
                pass
            else:
                execute_data.append(check_value)
        n = n + 1
    return execute_data

def crawl_2(stockid, start_date, end_date, existing_data):
    delta = str2date(end_date) - str2date(start_date)
    period = delta.days + 1 + 1
    execute_data = crawl_1(stockid, period, existing_data)          
    return execute_data

def crawl_3(stockid, existing_data):    
    period = 360
    execute_data = crawl_1(stockid, period, existing_data)          
    return execute_data

def crawl_n1(slist, days, existing_data):
    execute_data = []
    for stockid in slist:
        ds = crawl_1(stockid, days, existing_data)
        for d in ds:
            execute_data.append(d)
    return execute_data

def crawl_n2(slist, start_date, end_date, existing_data):
    execute_data = []
    for stockid in slist:
        ds = crawl_2(stockid, start_date, end_date, existing_data)
        for d in ds:
            execute_data.append(d)
    return execute_data

def crawl_n3(slist, existing_data):
    execute_data = []
    for stockid in slist:
        ds = crawl_3(stockid, existing_data)
        for d in ds:
            execute_data.append(d)
    return execute_data
    
def is_sunday(n):
    sdate = date.today() - timedelta(n)
    if (sdate.weekday() < 6): # Sunday = 6 and Monday = 0 
        return False
    else:
        return True

def format_date(n): # Structure Date Format 
    sdate = date.today() - timedelta(n)           
    sdate_str = date2str(sdate)
    return sdate_str

def date2str(sdate):
    syear = str(sdate.year)
    if (sdate.month<10):
        smonth = '0' + str(sdate.month)
    else:
        smonth = str(sdate.month)
    if (sdate.day<10):
        sday = '0' + str(sdate.day)
    else:
        sday = str(sdate.day)
    sdate_str = syear + '-' + smonth + '-' + sday
    return sdate_str

def str2date(sdate_str):
    sday = sdate_str.split('-')
    sdate = date(int(sday[0]),int(sday[1]),int(sday[2]))
    return sdate
        
def get_tuple_value(stockid, sdate_str): #stockid: '00001' sdate:'2017-06-01'
    sdate = str2date(sdate_str)
    tuple_value = (stockid, sdate)
    return tuple_value

def tuple2json(t):    
    stockid, sdate = t
    date_str = date2str(sdate)
    j = json.dumps({"stockid": stockid,
                    "sdate": date_str})
    return j