# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 18:06:08 2018

@author: Easy
"""

from datetime import date, timedelta
import json

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