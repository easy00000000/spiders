# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 10:00:43 2018

@author: E

Test RDS MySQL
"""

import MySQLdb
from crawl_worker.crawl_worker import settings

print('host is', settings.MYSQL_HOST)

conn = MySQLdb.connect(host = settings.MYSQL_HOST,
                       user = settings.MYSQL_USER, 
                       passwd = settings.MYSQL_PASSWD,
                       charset = 'utf8',
                       use_unicode = True
                       )

cursor = conn.cursor()
sql_command = 'show databases' #'SELECT StockID, Date FROM ' + StockID_Index_Table + ' GROUP BY StockID, Date'
# create database ccass_db;
# use ccass_db;
# create table stockid_date_index (StockID VARCHAR(5), Date DATE);

cursor.execute(sql_command)
results = cursor.fetchall()
conn.close()
if results is not None:
    for result in results:
        print(result)
