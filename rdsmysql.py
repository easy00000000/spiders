# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 10:00:43 2018

@author: E

Test RDS MySQL
"""

import MySQLdb
from crawl_worker.crawl_worker import settings

# MySQL Settings
# MYSQL_HOST = 'rm-bp1jgnu4ky75e1r3v8o.mysql.rds.aliyuncs.com'
# MYSQL_USER = 'root'
# MYSQL_PASSWD = 'Qwer1asdf'
# DB = 'mysql'
#CCASS_DB = 'ccass_db'
#StockID_Index_Table = 'stockid_date_index'
print('host is', settings.MYSQL_HOST)

conn = MySQLdb.connect(host = settings.MYSQL_HOST,
                       user = settings.MYSQL_USER, 
                       passwd = settings.MYSQL_PASSWD,
                       charset = 'utf8',
                       use_unicode = True
                       )

cursor = conn.cursor()
sql_command = 'show databases' #'SELECT StockID, Date FROM ' + StockID_Index_Table + ' GROUP BY StockID, Date'
cursor.execute(sql_command)
results = cursor.fetchall()
conn.close()
if results is not None:
    for result in results:
        print(result)
