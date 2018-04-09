# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 17:13:27 2018

@author: Easy
"""
import MySQLdb

# MySQL Settings
MYSQL_HOST = '172.17.0.3'
MYSQL_USER = 'root'
MYSQL_PASSWD = 'toor'
CCASS_DB = 'ccass_db'
StockID_Index_Table = 'stockid_date_index'

def read_existing_data():
     # Connect to MySQL
    conn = MySQLdb.connect(host = MYSQL_HOST,
                           db = CCASS_DB,
                           user = MYSQL_USER, 
                           passwd = MYSQL_PASSWD,
                           charset = 'utf8',
                           use_unicode = True
                           )
    cursor = conn.cursor()
    sql_command = 'SELECT StockID, Date FROM ' + StockID_Index_Table + ' GROUP BY StockID, Date'
    cursor.execute(sql_command)
    results = cursor.fetchall()
    conn.close()
    # Load Tuple to List
    existing_data = []
    for result in results:
        # result: ('00001', datetime.date(2018, 3, 2))
        existing_data.append(result)
    return existing_data