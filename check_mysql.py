# -*- coding: utf-8 -*-
"""
Created on Sat Apr 21 20:26:00 2018

@author: Easy
test tencent cdb_mysql
"""

import MySQLdb

MYSQL_HOST = 'gz-cdb-1ic3jobq.sql.tencentcdb.com'
MYSQL_PORT = 62848
MYSQL_USER = 'root'
MYSQL_PASSWD = 'Qwer1asdf'

print('host is', MYSQL_HOST)

def create_ccass_db(conn, cursor):
    sql_command = 'create database ccass_db'
    cursor.execute(sql_command)
    conn.commit()
    sql_command = 'use ccass_db'
    cursor.execute(sql_command)
    conn.commit()
    sql_command = 'create table stockid_date_index (StockID VARCHAR(5), Date DATE)'
    cursor.execute(sql_command)
    conn.commit()
    
conn = MySQLdb.connect(host = MYSQL_HOST,
                       port = MYSQL_PORT,
                       user = MYSQL_USER, 
                       passwd = MYSQL_PASSWD,
                       charset = 'utf8',
                       use_unicode = True
                       )

cursor = conn.cursor()
#create_ccass_db(conn, cursor)
sql_command = 'show databases' 
cursor.execute(sql_command)
results = cursor.fetchall()
conn.close()
if results is not None:
    for result in results:
        print(result)
        

    