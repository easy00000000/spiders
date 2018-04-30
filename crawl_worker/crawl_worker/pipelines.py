# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.conf import settings
import json
import MySQLdb
import logging

class Json_Pipeline(object):
    def open_spider(self, spider):
        self.file = open('brokerinfo.jl', 'a')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):        
        for tr in item['broker_info']:
            broker_info = []
            for td in tr.find_all('td'):
                broker_info.append(td.getText().strip())
            # Set ID for HKSFC
            if (broker_info[1] == 'HONG KONG SECURITIES CLEARING CO. LTD.'):
                broker_info[0] = 'SFC001'
            # Set empty ID = Name
            if (broker_info[0] == ''):
                broker_info[0] = broker_info[1]
            # remove Shares_Number's ','
            broker_info[3] = broker_info[3].replace(',','')
            # remove Shares_%'s '%'
            broker_info[4] = broker_info[4].replace('%','')
            br_data = {
                'StockID' : item['stockid'],
                'Date' : item['sdate'],
                'Broker_ID' : broker_info[0],
                'Broker_Name' : broker_info[1],
                'Shares_Number' : broker_info[3],
                'Share_Percent' : broker_info[4]
            }
            line = json.dumps(br_data) + "\n"
            self.file.write(line)
        return item

class MYSQL_Pipeline(object):
    def open_spider(self, spider):
        self.conn = MySQLdb.connect(host = settings.get('MYSQL_HOST'),
                                    port = settings.get('MYSQL_PORT'),
                                    user = settings.get('MYSQL_USER'), 
                                    passwd = settings.get('MYSQL_PASSWD'),
                                    db = settings.get('CCASS_DB'),
                                    charset = 'utf8',
                                    use_unicode = True
                                    )
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.conn.close()
        
    def process_item(self, item, spider):
        # Create Table of StockID if not exists
        try:
            mysql_command = "create table if not exists HK" + item['stockid'] 
            data_structure = " (StockID VARCHAR(5), \
                                Broker_ID VARCHAR(100), \
                                Broker_Name VARCHAR(100), \
                                Date DATE, \
                                Shares BIGINT, \
                                Percent FLOAT)"
            mysql_command = mysql_command + data_structure
            self.cursor.execute(mysql_command)
            self.conn.commit()
        except MySQLdb.Error as e:
            logging.error('Error %d %s' % (e.args[0], e.args[1]))
            #print('Error %d %s' % (e.args[0], e.args[1]))

        # Check Item into Index
        mysql_command = "select * from stockid_date_index where StockID=%s and Date=%s"
        self.cursor.execute(mysql_command,
                            (
                                item['stockid'],
                                item['sdate'],
                            ))
        result = self.cursor.fetchone()
        if result:
            pass
        else:
            try:
                mysql_command = "INSERT INTO stockid_date_index (StockID, Date) VALUES (%s, %s)"
                self.cursor.execute(mysql_command,
                                    (
                                        item['stockid'],
                                        item['sdate'],
                                    ))
                self.conn.commit()
                logging.warning("[Scrapy] parse %s on the date %s" %(str(item['stockid']), str(item['sdate'])))
                #print("[Scrapy] parse %s on the date %s" %(str(item['stockid']), str(item['sdate'])))
            except MySQLdb.Error as e:
                logging.error('Error %d %s' % (e.args[0], e.args[1]))
                #print('Error %d %s' % (e.args[0], e.args[1]))
    
            # Add Items into StockID Table
            for tr in item['broker_info']:
                broker_info = []
                for td in tr.find_all('td'):
                    broker_info.append(td.getText().strip())
                # Set ID for HKSFC
                if (broker_info[1] == 'HONG KONG SECURITIES CLEARING CO. LTD.'):
                    broker_info[0] = 'SFC001'
                # Set empty ID = Name
                if (broker_info[0] == ''):
                    broker_info[0] = broker_info[1]
                # remove Shares_Number's ','
                broker_info[3] = broker_info[3].replace(',','')
                # remove Shares_%'s '%'
                if len(broker_info)<5:
                    broker_info.append('0')
                else:
                    broker_info[4] = broker_info[4].replace('%','')
                try:
                    mysql_command = "INSERT INTO " + "HK" + item['stockid'] + " (StockID, Date, Broker_ID, Broker_Name, Shares, Percent) VALUES (%s, %s, %s, %s, %s, %s)" 
                    self.cursor.execute(mysql_command,
                                        (
                                            item['stockid'],
                                            item['sdate'],
                                            broker_info[0],
                                            broker_info[1],
                                            broker_info[3],
                                            broker_info[4],
                                        ))
                    self.conn.commit()
                except MySQLdb.Error as e:
                    logging.error('Error %d %s' % (e.args[0], e.args[1]))
                    #print('Error %d %s' % (e.args[0], e.args[1]))
        return item