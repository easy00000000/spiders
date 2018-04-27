# -*- coding: utf-8 -*-
"""
Created on Fri Apr 27 12:09:03 2018

@author: E
"""
import schedule
import time
import os
from subprocess import call as execute

def job():
    print("I'm working...")
    p=os.getcwd()
    f=p
    os.chdir(f)
    execute_command = 'python crawl_manager.py -d 1'        
    execute(execute_command.split())
    
#schedule.every(1).minutes.do(job)
#schedule.every().hour.do(job)
schedule.every().day.at("22:01").do(job) #UTC+8->Shanghai Time if at 5am morning of SH -> 22:01 at UTC
#schedule.every(5).to(10).days.do(job)
#schedule.every().monday.do(job)
#schedule.every().wednesday.at("5:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)