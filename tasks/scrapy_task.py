# -*- coding: utf-8 -*-
"""
Created on Sat Apr  7 22:17:28 2018

@author: Easy

Run as :
celery --loglevel=info -A tasks.scrapy_task worker --max-tasks-per-child 1
"""

from celery import Celery
from subprocess import call as execute
from datetime import datetime
import json
import os

app = Celery()
app.config_from_object('tasks.celeryconfig')
    
@app.task
def scrapy_ccass(d):
    p = os.getcwd()
    queue_folder = p+'/'+'queue'
    queue_file = datetime2filename(d=queue_folder)  
    with open(queue_file, 'a') as outfile:  
        json.dump(d, outfile)
    
    # change working directory and run spider   
    spider_folder = p+'/'+'crawl_worker'
    os.chdir(spider_folder)
    spider_name = 'get_ccass'
    execute_command = 'scrapy crawl ' + spider_name + ' -a queue_file=' + queue_file     
    execute(execute_command.split())
    
    # return working directory
    os.chdir(p)

def datetime2filename(d, l='json'):
    f = str(datetime.now())
    rs = [' ','.',':','-']
    for r in rs:
        f = f.replace(r,'')
    f = d+'/'+f+'.'+l
    return f
