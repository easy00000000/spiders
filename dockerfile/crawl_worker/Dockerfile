# -*- coding: utf-8 -*-
#Created on Wed Apr 11 17:38:13 2018
#
#@author: Easy

FROM python:3

RUN apt-get update

# USE vim and vi editor
RUN apt-get install -y --no-install-recommends \
        vim-tiny \
    && alias vim=vim.tiny

# PIP update
RUN pip install --upgrade pip

# PIP some tools
RUN pip install openpyxl
RUN pip install bs4

# ADD Python Mongo Lib
RUN pip install pymongo
RUN pip install log4mongo

# ADD Python MySQLDB Lib
RUN pip install mysqlclient

# ADD Python Celery Lib
RUN pip install celery

# ADD Python Scrapy Lib
RUN pip install scrapy

CMD ["bash", "/home/work/spiders/load_celery.sh"]
