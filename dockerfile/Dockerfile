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

# ADD Python MySQLDB Lib
RUN pip install mysqlclient

# ADD Python Celery Lib
RUN pip install celery \
    && pip install pika

# ADD Python Scrapy Lib
RUN pip install scrapy

RUN rm -rf /var/lib/apt/lists/*         

CMD ["bash"]