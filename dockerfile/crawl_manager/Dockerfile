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

# ADD Python MySQLDB Lib
RUN pip install mysqlclient

# ADD Python Celery Lib
RUN pip install celery

# ADD Python Scrapy Lib
RUN pip install scrapy

# ADD Python Selenium Lib
RUN pip install selenium

# Install Firefox
RUN cd ~ \
    && wget https://download-installer.cdn.mozilla.net/pub/firefox/releases/58.0/linux-x86_64/en-US/firefox-58.0.tar.bz2 \
    && tar -xvjf firefox-58.0.tar.bz2 \
    && wget https://github.com/mozilla/geckodriver/releases/download/v0.19.1/geckodriver-v0.19.1-linux64.tar.gz \
    && tar -xvzf geckodriver* \
    && chmod 777 geckodriver \
    && mv ~/geckodriver /usr/local/bin \
    && mv ~/firefox /opt/. \
    && ln -sf /opt/firefox/firefox /usr/local/bin \
    && rm *.bz2 \
    && rm *.gz

RUN apt-get install -y --no-install-recommends \
        libgtk-3-dev \
        xvfb

# Clean
RUN rm -rf /var/lib/apt/lists/* \
    && apt-get autoremove -y \
    && apt-get clean

CMD ["bash"]