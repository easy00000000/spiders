# -*- coding: utf-8 -*-
"""
Created on Sun Apr  8 15:08:54 2018

@author: Easy
"""
########################################
# Broker Seeting
########################################
BROKER_URL = 'amqp://guest:guest@67.209.179.247:5672'
BACKEND = 'rpc'

CELERY_DISABLE_RATE_LIMITS = True

CELERY_TIMEZONE = "Asia/Shanghai"
CELERY_ENABLE_UTC = True

CELERY_MESSAGE_COMPRESSIOM = 'gzip'