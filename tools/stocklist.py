# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 18:49:24 2018

@author: Easy
"""

def read(d='queue/', f='stocklist.txt'):
    stocklist = []
    stocklist_file = open(d+f, 'r')
    stocklists = stocklist_file.readlines()
    stocklist_file.close()
    for stockid in stocklists:
        stocklist.append(stockid.split(',')[0])
    return stocklist