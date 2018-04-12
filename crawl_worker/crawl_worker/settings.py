# -*- coding: utf-8 -*-

# Scrapy settings for crawl_ project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'crawl_worker'

SPIDER_MODULES = ['crawl_worker.spiders']
NEWSPIDER_MODULE = 'crawl_worker.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Set Log_Level: Defalut is DEBUG, Available Level include: CRITICAL, ERROR, WARNING, INFO, DEBUG
LOG_LEVEL = 'ERROR'

# Delay to Crawl Pages
DOWNLOAD_DELAY = 3.0

# Stock List Text File
STOCKLIST_FILE = '/home/work/crawl_worker/stocklist.txt'

ITEM_PIPELINES = {
    # 'crawl_worker.pipelines.Json_Pipeline': 280,
    'crawl_worker.pipelines.MYSQL_Pipeline': 300,
}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:21.0) Gecko/20130331 Firefox/21.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36'
]

# HTTP_PROXY = 'http://127.0.0.1:8123'
PROXY_FILE = '/home/work/proxy_ip/proxy_ip_list.txt'

# Retry when proxies fail
RETRY_TIMES = 10

# Retry on most error codes since proxies fail for different reasons
RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 403, 404, 407, 408, 429]

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    # Proxy
    # 'crawl_worker.middlewares.ProxyMiddleware': 80,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 100,
    'scrapy.downloadermiddlewares.httpproxy.HTTPProxyMiddleware': None,
    # User Agent
    'crawl_worker.middlewares.RandomUserAgentMiddleware': 400,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None
}

# MYSQL Setting
#MYSQL_HOST = '172.17.0.3'
MYSQL_HOST = 'rm-bp1jgnu4ky75e1r3v8o.mysql.rds.aliyuncs.com'
MYSQL_USER = 'root'
#MYSQL_PASSWD = 'toor'
MYSQL_PASSWD =  'Qwer1asdf'
CCASS_DB = 'ccass_db'
