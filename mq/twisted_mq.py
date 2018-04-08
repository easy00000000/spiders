# -*- coding: utf-8 -*-
"""
Created on Sat Mar 31 13:23:03 2018

@author: E
"""

import pika
from pika import exceptions
from pika.adapters import twisted_connection
from twisted.internet import defer, reactor, protocol, task
import json

# Rabbitmq Settings
RABBIT_BROKER_HOST = '67.209.179.247'
RABBIT_BROKER_PORT = 5672

exchange_mq='ccass_exchange'
routing_key_mq='ccass'
queue_mq='ccass'

queue_file = 'queue.txt'

@defer.inlineCallbacks
def run(connection):
    channel = yield connection.channel()
    yield channel.exchange_declare(exchange=exchange_mq, passive=False, durable=True, auto_delete=False)
    yield channel.queue_declare(queue=queue_mq)
    yield channel.queue_bind(exchange=exchange_mq, queue=queue_mq, routing_key=routing_key_mq)
    yield channel.basic_qos(prefetch_count=1)
    queue_object, consumer_tag = yield channel.basic_consume(queue=queue_mq, no_ack=False)
    l = task.LoopingCall(read, queue_object)
    l.start(0.01)
    
@defer.inlineCallbacks
def read(queue_object):        
    ch,method,properties,body = yield queue_object.get()
    if body:
        print (body)
        with open(queue_file, 'w') as outfile:  
            json.dump(body, outfile)
        #msg_dict = json.loads(body)
        #stockid = str(msg_dict['stockid'])
        #sdate = str(msg_dict['sdate'])
    yield ch.basic_ack(delivery_tag=method.delivery_tag)

credentials = pika.PlainCredentials("guest", "guest")
conn_params = pika.ConnectionParameters(credentials=credentials, socket_timeout=1)
cc = protocol.ClientCreator(reactor, twisted_connection.TwistedProtocolConnection, conn_params)
d = cc.connectTCP(RABBIT_BROKER_HOST, RABBIT_BROKER_PORT)
d.addCallback(lambda protocol: protocol.ready)
d.addCallback(run)
reactor.run()
