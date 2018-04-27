# -*- coding: utf-8 -*-
"""
Created on Fri Apr  6 10:19:34 2018

@author: E
"""

import pika
import json

# Rabbitmq Settings
RABBIT_BROKER_HOST = '67.209.179.247'
RABBIT_BROKER_PORT = 5672
    
def rabbit_consumer(queue='ccass', exchange='ccass_exchange', routing_key='ccass'):
    credentials = pika.PlainCredentials("guest", "guest")
    conn_params = pika.ConnectionParameters(host=RABBIT_BROKER_HOST,
                                            port=RABBIT_BROKER_PORT,
                                            credentials=credentials,
                                            socket_timeout=1)
    conn_broker = pika.BlockingConnection(conn_params)
    channel = conn_broker.channel()
    
    channel.exchange_declare(exchange=exchange,
                             passive=False,
                             durable=True,
                             auto_delete=False)
    
    channel.queue_declare(queue=queue)
    channel.queue_bind(queue=queue,
                       exchange=exchange,
                       routing_key=routing_key)
    
    method_frame, header_frame, body = channel.basic_get(queue=queue)
    if method_frame:
        #print(method_frame, header_frame, body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        msg_dict = json.loads(body)
        return(msg_dict)
        #print(str(msg_dict['stockid']), str(msg_dict['sdate']))
    else:
        return None

msg_dict = rabbit_consumer()
if msg_dict is not None:
    print(str(msg_dict['stockid']), str(msg_dict['sdate']))
else:
    print('Queue Empty')