# -*- coding: utf-8 -*-
"""
Created on Wed Mar  7 20:12:39 2018

@author: Easy
"""

import pika
import json

import structure_data

# Rabbitmq Settings
RABBIT_BROKER_HOST = '67.209.179.247'
RABBIT_BROKER_PORT = 5672

def rabbit_producer(bodies, queue='ccass', exchange='ccass_exchange', routing_key='ccass'):
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

    for body in bodies:
        msg = structure_data.tuple2json(body)
        msg_props = pika.BasicProperties()
        channel.basic_publish(exchange=exchange,
                              routing_key=routing_key,
                              properties=msg_props,
                              body=msg)

    conn_broker.close()
    
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
    
    def read_msg(channel, method, header, body):
        channel.basic_ack(delivery_tag=method.delivery_tag)
        msg_dict = json.loads(body)
        print(str(msg_dict['stockid']), str(msg_dict['sdate']))
        
    channel.basic_consume(read_msg,
                          queue=queue,
                          consumer_tag=queue)
    
    channel.start_consuming()