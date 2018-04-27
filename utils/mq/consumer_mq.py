# -*- coding: utf-8 -*-
"""
Created on Thu Apr  5 21:30:50 2018

@author: Easy
"""

import pika
import json

# Rabbitmq Settings
RABBIT_BROKER_HOST = '67.209.179.247'
RABBIT_BROKER_PORT = 5672

queue_file = 'queue.txt'
    
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
        with open(queue_file, 'a') as outfile:  
            json.dump(msg_dict, outfile)
            outfile.write('\n')
        
    channel.basic_consume(read_msg,
                          queue=queue,
                          consumer_tag=queue)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        return
    
rabbit_consumer()