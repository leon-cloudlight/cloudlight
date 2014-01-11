#!/usr/bin/python

import pika
import sys

parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2f')
connection = pika.BlockingConnection(parameters)

channel = connection.channel()

channel.exchange_declare(exchange="logs", exchange_type="fanout")
channel.exchange_declare(exchange="logs_direct", exchange_type="direct")

message = " fanout=>".join(sys.argv[1:]) or "info: hello world."
channel.basic_publish(exchange="logs", routing_key='', body=message)
print " [fanout] sent %r" % (message,)

message = " direct=>".join(sys.argv[1:]) or "info: hello world."
channel.basic_publish(exchange="logs_direct", routing_key='leon', body=message)
print " [direct] sent %r" % (message,)

channel.close()
connection.close()
