#!/usr/bin/python

import json
import pika
import argparse

msgbusUrl = "amqp://guest:guest@localhost:5672/%2f"
exchName = "derived_data_exchange"
publishInterval = 1 # second
msgCount = 10
msgType = "PUBLISH_DERIVED_DATA"
routingKey = "data.derived"
exchType = "topic"

# parse the arguments
argParser = argparse.ArgumentParser(
                description="Program to recieve the message")
argParser.add_argument("--exch_type",
                        default="topic",
                        help="Exchange type: topic, direct, fanout")
argParser.add_argument("--exch_name",
                        default=exchName,
                        help="Exchange name")
argParser.add_argument("--routing_key",
                        help="Routing key")

args = argParser.parse_args()
exchName = args.exch_name
exchType = args.exch_type
routingKey = args.routing_key



# Connect to the broker
parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2f')
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# declare the exchange
channel.exchange_declare(exchange=exchName,
                         exchange_type=exchType,
                         durable=True,
                         auto_delete=False)

# declare the queue
result = channel.queue_declare(queue="queue_receive", exclusive=True)
channel.queue_bind(exchange=exchName,
                   queue="queue_receive",
                   routing_key=routingKey)

print ' [*] waiting for data. To exit press ctrl+c'

def callback_receive(channel, method, header, body):
    msg = json.loads(body)
    print " [x] get message from topic: %r" % (msg,)
    channel.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(callback_receive,
                      queue="queue_receive",
                      no_ack=False,
                      consumer_tag="receive_log")

print "Ready for data!"
channel.start_consuming()
