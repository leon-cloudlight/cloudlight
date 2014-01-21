#!/usr/bin/python

"""
This script is used to generate random derived data message
to the message bus for testing purpose.

The messages are published over messagebus.
"""

import sys
import pika
import json
import time
import logging
import random
import uuid
import argparse

#
# Global variables
#
msgbusUrl = "amqp://guest:guest@localhost:5672/%2f"
derivedDataExchange = "mars_derived_data_exchange"
publishInterval = 1 # second
msgCount = 10
msgType = "data"
msgCmd = "publish_derived_data"
routingKey = "data.derived"

#
# Logging preparation
#
LOG_FORMAT = ('%(levelname) s %(asctime)s %(name) -10s [%(funcName) '
                      '-10s] %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(LOG_FORMAT)

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

#
# Define the derived data template which is used
# to generate random derived data set
#
derived_data_template = [
    { "metric": "cpu.workload", "max": 100., "min": 0. },
    { "metric": "mem.workload", "max": 100., "min": 0.},
    ]


"""
Method to generate a derived data set
"""
def new_derived_data():
    derived_data = { "timestamp": time.time(),
                     "dataset": [] }

    for metric_def in derived_data_template:
        value = random.random() * metric_def['max']
        metric_data = { metric_def["metric"]: value }
        derived_data['dataset'].append(metric_data)

    return derived_data


"""
Method to generate a derived data message
"""
def new_derived_data_msg(objId, msgType, data=None):
    if data == None:
        data = new_derived_data()

    msg = { "objectId": objId,
            "messageType": msgType,
            "messageCommand": msgCmd,
            "contents": { "derivedData": data }
          }
    return msg


msgbusConn = None
msgbusChannel = None
def init():
    logger.info("connecting to %s", msgbusUrl)
    try:
        global msgbusConn
        global msgbusChannel
        msgbusConn = pika.BlockingConnection(pika.URLParameters(msgbusUrl))
        msgbusChannel = msgbusConn.channel()
        msgbusChannel.exchange_declare(exchange=derivedDataExchange,
                                       exchange_type="topic",
                                       durable=True,
                                       auto_delete=False
                                       )
    except Exception as ex:
        logger.error("Error happens in initialization phase.")
        cleanup()

def cleanup():
    logger.info("cleaning up...")
    if msgbusChannel: msgbusChannel.close()
    if msgbusConn: msgbusConn.close()

def publish_message():
    i = 0
    obj_id = str(uuid.uuid1())
    while msgCount == 0 or i < msgCount:
        i += 1
        msg = new_derived_data_msg(obj_id, msgType)
        properties = pika.BasicProperties(app_id=__name__,
                                          content_type='application/json')
        msgbusChannel.basic_publish(exchange=derivedDataExchange,
                                    routing_key=routingKey,
                                    body=json.dumps(msg, ensure_ascii=True),
                                    properties=properties)
        logger.info(" [%d] publishing... %s to %s", i, str(msg), routingKey)
        time.sleep(publishInterval)


#
# Testing
#

if __name__ == '__main__':
    argParser = argparse.ArgumentParser(
            description = "Program to generate derived data messages")
    argParser.add_argument('--url',
                           default="amqp://guest:guest@localhost:5672/%2f",
                           help="AMQP broker URL")
    argParser.add_argument('--interval',
                           default=1,
                           type=int,
                           help="Publishing interval in seconds")
    argParser.add_argument('--exch',
                           default="mars_derived_data_exchange",
                           help="Exchange name to publish to")
    argParser.add_argument('--count',
                           default=0,
                           type=int,
                           help="Number of messages to publish")
    args = argParser.parse_args()

    msgbusUrl = args.url
    derivedDataExchange = args.exch
    publishInterval = args.interval
    msgCount = args.count

    init()
    publish_message()
    cleanup()














