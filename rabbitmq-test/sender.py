#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
                           '10.0.1.15'))
channel = connection.channel()
