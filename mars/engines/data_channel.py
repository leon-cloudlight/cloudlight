# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.


# This module implements the channel to handle the
# messages from the clients.

import sys
import pika
import json
import time
import threading
import threadpool

from oslo.config import cfg

from mars.common import log as logging
from mars.common import exception as exp
from mars.common import constants
from mars.engines.dap import pipeline_manager

LOG = logging.getLogger(__name__)

def createChannelHandlerFactory(channel_type):
    """Glboal factory method to create the message channel hander
    """
    if channel_type == constants.CHANNEL_DATA:
        return DataChannelHandler()
    elif channel_type == constants.CHANNEL_TIMER:
        return TimerChannelHandler()
    elif channel_type == constants.CHANNEL_CTRL:
        return ControlChannelHandler()
    else:
        raise exp.InvalidValue(key="channel_type", value=channel_type)


class ChannelHandler(object):
    """Abstract class for AMQP connections
    """
    def __del__(self):
        self.clean()

    def clean():
        pass

    def thread_consume_callback(self, args):
        self.plmgr = args['pipeline_manager']

    def thread_handle_result(self, request, result):
        pass

    def thread_handle_exception(self, request, exc):
        pass

    def callback_on_data_message(self, channel, method, headers, body):
        """The callback invoked when receives a data message from clients
        """
        msg = json.loads(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        LOG.info(_("Got a data message from app_id(%s): %r"
                    % (headers.app_id, msg))

        # No lock required since there is only one thread for data channel accept
        LOG.debug(_("Queue the data message on pipeline"))
        #TODO: build message object and handle the message according
        # to its type
        self.plmgr.dispatch_message(constants.RT_DATA, msg)
        return 0


class DataChannelHandler(ChannelHandler):
    """Class for message handler wrapper on data channel
    """

    def __init__(self):
        self.setup_amqp_channel()

    def setup_amqp_channel(self):
        """Method to setup the AMQP connections/channels/queues
        for data channel (derived data + derived event)
        """
        try:
            parameters = pika.URLParameters(cfg.CONF.amqp_url_mars)
            self.conn_local = pika.BlockingConnection(parameters)

            # create channel/exchange/queue for data analytic processing
            self.channel = self.conn_local.channel()
            self.exch_derived_data =  self.channel.exchange_declare(
                        exchange=constants.MARS_MSG_EXCHANGE_DATA,
                        exchange_type="topic",
                        durable=True,
                        auto_delete=False)
            # data and event messages are multiplexed on the same
            # channel. Create two queues to handle them separately.
            result = self.channel.queue_declare(
                        queue=constants.MARS_MSG_QUEUE_DERIVED_DATA,
                        exclusive=True)
            self.channel.queue_bind(
                    exchange=constants.MARS_MSG_EXCHANGE_DATA,
                    queue=constants.MARS_MSG_QUEUE_DERIVED_DATA,
                    routing_key="data.derived")
            result = self.channel.queue_declare(
                        queue=constants.MARS_MSG_QUEUE_DERIVED_EVENT,
                        exclusive=True)
            self.channel.queue_bind(
                    exchange=constants.MARS_MSG_EXCHANGE_DATA,
                    queue=constants.MARS_MSG_QUEUE_DERIVED_EVENT,
                    routing_key="event.derived")

        except Exception as e:
            self.clean()
            raise exp.MarsMessageError(
                    msg=_("%s: %r" % (__name__, e.message)))


    def clean(self):
        """Clean up the messagebus handlers
        """
        if self.channel: self.channel.close()
        if self.conn_local: self.conn_local.close()
        self.plmgr.stop_processing(constants.RT_DATA)
        self.plmgr.stop_processing(constants.RT_EVENT)

    def thread_consume_callback(self, args):
        LOG.debug(_("thread callback for data channel is executed"))
        super(DataChannelHandler, self).thread_consume_callback(args)
        self.channel.basic_consume(
                self.callback_on_data_message,
                queue=constants.MARS_MSG_QUEUE_DERIVED_DATA,
                no_ack=False,
                consumer_tag="derived_data")

        self.channel.basic_consume(
                self.callback_on_data_message,
                queue=constants.MARS_MSG_QUEUE_DERIVED_EVENT,
                no_ack=False,
                consumer_tag="derived_event")

        self.plmgr.start_processing(constants.RT_DATA)
        self.channel.start_consuming()

    def thread_handle_result(self, request, result):
        LOG.debug(_("handling thread result: %s" % result))

    def thread_handle_exception(self, request, exc):
        LOG.error(_("Exception occured in request #%r: %r" %
                    (request.requestID, exc)))
        raise exp.MarsChannelException(class=__name__, msg=str(exc))


class TimerChannelHandler(ChannelHandler):
    """Class for message handler wrapper on timer/rollup channel
    """

    def __init__(self):
        self.setup_amqp_channel()

    def setup_amqp_channel(self):
        """Method to setup the AMQP connections/channels/queues
        """
        try:
            parameters = pika.URLParameters(cfg.CONF.amqp_url_mars)
            self.conn_local = pika.BlockingConnection(parameters)

            # create channel/exchange/queue for data analytic processing
            self.channel = self.conn_local.channel()
            self.exch_timer =  self.channel.exchange_declare(
                        exchange=constants.MARS_MSG_EXCHANGE_TIMER,
                        exchange_type="topic",
                        durable=True,
                        auto_delete=False)
            # rollup and timer messages are multiplexed on the same
            # channel. Create two queues to handle them separately.
            result = self.channel.queue_declare(
                        queue=constants.MARS_MSG_QUEUE_ROLLUP,
                        exclusive=True)
            self.channel.queue_bind(
                    exchange=constants.MARS_MSG_EXCHANGE_TIMER,
                    queue=constants.MARS_MSG_QUEUE_ROLLUP,
                    routing_key="rollup.timer")
            result = self.channel.queue_declare(
                        queue=constants.MARS_MSG_QUEUE_TIMER,
                        exclusive=True)
            self.channel.queue_bind(
                    exchange=constants.MARS_MSG_EXCHANGE_TIMER,
                    queue=constants.MARS_MSG_QUEUE_TIMER,
                    routing_key="event.timer")

        except Exception as e:
            self.clean()
            raise exp.MarsMessageError(
                    msg=_("%s: %r" % (__name__, e.message)))


    def clean(self):
        """Clean up the messagebus handlers
        """
        if self.channel: self.channel.close()
        if self.conn_local: self.conn_local.close()
        self.plmgr.stop_processing(constants.TIMER)
        self.plmgr.stop_processing(constants.RT_ROLLUP)

    def thread_consume_callback(self, args):
        LOG.debug(_("thread callback for timer channel is executed"))
        super(TimerChannelHandler, self).thread_consume_callback(args)
        self.channel.basic_consume(
                self.callback_on_data_message,
                queue=constants.MARS_MSG_QUEUE_ROLLUP,
                no_ack=False,
                consumer_tag="timer_rollup")

        self.channel.basic_consume(
                self.callback_on_data_message,
                queue=constants.MARS_MSG_QUEUE_TIMER,
                no_ack=False,
                consumer_tag="timer_event")

        #TODO
        self.plmgr.start_processing(constants.RT_DATA)

        self.channel.start_consuming()

    def thread_handle_result(self, request, result):
        LOG.debug(_("handling thread result: %s" % result))

    def thread_handle_exception(self, request, exc):
        LOG.error(_("Exception occured in request #%r: %r" %
                    (request.requestID, exc)))
        raise exp.MarsChannelException(class=__name__, msg=str(exc))


class ControlChannelHandler(ChannelHandler):
    """Class for message handler wrapper on control channel
    """

    def __init__(self):
        self.setup_amqp_channel()

    def setup_amqp_channel(self):
        """Method to setup the AMQP connections/channels/queues
        """
        try:
            parameters = pika.URLParameters(cfg.CONF.amqp_url_pluto)
            self.conn_local = pika.BlockingConnection(parameters)

            self.channel = self.conn_local.channel()
            self.exch_control =  self.channel.exchange_declare(
                        exchange=constants.PLUTO_MSG_EXCHANGE_CTRL,
                        exchange_type="direct",
                        durable=True,
                        auto_delete=False)
            result = self.channel.queue_declare(
                        queue=constants.MARS_MSG_QUEUE_CTRL,
                        exclusive=True)
            self.channel.queue_bind(
                    exchange=constants.PLUTO_MSG_EXCHANGE_CTRL,
                    queue=constants.MARS_MSG_QUEUE_CTRL,
                    routing_key="pluto.control")

        except Exception as e:
            self.clean()
            raise exp.MarsMessageError(
                    msg=_("%s: %r" % (__name__, e.message)))


    def clean(self):
        """Clean up the messagebus handlers
        """
        if self.channel: self.channel.close()
        if self.conn_local: self.conn_local.close()

    def thread_consume_callback(self, args):
        LOG.debug(_("thread callback for control channel is executed"))
        super(ControlChannelHandler, self).thread_consume_callback(args)
        self.channel.basic_consume(
                self.callback_on_data_message,
                queue=constants.MARS_MSG_QUEUE_CTRL,
                no_ack=False,
                consumer_tag="control")

        # TODO
        self.plmgr.start_processing(constants.RT_DATA)

        self.channel.start_consuming()

    def thread_handle_result(self, request, result):
        LOG.debug(_("handling thread result: %s" % result))

    def thread_handle_exception(self, request, exc):
        LOG.error(_("Exception occured in request #%r: %r" %
                    (request.requestID, exc)))
        raise exp.MarsChannelException(class=__name__, msg=str(exc))

    def callback_on_data_message(self, channel, method, headers, body):
        result = super(ControlChannelHandler, self).callback_on_data_mesage(
                channel, method, headers, body)
        LOG.debug(_("Reply the control request..."))
        self.channel.basic_publish(body=str(result),
                                   exchange='',
                                   routing_key=header.reply_to)




