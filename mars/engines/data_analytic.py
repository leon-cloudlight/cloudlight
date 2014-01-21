# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

import sys
import pika
import json
import threadpool

from oslo.config import cfg

from mars.common import utils
from mars.common import log as logging
from mars.common import exception as exp
from mars.common import constants
from mars.engines import data_channel
from mars.engines.dap import pipeline_manager


LOG = logging.getLogger(__name__)

#import pdb;pdb.set_trace()

class MarsDataAnalyticService:
    """Class for data analytic engine main routine
    """

    def __init__(self):
        """Prepare the data analytic service
        """
        LOG.info(_("Creating Mars data analytic engine"))
        try:
            self.prepareMessageHandlers()
            self.prepareDAP()  # Data Analytic Pipeline
            self.prepareMOM()  # Monitor Data Model
#            self.prepareIDM()  # Index Data Model
        except exp.MarsException as e:
            LOG.error(_("Error in data analytic initialization: %s" % e))
            raise

    def __del__(self):
        self.cleanup()

    def prepareMessageHandlers(self):
        """Prepare the messagebus handlers (for both local and remote brokers)
        """
        try:
            self.channels = []
            for t in [constants.CHANNEL_DATA, constants.CHANNEL_TIMER,
                      constants.CHANNEL_CTRL]:
                channelHdr = data_channel.createChannelHandlerFactory(t)
                self.channels.append(channelHdr)
        except exp.MarsChannelException as e:
            LOG.error(_("Error in setting up channel handlers: %s" % str(e)))
            raise

    def cleanup(self):
        """Clean up the data analytic instance
        """
        pass

    def prepareDAP(self):
        """Prepare the data analytic pipeline

        Setup the DAP framework and create a set of pre-defined
        pipelines for RT/OL data/events analytic on different layers.
        Index compute units are assembed into the pipeline accordingly.

        A single pipeline is contextless (except it's own state) until
        a data/event is injected, in another word, it's completely data
        driven.

        The procesing routine associated with a pipeline is allocated to
        a thread instance in a thread pool for load balancing and scheduling.
        """
        try:
            LOG.info(_("preparing DAP"))
            self.infra_plmgr = (
                    pipeline_manager.DataAnalyticPipelineManager.get_instance(
                        layer=constants.INFRA))
        except exp.MarsPipelineError as e:
            LOG.error(_("Fail to setup data analytic pipeline: %s" % str(e)))
            raise

    def prepareMOM(self):
        """Prepare the montior data model

        Read from the persistent DB about the monitored data model on this
        node as well as the policy associated with the monitored objects.

        Construct the in-memory object structure for fast query/reference.
        """
        pass


    def start(self):
        """The main loop to handle the events
        """
        LOG.info(_("Starting Mars data analytic engine"))
        self.thread_pool = threadpool.ThreadPool(len(self.channels))
        requests = []
        params = [{'pipeline_manager': self.infra_plmgr}]
        for channel in self.channels:
            request = threadpool.makeRequests(
                                    channel.thread_consume_callback,
                                    params, channel.thread_handle_result,
                                    channel.thread_handle_exception)
            requests = requests + request
        for req in requests:
            self.thread_pool.putRequest(req)

        self.thread_pool.wait()

    def stop(self):
        LOG.info(_("Stopping Mars data analytic engine"))
        self.thread_pool.dismissWorkers(3)
        if self.dismissedWorkers:
            self.joinAllDismissedWorkers()





