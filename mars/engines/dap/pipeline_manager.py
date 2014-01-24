# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

# This module implements the DAP pipeline manager which
#   - manages the pipeline set
#   - provide API to query the pipeline status
#   - allow plug in the index compute units
#   - provide the callback entry to drive the data along
#       the index compute units
#   - provide callbacks in each stage

import sys
import threading
import threadpool
import time
import Queue
#sys.path.append('../../..')

from oslo.config import cfg

from mars.common import log as logging
from mars.common import exception as exp
from mars.common import constants
from mars.engines.dap import pipeline

LOG = logging.getLogger(__name__)

pipeline_opts = [
    cfg.IntOpt('num_of_infra_rt_data_pipelines', default=8,
               help=_("Num of RT data pipelines in infrastructure layer")),
    cfg.IntOpt('num_of_infra_rt_event_pipelines', default=4,
               help=_("Num of RT event pipelines in infrastructure layer")),
    cfg.IntOpt('num_of_infra_rollup_pipelines', default=8,
               help=_("Num of rollup pipelines in infrastructure layer")),
    cfg.IntOpt('num_of_infra_timer_pipelines', default=4,
               help=_("Num of offline timer pipelines in infrastructure layer")),
    cfg.IntOpt('num_of_rt_data_workers', default=2,
               help=_("Num of workers for RT data processing")),
    cfg.IntOpt('num_of_rt_event_workers', default=1,
               help=_("Num of workers for RT event processing")),
    cfg.IntOpt('num_of_rollup_workers', default=1,
               help=_("Num of workers for rollup processing")),
    cfg.IntOpt('num_of_timer_workers', default=2,
               help=_("Num of workers for timer event processing")),
]

cfg.CONF.register_opts(pipeline_opts)

class DataAnalyticPipelineManager(object):
    """Class for data analytic compute pipeline manager
    """
    _instance_infra = None

    def __init__(self, layer=constants.INFRA):
        """Initialize the DAP manager with given model layer
            Pre-allocate the pipelines as well as equip each pipeline.
        """
        self.layer = layer
        self.pipelines_free = {}
        self.cfgs = [
            (constants.RT_DATA,
             cfg.CONF.num_of_infra_rt_data_pipelines,
             cfg.CONF.num_of_rt_data_workers),
            (constants.RT_EVENT,
             cfg.CONF.num_of_infra_rt_event_pipelines,
             cfg.CONF.num_of_rt_event_workers),
            (constants.RT_ROLLUP,
             cfg.CONF.num_of_infra_rollup_pipelines,
             cfg.CONF.num_of_rollup_workers),
            (constants.TIMER,
             cfg.CONF.num_of_infra_timer_pipelines,
             cfg.CONF.num_of_timer_workers),
        ]
        self.threadpools = {}
        self.locks = {}
        self.message_queues = {}
        self.dispatch_threads = {}

        # Alocate the initial resources
        # options[0]: type, options[1]: # of pipeline
        # options[2]: # of workers
        for options in self.cfgs:
            self.pipelines_free[options[0]] = Queue.Queue()
            self.message_queues[options[0]] = Queue.Queue()
            self.threadpools[options[0]] = (
                threadpool.ThreadPool(options[2]))
            self.dispatch_threads[options[0]]['thread'] = (
                    threading.Thread(target=self.dispatch_thread,
                                     args=(options[0],)))
            self.dispatch_threads[options[0]]['quit'] = False
            for i in range(options[1]):
                self.pipelines_free[options[0]].put(
                    pipeline.createPipelineFactory(pipeline_type=options[0],
                                                   layer=self.layer))


    @classmethod
    def _create_instance(cls, layer=constants.INFRA):
        if layer == constants.INFRA:
            if cls._instance_infra is None:
                cls._instance_infra = cls(layer)
            return cls._instance_infra

    @classmethod
    def get_instance(cls, layer=constants.INFRA):
        if layer == constants.INFRA:
            return cls._create_instance(layer)

    def get_num_of_pipelines(self, pipeline_type):
        for options in self.cfgs:
            if pipeline_type == options[0]:
                return options[1]

    def get_num_of_free_pipelines(self, pipeline_type):
        return self.pipelines_free[pipeline_type].qsize()

    def allocate_pipeline(self, pipeline_type):
        """Method to allocate a pipeline and assemble it with pipes
        """
        try:
            pl = self.pipelines_free[pipeline_type].get(timeout=30)
            if pl is None:
                raise exp.MarsPipelinesError(
                        msg=_("Invalid pipeline instance")
            pl.assemble_compute_pipes()
            return pl
        except Queue.Empty:
            # exception due to empty pipeline slots [timeout]
            msg = "Failed to allocate the free pipeline[%s]" % pipeline_type;
            LOG.error(_("%s" % msg))
            raise exp.MarsPipelinesError(msg=msg)

    def free_pipeline(self, pipeline):
        """Method to free the pipeline object and dismiss the
        compute pipes plugged into it
        """

        if not pipeline is None:
            pl_type = pipeline.pipeline_type
            pipeline.dismiss_compute_pipes()
            self.pipelines_free[pl_type].put(pipeline)

    def start_processing(self, pipeline_type):
        """Method to poll the messages from queue and process
           it by one of worker threads
        """
        self.dispatch_threads[pipeline_type]['thread'].start()

    def stop_processing(self, pipeline_type):
        """Method to stop polling the messages processing
        """
        self.dispatch_threads[pipeline_type]['quit'] = True
        self.dispatch_threads[pipeline_type]['thread'].join(timeout=30)

    def dispatch_thread(self, args):
        """Dispatch thread is used to poll the pipeline workers
        """
        LOG.debug(_("dispatch_thread: starts polling pipeline[%s]" % args))
        pipeline_type = args
        while not self.dispatch_threads[pipeline_type]['quit']:
            try:
                self.threadpools[pipeline_type].poll(block=False)
                time.sleep(0.5)
            except threadpool.NoResultsPending:
                time.sleep(1)

    def dispatch_message(self, pipeline_type, message):
        """Method to dispatch the received message into the thread pool
            to process it along the pipeline
        """
        params = [{ "pipeline_type": pipeline_type,
                    "message": message }]
        requests = threadpool.makeRequests(
                        self.worker_process_callback, params,
                        self.worker_handle_result,
                        self.worker_handle_exception)
        for req in requests:
            self.threadpools[pipeline_type].putRequest(req)
            LOG.debug(_("pipeline thread id: #%s " % req.requestID))


    def worker_process_callback(self, args):
        LOG.debug(_("pipeline thread callback for processing message"))
        pl = self.allocate_pipeline(args['pipeline_type'])
        pl.run(args['message'])
        result = { 'pipeline_type': args['pipeline_type'],
                   'pipeline': pl }
        return result

    def worker_handle_result(self, request, result):
        LOG.debug(_("pipeline thread [#%s] callback for handling result" %
                    (request.requestID,)))
        pipeline_type = result['pipeline_type']
        if not result['pipeline'] is None:
            self.free_pipeline(result['pipeline'])

    def worker_handle_exception(self, request, exc):
        LOG.debug(_("pipeline thread callback for handling exception"))

        pipeline_type = result['pipeline_type']
        if not result['pipeline'] is None:
            self.free_pipeline(result['pipeline'])
        raise exp.MarsPipelineError(msg=_("%s" % exc))






