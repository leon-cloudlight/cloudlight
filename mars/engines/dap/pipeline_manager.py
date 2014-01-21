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
        self.pipelines_free = {
            constants.RT_DATA: [],
            constants.RT_EVENT: [],
            constants.RT_ROLLUP: [],
            constants.TIMER: [],
        }
        self.pipelines_in_use = {
            constants.RT_DATA: [],
            constants.RT_EVENT: [],
            constants.RT_ROLLUP: [],
            constants.TIMER: [],
        }
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

        for options in self.cfgs:
            self.threadpools[options[0]] = (
                threadpool.ThreadPool(options[2]))
            self.dispatch_threads[options[0]] = (
                    threading.Thread(target=self.dispatch_thread,
                                     args=(options[0],)))
            self.locks[options[0]] = threading.Lock()
            self.message_queues[options[0]] = Queue.Queue()
            for i in range(options[1]):
                LOG.debug(_("create %d %s pipelines in %s layer") %
                          (options[1], options[0], self.layer))
                self.pipelines_free[options[0]].append(
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
        return len(self.pipelines_free[pipeline_type])

    def allocate_pipeline(self, pipeline_type):
        pl = None
        if len(self.pipelines_free[pipeline_type]) != 0:
            pl = self.pipelines_free[pipeline_type].pop()
            self.pipelines_in_use[pipeline_type].append(pl)
        if not pl is None:
            pl.assemble_compute_pipes()
        return pl

    def free_pipeline(self, pipeline):
        pl_type = pipeline.pipeline_type
        self.pipelines_in_use[pl_type].remove(pipeline)
        self.pipelines_free[pl_type].append(pipeline)

    def start_processing(self, pipeline_type):
        """Method to poll the messages from queue and process
           it by one of worker threads
        """
        self.dispatch_threads[pipeline_type].start()

    def dispatch_thread(self, args):
        LOG.debug(_("dispatch_thread: %r" % args))
        pipeline_type = args
        while True:
            try:
                self.threadpools[pipeline_type].poll(block=False)
                time.sleep(0.5)
            except threadpool.NoResultsPending:
                time.sleep(1)

    def queue_message(self, pipeline_type, message):
        """Method to queue the received message into the thread pool
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
        pipeline_type = args['pipeline_type']
        msg = args['message']
        pl = self.allocate_pipeline(pipeline_type)
        # TODO: process the message
        time.sleep(1)
        result = { 'pipeline_type': args['pipeline_type'],
                   'pipeline': pl }
        return result

    def worker_handle_result(self, request, result):
        LOG.debug(_("pipeline thread [#%s] callback for handling result" %
                    (request.requestID,)))
        LOG.debug(_("result: %r " % result))
        pipeline_type = result['pipeline_type']
        with self.locks[pipeline_type]:
            self.free_pipeline(result['pipeline'])

    def worker_handle_exception(self, request, exc):
        LOG.debug(_("pipeline thread callback for handling exception"))

        pipeline_type = result['pipeline_type']
        with self.locks[pipeline_type]:
            self.free_pipeline(result['pipeline'])

    def thread_wait(self, pipeline_type, block=True, anyone=False):
        self.threadpools[pipeline_type].poll(block=block, anyone=anyone)



# testing
if __name__ == '__main__':
    plm = DataAnalyticPipelineManager.get_instance(
           layer=constants.INFRA)
    num_data_pl = plm.get_num_of_pipelines(constants.RT_DATA)
    num_free_data_pl = plm.get_num_of_free_pipelines(constants.RT_DATA)
    print "1 ------ "
    print "num of rt_data pipeline: %d (%d)" % (num_free_data_pl, num_data_pl)

    def entry_call(pl, msg):
        print "entry_call: enter the pipeline[%s]" % (pl.pipeline_type)

    def exit_call(pl, pipe_result):
        print "exit_call: exit the pipeline[%s]" % (pl.pipeline_type)

    pl = plm.allocate_pipeline(constants.RT_DATA, entry_call, exit_call)
    print "allocate one pipeline: %s" % (str(pl))

    num_free_data_pl = plm.get_num_of_free_pipelines(constants.RT_DATA)
    print "2 ------ "
    print "num of rt_data pipeline: %d (%d)" % (num_free_data_pl, num_data_pl)
    print "num of pipes: %d" % (pl.num_of_pipes)

    print "3 ----- "
    print "run the pipeline"
    msg = "hello world"
    pl.run(msg)

    plm.free_pipeline(pl)
    num_free_data_pl = plm.get_num_of_free_pipelines(constants.RT_DATA)
    print "3 ------ "
    print "num of rt_data pipeline: %d (%d)" % (num_free_data_pl, num_data_pl)









