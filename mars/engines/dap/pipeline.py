# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

import sys

from oslo.config import cfg

from mars.common import log as logging
from mars.common import exception as exp
from mars.common import constants
from mars.engines.dap import compute_pipe
from mars.core.index import infra_index

LOG = logging.getLogger(__name__)


def createPipelineFactory(pipeline_type,
                          layer=constants.INFRA,
                          entry_callback=None,
                          exit_callback=None):
    """global method to create the proper pipeline object
    """
    if pipeline_type == constants.RT_DATA:
        return RuntimeDataPipeline(layer=layer)
    elif pipeline_type == constants.RT_EVENT:
        return RuntimeEventPipeline(layer=layer)
    elif pipeline_type == constants.RT_ROLLUP:
        return RuntimeRollupPipeline(layer=layer)
    elif pipeline_type == constants.TIMER:
        return OfflineTimerPipeline(layer=layer)
    else:
        raise exp.InvalidValue(key="pipeline_type", value=pipeline_type)


class DataAnalyticPipeline(object):
    """Abstract class for data analytic pipeline entity
    """

    def __init__(self, pipeline_type, layer=constants.INFRA):
        """Initialize the pipeline (abstract)
        """
        self.pipeline_type = pipeline_type
        self.layer = layer
        self.pipes = []
        self.num_of_pipes = 0
        self.state = constants.PIPELINE_STATE_FREE
        self.entry_callback = self.entry_callback
        self.exit_callback = self.exit_callback

    def set_callbacks(self, entry_callback, exit_callback):
        self.entry_callback = entry_callback
        self.exit_callback = exit_callback

    def entry_callback(self, pipeline, message):
        LOG.debug(_("entry_call: enter the pipeline[%s]" %
                    (pipeline.pipeline_type)))

    def exit_callback(self, pipeline, result):
        LOG.debug(_("exit_call: quit the pipeline[%s]" %
                    (pipeline.pipeline_type)))

    def assemble_compute_pipes(self):
        """Abstract method to assemble the pipeline
        """
        pass

    def add_compute_pipe(self, pipe):
        """Method to equip a index compute pipe into this pipeline (append)
        """
        self.pipes.append(pipe)
        self.num_of_pipes += 1

    def run(self, args):
        """method to start running the pipe
        """
        msg = args  # so far message body is the single argument

        # call the entry callback first
        self.state = constants.PIPELINE_STATE_ENTRY
        pipe_result = self.entry_callback(self, msg)

        # call the compute pipes by order along the pipeline
        self.state = constants.PIPELINE_STATE_IN_USE
        for pipe in self.pipes:
            pipe.prepare(context=pipe_result, message=msg)
            pipe.compute(context=pipe_result, message=msg)
            pipe_result = pipe.finish(context=pipe_result, message=msg)

        # finally call the exit callback
        self.state = constants.PIPELINE_STATE_EXIT
        self.exit_callback(self, pipe_result)
        self.state = constants.PIPELINE_STATE_FREE

    def get_state(self):
        return self.state


class RuntimeDataPipeline(DataAnalyticPipeline):
    """Class represents the pipeline which has runtime derived data processed
    """

    def __init__(self, layer=constants.INFRA):
        super(RuntimeDataPipeline, self).__init__(
                constants.RT_DATA, layer)

    def assemble_compute_pipes(self):
        """Method assembes the associated compute pipes into
           runtime data processing pipeline.

           The compute pipes installed along the runtime data pipeline are:
           workload, abnormality, health, stress, risk
        """
        if self.layer == constants.INFRA:
            index_cls_list = [infra_index.WorkloadIndex,
                              infra_index.AbnormalityIndex,
                              infra_index.HealthIndex,
                              infra_index.StressIndex,
                              infra_index.RiskIndex]
            for index_cls in index_cls_list:
                index = index_cls()
                pipe = compute_pipe.ComputePipe(index)
                super(RuntimeDataPipeline, self).add_compute_pipe(pipe)


class RuntimeEventPipeline(DataAnalyticPipeline):
    """Class represents the pipeline which has runtime events processed
    """

    def __init__(self, layer=constants.INFRA):
        super(RuntimeEventPipeline, self).__init__(
                constants.RT_EVENT, layer)

    def assemeble_compute_pipes(self):
        pass


class RuntimeRollupPipeline(DataAnalyticPipeline):
    """Class represents the pipeline which has runtime rollup events processed
    """

    def __init__(self, layer=constants.INFRA):
        super(RuntimeRollupPipeline, self).__init__(
                constants.RT_ROLLUP, layer)

    def assemeble_compute_pipes(self):
        pass


class OfflineTimerPipeline(DataAnalyticPipeline):
    """Class represents the pipeline which has offline timer events processed
    """

    def __init__(self, layer=constants.INFRA):
        super(OfflineTimerPipeline, self).__init__(
                constants.TIMER, layer)

    def assemeble_compute_pipes(self):
        pass











