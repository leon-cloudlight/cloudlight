# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

# This module implements the skeleton of index compute pipe
# unit which is plugged into the dap pipeline.

import sys

from oslo.config import cfg

from mars.common import log as logging
from mars.common import exception as exp
from mars.common import constants

LOG = logging.getLogger(__name__)


class ComputePipe:
    """This class represents the index compute pipe unit

        A compute pipe object should bind with a specific
        index object.
    """

    def __init__(self, index):
        self.index_obj = index

    def prepare(self, context, message):
        LOG.debug(_("prepare: %s" % message))
        pass

    def compute(self, context, message):
        LOG.debug(_("compute: %s" % message))
        pass

    def finish(self, context, message):
        LOG.debug(_("finish: %s" % message))
        pass









