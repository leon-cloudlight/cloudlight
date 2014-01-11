# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

import sys

from oslo.config import cfg

from mars.common import log as logging
from mars.common import exception

LOG = logging.getLogger(__name__)

class MarsDataAnalyticService:
    """Class for data analytic engine main routine
    """

    def __init__(self):
        #import pdb;pdb.set_trace()
        LOG.info(_("Creating Mars data analytic engine"))

    def start(self):
        LOG.debug(_("Starting Mars data analytic engine"))

    def stop(self):
        LOG.debug(_("Stopping Mars data analytic engine"))
