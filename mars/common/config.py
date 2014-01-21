# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

"""
Global functions for Mars configuration
"""

import os

from oslo.config import cfg
from mars.common import log as logging

VERSION = "0.0.1"

core_opts = [
    cfg.StrOpt('amqp_url_mars',
               default="amqp://guest:guest@localhost:5672/%2f",
               help=_("AMQP URL for the local broker on Mars node")),
    cfg.StrOpt('amqp_url_pluto',
               default="amqp://guest:guest@localhost:5672/%2f",
               help=_("AMQP URL for the broker on Pluto node")),
]

# Register the configuration options
cfg.CONF.register_opts(core_opts)

LOG = logging.getLogger(__name__)

def parse(args):
    """Parse the mars start-up options
    """
    cfg.CONF(args=args, project='mars', version=VERSION)

def setup_logging(conf):
    """Sets up the logging options for a log with supplied name.

    :param conf: a cfg.ConfOpts object
    """
    product_name = "mars"
    logging.setup(product_name)
    LOG.info(_("Logging enabled!"))


