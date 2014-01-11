# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

"""
Global functions for Mars configuration
"""

import os

from oslo.config import cfg

VERSION = "0.0.1"

core_opts = [
    cfg.StrOpt('amqp_url_mars',
               default="amqp://guest:guest@localhost:5672/%2f",
               help=_("AMQP URL for the local broker on Mars node")),
]

# Register the configuration options
cfg.CONF.register_opts(core_opts)

def parse(args):
    """Parse the mars start-up options
    """
    cfg.CONF(args=args, project='mars', version=VERSION)




