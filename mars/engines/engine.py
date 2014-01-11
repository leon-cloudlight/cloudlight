# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

"""
The factory API to create the Mars engines
"""

import sys

from oslo.config import cfg
from mars.engines import data_analytic
from mars.common import log as logging
from mars.common import exception

engine_opts = [
    cfg.StrOpt('engine', default="analytic",
               help=_("The engine which is running on Mars node")),
]

cfg.CONF.register_opts(engine_opts)

LOG = logging.getLogger(__name__)

def createEngineFactory():
    """This method is the creation factory for the specific
       engine running on Mars node

       The engine type is defined in config file with "--engine"
       option. The valid values are "analytic", "alert", "dt"
    """


    if cfg.CONF.engine == "analytic":
        engine = data_analytic.MarsDataAnalyticService()
    else:
        LOG.error(_("Invalid configuration for engine option: %s"),
                    cfg.CONF.engine)
        raise exception.InvalidConfigurationOption(
                opt_name="engine", opt_value=cfg.CONF.engine)

    return engine



