# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

# Common utility functions

import sys

from oslo.config import cfg

from mars.common import log as logging
from mars.common import exception as exp
from mars.common import constants


def debug_breakpoint():
    # insert a debug breakpoint
    import pdb;pdb.set_trace()

