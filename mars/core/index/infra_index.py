# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

# This module contains the implementation of all status
# index for infrastructure layer

import sys

from oslo.config import cfg

from mars.common import log as logging
from mars.common import exception as exp
from mars.common import constants

LOG = logging.getLogger(__name__)


class InfraIndex:
    """Abstract class for infrastructure index
    """

    def __init__(self):
        pass


class WorkloadIndex(InfraIndex):
    """Class for workload index
    """
    def __init__(self):
        pass


class AbnormalityIndex(InfraIndex):
    """Class for abnormality index
    """
    def __init__(self):
        pass


class HealthIndex(InfraIndex):
    """Class for health index
    """
    def __init__(self):
        pass


class StressIndex(InfraIndex):
    """Class for stress index
    """
    def __init__(self):
        pass


class RiskIndex(InfraIndex):
    """Class for risk index
    """
    def __init__(self):
        pass







