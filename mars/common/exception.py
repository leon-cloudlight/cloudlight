# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

"""
Mars base exception definition
"""

_FATAL_EXCEPTION_FORMAT_ERRORS = False

class MarsException(Exception):
    """Base Mars Exception

    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.
    """
    message = _("An unknown exception occurred.")

    def __init__(self, **kwargs):
        try:
            super(MarsException, self).__init__(self.message % kwargs)
        except Exception:
            if _FATAL_EXCEPTION_FORMAT_ERRORS:
                raise
            else:
                # at least get the core message out if something happened
                super(MarsException, self).__init__(self.message)


class InvalidConfigurationOption(MarsException):
    message = _("An invalid value was provided for %(opt_name)s: "
                "%(opt_value)s")

class Invalid(MarsException):
    def __init__(self, message=None):
        self.message = message
        super(Invalid, self).__init__()

class MarsMessageError(MarsException):
    message = _("Error happened for messagebus handler on Mars node: "
                "%(msg)")

class PlutoMessageError(MarsException):
    message = _("Error happend for messagebus handler with Pluto node: "
                "%(msg)")

class InvalidValue(MarsException):
    message = _("Invalid variable value: %(key) = %(value)")

class MarsChannelException(MarsException):
    message = _("Error happened on message channel %(class): %(msg)")

class MarsPipelineError(MarsException):
    message = _("Error happened in event processing pipeline: %(msg)")

