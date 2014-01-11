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
