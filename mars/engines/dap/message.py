# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

import sys

from oslo.config import cfg

from mars.common import log as logging
from mars.common import exception as exp
from mars.common import constants

LOG = logging.getLogger(__name__)


class Message(object):
    """Abstract class represents a mesage object
    """
    payload_properties = ["header", "body", "extension"]
    header_properties = ["message_type",
                         "message_subtype",
                         "message_source",
                         "associated_objects"]

    def __init__(self):
        self.message_payload = None

    def __init__(self, message):
        self._message_payload = message
        if self._message_payload is None:
            raise.exp.InvalidValue(key="message", value="None")
        self._message_header = message['header']
        self._message_body = message['body']
        self._message_ext = message['extension']

    def validate_message(self, message):
        """Method to validate the message in high level
        """
        for p in message.keys():
            if p in payload_properties:
                continue
            else:
                raise.exp.InvalidValue(key=p, value=message[p])
        if message['header'] is None:
            raise.exp.InvalidValue(key="header", value="None")

    @property
    def message_type(self):
        name = header_properties[0]
        return self._message_header[name]

    @message_type.setter
    def message_type(self, value):
        name = header_properties[0]
        self._message_header[name] = value

    @property
    def message_subtype(self):
        name = header_properties[1]
        return self._message_header[name]

    @message_subtype.setter
    def message_subtype(self, value):
        name = header_properties[1]
        self._message_header[name] = value

    @property
    def message_source(self):
        name = header_properties[2]
        return self._message_header[name]

    @message_source.setter
    def messasge_source(self, value):
        name = header_properties[2]
        self._message_header[name] = value

    @property
    def associated_objects(self):
        name = header_properties[3]
        return self._message_header[name]

    @message_source.setter
    def messasge_source(self, value):
        name = header_properties[3]
        self._message_header[name] = value


class RuntimeDataMessage(Message)
    """Class represents the runtime derived data message
    and the associated operations
    """

    def __init__(self):
        self._message_payload = {
           'header': { 'message_type': constants.RT_DATA,
                       'message_subtype': "",
                       'message_source': "",
                       'associated_objects': {}
           },
           'body': {},
           'extension': {}
        }

    def __init__(self, message):
        super(RuntimeDataMessage, self).__init__(message)

    def validate_message(self, message):
        """Method to validate the message in high level
        """
        for p in message.keys():
            if p in payload_properties:
                continue
            else:
                raise.exp.InvalidValue(key=p, value=message[p])
        if message['header'] is None:
            raise.exp.InvalidValue(key="header", value="None")
