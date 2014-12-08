import logging
from abc import ABCMeta, abstractmethod


class AWSObject(object):

    __metaclass__ = ABCMeta

    def __init__(self, connection=None, logger=None):
        self.conn = connection
        self.logger = logger or logging.getLogger(__name__)
        self.storage_set_name = None

