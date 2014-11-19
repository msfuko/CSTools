import logging
from abc import ABCMeta, abstractmethod


class AWSStorageObject(object):

    __metaclass__ = ABCMeta

    def __init__(self, connection=None, logger=None):
        self.conn = connection
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    def get_storage_set(self, name):
        pass

    @abstractmethod
    def list(self, storage_set):
        pass

    @abstractmethod
    def get_batch_list(self, storage_set, markers, max_count=None):
        pass

    @abstractmethod
    def get_item(self, storage_set, key):
        pass