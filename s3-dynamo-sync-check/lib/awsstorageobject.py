import logging
from abc import ABCMeta, abstractmethod


class AWSStorageObject(object):

    __metaclass__ = ABCMeta

    def __init__(self, connection=None, logger=None):
        self.conn = connection
        self.logger = logger or logging.getLogger(__name__)
        self.storage_set_name = None

    def set_storage_set_name(self, name):
        self.storage_set_name = name

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def get_storage_set(self, name=None):
        pass

    @abstractmethod
    def list(self, storage_set):
        pass

    @abstractmethod
    def get_batch_list(self, storage_set, marker, max_count=None):
        pass

    @abstractmethod
    def get_item(self, storage_set, key):
        pass

    @abstractmethod
    def get_last_item_set(self, storage_subset):
        pass