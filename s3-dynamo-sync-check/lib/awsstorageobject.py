from abc import ABCMeta, abstractmethod
from awsobject import AWSObject


class AWSStorageObject(AWSObject):

    __metaclass__ = ABCMeta

    def __init__(self, connection=None, logger=None):
        super(AWSStorageObject, self).__init__(connection=connection, logger=logger)
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

    @abstractmethod
    def update_primary_key(self, storage_set, key, new_key):
        pass

    @abstractmethod
    def update_record(self, storage_set, field, key, new_key):
        pass