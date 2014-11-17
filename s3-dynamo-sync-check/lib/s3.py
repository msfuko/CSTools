import logging
from . import Singleton


#class S3(Singleton):
class S3():

    def __init__(self, connection=None, logger=None):
        self.conn = connection
        self.logger = logger or logging.getLogger(__name__)
        self.MAXKEYS = 100

    def get_bucket(self, name):
        return self.conn.get_bucket(name)

    def get_keys_by_markers(self, bucket, markers, max_keys=None):
        max_keys = self.MAXKEYS if not max_keys else max_keys
        return bucket.get_all_keys(maxkeys=max_keys, markers=markers)

    def get_all_keys(self, bucket):
        return bucket.list()

    def get_key(self, bucket, key):
        return bucket.get_key(key)