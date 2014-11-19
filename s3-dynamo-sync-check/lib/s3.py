from .awsstorageobject import AWSStorageObject


class S3(AWSStorageObject):

    def get_storage_set(self, name):
        return self.conn.get_bucket(name)

    def get_batch_list(self, bucket, markers, max_count=None):
        max_count = 100 if not max_count else max_count
        return bucket.get_all_keys(maxkeys=max_count, marker=markers)

    def list(self, bucket):
        return bucket.list()

    def get_item(self, bucket, key):
        return bucket.get_key(key)