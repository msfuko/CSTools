from .awsstorageobject import AWSStorageObject


class S3(AWSStorageObject):

    def __str__(self):
        return "s3"

    def get_storage_set(self, name=None):
        return self.conn.get_bucket(name or self.storage_set_name)

    def get_batch_list(self, bucket, marker, max_count=None):
        max_count = 100 if not max_count else max_count
        return bucket.get_all_keys(maxkeys=max_count, marker=marker)

    def list(self, bucket):
        return bucket.list()

    def get_item(self, bucket, key):
        return bucket.get_key(key)

    def get_last_item_set(self, key_set):
        return key_set[-1].name if len(key_set) else None
