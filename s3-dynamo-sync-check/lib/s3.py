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

    def update_primary_key(self, bucket, key, new_key):
        """
        Copy then delete to rename
        :param bucket:
        :param key:
        :param new_key:
        :return:
        """
        key.copy(dst_bucket=bucket.name, dst_key=new_key, preserve_acl=True, encrypt_key=True)
        key.delete()