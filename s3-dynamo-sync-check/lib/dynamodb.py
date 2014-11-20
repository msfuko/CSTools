from .awsstorageobject import AWSStorageObject


class DynamoDB(AWSStorageObject):

    def __str__(self):
        return "dynamodb"

    def get_storage_set(self, name=None):
        return self.conn.get_table(name or self.storage_set_name)

    def get_batch_list(self, table, marker, max_count=None):
        return table.scan(request_limit=max_count, exclusive_start_key=marker)

    def list(self, table):
        return table.scan()

    def get_item(self, table, hash_key):
        try:
            #item = table.get_item(hash_key=hash_key, range_key=range_key)
            item = table.get_item(hash_key=hash_key)
            return item
        except Exception as e:
            from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
            if isinstance(e, DynamoDBKeyNotFoundError):
                self.logger.error(e.message + " : " + hash_key)
            return None

    def get_last_item_set(self, item_set):
        return item_set.last_evaluated_key[0] if item_set and item_set.last_evaluated_key else None

