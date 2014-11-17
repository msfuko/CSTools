import logging


class DynamoDB():

    def __init__(self, connection=None, logger=None):
        self.conn = connection
        self.logger = logger or logging.getLogger(__name__)

    def get_table(self, name):
        return self.conn.get_table(name)

    def get_table_item(self, table, hash_key, range_key=None):
        try:
            item = table.get_item(hash_key=hash_key, range_key=range_key)
            return item
        except Exception as e:
            from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
            if isinstance(e, DynamoDBKeyNotFoundError):
                self.logger.error(e.message + " : " + hash_key)
            return None

    def scan(self, table):
        return table.scan()