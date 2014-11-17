import argparse
import logging.config
import threadpool
from lib.connection import Connection
from lib.s3 import S3
from lib.dynamodb import DynamoDB


def s3_checker(key):
    """check result_set is all in DynamoDB
    """
    global db_connection, db_table
    db = DynamoDB(connection=db_connection.new_connection())
    if db:
        table = db.get_table(db_table)
        item = db.get_table_item(table, hash_key=key.name)
        if item:
            pass
        else:
            # the ones not in dynamoDB but in s3
            return key.name


def dynamodb_checker(key):
    key = key['FileKey']
    global s3_connection, s3_bucket
    s3 = S3(connection=s3_connection.new_connection())
    if s3:
        bucket = s3.get_bucket(s3_bucket)
        s3obj = s3.get_key(bucket, key)
        return None if s3obj else key
    return key


def nonsync_logging(request, key):
    """the ones in s3 but not in DB
    """
    if key:
        logger.info("Not Sync object - " + key)


def get_result_set(region, bucket, table, base):
    base = base.lower()
    connection = Connection(base, region)
    if base == "s3":
        s3 = S3(connection=connection.new_connection())
        bucket = s3.get_bucket(bucket)
        result_set = bucket.list() if bucket else None
    else:
        db = DynamoDB(connection=connection.new_connection())
        tbl = db.get_table(table)
        result_set = tbl.scan() if tbl else None
    return result_set


def main(region, bucket, table, base='S3', threadcnt='1'):
    result_set = get_result_set(region, bucket, table, base)
    function = globals()[base + "_checker"]
    pool = threadpool.ThreadPool(int(threadcnt))
    reqs = threadpool.makeRequests(function, result_set, nonsync_logging)
    [pool.putRequest(req) for req in reqs]
    pool.wait()


if __name__ == '__main__':
    # args
    parser = argparse.ArgumentParser(description='Central Storage S3 & DynamoDB verification tool')
    parser.add_argument("-r", "--region", type=str, help="target AWS region", required=True)
    parser.add_argument("-b", "--bucket", type=str, help="S3 bucket name", required=True)
    parser.add_argument("-t", "--table", type=str, help="DynamoDB table name", required=True)
    parser.add_argument("-c", "--threadcount", type=str, help="thread count", required=False)
    parser.add_argument("-a", "--base", type=str, help="s3 | dynamodb", required=False)

    # logging
    logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    # parse args
    args = parser.parse_args()
    db_connection = Connection("dynamodb", args.region)
    db_table = args.table
    s3_connection = Connection("s3", args.region)
    s3_bucket = args.bucket

    if db_connection:
        main(args.region, args.bucket, args.table, args.base, args.threadcount)
    else:
        raise ImportError("No DB connection")