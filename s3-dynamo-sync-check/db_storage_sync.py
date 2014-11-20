import argparse
import logging.config
import threadpool
from lib.connection import Connection
from lib.s3 import S3
from lib.dynamodb import DynamoDB

LOG_PATH = "/tmp/abc.log"


def s3_checker(key):
    """THREAD METHOD - check result_set is all in DynamoDB
    """
    global db_connection, db_table
    db = DynamoDB(connection=db_connection.new_connection())
    if db:
        table = db.get_storage_set(db_table)
        item = db.get_item(table, hash_key=key.name)
        return None if item else key.name


def dynamodb_checker(key):
    """THREAD METHOD - check result_set is all in S3
    """
    #TODO - merge with s3_checker
    key = key['FileKey']
    global s3_connection, s3_bucket
    s3 = S3(connection=s3_connection.new_connection())
    if s3:
        bucket = s3.get_storage_set(s3_bucket)
        s3obj = s3.get_item(bucket, key)
        return None if s3obj else key


def nonsync_logging(request, key):
    """non-sync item will be logged here
    """
    if key:
        logger.info("Not Sync object - " + key)


def get_result_set(region, bucket, table, base):
    """get result set from storage object
    """
    connection = Connection(base, region)
    base = base.lower()
    storage_obj = None

    if base == "s3":
        storage_obj = S3(connection=connection.new_connection())
        storage_obj.set_storage_set_name(bucket)
    elif base == "dynamodb":
        storage_obj = DynamoDB(connection=connection.new_connection())
        storage_obj.set_storage_set_name(table)
    storage_set = storage_obj.get_storage_set()
    result_set = storage_obj.list(storage_set) if storage_set else None

    return result_set


def main(region, bucket, table, base='S3', threadcnt='1'):
    logger.info("Start to process as base %s, bucket=%s, table=%s" % (base, bucket, table))
    result_set = get_result_set(region, bucket, table, base)
    function = globals()[base + "_checker"]
    pool = threadpool.ThreadPool(int(threadcnt))
    reqs = threadpool.makeRequests(function, result_set, nonsync_logging)
    [pool.putRequest(req) for req in reqs]
    pool.wait()
    logger.info("End of process with base %s, bucket=%s, table=%s" % (base, bucket, table))

if __name__ == '__main__':
    # args
    parser = argparse.ArgumentParser(description='Central Storage S3 & DynamoDB verification tool')
    parser.add_argument("-r", "--region", type=str, help="target AWS region", required=True)
    parser.add_argument("-b", "--bucket", type=str, help="S3 bucket name", required=True)
    parser.add_argument("-t", "--table", type=str, help="DynamoDB table name", required=True)
    parser.add_argument("-c", "--threadcount", type=str, help="thread count", required=False)
    parser.add_argument("-a", "--base", type=str, help="s3 | dynamodb", required=False)

    # logging
    logging.config.fileConfig('logging.ini', disable_existing_loggers=False, defaults={'logfilename': '/tmp/mylog.log'})
    logger = logging.getLogger(__name__)

    # FIXME - file logging
    fh = logging.FileHandler(LOG_PATH)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

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