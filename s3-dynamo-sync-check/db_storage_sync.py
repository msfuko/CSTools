import argparse
import logging.config
import threadpool
from lib.connection import Connection
from lib.s3 import S3
from lib.dynamodb import DynamoDB

BATCH_QUERY_CNT = 10000


def s3_checker(key):
    """check result_set is all in DynamoDB
    """
    global db_connection, db_table
    db = DynamoDB(connection=db_connection.new_connection())
    if db:
        table = db.get_storage_set(db_table)
        item = db.get_item(table, hash_key=key.name)
        if item:
            pass
        else:
            # the ones not in dynamoDB but in s3
            return key.name


def dynamodb_checker(key):
    #TODO - merge with s3_checker
    key = key['FileKey']
    global s3_connection, s3_bucket
    s3 = S3(connection=s3_connection.new_connection())
    if s3:
        bucket = s3.get_storage_set(s3_bucket)
        s3obj = s3.get_item(bucket, key)
        return None if s3obj else key
    return key


def nonsync_logging(request, key):
    """the ones in s3 but not in DB
    """
    if key:
        logger.info("Not Sync object - " + key)


def get_result_set(region, bucket, table, base, marker):
    connection = Connection(base, region)
    base = base.lower()
    storage_obj = None
    base_set = None

    if base == "s3":
        base_set = bucket
        storage_obj = S3(connection=connection.new_connection())
    elif base == "dynamodb":
        base_set = table
        storage_obj = DynamoDB(connection=connection.new_connection())
    storage_set = storage_obj.get_storage_set(base_set)
    result_set = storage_obj.get_batch_list(storage_set, marker, BATCH_QUERY_CNT)
    #result_set = storage_obj.list(storage_set) if storage_set else None

    return result_set


def get_next_marker(base, result_set):
    if base == "s3":
        return result_set[-1].name
    elif base == "dynamodb":
        return result_set.kwargs['exclusive_start_key'].values()


def main(region, bucket, table, base='S3', threadcnt='1'):
    marker = None
    while True:
        result_set = get_result_set(region, bucket, table, base, marker)
        #if not len(result_set):
        if not result_set:
            print "BBBBBREAK"
            break

        function = globals()[base + "_checker"]
        pool = threadpool.ThreadPool(int(threadcnt))
        reqs = threadpool.makeRequests(function, result_set, nonsync_logging)
        [pool.putRequest(req) for req in reqs]
        pool.wait()
        marker = get_next_marker(base, result_set)
        print marker


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
    fh = logging.FileHandler('/tmp/abc.log')
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
        logger.info("Start to process")
        main(args.region, args.bucket, args.table, args.base, args.threadcount)
    else:
        raise ImportError("No DB connection")