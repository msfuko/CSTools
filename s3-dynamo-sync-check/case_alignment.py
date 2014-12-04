import argparse
import logging.config
import threadpool
import traceback
from lib.connection import Connection
from lib.s3 import S3
from lib.dynamodb import DynamoDB
from lib.mssql import MSSql

LOG_PATH = "/tmp/case_err.log"


def nonsync_logging(request, key):
    """non-sync item will be logged here
    """
    if key:
        logger.info("Not Sync object - " + key)


def case_alignment(key):
    try:
        # get connection
        global db_table, s3_bucket, dryrun
        db_connection = Connection("dynamodb", args.region)
        s3_connection = Connection("s3", args.region)

        # find file key
        key = key['SHA1']
        file_key = '/'.join(('frs', key[:2], key[2:5], key[5:8], key[8:13], key))
	print key, file_key

        # case update
        db = DynamoDB(connection=db_connection.new_connection())
        if db and not dryrun:
            table = db.get_storage_set(db_table)
            item = db.get_item(table, hash_key=file_key)
            db.update_primary_key(table, item, file_key.lower())
        s3 = S3(connection=s3_connection.new_connection())
        if s3 and not dryrun:
            bucket = s3.get_storage_set(s3_bucket)
            s3obj = s3.get_item(bucket, file_key)
            s3.update_primary_key(bucket, s3obj, file_key.lower())
        return None
    except:
	logger.error(traceback.format_exc())
        return key


def get_result_set():
    conn = MSSql("host", "username", "password")
    conn.connect("FRSCentralStorage")
    result_set = conn.query("SELECT top 100 SHA1 FROM FileValidation WITH (NOLOCK) WHERE BINARY_CHECKSUM(sha1) = BINARY_CHECKSUM(Upper(sha1))")
    return result_set


def main(bucket, table, threadcnt):
    logger.info("Start to process - bucket=%s, table=%s" % (bucket, table))
    result_set = get_result_set()
    pool = threadpool.ThreadPool(int(threadcnt))
    reqs = threadpool.makeRequests(case_alignment, result_set, nonsync_logging)
    [pool.putRequest(req) for req in reqs]
    pool.wait()
    logger.info("End of process with bucket=%s, table=%s" % (bucket, table))


if __name__ == '__main__':
    # args
    parser = argparse.ArgumentParser(description='Central Storage S3 & DynamoDB verification tool')
    parser.add_argument("-r", "--region", type=str, help="target AWS region", required=True)
    parser.add_argument("-b", "--bucket", type=str, help="S3 bucket name", required=True)
    parser.add_argument("-t", "--table", type=str, help="DynamoDB table name", required=True)
    parser.add_argument("-c", "--threadcount", type=str, help="thread count", default="1", required=False)
    parser.add_argument('--dryrun', action='store_true')

    # logging
    logging.config.fileConfig('logging.ini', disable_existing_loggers=False, defaults={'logfilename': LOG_PATH})
    logger = logging.getLogger(__name__)

    # FIXME - file logging
    fh = logging.FileHandler(LOG_PATH)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # parse args
    args = parser.parse_args()

    db_table = args.table
    s3_bucket = args.bucket
    dryrun = args.dryrun
    main(args.bucket, args.table, args.threadcount)
