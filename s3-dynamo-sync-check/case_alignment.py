import argparse
import logging.config
import traceback
import lib.worker
from lib.connection import Connection
from lib.s3 import S3
from lib.dynamodb import DynamoDB
from lib.mssql import MSSql
LOG_PATH = "/tmp/case_err.log"


def nonsync_logging(request, (success, key)):
    """non-sync item will be logged here
    """
    if success:
        logger.info("Success on %s" % key)
    else:
        logger.error("Not Sync object - " + key)


def case_alignment(key):
    who = None
    try:
        # get connection
        global db, s3, db_table, s3_bucket, dryrun

        # find file key
        key = key['SHA1']
        file_key = '/'.join(('frs', key[:2], key[2:5], key[5:8], key[8:13], key))

        # case update
        if not dryrun:
            who = "db"
            table = db.get_storage_set(db_table)
            item = db.get_item(table, hash_key=file_key)
            if item:
                db.update_record(table, "SHA1", item, item['SHA1'].lower())
                db.update_primary_key(table, item, file_key.lower())
            else:
                return False, "%s - %s" % (who, file_key)

        if not dryrun:
            who = "s3"
            bucket = s3.get_storage_set(s3_bucket)
            s3obj = s3.get_item(bucket, file_key)
            if s3obj:
                s3.update_primary_key(bucket, s3obj, file_key.lower())
            else:
                return False, "%s - %s" % (who, file_key)
        return True, file_key
    except:
        logger.error(traceback.format_exc())
        return False, "%s - %s" % (who, key)


def get_result_set():
    conn = MSSql("host", "username", "password")
    conn.connect("FRSCentralStorage")
    result_set = conn.query("select SHA1 from FileInfo WITH(NOLOCK) where sha1 COLLATE SQL_Latin1_General_CP1_CS_AS = upper(sha1)")
    return result_set


def get_test_result_set():
    """
    temp added for testing
    :return:
    """
    connection = Connection("dynamodb", "us-west-1")
    storage_obj = DynamoDB(connection=connection.new_connection())
    storage_obj.set_storage_set_name("cs-file-metadata")
    storage_set = storage_obj.get_storage_set()
    result_set = storage_obj.list(storage_set) if storage_set else None
    return result_set


def main(bucket, table, threadcnt):
    logger.info("Start to process - bucket=%s, table=%s" % (bucket, table))
    if test:
        result_set = get_test_result_set()
    else:
        result_set = get_result_set()
    pool = lib.worker.ThreadPool(int(threadcnt))
    logger.debug("Make request")
    reqs = lib.worker.makeRequests(case_alignment, result_set, nonsync_logging)
    logger.debug("Put request")
    [pool.putRequest(req) for req in reqs]
    logger.debug("Wait")
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
    parser.add_argument('--test', help="use test set", action='store_true')

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
    test = args.test
    db_connection = Connection("dynamodb", args.region)
    db = DynamoDB(connection=db_connection.new_connection())
    s3_connection = Connection("s3", args.region)
    s3 = S3(connection=s3_connection.new_connection())
    main(args.bucket, args.table, args.threadcount)
