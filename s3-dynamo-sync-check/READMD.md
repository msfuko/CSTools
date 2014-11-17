CS Tools - DynamoDB & S3 Sync Checker
=====
## Prerequisites
- [python](https://www.python.org/) 2.7.x
- [boto](https://aws.amazon.com/sdkforpython/) 2.28.0
- [paramiko](http://www.paramiko.org/)
- [threadpool](https://pypi.python.org/pypi/threadpool)
- [geventconnpool](https://pypi.python.org/pypi/geventconnpool/0.2.1)

## How to Run
```
python db_storage_sync.py -r [region] -b [bucket name] -t [table name] -c [thread num#] --base [base (s3/dynamodb)]
```
For example:
```
python db_storage_sync.py -r us-west-1 -b cs-trend -t cs-file-metadata -c 100 -a s3
```
Above command will use s3 bucket cs-trend as a base to run by 100 threads and check whether all file in the cs-file-metadata table in region us-west-1.
