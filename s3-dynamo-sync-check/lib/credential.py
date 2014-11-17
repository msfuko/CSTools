import os


class Credential():

    def __init__(self):
        if os.environ.has_key('AWS_ACCESS_KEY_ID') and os.environ.has_key('AWS_SECRET_ACCESS_KEY'):
            self.aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
            self.aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        else:
            raise ValueError("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")

    def get_aws_access_key(self):
        return self.aws_access_key_id

    def get_aws_secret_key(self):
        return self.aws_secret_access_key
