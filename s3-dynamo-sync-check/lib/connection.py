import importlib
from geventconnpool import ConnectionPool
from .credential import Credential


class Connection(ConnectionPool):

    def __init__(self, service, region='us-east-1'):
        ConnectionPool.__init__(self, 10)
        self.service = service
        self.region = region

    def __enter__(self):
        # Use for context management style
        return self._new_connection()

    def __exit__(self):
        # Use for context management style
        self.service = None
        self.region = None

    def new_connection(self):
        return self._new_connection()

    def _new_connection(self):
        credential = Credential()
        try:
            #print "boto.%s" % self.service
            module = importlib.import_module("boto.%s" % self.service)
            return module.connect_to_region(self.region, aws_access_key_id=credential.get_aws_access_key(),
                                            aws_secret_access_key=credential.get_aws_secret_key())
        except:
            raise RuntimeError("error connecting to AWS")
