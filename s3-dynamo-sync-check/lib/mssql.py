import pymssql


class MSSql(object):

    def __init__(self, host, user, passwd):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.connection = None

    def connect(self, database):
        self.connection = pymssql.connect(host=self.host, user=self.user,
                                          password=self.passwd, database=database, as_dict=True)

    def query(self, query):
        cur = self.connection.cursor()
        cur.execute(query)
        return cur

    def close(self):
        self.connection.close()
