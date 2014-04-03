import psycopg2 as pg2
import simplejson
import pandas.io.sql as psql

class NZEMDB(object):
    """docstring for NZEMDB"""
    def __init__(self, DB_KEY):
        super(NZEMDB, self).__init__()

        self.refresh_config()

        self.db_key = DB_KEY
        self.schemas = self.CONFIG[db_key]["schemas"]

        self.db = self.CONFIG[DB_KEY]["database_name"]
        self.user = self.CONFIG[DB_KEY]["database_user"]
        self.host = "localhost"
        self.password = self.CONFIG[DB_KEY]["database_pass"]
        self.conn_string = "dbname=%s user=%s password=%s host=%s" % (
                            self.db, self.user, self.password, self.host)


    def refresh_config(self):
        """ This permits hot loading of the config file instead of linking
        it to only be initialised on startup
        """
        with open(config_name, 'rb') as f:
            self.CONFIG = simplejson.load(f)

        return self


    def insert_from_csv(self, table, csvfile):

        query = """COPY %s FROM %s DELIMITER ',' CSV HEADER;"""
        self.execute_and_commit_sql(query)


    def execute_and_commit_sql(self, sql):

        with pg2.connect(self.conn_string) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                conn.commit()


    def create_all_tables(self):

        for key in self.schemas.keys():
            schema = self.schemas[key]
            self.execute_and_commit_sql(schema)


    def query_to_df(self, sql):

        with pg2.connect(self.conn_string) as conn:
            df = psql.read_sql(sql, conn)

        return df





