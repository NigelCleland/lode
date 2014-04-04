import psycopg2 as pg2
import simplejson
import pandas.io.sql as psql
import os

# Do some wizardry to get the location of the config file
file_path = os.path.abspath(__file__)
module_path = os.path.split(os.path.split(file_path)[0])[0]
config_name = os.path.join(module_path, 'config.json')

class NZEMDB(object):
    """docstring for NZEMDB"""
    def __init__(self, DB_KEY):
        super(NZEMDB, self).__init__()

        self.refresh_config()

        self.db_key = DB_KEY
        self.schemas = self.CONFIG[DB_KEY]["schemas"]

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

        with open(csvfile, 'rb') as f:
            header = f.readline()

        tabname="%s(%s)" % (table, header)

        query = """COPY %s FROM '%s' DELIMITER ',' CSV HEADER;""" % (tabname, csvfile)
        self.execute_and_commit_sql(query)


    def execute_and_commit_sql(self, sql):

        with pg2.connect(self.conn_string) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                conn.commit()


    def execute_and_fetchall_sql(self, sql):
        with pg2.connect(self.conn_string) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                records = curs.fetchall()

        return records


    def get_column_names(self, table):

        sql = """SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='%s'""" % table

        return execute_and_fetchall_sql(sql)


    def create_all_tables(self):

        for key in self.schemas.keys():
            with open(self.schemas[key], 'rb') as f:
                sql = f.read()

            sql = """%s""" % sql
            self.execute_and_commit_sql(sql)


    def query_to_df(self, sql):

        with pg2.connect(self.conn_string) as conn:
            df = psql.read_sql(sql, conn)

        return df


    def insert_many_csv(self, table, folder):

        allcsv_files = glob.glob(folder + "/*.csv")
        for f in allcsv_files:
            self.insert_from_csv(table, f)


if __name__ == '__main__':
    pass





