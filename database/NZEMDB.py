import psycopg2 as pg2
import simplejson
import pandas.io.sql as psql
import os
import shutil
import glob
import sh
import csv
import re
import datetime

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


    def get_year(self, fName):
        """ Parse a filename to get the year it is in """
        year = re.findall(r"\d+", os.path.basename(fName))[0]

        if len(year) == 6:
            date = datetime.datetime.strptime(year, "%Y%m")
        else:
            date = datetime.datetime.strptime(year, "%Y%m%d")

        return date.strftime("%Y")


    def insert_from_csv(self, table, csvfile):
        year = self.get_year(csvfile)
        table_year = "%s_%s" % (table, year)
        table_headers = self.get_column_names(table_year)
        insert_headers = ",".join([x[0] for x in table_headers if "key" not in x[0]])
        tabname = "%s(%s)" % (table_year, insert_headers)

        existing_headers = self.check_csv_headers(csvfile, table_headers)

        if existing_headers:
            query = """COPY %s FROM '%s' DELIMITER ',' CSV HEADER;""" % (tabname, csvfile)
        else:
            query = """COPY %s FROM '%s' DELIMITER ',' CSV;""" % (tabname, csvfile)

        try:
            self.execute_and_commit_sql(query)
        except pg2.ProgrammingError as e:
            print e
            fName = os.path.basename(csvfile)
            homeName = os.path.expanduser("~/%s" % fName)
            shutil.copy(csvfile, homeName)
            os.chmod(homeName, 0644)
            query = """COPY %s FROM '%s' DELIMITER ',' CSV HEADER;""" % (tabname, homeName)
            try:
                self.execute_and_commit_sql(query)
            except pg2.DataError as e:
                print e
                self.strip_fileendings(homeName)
                self.execute_and_commit_sql(query)

            os.remove(homeName)

    def strip_fileendings(self, fName):
        print "Attempting to strip the shitty endings"
        with open(fName, 'rb') as f:
            data = f.readlines()

        data_new = [d.replace("\r\n", "\n") for d in data]

        with open(fName, 'wb') as f:
            for row in data_new:
                f.write(row)

    def check_csv_headers(self, csvfile, table_headers):

        with open(csvfile, 'rb') as f:
            header = f.readline()

        if table_headers[5][0] not in header.lower():
            return False

        return True


    # def prepend_csvrow(self, fName, new_header):
    #     with open(fName, 'rb') as original:
    #         data = original.read()
    #         reader = csv.reader(original, delimiter=",")
    #         data = [row.replace for row in reader]

    #     with open(fName, 'wb') as modified:
    #         writer = csv.writer(modified, delimiter=',', lineterminator='\n')
    #         writer.writerow(new_header)
    #         for row in data:
    #             writer.writerow(row)

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

        return self.execute_and_fetchall_sql(sql)[::-1]


    def create_all_tables(self):

        for key in self.schemas.keys():
            with open(self.schemas[key]["schema_location"], 'rb') as f:
                sql = f.read()

            sql = """%s""" % sql
            if self.schemas[key]["split_by_year"]:
                for year in self.schemas[key]["split_years"]:
                    sql_year = sql % year
                    self.execute_and_commit_sql(sql_year)
            else:
                self.execute_and_commit_sql(sql)


    def drop_table(self, table):
        sql = """DROP TABLE %s""" % table
        self.execute_and_commit_sql(sql)


    def drop_tables(self, master_table):

        if self.schemas[master_table]["split_by_year"]:
            for year in self.schemas[master_table]["split_years"]:
                self.drop_table("_".join([master_table, str(year)]))


    def query_to_df(self, sql):

        with pg2.connect(self.conn_string) as conn:
            df = psql.read_sql(sql, conn)

        return df


    def insert_many_csv(self, table, folder):

        allcsv_files = glob.glob(folder + "/*.csv")
        for f in allcsv_files:
            print f
            self.insert_from_csv(table, f)
            print "%s succesfully loaded to %s" % (f, table)

    def list_all_tables(self):
        return self.execute_and_fetchall_sql("SELECT * FROM pg_catalog.pg_tables")

    def get_table_size(self, table):
        SQL = """ SELECT pg_size_pretty(pg_total_relation_size('%s'));""" % table
        return self.execute_and_fetchall_sql(SQL)



if __name__ == '__main__':
    pass





