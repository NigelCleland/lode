#!/usr/bin/python
import psycopg2 as pg2
import simplejson
import pandas.io.sql as psql
import pandas as pd
import os
import shutil
import glob
import sh
import csv
import re
import datetime
from collections import defaultdict
from dateutil.parser import parse
import datetime

from OfferPandas import Frame

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
        except pg2.IntegrityError as e:
            print "You're trying to add rows which already exist!"
            print e

        except pg2.ProgrammingError as e:
            print e
            #print "Going to do some permission editing from a local drive"
            #print "You're probably running from an external drive if you see"
            #print "This often"
            fName = os.path.basename(csvfile)
            homeName = os.path.expanduser("~/%s" % fName)
            shutil.copy(csvfile, homeName)
            os.chmod(homeName, 0644)
            query = """COPY %s FROM '%s' DELIMITER ',' CSV HEADER;""" % (tabname, homeName)
            try:
                self.execute_and_commit_sql(query)
            except pg2.IntegrityError as e:
                print "You're trying to add rows which already exist!"
                print e
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

        if table_headers[3][0] not in header.lower():
            return False

        return True

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


    def columnise_trading_periods(self, df):
        pass


    def insert_many_csv(self, table, folder):

        allcsv_files = glob.glob(folder + "/*.csv")
        for f in allcsv_files:
            print "Attempting to load %s" % f
            self.insert_from_csv(table, f)
            print "%s succesfully loaded to %s" % (f, table)

    def update_table(self, table):
        pass

    def list_all_tables(self):
        return self.execute_and_fetchall_sql("SELECT * FROM pg_catalog.pg_tables")

    def get_table_size(self, table):
        SQL = """ SELECT pg_size_pretty(pg_total_relation_size('%s'));""" % table
        return self.execute_and_fetchall_sql(SQL)


    def get_all_table_sizes(self, master_table):
        if self.schemas[master_table]["split_by_year"]:
            for year in self.schemas[master_table]["split_years"]:
                table = master_table + '_%s' % year
                print table, self.get_table_size(table)


    def query_offer(self, master_table, dates=None, begin_date=None,
                    end_date=None, companies=None, stations=None, periods=None,
                    begin_period=None, end_period=None,
                    grid_points=None, as_offerframe=True):

        # Error checking on the dates and period range consistencies
        self._check_required_range(dates, begin_date, end_date)
        self._check_optional_range(periods, begin_period, end_period)

        # Set up the initial SQL queries with dates loaded in
        all_queries = self.create_date_limited_sql(master_table, dates=dates,
                                                   begin_date=begin_date,
                                                   end_date=end_date)

        # Map grid locations based upon which query is being run.
        grid_map = {"energy_offers": "grid_injection_point",
                    "generatorreserves_offers": "grid_point",
                     "ilreserves_offers": "grid_exit_point"}

        # Add all of the other constraints:
        completed_queries = []
        for sql in all_queries:

            if periods:
                sql += self.add_equality_constraint('trading_period', periods)

            elif (begin_period and end_period):
                sql += self.add_range_constraint('trading_period',
                                                  begin_period, end_period)

            if companies:
                sql += self.add_equality_constraint('company', companies)

            if stations:
                sql += self.add_equality_constraint('station', stations)

            if grid_points:

                sql += self.add_equality_constraint(grid_map[master_table],
                                                     grid_points)

            # Once all constraints have been added end the SQL statement
            sql += ';'
            completed_queries.append(sql)

        # Run all of the required queries, returning the result as a DF
        all_information = pd.concat((self.query_to_df(q) for
                                     q in completed_queries),
                                     ignore_index=True)

        # Optionally modify to an offer frame.
        if as_offerframe:
            df = Frame(all_information)
            return df.modify_frame()


        return all_information


    def create_date_limited_sql(self, master_table, dates=None,
                                begin_date=None, end_date=None):
        all_queries = []
        # If passing a single date
        if dates:
            if type(dates) == str:
                dt = parse(dates)
                SQL = """ SELECT * FROM %s_%s WHERE trading_date='%s'""" % (
                    master_table, dt.year, dt.strftime('%d-%m-%Y'))

                all_queries.append(SQL)

            elif hasattr(dates, '__iter__'):
                dts = [parse(x, dayfirst=True) for x in dates]
                # Map the specific dates to the specific years
                years_dict = defaultdict(list)
                for dt in dts:
                    years_dict[dt.year].append(dt)

                for year in years_dict:
                    strings = "','".join([x.strftime("%d-%m-%Y") for x in years_dict[year]])
                    date_string = "('%s')" % strings
                    SQL = """ SELECT * FROM %s_%s WHERE trading_date in %s""" % (master_table, year, date_string)

                    all_queries.append(SQL)

        # Work with Date Ranges
        else:
            dt_begin = parse(begin_date, dayfirst=True)
            dt_end = parse(end_date, dayfirst=True)

            # Generic Year Begin and Year End values
            ybegin, yend = "01-01-%s", "31-12-%s"

            # Iterate through the years creating a query for each year.
            # Check if years match a beginning or ending date, otherwise
            # Use the generic 1st January and 31st December dates
            for year in range(dt_begin.year, dt_end.year+1):
                if year == dt_begin.year:
                    beg = dt_begin.strftime('%d-%m-%Y')
                else:
                    beg = ybegin % year
                if year == dt_end.year:
                    ed = dt_end.strftime('%d-%m-%Y')
                else:
                    ed = yend % year

                SQL = """SELECT * FROM %s_%s WHERE trading_date BETWEEN '%s' AND '%s'""" % (master_table, year, beg, ed)

                all_queries.append(SQL)

        return all_queries



    def add_equality_constraint(self, column, values):

        if not hasattr(values, '__iter__'):
            return self.add_single_selection_constraint(column, values)
        else:
            return self.add_multiple_section_constraint(column, values)


    def add_range_constraint(self, column, begin, end):
        return """ AND %s BETWEEN '%s' AND '%s'""" % (column, begin, end)

    def add_single_selection_constraint(self, column, value):
        return """ AND %s='%s'""" % (column, value)

    def add_multiple_section_constraint(self, column, values):
        joined = "','".join(values)
        jvalues = "('%s')" % joined
        return """ AND %s IN %s""" % (column, jvalues)

    def _check_required_range(self, specific=None, begin=None, end=None):
        if not specific and not (begin and end):
            raise ValueError('You must pass some form of date filter')

        if specific and (begin and end):
            raise ValueError('Cannot pass both a range and specific dates')

        if (begin and not end) or (end and not begin):
            raise ValueError("Must pass both begin and end for date range")

    def _check_optional_range(self, specific=None, begin=None, end=None):
        if specific and (begin and end):
            raise ValueError('Cannot pass both a range and specific')

        if (begin and not end) or (end and not begin):
            raise ValueError("Must pass both begin and end for ranges")



if __name__ == '__main__':
    pass





