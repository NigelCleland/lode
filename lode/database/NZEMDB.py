#!/usr/bin/python
import psycopg2 as pg2
import pandas.io.sql as psql
import pandas as pd
import os
import shutil
import glob
import datetime
from collections import defaultdict
from dateutil.parser import parse

from lode.utilities.util import (parse_date, load_config,
                                 get_file_year_str, meta_path)

from lode.database.helpers import (check_csv_headers,
                                   strip_fileendings)

from itertools import izip
from OfferPandas import Frame


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
        self.conn_string = """dbname=%s user=%s password=%s
                              host=%s""" % (self.db, self.user, self.password,
                                            self.host)

    def refresh_config(self):
        """ This permits hot loading of the config file instead of linking
        it to only be initialised on startup
        """
        self.CONFIG = load_config()

        return self

    def insert_to_database(self, query, csvfile, tabname):
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
            query = """COPY %s FROM '%s' DELIMITER ',' CSV HEADER;
                    """ % (tabname, homeName)
            try:
                self.execute_and_commit_sql(query)
            except pg2.IntegrityError as e:
                print "You're trying to add rows which already exist!"
                print e
            except pg2.DataError as e:
                print e
                strip_fileendings(homeName)
                self.execute_and_commit_sql(query)

            os.remove(homeName)

    def insert_from_csv(self, table, csvfile):

        # Check the Schemas for the split year:
        if self.schemas[table]['split_by_year']:
            year = get_file_year_str(csvfile)
            table_name = "%s_%s" % (table, year)
        else:
            table_name = table

        table_headers = self.get_column_names(table_name)
        insert_headers = ",".join([x[0] for x in
                                   table_headers if "key" not in x[0]])
        tabname = "%s(%s)" % (table_name, insert_headers)

        existing_headers = check_csv_headers(csvfile, table_headers)

        if existing_headers:
            query = """COPY %s FROM '%s' DELIMITER ',' CSV HEADER;
                    """ % (tabname, csvfile)
        else:
            query = """COPY %s FROM '%s' DELIMITER ',' CSV;
                    """ % (tabname, csvfile)

        self.insert_to_database(query, csvfile, tabname)

    def strip_fileendings(self, fName):
        print "Attempting to strip the shitty endings"
        with open(fName, 'rb') as f:
            data = f.readlines()

        data_new = [d.replace("\r\n", "\n") for d in data]

        with open(fName, 'wb') as f:
            for row in data_new:
                f.write(row)

    def execute_and_commit_sql(self, sql):

        with pg2.connect(self.conn_string) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                conn.commit()

    def ex_sql_and_fetch(self, sql):
        with pg2.connect(self.conn_string) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                records = curs.fetchall()

        return records

    def get_column_names(self, table):

        sql = """SELECT column_name FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='%s'""" % table

        return self.ex_sql_and_fetch(sql)[::-1]

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
        """ Use a Generator here as we're Lazy """

        with pg2.connect(self.conn_string) as conn:
            yield psql.read_sql(sql, conn)

    def insert_many_csv(self, table, folder):

        allcsv_files = glob.glob(folder + "/*.csv")
        for f in allcsv_files:
            print "Attempting to load %s" % f
            self.insert_from_csv(table, f)
            print "%s succesfully loaded to %s" % (f, table)

    def list_all_tables(self):
        return self.ex_sql_and_fetch("SELECT * FROM pg_catalog.pg_tables")

    def get_table_size(self, table):
        SQL = """ SELECT pg_size_pretty(pg_total_relation_size('%s'));
              """ % table
        return self.ex_sql_and_fetch(SQL)

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

    def query_nodal_demand(self, begin_date=None, end_date=None, nodes=None,
                           dates=None, begin_period=None, end_period=None,
                           periods=None, minimum_demand=None,
                           maximum_demand=None, apply_meta=False,
                           meta_group=None, meta_agg=None,
                           excl_nodes=None):

        # Helper function for the wind nodes as wind is a negative load at the
        # moment on the GXPs
        if excl_nodes == "Wind":
            excl_nodes = ("TWC2201", "WDV1101", "WWD1101", "WWD1102",
                          "WWD1103", "TWH0331")

        # Error checking on the dates and period range consistencies
        self._check_required_range(dates, begin_date, end_date)
        self._check_optional_range(periods, begin_period, end_period)

        # Set up the initial SQL queries with dates loaded in
        all_queries = self.create_date_limited_sql("nodal_demand", dates=dates,
                                                   begin_date=begin_date,
                                                   end_date=end_date,
                                                   date_col="Trading_date")

        completed_queries = []
        for sql in all_queries:
            if periods:
                sql += self.add_equality_constraint('Trading_period', periods)

            elif (begin_period and end_period):
                sql += self.add_range_constraint('Trading_period',
                                                 begin_period, end_period)

            if nodes:
                sql += self.add_equality_constraint("Node", nodes)

            if minimum_demand:
                sql += self.add_minimum_constraint("Demand", minimum_demand)

            if maximum_demand:
                sql += self.add_maximum_constraint("Demand", maximum_demand)

            # Exclude certain nodes
            if excl_nodes:
                sql += self.add_exclusion_constraint("Node", excl_nodes)

            sql += ';'
            completed_queries.append(sql)

        demand = pd.concat((self.query_to_df(q) for q in completed_queries),
                           ignore_index=True)

        if apply_meta:
            meta_info = pd.read_csv(meta_path)
            demand = demand.merge(meta_info, left_on="node", right_on="Node")

            if meta_group and meta_agg:
                grouped = demand.groupby(meta_group)
                aggregate = grouped.aggregate(meta_agg)

                return aggregate

        return demand

    def query_nodal_price(self, begin_date=None, end_date=None, nodes=None,
                          dates=None, begin_period=None, end_period=None,
                          periods=None, minimum_price=None,
                          maximum_price=None, apply_meta=False):
        """ Query the Nodal Price Database to obtain relevant information
        """
        # Error checking on the dates and period range consistencies
        self._check_required_range(dates, begin_date, end_date)
        self._check_optional_range(periods, begin_period, end_period)

        # Set up the initial SQL queries with dates loaded in
        all_queries = self.create_date_limited_sql("nodal_prices", dates=dates,
                                                   begin_date=begin_date,
                                                   end_date=end_date,
                                                   date_col="Trading_date")

        completed_queries = []
        for sql in all_queries:
            if periods:
                sql += self.add_equality_constraint('Trading_period', periods)

            elif (begin_period and end_period):
                sql += self.add_range_constraint('Trading_period',
                                                 begin_period, end_period)

            if nodes:
                sql += self.add_equality_constraint("Node", nodes)

            if minimum_price:
                sql += self.add_minimum_constraint("Price", minimum_price)

            if maximum_price:
                sql += self.add_maximum_constraint("Price", maximum_price)

            sql += ';'
            completed_queries.append(sql)

        prices = pd.concat((self.query_to_df(q) for q in completed_queries),
                           ignore_index=True)

        if apply_meta:
            meta_info = pd.read_csv(meta_path)
            prices = prices.merge(meta_info, left_on="node", right_on="Node")

        return prices

    def create_date_limited_sql(self, master_table, dates=None,
                                begin_date=None, end_date=None,
                                date_col="trading_date", range_break="Year"):
        all_queries = []
        # If passing a single date
        if dates:
            if type(dates) == str:
                dt = parse(dates)
                return [""" SELECT * FROM %s_%s WHERE %s='%s'""" % (
                    master_table, dt.year, date_col, dt.strftime('%d-%m-%Y'))]

            elif hasattr(dates, '__iter__'):
                return list(self._singular_sql_dates(dates, master_table,
                                                     date_col))

        # Work with Date Ranges
        else:
            if range_break == "Year":
                return list(self._yearly_sql_dates(begin_date, end_date,
                                                   master_table, date_col))

            elif range_break == "Month":
                return list(self._monthly_sql_dates(begin_date, end_date,
                                                    master_table, date_col))

            else:
                raise ValueError("""Range Breaks for %s have not been
                                    implemented, try 'Year'""" % range_break)

        return all_queries

    def _singular_sql_dates(self, dates, master_table, date_col):
        dts = [parse(x, dayfirst=True) for x in dates]
        # Map the specific dates to the specific years
        years_dict = defaultdict(list)
        for dt in dts:
            years_dict[dt.year].append(dt)

        for year in years_dict:
            strings = "','".join([x.strftime("%d-%m-%Y") for x in
                                  years_dict[year]])

            date_string = "('%s')" % strings
            query_string = """ SELECT * FROM %s_%s WHERE %s in %s"""
            SQL = query_string % (master_table, year, date_col, date_string)

            yield SQL

    def _yearly_sql_dates(self, begin_date, end_date, master_table, date_col):
        # Parse the dates as they're probably strings
        begin_date = parse_date(begin_date)
        end_date = parse_date(end_date)

        # Set up dummy strings
        jan1, dec31 = "01-01-%s", "31-01-%s"
        query_string = """SELECT * FROM %s_%s WHERE %s BETWEEN '%s' AND '%s'"""

        # Return the first string
        yield query_string % (master_table, begin_date.year, date_col,
                              begin_date.strftime('%d-%m-%Y'),
                              dec31 % begin_date.year)

        # Yield the intermediate dates if any
        years = range(begin_date.year + 1, end_date.year)
        if len(years) > 0:
            for year in years:
                yield query_string % (master_table, year, date_col,
                                      jan1 % year, dec31 % year)

        # Yield the last date
        if end_date.year != begin_date.year:
            yield query_string % (master_table, begin_date.year, date_col,
                                  jan1 % end_date.year,
                                  end_date.strftime('%d-%m-%Y'))

    def _monthly_sql_dates(self, begin_date, end_date, master_table, date_col):

        query_string = """SELECT * FROM %s_%s WHERE %s BETWEEN '%s' AND '%s'"""

        # Parse the dates as they're probably strings
        begin_date = parse_date(begin_date)
        end_date = parse_date(end_date)

        month_range = list(pd.date_range(begin_date, end_date, freq="M"))
        month_range_p1 = [x + datetime.timedelta(days=1) for x in
                          month_range[:-1]]

        if month_range[-1] == end_date:
            end_dates = month_range
        else:
            end_dates = month_range + [end_date]

        # Can I do this functionally? I don't want to mutate the data
        # structures, currently copying the list
        begin_dates = [begin_date] + month_range_p1

        for s, e in izip(begin_dates, end_dates):
            beg = s.strftime('%d-%m-%Y')
            end = e.strftime('%d-%m-%Y')

            yield query_string % (master_table, s.year, date_col, beg, end)

    def add_equality_constraint(self, column, values):

        if not hasattr(values, '__iter__'):
            return self.add_single_selection_constraint(column, values)
        else:
            return self.add_multiple_section_constraint(column, values)

    def add_exclusion_constraint(self, column, values):

        if not hasattr(values, '__iter__'):
            return self.add_single_exclusion_constraint(column, values)
        else:
            return self.add_multiple_exclusion_constraint(column, values)

    def add_minimum_constraint(self, column, value):
        return """ AND %s >= '%s'""" % (column, value)

    def add_maximum_constraint(self, column, value):
        return """ AND %s <= '%s'""" % (column, value)

    def add_range_constraint(self, column, begin, end):
        return """ AND %s BETWEEN '%s' AND '%s'""" % (column, begin, end)

    def add_single_selection_constraint(self, column, value):
        return """ AND %s='%s'""" % (column, value)

    def add_multiple_section_constraint(self, column, values):
        joined = "','".join(values)
        jvalues = "('%s')" % joined
        return """ AND %s IN %s""" % (column, jvalues)

    def add_single_exclusion_constraint(self, column, value):
        return """ AND %s!='%s'""" % (column, value)

    def add_multiple_exclusion_constraint(self, column, values):
        joined = "','".join(values)
        jvalues = "('%s')" % joined
        return """ AND %s NOT IN %s""" % (column, jvalues)

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
