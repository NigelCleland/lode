from lode.utilities.util import load_config
from lode.database.sql_utilities import (ex_sql_and_fetch,
                                         execute_and_commit_sql)


def create_all_tables(db):

    config = load_config()
    schemas = config[db]['schemas']

    for key in schemas.keys():
        with open(schemas[key]["schema_location"], 'rb') as f:
            sql = f.read()

        sql = """%s""" % sql
        if schemas[key]["split_by_year"]:
            for year in schemas[key]["split_years"]:
                sql_year = sql % year
                execute_and_commit_sql(db, sql_year)
        else:
            execute_and_commit_sql(db, sql)


def list_databases():
    """ Helper function to list the different databases that can be connected
    to, useful if running in an interaction session, saves hunting through
    the config
    """
    config = load_config()

    databases = [x for x in config.keys() if "schemas" in config[x]]
    return databases


def list_tables(database):
    """ Helper function to list the tables associated with a particular DB"""
    config = load_config()
    tables = [x for x in config[database]['schemas']]

    return tables


def get_table_size(db, table):
    SQL = """ SELECT pg_size_pretty(pg_total_relation_size('%s'));
          """ % table
    return ex_sql_and_fetch(db, SQL)


def get_all_table_sizes(db, master_table):
    config = load_config()
    schemas = config[db]['schemas']

    if schemas[master_table]["split_by_year"]:
        for year in schemas[master_table]["split_years"]:
            table = master_table + '_%s' % year
            print table, get_table_size(db, table)
    # In case there was only a single table
    else:
        print master_table, get_table_size(db, master_table)


def check_csv_headers(csvfile, headers):
    """ Check to make sure the headers of the CSV file are as expected """

    with open(csvfile, 'rb') as f:
        csv_header = f.readline()

    # Check the lower ones
    if headers[1][0] not in csv_header.lower():
        return False

    return True


def strip_fileendings(fName):
    print "Attempting to strip the shitty endings"
    with open(fName, 'rb') as f:
        data = f.readlines()

    data_new = [d.replace("\r\n", "\n") for d in data]

    with open(fName, 'wb') as f:
        for row in data_new:
            f.write(row)


def drop_table(db, table):
    sql = """DROP TABLE %s""" % table
    execute_and_commit_sql(db, sql)


def drop_tables(db, master_table):

    config = load_config()
    schemas = config[db]['schemas']

    if schemas[master_table]["split_by_year"]:
        for year in schemas[master_table]["split_years"]:
            drop_table("_".join([master_table, str(year)]))

if __name__ == '__main__':
    pass
