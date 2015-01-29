import psycopg2 as pg2
from lode.utilities.util import load_config, meta_path
import pandas.io.sql as psql
import pandas as pd


def list_databases():
    """
    Helper function to list the different databases that can be connected
    to, useful if running in an interaction session, saves hunting through
    the config file

    Parameters:
    -----------
    None

    Returns:
    --------
    databases: List of possible databases which may be connected to

    """
    config = load_config()

    databases = [x for x in config.keys() if "schemas" in config[x]]
    return databases


def list_tables(database):
    """
    List the tables contained within a Database by checking the
    Configuration File which is present in the repository. This does not imply
    that these tables have already been created:

    Parameters:
    -----------
    database: A string with the database to check

    Returns:
    --------
    tables: List containing all known tables by checking the Schemas

    """
    config = load_config()
    tables = [x for x in config[database]['schemas']]

    return tables


def check_csv_headers(csvfile, headers):
    """
    Check to make sure the headers of the CSV file are as expected

    Parameters:
    -----------
    csvfile: Filename as a string to check the headers of
    headers: Headers as a list to check against the csv headers.

    Returns:
    --------
    Boolean: True if headers are in the file, False otherwise

    """

    with open(csvfile, 'rb') as f:
        csv_header = f.readline()

    # Check the lower ones
    if headers[1][0] not in csv_header.lower():
        return False

    return True


def strip_fileendings(fName):
    """
    Some files will containing Windows style endings of \r\n which just
    generally screw everything up. This attemps to replace all of these
    instances on a line by line basis before writing it back to the same
    file. Note that this does overwrite the file in question although it
    should only influence line endings.

    Parameters:
    -----------
    fName: Name of file to be cleaned up

    Returns:
    --------
    None: Implicit


    """
    with open(fName, 'rb') as f:
        data = f.readlines()

    data_new = [d.replace("\r\n", "\n") for d in data]

    with open(fName, 'wb') as f:
        for row in data_new:
            f.write(row)


def check_required_range(specific=None, begin=None, end=None):
    """
    Raise ValueErrors if neither a range nor a specific instance are
    passed. This ensures that you do not try to query the entire database
    accidentally which would wipe out the machine.
    """

    if not specific and not (begin and end):
        raise ValueError('You must pass some form of date filter')

    if specific and (begin and end):
        raise ValueError('Cannot pass both a range and specific dates')

    if (begin and not end) or (end and not begin):
        raise ValueError("Must pass both begin and end for date range")


def check_optional_range(specific=None, begin=None, end=None):
    """
    Specific instances and range filters conflict with one another.
    This ensure you only pass one and that you pass the correct beginning
    and end point of a range.
    """
    if specific and (begin and end):
        raise ValueError('Cannot pass both a range and specific')

    if (begin and not end) or (end and not begin):
        raise ValueError("Must pass both begin and end for ranges")


def return_connection(db_key):
    """
    Take a database name and return connection information from the
    configuration file as a string which may be used to generate a psycopg
    connection:

    Parameters:
    -----------
    db_key: String with the database key name

    Returns:
    --------
    string containing the connection information for the database
    """

    config = load_config()

    db = config[db_key]["database_name"]
    user = config[db_key]["database_user"]
    host = "localhost"
    password = config[db_key]["database_pass"]

    return """dbname=%s user=%s password=%s host=%s""" % (db, user, password,
                                                          host)


def ex_sql_and_fetch(db, sql, result_num=None):
    """
    Execute an SQL query and return the results, optionally only
    return a subset if desired via the result_num keyword argument

    Parameters:
    -----------
    db: Database to connect to (string)
    sql: SQL statement to execute
    result_num: Optional, number of results to fetch, defaults to all

    Returns:
    --------
    records: The Database SQL query results

    """
    conn_string = return_connection(db)
    with pg2.connect(conn_string) as conn:
        with conn.cursor() as curs:
            curs.execute(sql)
            if type(result_num) is int:
                records = curs.fetchmany(result_num)
            else:
                records = curs.fetchall()

    return records


def execute_and_commit_sql(db, sql):
    """
    Create a connection to the Database and then execute a specific SQL
    query upon it. The query is automatically committed to the Database
    and thus should be used with care.

    Parameters:
    -----------
    db: What Database to connect to
    sql: What SQL query to execute

    """
    conn_string = return_connection(db)
    with pg2.connect(conn_string) as conn:
        with conn.cursor() as curs:
            curs.execute(sql)
            conn.commit()


def get_column_names(db, table):
    """

    """
    sql = """SELECT column_name FROM information_schema.columns
             WHERE table_schema='public' AND table_name='%s'""" % table

    return ex_sql_and_fetch(db, sql)[::-1]


def query_to_df(db, sql):
    """
    Query a particular database using a predefined SQL string

    Parameters:
    -----------
    db: String with the name of the database
    sql: The SQL to be run

    Returns:
    --------
    DataFrame: Object containing the results of the query
    """
    conn_string = return_connection(db)
    with pg2.connect(conn_string) as conn:
        return psql.read_sql(sql, conn)


def multi_query(db, queries):
    """
    Run multiple SQL queries on the same database and concatenate the results
    together into the same DataFrame

    Parameters:
    -----------
    db: The Database to run the queries on (string)
    queries: Iterable object containing the SQL queries as strings

    Returns:
    --------
    DataFrame: Pandas DataFrame containing the information concatenated
               together
    """
    return pd.concat((query_to_df(db, q) for q in queries), ignore_index=True)


def list_all_tables(db):
    """
    This function lists all of the user created tables from the configuration
    which exist. It queries all of the tables in the database as well as what
    should exist from the configuration file and compares the two.

    Parameters:
    -----------
    db: What database should be queried

    """
    # Get the tables which exist in the database
    db_tables =  ex_sql_and_fetch(db, "SELECT * FROM pg_catalog.pg_tables")
    tables = [t[1] for t in db_tables]
    # Get the master tables from the Config
    config_tables = load_config()[db]['schemas'].keys()

    # Check to eliminate tables which don't exist from the Config
    relevant = [t for t in tables for c in config_tables if c in t]
    return relevant


def merge_meta(df, col='demand'):
    """
    Apply meta information to a specific dataframe and remove all periods where
    no existing data was present before hand in a specific column. An outer
    join is used as the meta data may be incomplete.

    Parameters:
    -----------
    df: DataFrame to apply the meta information to
    col: What column to remove data based on, e.g. Nan values

    Returns:
    --------
    DataFrame: The existing df with meta information applied where possible.
    """
    meta_info = pd.read_csv(meta_path)
    df = df.merge(meta_info, left_on="node", right_on="Node",
                  how='outer')
    return df.ix[df[col].dropna().index]


def drop_table(database, table):
    """
    Drop a specific table from the Database, this will not work with the
    "Master Tables" and instead a specific table name must be called
    as the argument. Will raise a print warning but will not introduce
    any other warnings.

    Parameters:
    -----------
    database: Database to drop the table from (string)
    table: Which table to drop (string)

    """
    sql = """DROP TABLE %s""" % table
    print "Dropping Table %s from the Database %s" % (table, database)
    execute_and_commit_sql(database, sql)
    return None

def drop_all_tables(database, master_table):
    """
    This will drop all of the tables which match the base master table format.
    E.g. for all of the Energy Offer tables this will drop all of them.
    It is quite a useful function if you want to start from scratch on a DB
    and load all of the data in again.

    Parameters:
    -----------
    database: The Database to drop the table from
    master_table: What tables to drop.
    """

    config = load_config()
    table_info = config[database]['schemas'][master_table]

    if table_info["split_by_year"]:
        for year in table_info['split_years']:
            table_name = "_".join([master_table, str(year)])
            drop_table(database, table_name)

    # If there is not split by year configuration default to dropping the
    # master table as the name should be the name of the table in the DB
    else:
        drop_table(database, master_table)

    return None


if __name__ == '__main__':
    pass
