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

    return """dbname=%s user=%s password=%s host=%s
           """ % (db, user, password, host)


def ex_sql_and_fetch(db, sql):
    """

    """
    conn_string = return_connection(db)
    with pg2.connect(conn_string) as conn:
        with conn.cursor() as curs:
            curs.execute(sql)
            records = curs.fetchall()

    return records


def execute_and_commit_sql(db, sql):
    """

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
    return ex_sql_and_fetch(db, "SELECT * FROM pg_catalog.pg_tables")


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


if __name__ == '__main__':
    pass
