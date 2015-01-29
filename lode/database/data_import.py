import psycopg2 as pg2
import pandas as pd
import numpy as np

from lode.database.utilities import (execute_and_commit_sql,
                                     strip_fileendings,
                                     get_column_names,
                                     check_csv_headers,
                                     list_all_tables,
                                     ex_sql_and_fetch)

from lode.utilities.util import (get_file_year_str, parse_date)


def insert_to_database(database, sql, table, csvfile):
    """

    """

    try:
        execute_and_commit_sql(database, sql)
    except pg2.ProgrammingError as e:
        print e

        # Copy the file from the existing location to the home directory
        fName = os.path.basename(csvfile)
        homeName = os.path.expanduser("~/%s" % fName)
        shutil.copy(csvfile, homeName)

        # Modify the permissions to give control over the file to the system
        os.chmod(homeName, 0644)

        # Recreate a new query
        sql = """COPY %s FROM '%s' DELIMITER ',' CSV HEADER;""" % (table,
                                                                   homeName)

        try:
            execute_and_commit_sql(sql)
        except pg2.DataError as e:
            strip_fileendings(homeName)
            execute_and_commit_sql(sql)
        finally:
            os.remove(homeName)


def load_csv_with_headers(csvfile, database, tablename, replace=True):
    """
    CSV Files do not necessarily have headers in tact within them.
    By passing a database and what table is expected then we can update the
    headers as needed. Can optionally replace the CSV file with one which
    has headers.

    Parameters:
    -----------
    csvfile: What CSV file to load, full path
    database: What database it relates to
    tablename: What table it relates to
    replace: Whether to replace the existing CSV file with one which had table
             names

    Returns:
    --------
    df: A Pandas DataFrame containing the CSV file with headers
    """

    # Query the table headers and check these
    table_headers = get_column_names(database, tablename)
    headers = ",".join([x[0] for x in table_headers if "key" not in x[0]])
    existing_headers = check_csv_headers(csvfile, headers)

    # Load the file with headers
    if existing_headers:
        df = pd.read_csv(csvfile)
        # Lowercase the column names to ensure consistency
        df.rename(columns={x: x.lower() for x in df.columns}, inplace=True)
    else:
        df = pd.read_csv(csvfile, names=headers)
        df.to_csv(csvfile, headers=True, index=False)

    return df


def unloaded_csv_dates(csvfile, database, table, column="trading_date",
                       replace=False):
    """

    """

    # Get the appropriate table name
    table = match_csv_to_table(csvfile, database, table)

    df = load_csv_with_headers(csvfile, database, table, replace=replace)
    csv_dates = [parse_date(x) for x in (df[column].unique()]

    # Get all of the existing dates from the
    sql = """SELECT DISTINCT trading_date FROM %s""" % table
    db_dates = ex_sql_and_fetch(database, sql)


def existing_database_entries(database, table, column):
    sql = """SELECT DISTINCT %s FROM %s""" % (column, table)
    unique = ex_sql_and_fetch(database, sql)
    # Flatten the list and return as an array using Numpy
    return np.ravel(unique)


def match_csv_to_table(csvfile, database, table):
    """

    """
    year = get_file_year_str(csvfile)
    existing_tables = list_all_tables(database)

    # Update the tablename if the yearly edition exists, otherwise leave it
    # as the prexisting table name
    if "%s_%s" % (table, year) in existing_tables:
        table = "%s_%s" % (table, year)

    return table






