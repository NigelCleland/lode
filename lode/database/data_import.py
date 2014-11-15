import os
import shutil
import glob
import psycopg2 as pg2
import lode.database.utilities as ldu
import lode.database.sql_utilities as ldsu
from lode.utilities.util import get_file_year_str


def insert_many_csv(self, table, folder):

    allcsv_files = glob.glob(folder + "/*.csv")
    for f in allcsv_files:
        print "Attempting to load %s" % f
        insert_from_csv(table, f)
        print "%s succesfully loaded to %s" % (f, table)


def insert_to_database(db, query, csvfile, tabname):

    try:
        execute_import(db, query, csvfile)

    # If we run into a Programming Error it is probably due to
    # file permissions on an external HDD
    except pg2.ProgrammingError:
        fName = os.path.basename(csvfile)
        homeName = os.path.expanduser("~/%s" % fName)
        shutil.copy(csvfile, homeName)
        os.chmod(homeName, 0644)
        query = """COPY %s FROM '%s' DELIMITER ',' CSV HEADER;
                """ % (tabname, homeName)

        execute_import(db, query, homeName)

        os.remove(homeName)


def execute_import(db, query, csv_file):

    try:
        ldsu.execute_and_commit_sql(db, query)
    # Rows which already exist should be skipped
    except pg2.IntegrityError as e:
        print "You're trying to add rows which already exist!"
        print e

    # Due to the file encodings a Data Error can occur, so we strip the
    # File endings in this case
    except pg2.DataError as e:
        print e
        ldu.strip_fileendings(csv_file)
        ldsu.execute_and_commit_sql(db, query)


def insert_from_csv(db, table, csvfile):
    config = ldu.load_config()
    schemas = config[db]['schemas']

    # Check the Schemas for the split year:
    if schemas[table]['split_by_year']:
        year = get_file_year_str(csvfile)
        table_name = "%s_%s" % (table, year)
    else:
        table_name = table

    table_headers = ldsu.get_column_names(db, table_name)
    # Create headers to work on inserting to the table
    head_gen = (x[0] for x in table_headers if "key" not in x[0])
    insert_headers = ",".join(head_gen)

    # Create the table name
    tabname = "%s(%s)" % (table_name, insert_headers)

    # Check if any existing headers exist as it modifies the SQL
    existing_headers = ldu.check_csv_headers(csvfile, table_headers)

    csv = "CSV"
    if existing_headers:
        csv += " HEADER"

    query = """COPY %s FROM '%s' DELIMITER ',' %s;
                """ % (tabname, csvfile, csv)

    insert_to_database(db, query, csvfile, tabname)
