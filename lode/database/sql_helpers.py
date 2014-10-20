import psycopg2 as pg2
from lode.utilities.util import load_config
import pandas.io.sql as psql


def return_connection(db_key):

    config = load_config()

    db = config[db_key]["database_name"]
    user = config[db_key]["database_user"]
    host = "localhost"
    password = config[db_key]["database_pass"]

    return """dbname=%s user=%s password=%s host=%s
           """ % (db, user, password, host)


def ex_sql_and_fetch(db, sql):
    conn_string = return_connection(db)
    with pg2.connect(conn_string) as conn:
        with conn.cursor() as curs:
            curs.execute(sql)
            records = curs.fetchall()

    return records


def execute_and_commit_sql(db, sql):
    conn_string = return_connection(db)
    with pg2.connect(conn_string) as conn:
        with conn.cursor() as curs:
            curs.execute(sql)
            conn.commit()


def get_column_names(db, table):

    sql = """SELECT column_name FROM information_schema.columns
             WHERE table_schema='public' AND table_name='%s'""" % table

    return ex_sql_and_fetch(db, sql)[::-1]


def query_to_df(db, sql):
    """ Use a Generator here as we're Lazy """
    conn_string = return_connection(db)
    with pg2.connect(conn_string) as conn:
        yield psql.read_sql(sql, conn)


def list_all_tables(db):
    return ex_sql_and_fetch(db, "SELECT * FROM pg_catalog.pg_tables")
