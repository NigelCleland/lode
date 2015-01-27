from collections import defaultdict
import pandas as pd
import datetime
from lode.utilities.util import parse_date
from itertools import izip


def create_date_limited_sql(master_table, dates=None,
                            begin_date=None, end_date=None,
                            date_col="trading_date", range_break="Year"):
    """
    This is the initialisation function which takes into account the separation
    of the tables on a year by year basis in the Database (due to memory
    considerations). It is called with either the dates keyword or both the
    begin_date and end_date keywords in order to change the behaviour apparent.
    These are ensured to exist by the check_required_range function which has
    been previously run. Will return a list of queries which may then be
    modified as needed to construct the full custom queries desired

    Parameters:
    -----------
    master_table: The master table to query
    dates: A string or iterable object of dates to return
    begin_date: The first date in a range of dates to return
    end_date: The last date in a range of dates to return
    date_col: What column the dates are contained in
    range_break: A modification for performance purposes, default set to Year
                 but setting to 'Month' may result in reduced memory usage.

    Returns:
    --------
    all_queries: A list of SQL queries contained as strings
    """

    all_queries = []
    # If passing a selection of date objects
    if dates:
        if hasattr(dates, '__iter__'):
            return list(singular_sql_dates(master_table, dates, date_col))

        else:
            dt = parse_date(dates)
            return ["""SELECT * FROM %s_%s WHERE %s='%s'""" % (
                master_table, dt.year, date_col, dt.strftime('%d-%m-%Y'))]

    # Work with Date Ranges
    else:
        if range_break == "Year":
            return list(yearly_sql_dates(begin_date, end_date, master_table,
                                         date_col))

        elif range_break == "Month":
            return list(monthly_sql_dates(begin_date, end_date, master_table,
                                          date_col))

        else:
            raise ValueError("""Range Breaks for %s have not been
                                implemented, try 'Year'""" % range_break)

    return all_queries


def singular_sql_dates(master_table, dates, date_col):
    """
    For a list of dates this will create a series of SQL queries with the
    basic format SELECT * FROM table where dates in date_col. It has a little
    bit of magic in it to handle the fact that tables are sharded into smaller
    tables, often on a year by year basis.

    It also handles parsing dates if needed (although datetime objects may be
    passed to the method).

    Parameters:
    -----------
    master_table: The root database table to construct the query for, e.g. for
                  nodal_prices_2012  this would just simply be nodal_prices
    dates: Either a singluar date string or object or a list of date strings
           and objects to construct the SQL queries for. These may be in any
           order for any number of dates
    date_col: The column name for the dates in the SQL database

    Returns:
    --------
    SQL: This is a generator expression where each iteration is a separate
         SQL query for each year with all of the dates from that year contained
         as a selection query

    """

    dts = [parse_date(x) for x in dates]

    # Map the specific dates to the specific years isomg a default dict
    years_dict = defaultdict(list)
    for dt in dts:
        years_dict[dt.year].append(dt)

    # Iterate through each of the years and add the trading dates belonging
    # to each query to a specific query, yield this SQL string as a generator
    for year in years_dict:
        # Set up a custom string and then place it inside brackets for
        # Use in the query
        strings = join_date_strings(years_dict[year])
        date_string = "('%s')" % strings

        # The base query string to use
        query_string = """ SELECT * FROM %s_%s WHERE %s in %s"""

        # Substitute the values into the string
        SQL = query_string % (master_table, year, date_col, date_string)

        yield SQL


def yearly_sql_dates(begin_date, end_date, mtable, date_col,
                     df="%d-%m-%Y"):
    """
    Create full year SQL dates which ar every useful if a range goes over
    the yearly amount. For example, if the requested date range is
    20/12/2014 to 20/02/2015 then this requires querying both the 2014 and
    the 2015 table. This function creates two separate queries for each of
    the tables which allows this to happen behind the scenes. Should only
    be called if the yearly overlap is different.

    Note that this is a kind of a hacky use of a generator to create the
    SQL queries desired but this is due to the desire to use smaller table
    sizes on RAM limited machines. For example, a year of data is often 1GB
    of data and as there are 10+ years this exceeds the RAM available on an
    8GB machine easily.

    Parameters:
    -----------
    begin_date: String or datetime object of the first date to consider
    end_date: String or datetime object of the last date to consider
    mtable: What table to query (master)
    date_col: The column containing the trading dates
    df: The date format to use if needed

    Returns:
    --------
    query_string: A string containing the appropriate date SQL query as a base

    """
    # Parse the dates as they're probably strings
    begin_date = parse_date(begin_date)
    end_date = parse_date(end_date)

    # Set up dummy strings
    jan1, dec31 = "01-01-%s", "31-01-%s"
    query_string = """SELECT * FROM %s_%s WHERE %s BETWEEN '%s' AND '%s'"""

    if begin_date.year == end_date.year:
        fd, ld = begin_date.strftime(df), end_date.strftime(df)
        yield query_string % (mtable, begin_date.year, date_col, fd, ld)

    else:

        # Return the first string
        # From the beginning date to the 31st of December
        fd, ld = begin_date.strftime(df), dec31 % begin_date.year
        yield query_string % (mtable, begin_date.year, date_col, fd, ld)

        # Yield the intermediate dates if any
        # For example, if we are "12/12/2013" and "02/04/2015" we would
        # still want  all of the dates in 2014 to be returned.
        years = range(begin_date.year + 1, end_date.year)
        if len(years) > 0:
            for year in years:
                fd, ld = jan1 % year, dec31 % year
                yield query_string % (mtable, year, date_col, fd, ld)

        # Yield the last date
        # This is from the 1st of January of that year to the ending date
        if end_date.year != begin_date.year:
            fd, ld = jan1 % end_date.year, end_date.strftime(df)
            yield query_string % (mtable, end_date.year, date_col, fd, ld)


def monthly_sql_dates(begin_date, end_date, mtable, date_col, df='%d-%m-%Y'):
    """
    Returns queries which have been isolated on a per month basis which can
    then be fed into a DataFrame.

    This is a modified version of the yearly SQL dates return function which
    has been implemented due to memory considerations. There is a cut off
    where the additional overhead from running more queries is less than the
    overhead of holding large datasets in memory and coercing these to Pandas
    DataFrames. By using a generator expressions we are able to overcome this
    and keep total RAM usage reduced as the garbage collection runs.

    Parameters:
    -----------
    begin_date: The first date as either a datetime object or string
    end_date: The last date as either a datetime object or string
    mtable: The master table to query from
    date_col: What column contains date information
    df: What date format to use

    Returns:
    --------
    query_string: The SQL query which we may then modify

    """
    query_string = """SELECT * FROM %s_%s WHERE %s BETWEEN '%s' AND '%s'"""

    # Parse the dates as they're probably strings
    begin_date = parse_date(begin_date)
    end_date = parse_date(end_date)

    # Add an additional day in order to get the next months
    month_range = list(pd.date_range(begin_date, end_date, freq="M"))
    month_range_p1 = [x + datetime.timedelta(days=1) for x in
                      month_range]

    if month_range[-1] == end_date:
        end_dates = month_range
    else:
        end_dates = month_range + [end_date]

    # Can I do this functionally? I don't want to mutate the data
    # structures, currently copying the list
    begin_dates = [begin_date] + month_range_p1

    for s, e in izip(begin_dates, end_dates):
        beg, end = s.strftime(df), e.strftime(df)

        yield query_string % (mtable, s.year, date_col, beg, end)


def join_date_strings(dates, separator="','", df="%d-%m-%Y"):
    """
    Join a list of dates together in a specific string time format separated
    by a custom string. In many cases this is used to get it into the SQL
    format string needed
    """
    return separator.join([x.strftime(df) for x in dates])


def add_equality_constraint(column, values):

    if not hasattr(values, '__iter__'):
        return add_single_selection_constraint(column, values)
    else:
        return add_multiple_selection_constraint(column, values)


def add_exclusion_constraint(column, values):

    if not hasattr(values, '__iter__'):
        return add_single_exclusion_constraint(column, values)
    else:
        return add_multiple_exclusion_constraint(column, values)


def add_minimum_constraint(column, value):
    return """ AND %s >= '%s'""" % (column, value)


def add_maximum_constraint(column, value):
    return """ AND %s <= '%s'""" % (column, value)


def add_range_constraint(column, begin, end):
    return """ AND %s BETWEEN '%s' AND '%s'""" % (column, begin, end)


def add_single_selection_constraint(column, value):
    return """ AND %s='%s'""" % (column, value)


def add_multiple_selection_constraint(column, values):
    joined = "','".join(values)
    jvalues = "('%s')" % joined
    return """ AND %s IN %s""" % (column, jvalues)


def add_single_exclusion_constraint(column, value):
    return """ AND %s!='%s'""" % (column, value)


def add_multiple_exclusion_constraint(column, values):
    joined = "','".join(values)
    jvalues = "('%s')" % joined
    return """ AND %s NOT IN %s""" % (column, jvalues)

if __name__ == '__main__':
    pass
