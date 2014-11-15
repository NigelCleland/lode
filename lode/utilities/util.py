import datetime
import pandas
from dateutil.parser import parse
import simplejson
import os
import re

# Configuration is located here
file_path = os.path.abspath(__file__)
module_path = os.path.split(os.path.split(file_path)[0])[0]
config_name = os.path.join(module_path, 'config.json')

# Meta information is located here
meta_path = os.path.join(module_path, "static/nodal_metadata.csv")


def parse_date(x, dayfirst=True):

    allowable_dates = (datetime.datetime, pandas.tslib.Timestamp,
                       datetime.date)
    if type(x) in allowable_dates:
        # Upcast to a datetime object
        if type(x) == datetime.date:
            return datetime.datetime.fromordinal(x.toordinal())
        return x

    # Sometimes dates are ints for weird reasons which fails with parse
    if type(x) == int:
        x = str(x)

    return parse(x, dayfirst=True)


def load_config(config_name=config_name):
    with open(config_name, 'rb') as f:
        return simplejson.load(f)


def get_file_year_str(x):
    """ Take a filename and try to infer the year from it,
    return the guess as a string
    """

    # This will find all of thje digits we get
    year = re.findall(r"\d+", os.path.basename(x))[0]
    # Quick check as the year may be raw
    if len(year) == 4:
        return year
    elif len(year) == 6:
        date = datetime.datetime.strptime(year, "%Y%m")
    elif len(year) == 8:
        date = datetime.datetime.strptime(year, "%Y%m%d")
    else:
        raise ValueError("Don't recognise the year %s" % year)

    return date.strftime("%Y")


def create_timestamp(x, offset=30):
    """ Create a Timestamp from an object, accepts different formats:
        1. Tuple of date + period
        2. Int or String of Trading Period ID

    These timestamps have an offset associated which may be used, By default
    it is set to the beginning of the period. Other possible value are
        0: End of the period
        15: Middle of the period
        30: Beginning of the period

    Usage:
    ------

    ts = create_timestamp("2014051733") # Accepts trading period ID
    ts = create_timestamp(2014051733) # Accepts integers

    # Accepts iterables of date, period combinations
    ts = create_timestamp(("2014-05-17", 44))

    # Dates can be datetime objects
    ts = create_timestamp((datetime.date(2014, 5, 17), 14))

    """

    # Object is has a date and period
    if hasattr(x, '__iter__'):
        date, period = x
        parsed_date = parse_date(date)
    else:
        # Ensure it is an integer
        tpid = int(x)
        parsed_date, period = parse_date(tpid / 100), tpid % 100

    # Fucking daylight saving
    if period > 48:
        period = 1

    # Minutes for the end of the trading period
    minutes = period * 30 - offset

    # Create the timestamp

    return parsed_date + datetime.timedelta(minutes=minutes)


def create_tpid(x):
    """ General purpose utility function to create a Trading Period ID
    A TPID has the general format YYYYMMDDPP and can be useful in some
    situations as a method of merging entries in databases:

    Usage:
    ------
    tpid = create_tpid(datetime_object)
    tpid = create_tpid(("date", period))

    """

    # Check if it is a datetime object
    if type(x) in (datetime.datetime, pandas.tslib.Timestamp):
        date = x
        # Get the trading period
        period = 1 + 2 * x.hour + (x.minute / 30)
    # Check if it is a tuple
    elif hasattr(x, '__iter__'):
        # Note force the int on the period, type errors yo
        date, period = parse_date(x[0]), int(x[1])
    else:
        try:
            if len(str(x)) == 10:
                return str(x)
        except:
            print "Not sure what this is"

    # Create the trading period ID and return it
    tpid = date.strftime('%Y%m%d') + '%02d' % period

    return tpid
