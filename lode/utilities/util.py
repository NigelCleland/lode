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
