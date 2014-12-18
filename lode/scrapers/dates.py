""" Date Handling utilities for the scraper
"""

import datetime
import os
import calendar


def scrub_string(string, pattern, rename=None):
    """ Manipulate a string which is passed to ideally get a datetime
    object from it which can be used in other functions

    Not going to use a regex to find the digits as on occasion this can lead
    to some slightly wonky behaviour, instead I'm going to carefully eliminate
    on the point of view of a pattern.
    """

    ext = os.path.splitext(string)[1]
    # First we need to remove the unicode slash character
    nonu_string = string.replace('%2F', '/')

    base_string = os.path.basename(nonu_string)
    if ext == '.gdx':
        base_string = gdx_scrub(base_string)

    # There have been cases of the names changing over time
    if rename:
        base_string = base_string.replace(rename, pattern)

    # Get rid of the final little pieces of information which may be ugly
    # Including .csv for the files with double extension (e.g. compressed ones)
    final_replacers = (pattern, ext, '_', '.csv')
    for each in final_replacers:
        base_string = base_string.replace(each, '')

    return base_string


def gdx_scrub(string, replacers=('x_F', '_F', 'a', 'b')):

    # We don't want to have any of the downloads for the Interim
    # Final pricing solutions. This can cause some issues later on
    if "_I" in string:
        return None

    # Remove the variables which may be in it signifying modifications
    for each in replacers:
        string = string.replace(each, '')

    # Remove the x.gdx, cannot blindly do this in a loop as x's exist naturally

    string = string.replace('x.gdx', '.gdx')

    return string


def parse_stringdate(string, pattern, format="%Y", rename=None,
                     failure_mode=False):

    datestring = scrub_string(string, pattern, rename=rename)

    # Upcast to a tuple to make the iteration work
    if type(format) == str:
        format = (format, )

    # Try all of the different formats until one is matching
    for f in format:
        try:
            return datetime.datetime.strptime(datestring, f), f
        except ValueError:
            pass

    # If none match return None
    return None


def parse_stringdate_list(string, pattern, format=("%Y%m%d", "%Y%m")):
    """ Look at a string and return a list of date objects corresponding to
    it, for a single date return that date. For a month the list of all dates
    within that month should be returned

    Parameters
    ----------
    string: The string to be parsed
    pattern: pattern to be matched against
    format: The list of formats to try

    Returns
    -------
    list: A list containing dates, either a single date or a full month of
          dates. If formats do not match an empty list should be returned

    """

    date, format = parse_stringdate(string, pattern, format)

    if format == "%Y%m%d":
        return [date]
    elif format == "%Y%m":
        y, m = date.year, date.month
        last_day = calendar.monthrange(y, m)[1]
        return [datetime.datetime(y, m, i) for i in xrange(1, last_day + 1)]
    else:
        return []
