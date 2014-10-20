#import lode
import lode.utilities.util as util
import datetime
from nose.tools import assert_raises


def test_parse_date():

    s1 = "2008-01-15"
    s2 = "2008-11-12"

    d1 = datetime.date(2008, 1, 1)
    d2 = datetime.datetime(2010, 3, 4)

    p1 = util.parse_date(s1)
    p2 = util.parse_date(s2)
    p3 = util.parse_date(d1)
    p4 = util.parse_date(d2)

    assert p1 == datetime.datetime(2008, 1, 15)
    assert p2 == datetime.datetime(2008, 11, 12)
    assert p3 == datetime.datetime(2008, 1, 1)
    assert p4 == datetime.datetime(2010, 3, 4)


def test_load_config():

    config = util.load_config()
    # Make sure that it is a dict
    assert type(config) == dict
    # Check that the right values are there
    assert 'offer_database' in config.keys()


def test_get_file_year():

    f1 = "path/to/filename2004"
    f2 = "path/to/filename200406"
    f3 = "path/to/filename20040628"
    f4 = "path/to/filename05_20040628"

    # Handle the three common cases
    y1 = util.get_file_year_str(f1)
    y2 = util.get_file_year_str(f2)
    y3 = util.get_file_year_str(f3)

    assert y1 == '2004'
    assert y2 == '2004'
    assert y3 == '2004'

    # Should not be an integer
    assert y1 != 2004

    # Should raise a Value Error on funky dates
    assert_raises(ValueError, util.get_file_year_str, f4)
