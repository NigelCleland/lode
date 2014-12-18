from lode.scrapers.general import *
import datetime

def test_list_differences():

    s1 = ["a", "b", "c"]
    s2 = ["a", "b", "d"]

    # Check to see that the same returns an empty list
    s3 = [datetime.datetime(2014, 2, 3), datetime.datetime(2014, 2, 4)]
    s4 = s3

    s5 = [datetime.datetime(2014, 2, 3), datetime.datetime(2014, 2, 4)]
    s6 = [datetime.datetime(2014, 3,5)]

    assert list_differences(s1, s2) == ["c"]
    assert list_differences(s3, s4) == []
    assert list_differences(s5, s6) == s5
