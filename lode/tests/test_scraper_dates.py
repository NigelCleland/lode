import datetime

from lode.scrapers.dates import *


def test_scrub_string():

    s1 = "test/path/f70020140503.csv.Z"
    f1 = "f700"

    s2 = "FP_201405x_F.gdx"
    f2 = "FP_"

    assert scrub_string(s1, f1) == "20140503"
    assert scrub_string(s2, f2) == "201405"


def test_gdx_scrub():

    s1 = "FP_201405_F.gdx"
    s2 = "FP_20140327x_I.gdx"
    s3 = "FP_20140327a_F.gdx"

    r1 = "FP_201405.gdx"
    r2 = None
    r3 = "FP_20140327.gdx"

    assert gdx_scrub(s1) == r1
    assert gdx_scrub(s2) == r2
    assert gdx_scrub(s3) == r3


def test_parse_stringdate():

    s1 = "pattern2004"
    p1 = "pattern"
    f1 = "%Y"

    # Check it can handle the iteration through different date formats
    s2 = "pattern20140515.csv"
    f2 = ("%Y", "%Y%m", "%Y%m%d")

    r1 = (datetime.datetime(2004, 1, 1), "%Y")
    r2 = (datetime.datetime(2014, 5, 15), "%Y%m%d")

    assert parse_stringdate(s1, p1, format=f1) == r1
    assert parse_stringdate(s2, p1, f2) == r2


def test_parse_stringdate_list():

    # Check it can handle the iteration through different date formats
    s1 = "pattern20140515.csv"
    f1 = ("%Y", "%Y%m", "%Y%m%d")
    p = "pattern"

    r1 = [datetime.datetime(2014, 5, 15)]

    assert parse_stringdate_list(s1, p, format=f1) == r1

    # Check it can handle the generation of the month of dates
    s2 = "pattern201405.csv"

    r2 = [datetime.datetime(2014, 5, x) for x in range(1, 32)]

    assert parse_stringdate_list(s2, p, format=f1) == r2
