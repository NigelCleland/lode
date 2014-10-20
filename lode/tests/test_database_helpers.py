from lode.database.helpers import (list_tables, list_databases,
                                   check_csv_headers)
from lode.utilities.util import module_path
import os


def test_list_tables():

    tables = list_tables('offer_database')
    assert 'energy_offers' in tables


def test_list_databases():

    databases = list_databases()
    assert 'offer_database' in databases


def test_check_csv_headers():

    csvfile = os.path.join(module_path, 'tests/data/offers20140813.csv')

    expected_headers = [['dummy'], ['company']]
    fail_headers = [['dummy'], ['Company']]

    assert check_csv_headers(csvfile, expected_headers)
    assert not check_csv_headers(csvfile, fail_headers)
