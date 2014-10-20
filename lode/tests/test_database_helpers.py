from lode.database.helpers import list_tables, list_databases


def test_list_tables():

    tables = list_tables('offer_database')
    assert 'energy_offers' in tables


def test_list_databases():

    databases = list_databases()
    assert 'offer_database' in databases


def test_check_csv_headers():

    csvfile = os.path
