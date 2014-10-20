from lode.utilities.util import load_config


def list_databases():
    """ Helper function to list the different databases that can be connected
    to, useful if running in an interaction session, saves hunting through
    the config
    """
    config = load_config()

    databases = [x for x in config.keys() if "schemas" in config[x]]
    return databases


def list_tables(database):
    """ Helper function to list the tables associated with a particular DB"""
    config = load_config()
    tables = [x for x in config[database]['schemas']]

    return tables


def check_csv_headers(csvfile, headers):
    """ Check to make sure the headers of the CSV file are as expected """

    with open(csvfile, 'rb') as f:
        csv_header = f.readline()

    # Check the lower ones
    if headers[1][0] not in csv_header.lower():
        return False

    return True


def strip_fileendings(fName):
    print "Attempting to strip the shitty endings"
    with open(fName, 'rb') as f:
        data = f.readlines()

    data_new = [d.replace("\r\n", "\n") for d in data]

    with open(fName, 'wb') as f:
        for row in data_new:
            f.write(row)


if __name__ == '__main__':
    pass
