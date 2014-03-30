import requests
import BeautifulSoup
import urllib
import pandas
import subprocess
import os
import sys
import datetime
import calendar
import simplejson
import glob
import pandas as pd
import itertools
import shutil

with open('../config.json', 'rb') as f:
    CONFIG = simplejson.load(f)


def download_dates(pattern):

    file_db = build_file_database(pattern)
    url_names, url_db = build_recent_url_base(pattern)

    unique_dates = compare_sets(url_names, file_db)

    for d in unique_dates:
        url_name, url_seed = url_db[d]
        # Download the file
        fName = download_csv_file(url_name, url_seed)
        # Extract it
        extraced_name = extract_csvz_file(fName)
        # Move it
        move_completed_file(extraced_name, CONFIG['energy_offer_folder'])
        print "%s for %s successfully downloaded % (pattern, d.strftime('%Y%m%d'))




def download_csv_file(url_name, url_seed):
    """ Taking the seed and name download the file """

    full_url = CONFIG['wits_base_url'] + url_seed
    save_fName = os.path.join(CONFIG['temporary_location'], url_name)

    urllib.urlretrieve(full_url, save_fName)
    return save_fName

def extract_csvz_file(fName):

    # Change directories so 7Z works
    cwd = os.getcwd()
    os.chdir(CONFIG['temporary_location'])

    call_signature = ['7z', 'e', fName]
    subprocess.call(call_signature, stdout=sys.stdout, stderr=sys.stderr,
        )

    # Restore the original directory
    os.chdir(cwd)

    return fName.replace('.Z', '')

def move_completed_file(fName, save_loc):

    basename = os.path.basename(fName)
    end_location = os.path.join(save_loc, basename)

    shutil.move(fName, end_location)


def compare_sets(set_one, set_two):
    """ Make sure both set_one and set_two are sets then get the difference"""
    s1 = set(set_one)
    s2 = set(set_two)
    return s1.difference(s2)


def build_file_database(pattern, csv_ext='.csv'):
    """ Look through the folder location and get the first and last dates.
    """

    # Look through the config folder to get the names
    all_files = glob.glob(CONFIG['energy_offer_folder'] + '/*')

    # Get the Dates from the file names:
    all_dates = [date_from_filename(f, pattern, csv_ext) for f in all_files]

    # Flatten the list.
    flat_dates = list(itertools.chain.from_iterable(all_dates))

    return flat_dates

def build_recent_url_base(pattern, csvz_ext='.csv.Z'):
    """ Only consider the past 28 days to build a list of URLS to compare
    against the list of dates we already have.

    To seed the Database initially we can parse the historic database
    """

    wits_url = requests.get(CONFIG['wits_ongoing_url'])
    soup = BeautifulSoup.BeautifulSoup(wits_url.text)
    links = soup.findAll('a')

    offer_links = [x for x in links if "offers" in x.text]
    offer_names = [x.text for x in offer_links]
    offer_urls = [x['href'] for x in offer_links]

    dates = [date_from_filename(x.text, pattern, csvz_ext) for
                    x in offer_links]
    flat_dates = list(itertools.chain.from_iterable(dates))

    date_dictionary = {x: (y, z) for x, y, z in zip(flat_dates, offer_names, offer_urls)}

    return flat_dates, date_dictionary



def build_historic_url_base():
    """ Get all the URLS and dates for this historic database
    """
    pass


def date_from_filename(fName, pattern, ext):
    """ Get a date from a filename"""

    # We're just stripping out the offers and the extension here
    # Want to work on just the filename, not the full path
    base_str = os.path.basename(fName)
    datestring = base_str.replace(pattern, '').replace(ext, '')

    # Check if it's a monthly file or a daily one
    if len(datestring) < 7:
        date = datetime.datetime.strptime(datestring, '%Y%m')
        dates = [datetime.datetime(date.year, date.month, x) for x in
            range(1, calendar.monthrange(date.year, date.month)[1] +1)]
        return dates

    return [datetime.datetime.strptime(datestring, '%Y%m%d')]





if __name__ == '__main__':
    pass
