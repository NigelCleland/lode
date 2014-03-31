#!/usr/bin/python

import requests
import BeautifulSoup
import urllib2
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

import socket
socket.setdefaulttimeout(10)

file_path = os.path.abspath(__file__)
module_path = os.path.split(os.path.split(file_path)[0])[0]
config_name = os.path.join(module_path, 'config.json')

with open(config_name, 'rb') as f:
    CONFIG = simplejson.load(f)


# map the file locations to different areas:
FILE_LOCATIONS = {'offers': CONFIG['energy_offer_data_folder'],
                  'ilreserves': CONFIG['il_offer_data_folder'],
                  'generatorreserves': CONFIG['plsr_offer_data_folder'],
                  'diffbids': CONFIG['diffbid_offer_data_folder'],
                  'bids': CONFIG['bids_offer_data_folder']}


def download_dates(pattern):

    file_location = FILE_LOCATIONS[pattern]

    file_db = build_file_database(pattern, file_location)
    url_names, url_db = build_recent_url_base(pattern)

    unique_dates = compare_sets(url_names, file_db)

    for d in unique_dates:
        url_name, url_seed = url_db[d]
        # Download the file
        fName = download_csv_file(url_name, url_seed)
        if fName is not None:
            # Extract it
            extracted_name = extract_csvz_file(fName)
            # Move it
            move_completed_file(extracted_name, file_location)
            print "%s for %s successfully downloaded" % (pattern, d.strftime('%Y%m%d'))

        else:
            print "%s was a dead link, continuing full steam" % url_seed

def build_historic_db(pattern):

    file_location = FILE_LOCATIONS[pattern]

    historic_files = build_historic_url_base(pattern)
    existing_files = build_monthly_file_database(pattern, file_location)

    unique_keys = list(compare_sets(historic_files.keys(), existing_files))

    for url_name in unique_keys:
        url_seed = historic_files[url_name]
        print "Beginning to download %s" % url_name
        fName = download_csv_file(url_name, url_seed['href'])

        if fName is not None:
            extracted_name = extract_csvz_file(fName)

            move_completed_file(extracted_name, file_location)
            print "%s successfully downloaded" % url_name
        else:
            print "%s was a dead link, continuing full steam" % url_seed


def download_csv_file(url_name, url_seed):
    """ Taking the seed and name download the file """

    full_url = CONFIG['wits_base_url'] + url_seed
    save_fName = os.path.join(CONFIG['temporary_location'], url_name)

    # Open the URL
    try:
        opened = urllib2.urlopen(full_url)
        with open(save_fName, 'wb') as w:
            w.write(opened.read())
    except Exception:
        return None

    return save_fName


def extract_csvz_file(fName):

    # Change directories so 7Z works
    cwd = os.getcwd()
    os.chdir(CONFIG['temporary_location'])

    call_signature = ['7z', 'e', fName]
    subprocess.call(call_signature, stdout=sys.stdout, 
                    stderr=sys.stderr)

    os.remove(fName)

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


def build_monthly_file_database(pattern, file_location):

    all_files = glob.glob(file_location + '/*.csv')
    completed_keys = [os.path.basename(x) + '.Z' for x in all_files]
    return completed_keys



def build_file_database(pattern, file_location, csv_ext='.csv'):
    """ Look through the folder location and get the first and last dates.
    """

    # Look through the config folder to get the names
    all_files = glob.glob(file_location + '/*')

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

    offer_links = [x for x in links if pattern in x.text]
    offer_names = [x.text for x in offer_links]
    offer_urls = [x['href'] for x in offer_links]

    dates = [date_from_filename(x.text, pattern, csvz_ext) for
                    x in offer_links]
    flat_dates = list(itertools.chain.from_iterable(dates))

    date_dictionary = {x: (y, z) for x, y, z in zip(flat_dates, offer_names, offer_urls)}

    return flat_dates, date_dictionary



def build_historic_url_base(pattern):
    """ Get all the URLS and dates for this historic database:
    This is a carpet bomb strategy used to populate a folder from scratch.
    """
    wits_url = requests.get(CONFIG['wits_historic_url'])
    soup = BeautifulSoup.BeautifulSoup(wits_url.text)
    links = soup.findAll('a')

    offer_links = [x for x in links if pattern in os.path.basename(x['href'])]
    offer_names = [os.path.basename(x["href"]) for x in offer_links]

    dictionary = {x: y for x, y in zip(offer_names, offer_links)}

    return dictionary


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

    try:
        mode = sys.argv[1]
    except IndexError:
        mode = "Daily"

    patterns = ('offers', 'generatorreserves', 'ilreserves')

    if mode == "Monthly":
        print "Attempting to populate Monthly Database Files"
        for pattern in patterns:
            build_historic_db(pattern)
    else:
        print "Attempting to update Daily Database Files"
        for pattern in patterns:
            download_dates(pattern)

