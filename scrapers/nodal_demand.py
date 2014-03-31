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

file_path = os.path.abspath(__file__)
module_path = os.path.split(os.path.split(file_path)[0])[0]
config_name = os.path.join(module_path, 'config.json')

with open(config_name, 'rb') as f:
    CONFIG = simplejson.load(f)


def build_url_demand(url, pattern):
	
	wits_url = requests.get(url)
	soup = BeautifulSoup.BeautifulSoup(wits_url.text)
	links = soup.findAll('a')

	demand_links = [x for x in links if pattern in x['href']]
	dict_mapping = {parse_name_to_date(x): x['href'] for x in demand_links}

	return dict_mapping


def parse_name_to_date(tag):
	string_rep = " ".join([y for y in tag.text.split(' ') if y != ''])
	return datetime.datetime.strptime(string_rep, '%d %B %Y')


def build_historic_url_demand():
	pass

def build_demand_file_db(pattern, file_location):
	
	# Get the Files
	all_files = glob.glob(file_location + '/*.csv')

	# Get the dates
	dates = [date_from_filename(x, pattern, '.csv') for x in all_files]
	flat_dates = list(itertools.chain.from_iterable(dates))

	return flat_dates


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

def compare_sets(set_one, set_two):
    """ Make sure both set_one and set_two are sets then get the difference"""
    s1 = set(set_one)
    s2 = set(set_two)
    return s1.difference(s2)


def download_recent_demand():

	url = CONFIG['wits_recent_demand']
	download_url_demand(url)


def download_historic_demand():

	seed_url = CONFIG["wits_historic_demand_base"]
	seed_links = get_links(seed_url)

	month_links = [x for x in seed_links if "demand_pages" in x['href']]

	for month in month_links:
		new_url = os.path.join(CONFIG['wits_base_url'], 'comitFta', month['href'])
		print new_url
		download_url_demand(new_url)


def get_links(url):

	r = requests.get(url)
	soup = BeautifulSoup.BeautifulSoup(r.text)
	return soup.findAll('a')


def download_url_demand(url):

	pattern = "DemandDaily"
	file_location = CONFIG['demand_data_folder']
	ext = ".csv.gz"

	url_demand = build_url_demand(url, pattern)
	file_demand = build_demand_file_db(pattern, file_location)

	missing_keys = list(compare_sets(url_demand.keys(), file_demand))

	for date in missing_keys:
		url_seed = url_demand[date]
		url_name = "".join([pattern, date.strftime('%Y%m%d'), '.csv'])
		fName = download_csv_file(url_seed)
		print fName

		if fName is not None:
			extracted_name = extract_csvz_file(fName, '.gz')
			print extracted_name

			# Rename the file
			extract_path = os.path.split(extracted_name)[0]
			move_name = os.path.join(extract_path, url_name)
			shutil.move(extracted_name, move_name)

			# Move it to the final folder
			move_completed_file(move_name, file_location)
			print "%s successfully downloaded" % url_name
		else:
			print "%s was a dead link, continuing full steam" % url_seed


def download_csv_file(url_seed):
    """ Taking the seed and name download the file """

    full_url = CONFIG['wits_base_url'] + url_seed
    save_fName = os.path.join(CONFIG['temporary_location'], 
    						  os.path.basename(url_seed))

    # Open the URL
    try:
        opened = urllib2.urlopen(full_url)
        with open(save_fName, 'wb') as w:
            w.write(opened.read())
    except Exception:
        print "Had difficulties downloading %s, continuing anyway" % full_url

    return save_fName

def extract_csvz_file(fName, ext):

    # Change directories so 7Z works
    cwd = os.getcwd()
    os.chdir(CONFIG['temporary_location'])

    call_signature = ['7z', 'e', fName]
    subprocess.call(call_signature, stdout=sys.stdout, 
                    stderr=sys.stderr)

    os.remove(fName)

    # Restore the original directory
    os.chdir(cwd)

    return fName.replace(ext, '')

def move_completed_file(fName, save_loc):

    basename = os.path.basename(fName)
    end_location = os.path.join(save_loc, basename)

    shutil.move(fName, end_location)










