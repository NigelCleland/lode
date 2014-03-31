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

def get_links(url):

    r = requests.get(url)
    soup = BeautifulSoup.BeautifulSoup(r.text)
    return soup.findAll('a')


def download_monthly_prices():

	url = CONFIG['emi_final_prices']
	file_location = CONFIG['final_price_data_folder']
	pattern = 'Final_prices'

	existing_files = build_file_db(file_location, pattern)
	url_files = build_url_db(url, pattern)

	new_dates = compare_sets(url_files.keys(), existing_files)

	for date in new_dates:
		url_seed = url_files[date]
		fName = download_csv_file(url_seed)

		if fName is not None:

			move_completed_file(fName, file_location)
			print "%s succesfully downloaded" % os.path.basename(fName)

		else:
			print "%s was a dead link, continuing full steam" % url_seed




def build_file_db(file_location, pattern):

	# Get the Files
    all_files = glob.glob(file_location + '/*.csv')

    # Get the dates
    flat_dates = [monthly_dates_from_filename(x, pattern, '.csv') for x in all_files]
    return flat_dates


def build_url_db(url, pattern):

	all_links = get_links(url)
	all_files = [x for x in all_links if pattern in x['href']]

	DB = {monthly_dates_from_filename(x['href'], pattern, '.csv', rename="Final_pricing"): x['href'] for x in all_files}

	return DB


def monthly_dates_from_filename(x, pattern, ext, rename=None):

	base_str = os.path.basename(x)
	if rename:
		base_str = base_str.replace(rename, pattern)
	datestring = base_str.replace(pattern, '').replace(ext, '').replace('_', '')

	return datetime.datetime.strptime(datestring, '%Y%m')

def compare_sets(set_one, set_two):
    """ Make sure both set_one and set_two are sets then get the difference"""
    s1 = set(set_one)
    s2 = set(set_two)
    return s1.difference(s2)


def download_csv_file(url_seed):
    """ Taking the seed and name download the file """

    full_url = CONFIG['emi_base_url'] + url_seed
    save_fName = os.path.join(CONFIG['temporary_location'], 
                              os.path.basename(url_seed))

    # Open the URL
    try:
        opened = urllib2.urlopen(full_url)
        with open(save_fName, 'wb') as w:
            w.write(opened.read())
    except Exception:
        print "Had difficulties downloading %s, continuing anyway" % full_url
        save_fName = None

    return save_fName

def move_completed_file(fName, save_loc):

    basename = os.path.basename(fName)
    end_location = os.path.join(save_loc, basename)
    end_location = end_location.replace('Final_pricing', 'Final_prices')

    shutil.move(fName, end_location)


if __name__ == '__main__':
	download_monthly_prices()
