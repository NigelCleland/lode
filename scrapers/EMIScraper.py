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

from xmlutils.xml2csv import xml2csv

import socket
socket.setdefaulttimeout(10)

file_path = os.path.abspath(__file__)
module_path = os.path.split(os.path.split(file_path)[0])[0]
config_name = os.path.join(module_path, 'config.json')



class EMIScraper(object):
    """Master Class for Scraping and Downloading EMI Files"""
    def __init__(self):
        super(EMIScraper, self).__init__()
        self.refresh_config()

        self.base_url = self.CONFIG['emi_base_url']
        self.temp_loc = self.CONFIG['temporary_location']


    def refresh_config(self):
        with open(config_name, 'rb') as f:
            self.CONFIG = simplejson.load(f)


    def get_links(self, url):
        r = requests.get(url)
        soup = BeautifulSoup.BeautifulSoup(r.text)
        self.all_links = soup.findAll('a')
        return self


    def build_url_db(self, url, pattern, ext, match_type='href', 
                     rename=None, date_type="Monthly"):

        self.get_links(url)
        if match_type == 'href':
            self.pattern_files = [x for x in self.all_links if pattern in x['href']]
        elif match_type == 'text':
            self.pattern_files = [x for x in self.all_links if pattern in x.text]

        self.build_url_dates(pattern, ext, rename=rename, date_type=date_type)


    def build_url_dates(self, pattern, ext, rename=None, date_type="Monthly"):

        if ext in ('.gdx', '.XML', '.pdf'):
            single_mode = True
        else:
            single_mode = False

        if date_type == "Monthly":
            self.url_dates = [self.parse_monthly_dates(x['href'], pattern, ext, 
                                rename=rename)for x in self.pattern_files]

            self.url_base = {self.parse_monthly_dates(x['href'], pattern, ext, 
                                rename=rename): x['href'] for x in self.pattern_files}


        elif date_type == "Daily":
            all_dates = [self.parse_daily_dates(x['href'], pattern, ext, rename=rename)
                            for x in self.pattern_files]
            self.url_dates = list(itertools.chain.from_iterable(all_dates))
            self.url_base = {self.parse_daily_dates(x['href'], pattern, ext, 
                                rename=rename, single_mode=single_mode): x['href'] for x in self.pattern_files}


    def build_unique_url_dates(self):

        self.unique_urls = self.get_list_difference(self.url_dates, 
                                                    self.existing_dates)


    def parse_monthly_dates(self, x, pattern, ext, rename=None):
        datestring = self.scrub_string(x, pattern, ext, rename=rename)
        return datetime.datetime.strptime(datestring, '%Y%m')


    def scrub_string(self, x, pattern, ext, rename):

        base_str = os.path.basename(x)
        if ext == '.gdx':
            replacers = ('x_F', '_F', '_I', 'a', 'b')
            for each in replacers:
                base_str = base_str.replace(each, '')

            # cannot just blindly purge all x's as they're in the extension
            base_str = base_str.replace('x.gdx', '.gdx')
        
        if rename:
            base_str = base_str.replace(rename, pattern)

        final_replacers = (pattern, ext, ext.lower(), ext.upper(), '_', ".csv")
        datestring = base_str
        for each in final_replacers:
            datestring = datestring.replace(each, '')

        return datestring


    def parse_daily_dates(self, x, pattern, ext, rename=None, single_mode=False):
        datestring = self.scrub_string(x, pattern, ext, rename=rename)
        # Check if it's a monthly file or a daily one
        if len(datestring) == 7:
            date = datetime.datetime.strptime(datestring, '%Y%m')
            dates = [datetime.datetime(date.year, date.month, x) for x in
                range(1, calendar.monthrange(date.year, date.month)[1] +1)]
            return dates

        elif len(datestring) == 8:
            if single_mode:
                return datetime.datetime.strptime(datestring, '%Y%m%d')
            
            return [datetime.datetime.strptime(datestring, '%Y%m%d')]

        else:
            return None


    def build_file_db(self, file_location, pattern, ext, rename=None, date_type="Monthly"):

        all_files = glob.glob(file_location + '/*' + ext)

        if date_type == "Monthly":
            flat_dates = [self.parse_monthly_dates(x, pattern, ext, rename=rename)
                            for x in all_files]

        elif date_type == "Daily":
            all_dates = [self.parse_daily_dates(x, pattern, ext, rename=rename)
                            for x in all_files]
            flat_dates = list(itertools.chain.from_iterable(all_dates))


        self.existing_dates = flat_dates


    def get_list_difference(self, set_one, set_two):
        s1 = set(set_one)
        s2 = set(set_two)
        return list(s1.difference(s2))


    def download_csv_file(self, url_seed):

        if url_seed[0] == '/':
            url_seed = url_seed[1:]

        full_url = os.path.join(self.base_url, url_seed)
        save_location = os.path.join(self.temp_loc, os.path.basename(url_seed))
        try:
            opened = urllib2.urlopen(full_url)
            with open(save_location, 'wb') as w:
                w.write(opened.read())
        except Exception:
            print "Had Difficulties downloading %s, continuing anyway" % full_url
            save_location = None

        return save_location


    def download_from_urls(self, url, pattern, file_location, rename=None, 
                           date_type="Monthly", ext='.csv'):

        self.build_file_db(file_location, pattern, ext, rename=rename, 
                           date_type=date_type)
        self.build_url_db(url, pattern, ext, rename=rename, date_type=date_type)
        self.build_unique_url_dates()

        for key in self.unique_urls:
            url_seed = self.url_base[key]
            fName = self.download_csv_file(url_seed)

            if fName is not None:
                final_location = self.move_completed_file(fName, file_location, 
                                            pattern, rename=rename)
                print "%s succesfully downloaded" % os.path.basename(fName)

                if ext == ".XML":
                    self.parse_xml_to_csv(final_location)

            else:
                print "%s was a dead link, continuing full steam" % url_seed


    def move_completed_file(self, fName, save_loc, pattern, rename=None):

        basename = os.path.basename(fName)
        end_location = os.path.join(save_loc, basename)
        if rename:
            end_location = end_location.replace(rename, pattern)

        shutil.move(fName, end_location)

        return end_location


    def synchronise_information(self, seed):
        self.set_parameters(seed)

        self.download_from_urls(self.url, self.pattern, self.file_location,
                rename=self.rename, date_type=self.date_type, ext=self.ext)


    def set_parameters(self, seed):
        self.file_location = self.CONFIG[seed]['file_location']
        self.url = self.CONFIG[seed]['url']
        self.pattern = self.CONFIG[seed]['pattern']
        self.date_type = self.CONFIG[seed]['date_type']
        self.ext = self.CONFIG[seed]['extension']
        self.rename = self.CONFIG[seed].get('rename', None)


    def print_seeds(self):
        for key in self.CONFIG.keys():
            if "EMI" in key:
                print key


    def refresh_all_information(self):
        self.refresh_config()
        seeds = [key for key in self.CONFIG.keys() if "EMI" in key]
        for seed in seeds:
            print "Beginning Synchronisation for %s" % seed
            self.synchronise_information(seed)


    def parse_xml_to_csv(self, fName):

        output_name = fName.replace('.XML', '.csv')
        converter = xml2csv(fName, output_name, encoding="utf-8")
        converter.convert(tag="Row")



if __name__ == '__main__':
    EMI = EMIScraper()
    EMI.refresh_all_information()
