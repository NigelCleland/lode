#!/usr/bin/python

import requests
import BeautifulSoup
import urllib2
import os
import sys
import datetime
import calendar
import simplejson
import glob
import itertools
import shutil

# The XML parser for frequency keeping.
from xmlutils.xml2csv import xml2csv

# This stops a download which has timed out from bringing
# down everything.
# A consequence is that you may need to run the script multiple times.
import socket
socket.setdefaulttimeout(10)


# Do some wizardry to get the location of the config file
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
        """ This permits hot loading of the config file instead of linking
        it to only be initialised on startup
        """
        with open(config_name, 'rb') as f:
            self.CONFIG = simplejson.load(f)

        return self

    def get_links(self, url):
        """ Use Requests and Beautiful Soup to get all of the links
        on a website.

        Parameters:
        -----------
        url: A well formed url to be hit
        """
        r = requests.get(url)
        soup = BeautifulSoup.BeautifulSoup(r.text)
        self.all_links = soup.findAll('a')
        return self

    def build_url_db(self, url, pattern, ext, match_type='href',
                     rename=None, date_type="Monthly"):
        """ Hits a URL and gets all of the links from this URL then
        checks these against a pattern.

        Finishes by building the lists of dates from these obejcts

        Parameters:
        -----------
        url: A well formed url to be hit
        pattern: Pattern in the basename to match against
        ext: Filename extension
        rename: Optional second string incase filenames have changed.
        match_type: Match against either the link itself or the name it was
                    given, recommended to match against the link itself.
        date_type: Which parser to use
        """

        self.get_links(url)
        if match_type == 'href':
            self.pattern_files = [
                x for x in self.all_links if pattern in x['href']]
        elif match_type == 'text':
            self.pattern_files = [
                x for x in self.all_links if pattern in x.text]

        self.build_url_dates(pattern, ext, rename=rename, date_type=date_type)

        return self

    def build_url_dates(self, pattern, ext, rename=None, date_type="Monthly"):
        """ Once the list of URL links has been populated this function
        will parse them to datetime objects.

        Parameters:
        -----------
        pattern: Pattern in the basename to match against
        ext: Filename extension
        rename: Optional second string incase filenames have changed.
        date_type: Which parser to use

        """

        if ext in ('.gdx', '.XML', '.pdf'):
            single_mode = True
        else:
            single_mode = False

        if date_type == "Monthly":
            self.url_dates = [self.parse_monthly_dates(x['href'], pattern, ext,
                        rename=rename)for x in self.pattern_files]

            self.url_base = {self.parse_monthly_dates(x['href'], pattern, ext,
                                                      rename=rename): x['href'] for
                             x in self.pattern_files}

        elif date_type == "Daily":
            all_dates = [self.parse_daily_dates(x['href'], pattern, ext,
                                                rename=rename) for
                         x in self.pattern_files]

            self.url_dates = list(itertools.chain.from_iterable(all_dates))

            self.url_base = {self.parse_daily_dates(x['href'], pattern, ext,
                                                    rename=rename,
                                                    single_mode=single_mode): x['href'] for
                             x in self.pattern_files}

        return self

    def build_unique_url_dates(self):
        """ Compare two lists of dates and get the unique ones"""

        self.unique_urls = self.get_list_difference(self.url_dates,
                                                    self.existing_dates)

        return self

    def parse_monthly_dates(self, x, pattern, ext, rename=None):
        """ Take a url or filename string and get a monthly date out of it

        Parameters:
        -----------
        x: The string to be parsed
        pattern: Pattern in the basename to match against
        ext: Filename extension
        rename: Optional second string incase filenames have changed.

        Returns:
        --------
        datetime object of a monthly date

        """
        datestring = self.scrub_string(x, pattern, ext, rename=rename)
        return datetime.datetime.strptime(datestring, '%Y%m')

    def scrub_string(self, x, pattern, ext, rename):
        """ Take a string and get it into a position where it could be
        converted to a datetime object

        Parameters:
        -----------
        x: The string to be parsed
        pattern: Pattern in the basename to match against
        ext: Filename extension
        rename: Optional second string incase filenames have changed.

        Returns:
        --------
        datestring: A string which can be parsed
        """
        base_str = os.path.basename(x)
        if ext == '.gdx':
            # GDX files are named weird. It's a real issue and a major pain.
            # Could fix this to handle updating?
            # E.g. emphasis _F over _I etc.
            replacers = ('x_F', '_F', '_I', 'a', 'b')
            for each in replacers:
                base_str = base_str.replace(each, '')

            # cannot just blindly purge all x's as they're in the extension
            base_str = base_str.replace('x.gdx', '.gdx')

        # In case a file changes midway through.
        if rename:
            base_str = base_str.replace(rename, pattern)

        # Get rid of some of the last little things which can get left over
        # in the file names
        final_replacers = (pattern, ext, ext.lower(), ext.upper(), '_', ".csv")
        datestring = base_str
        for each in final_replacers:
            datestring = datestring.replace(each, '')

        return datestring

    def parse_daily_dates(self, x, pattern, ext, rename=None,
                          single_mode=False):
        """ This has two modes of operation, single_mode or multi mode.
        In single mode  a single datetime object is returned. This is needed
        to use the datetime objects as dictionary keys

        In multimode a list of dates may be returned. This can then be
        flattened. This is useful for situations when a combination of
        monthly and daily files exist.

        Parameters:
        -----------
        x: The string to be parsed
        pattern: Pattern in the basename to match against
        ext: Filename extension
        rename: Optional second string incase filenames have changed.
        single_mode: Boolean, which mode to use

        Returns:
        --------
        datetime object or list of datetime objects depending upon single mode
        """
        datestring = self.scrub_string(x, pattern, ext, rename=rename)
        # Check if it's a monthly file or a daily one
        if len(datestring) == 7:
            date = datetime.datetime.strptime(datestring, '%Y%m')
            dates = [datetime.datetime(date.year, date.month, x) for x in
                     range(1, calendar.monthrange(date.year, date.month)[1] + 1)]
            return dates

        elif len(datestring) == 8:
            if single_mode:
                return datetime.datetime.strptime(datestring, '%Y%m%d')

            return [datetime.datetime.strptime(datestring, '%Y%m%d')]

        else:
            return None

    def build_file_db(self, file_location, pattern, ext, rename=None,
                      date_type="Monthly"):
        """ Takes a folder location and a pattern and then creates
        a list of dates. The information is typically time indexed and
        therefore we can create another list of dates from the scraped
        information to compare the two.

        Parameters:
        -----------
        file_location: Folder location where the files are stored
        pattern: The pattern which excludes date information
        ext: Extension of the file
        rename: Incase the name procedure has changed...
        date_type: Default "Monthly" which date parser to use.

        """

        all_files = glob.glob(file_location + '/*' + ext)

        if date_type == "Monthly":
            flat_dates = [self.parse_monthly_dates(x, pattern, ext,
                                                   rename=rename) for x in all_files]

        elif date_type == "Daily":
            all_dates = [self.parse_daily_dates(x, pattern, ext, rename=rename)
                         for x in all_files]
            flat_dates = list(itertools.chain.from_iterable(all_dates))

        self.existing_dates = flat_dates

    def get_list_difference(self, set_one, set_two):
        """ Use Set Logic to get the difference between two sets.
        This is how we check for uniqueness between the built url list
        and the secondary file system list.

        Parameters:
        -----------
        set_one: Must be the list of the URL dates
        set_two: The list of the filesystem dates

        """
        s1 = set(set_one)
        s2 = set(set_two)
        return list(s1.difference(s2))

    def download_csv_file(self, url_seed):
        """ Given a url seed, e.g. a relative URL download the link.
        This is typically a file which will be saved in a temporary location
        as specified in the temporary location until any other modificaitons
        have been made.

        Parameters:
        -----------
        url_seed: A relative url contained in a string

        Returns:
        --------
        save_location: Location where the string was saved to, or None

        """

        # os.path.join shits itself if the second string has a leading slash.
        if url_seed[0] == '/':
            url_seed = url_seed[1:]

        full_url = os.path.join(self.base_url, url_seed)
        save_location = os.path.join(self.temp_loc, os.path.basename(url_seed))
        try:
            opened = urllib2.urlopen(full_url)
            with open(save_location, 'wb') as w:
                w.write(opened.read())
        except Exception:
            # We keep going as the links can be dead.
            print "Had Difficulties downloading %s, continuing anyway" % full_url
            save_location = None

        return save_location

    def download_from_urls(self, url, pattern, file_location, rename=None,
                           date_type="Monthly", ext='.csv'):

        self.build_file_db(file_location, pattern, ext, rename=rename,
                           date_type=date_type)
        self.build_url_db(url, pattern, ext, rename=rename,
                          date_type=date_type)
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
        """ Moves a file to a new location, has support for doing a final
        renaming of the file.

        Parameters:
        -----------
        fName: Original file name in the temporary folder
        save_loc: Directory where the file is to be saved
        pattern: The pattern which excludes date information
        rename: Incase the name procedure has changed...

        Returns:
        --------
        end_location: Location of the saved file

        """
        basename = os.path.basename(fName)
        end_location = os.path.join(save_loc, basename)
        if rename:
            end_location = end_location.replace(rename, pattern)

        shutil.move(fName, end_location)

        return end_location

    def synchronise_information(self, seed):
        """ Updates the information depending upon the seed and then
        creates two lists of date objects. One for the URL source one for
        the local source. These two lists are then compared against one
        another to create a list of differences.

        Following this comparison the non-unique dates are downloaded one
        after another. All of the information pertinent to each string
        is contained in the CONFIG file which is included in the REPO.
        A number of seeds are precompiled.

        Parameters:
        -----------
        seed: A string containing information which is parsed in the CONFIG
        file.

        """
        self.set_parameters(seed)

        self.download_from_urls(self.url, self.pattern, self.file_location,
                                rename=self.rename, date_type=self.date_type, ext=self.ext)

    def set_parameters(self, seed):
        """ Update the relevant parameters depending on the seed passed """
        self.file_location = self.CONFIG[seed]['file_location']
        self.url = self.CONFIG[seed]['url']
        self.pattern = self.CONFIG[seed]['pattern']
        self.date_type = self.CONFIG[seed]['date_type']
        self.ext = self.CONFIG[seed]['extension']
        self.rename = self.CONFIG[seed].get('rename', None)

        # Make the locations if they do not exist.
        if not os.path.isdir(self.file_location):
            os.mkdir(self.file_location)

    def print_seeds(self):
        """Viewing tool to see the available seeds """
        for key in self.CONFIG.keys():
            if "EMI" in key:
                print key

    def refresh_all_information(self):
        """ An aggregate function. Loops over all of the relevant seeds
        in the config file and will attempt to refresh all of them.
        Should take no action if all of the information is up to date.
        """
        self.refresh_config()
        seeds = [key for key in self.CONFIG.keys() if "EMI" in key]
        for seed in seeds:
            print "Beginning Synchronisation for %s" % seed
            self.synchronise_information(seed)

    def parse_xml_to_csv(self, fName, tag="Row"):
        """ This will convert an XML file to a CSV file.
        It is currently soft linked to work with the FK offers through
        the tag specification.

        However, this could likely be adapted if there are other XML
        formated that are specified.

        Parameters
        ----------
        fName: XML filename
        tag: XML tag information

        Returns:
        --------
        """

        output_name = fName.replace('.XML', '.csv')
        converter = xml2csv(fName, output_name, encoding="utf-8")
        converter.convert(tag=tag)

        return self


if __name__ == '__main__':
    EMI = EMIScraper()
    EMI.refresh_all_information()
