import requests
from bs4 import BeautifulSoup
import urllib2
import os
import sys
import datetime
import calendar
import simplejson
import glob
import itertools
import shutil
import subprocess

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


class Scraper(object):
    """docstring for Scraper"""
    def __init__(self):
        super(Scraper, self).__init__()
        self.refresh_config()


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
        soup = BeautifulSoup(r.text)
        self.all_links = soup.findAll('a')
        return self


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

    def download_file(self, url_seed):
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

    def download_all_from_urls(self, url, pattern, file_location, rename=None, date_type="Monthly", ext=None, match_type=None, cext=None):

        self.build_file_db(file_location, pattern, ext, rename=rename, date_type=date_type)
        self.build_url_db(url, pattern, ext+cext, rename=rename, date_type=date_type, match_type=match_type)
        self.build_unique_url_dates()

        self.completed_seeds = []

        for key in self.unique_urls:

            url_seed = self.url_base[key]

            if url_seed not in self.completed_seeds:
                fName = self.download_file(url_seed)

                if cext != '' and fName is not None:
                    fName = self.extract_csvz_file(fName, cext)

                if fName is not None:
                    final_location = self.move_completed_file(fName, file_location, pattern, rename=rename)
                    print "%s successfully downloaded" % os.path.basename(fName)

                    if ext == ".XML":
                        self.parse_xml_to_csv(final_location)

                else:
                    print "%s was a dead link, continuing full steam" % url_seed

            self.completed_seeds.append(url_seed)



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

    def extract_csvz_file(self, fName, cext):
        """ Extract a compressed file, returns the filename without that
        extension.
        """
        # Change directories so 7Z works
        cwd = os.getcwd()
        os.chdir(self.temp_loc)

        call_signature = ['7z', 'e', fName]
        subprocess.call(call_signature, stdout=sys.stdout,
                        stderr=sys.stderr)

        os.remove(fName)

        # Restore the original directory
        os.chdir(cwd)

        return fName.replace(cext, '')

    def build_unique_url_dates(self):
        """ Compare two lists of dates and get the unique ones"""

        self.unique_urls = self.get_list_difference(self.url_dates,
                                                    self.existing_dates)

        return self



    def build_url_dates(self, pattern, ext, rename=None, date_type="Monthly"):

        if pattern == "DemandDaily":
            self.match_demand_urls()

        else:

            if date_type == "Monthly":
                self.url_dates = [self.parse_monthly_dates(x['href'], pattern, ext, rename=rename) for x in self.pattern_files]
                self.url_base = {self.parse_monthly_dates(x['href'], pattern, ext, rename=rename): x['href'] for x in self.pattern_files}

            elif date_type == "Daily":
                all_dates = [self.parse_daily_dates(x['href'], pattern, ext, rename=rename) for x in self.pattern_files]
                # Flatten out the dates
                self.url_dates = list(itertools.chain.from_iterable(all_dates))

                # Here, for each unique date in the parsed file (e.g. Jan 2009 has 31 unique dates)
                # We match these up against.
                # Later we filter out duplicate files when we download so this shouldn't
                # Be too much of an issue (hopefully)
                self.url_base = {}
                for x in self.pattern_files:
                    url = x['href']
                    for date in list(itertools.chain.from_iterable([self.parse_daily_dates(x['href'], pattern, ext, rename=rename)])):
                        self.url_base[date] = url




    def parse_monthly_dates(self, x, pattern, ext, rename=None):
        datestring = self.scrub_string(x, pattern, ext, rename=rename)
        return datetime.datetime.strptime(datestring, '%Y%m')



    def parse_daily_dates(self, x, pattern, ext, rename=None, single_mode=False, date_format=None):
        datestring = self.scrub_string(x, pattern, ext, rename=rename)

        if len(datestring) == 6:
            date = datetime.datetime.strptime(datestring, "%Y%m")
            y, m = date.year, date.month
            dates = [datetime.datetime(y, m, x) for x in range(1, calendar.monthrange(y, m)[1] + 1)]
            return dates

        elif len(datestring) == 8:
            if single_mode:
                return datetime.datetime.strptime(datestring, "%Y%m%d")

            return [datetime.datetime.strptime(datestring, "%Y%m%d")]

        else:
            return None


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


    def build_url_db(self, url, pattern, ext, match_type='href', rename=None, date_type="Monthly"):
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

        if match_type == "href":
           self.pattern_files = [x for x in self.all_links if pattern in os.path.basename(x["href"])]
        elif match_type == 'text':
           self.pattern_files = [x for x in self.all_links if pattern in x.text]


        self.build_url_dates(pattern, ext, rename=rename, date_type=date_type)


    def build_file_db(self, file_location, pattern, ext, rename=None, date_type="Monthly"):

        all_files = glob.glob(file_location + '/*' + ext)

        if date_type == "Monthly":
            flat_dates = [self.parse_monthly_dates(x, pattern, ext,
                                               rename=rename) for x in all_files]

        elif date_type == "Daily":
            all_dates = [self.parse_daily_dates(x, pattern, ext, rename=rename)
                     for x in all_files]
            flat_dates = list(itertools.chain.from_iterable(all_dates))

        self.existing_dates = flat_dates

    def match_demand_urls(self):

        self.url_dates = [self.parse_demand_date(x) for x in self.pattern_files]
        self.url_base = {self.parse_demand_date(x): x['href'] for x in self.pattern_files}


    def parse_demand_date(self, x):
        string_rep = " ".join([y for y in x.text.split(' ') if y != ''])
        return datetime.datetime.strptime(string_rep, "%d %B %Y")


    def hit_historic_nodal_demand(self):

        self.get_links(self.url)
        month_links = [x for x in self.all_links if "demand_pages" in x]
        all_urls = [os.path.join(self.base_url, 'comitFta',
                                 x['href']) for x in month_links]
        return all_urls



    def set_parameters(self, seed):

        # These parameters must be passed
        self.file_location = self.CONFIG[seed]['file_location']
        self.url = self.CONFIG[seed]['url']
        self.pattern = self.CONFIG[seed]['pattern']
        self.date_type = self.CONFIG[seed]['date_type']
        self.match_type = self.CONFIG[seed]['match_type']
        self.ext = self.CONFIG[seed]['extension']
        self.base_url = self.CONFIG[seed]['base_url']
        self.temp_loc = self.CONFIG[seed]['temp_loc']

        # These parameters are not necessarily required
        # Will default to either None or an empty string
        self.cext = self.CONFIG[seed].get('cextension', "")
        self.rename = self.CONFIG[seed].get('rename', None)


        if not os.path.isdir(self.file_location):
            os.mkdir(self.file_location)


    def scrape_seed(self, seed):

        self.set_parameters(seed)

        print "Preparing to Scrape %s" % seed
        print "Hitting %s and saving to %s" % (self.url, self.file_location)

        # Need slightly different behaviour due to the structure of the
        # Historic nodal demand. The page is paginated into a really terrible
        # structure
        if seed == "WITS_Historic_Nodal_Demand":
            for url in self.hit_historic_nodal_demand():
                self.download_all_from_urls(url, self.pattern,
                                            self.file_location,
                                            rename=self.rename,
                                            date_type=self.date_type,
                                            ext=self.ext,
                                            match_type=self.match_type,
                                            cext=self.cext)

        else:
            self.download_all_from_urls(self.url, self.pattern,
                                        self.file_location, rename=self.rename,
                                        date_type=self.date_type, ext=self.ext,
                                        match_type=self.match_type,
                                        cext=self.cext)

        print "Completed Scrape for %s" % seed




if __name__ == '__main__':
    pass

