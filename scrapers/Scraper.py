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

    def download_all_from_urls(self, url, pattern, file_location, rename=None,
                               date_type="Monthly", ext=None, match_type=None,
                               cext=None):
        """ Hit a URL with a pattern, download all of the links on the page
        which match the pattern but do not have corresponding dates which
        already exist in our local file storage.

        Creates two lists of dates, one from the URL, one from the local dir.
        Uses these to create a unique list of dates which are then
        selectively scraped.

        Has optional support for decompressing, using 7zip.
        Has support for parsing XMl to CSV using xml2csv

        Will move the files to the ffinal location.
        In addition it keeps track of what it has downloaded
        so that it doesn't accidentally download the same file multiple times
        even if unique dates for that file exist.

        Parameters:
        -----------
        url: A well formed url to be hit
        pattern: Pattern in the basename to match against
        self.file_location: File Save Location
        ext: Filename extension
        rename: Optional second string incase filenames have changed.
        date_type: Which parser to use
        match_type: Match against either the link itself or the name it was
                    given, recommended to match against the link itself.
        cext: Compression extension if the file is compressed
        """

        self.build_file_db(file_location, pattern, ext, rename=rename,
                           date_type=date_type)
        self.build_url_db(url, pattern, ext+cext, rename=rename,
                          date_type=date_type, match_type=match_type)
        self.build_unique_url_dates()

        self.completed_seeds = []

        for key in self.unique_urls:

            url_seed = self.url_base[key]

            if url_seed not in self.completed_seeds:
                fName = self.download_file(url_seed)


                if cext != '' and fName is not None:
                    fName = self.extract_csvz_file(fName, cext)

                # Rename the wonky demand files
                if pattern == "DemandDaily" and fName is not None:
                    new_fName = os.path.join(self.temp_loc, pattern +
                                             key.strftime("%Y%m%d") + ".csv")
                    shutil.move(fName, new_fName)
                    fName = new_fName


                if fName is not None:
                    final_location = self.move_completed_file(fName,
                                         file_location, pattern, rename=rename)
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
        rename: Incase the name procedure has changed.

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
        """ Once the list of URL links has been populated this function
        will parse them to datetime objects.

        Parameters:
        -----------
        pattern: Pattern in the basename to match against
        ext: Filename extension
        rename: Optional second string incase filenames have changed.
        date_type: Which parser to use

        """
        if pattern == "DemandDaily":
            self.match_demand_urls()

        else:

            if date_type == "Monthly":
                self.url_dates = [self.parse_monthly_dates(x['href'], pattern,
                                        ext, rename=rename) for
                                        x in self.pattern_files]

                self.url_base = {self.parse_monthly_dates(x['href'], pattern,
                                     ext, rename=rename): x['href'] for
                                     x in self.pattern_files}

            elif date_type == "Daily":
                all_dates = [self.parse_daily_dates(x['href'], pattern, ext,
                             rename=rename) for x in self.pattern_files]
                # Flatten out the dates
                self.url_dates = list(itertools.chain.from_iterable(all_dates))

                # Here, for each unique date in the parsed file
                # (e.g. Jan 2009 has 31 unique dates)
                # We match these up against.
                # Later we filter out duplicate files when we download so
                # this shouldn't be too much of an issue (hopefully)
                self.url_base = {}
                for x in self.pattern_files:
                    url = x['href']
                    url_dates = [self.parse_daily_dates(x['href'],
                                        pattern, ext, rename=rename)]
                    dates = list(itertools.chain.from_iterable(url_dates))
                    for date in dates:
                        self.url_base[date] = url




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



    def parse_daily_dates(self, x, pattern, ext, rename=None,
                          date_format=None):
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

        Returns:
        --------
        datetime object or list of datetime objects depending upon single mode
        """

        datestring = self.scrub_string(x, pattern, ext, rename=rename)

        if len(datestring) == 6:
            date = datetime.datetime.strptime(datestring, "%Y%m")
            y, m = date.year, date.month
            dates = [datetime.datetime(y, m, x) for x in range(1, calendar.monthrange(y, m)[1] + 1)]
            return dates

        elif len(datestring) == 8:
            return [datetime.datetime.strptime(datestring, "%Y%m%d")]
        else:
            return []


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
           self.pattern_files = [x for x in self.all_links if
                                  pattern in os.path.basename(x["href"])]
        elif match_type == 'text':
           self.pattern_files = [x for x in self.all_links if
                                    pattern in x.text]


        self.build_url_dates(pattern, ext, rename=rename, date_type=date_type)


    def build_file_db(self, file_location, pattern, ext, rename=None,
                      date_type="Monthly"):
        """ This is the key function which checks which files already exist
        in the local storage. It works by matching local files against the
        same pattern as the URL files. The assumption being that no
        wonky renaming has occurred.

        It returns a list of datetime objects from which we can construct
        a unique set of datetime objects which we will then parse. This helps
        to remove a significant amount of redundency.

        Paremeters:
        -----------
        file_location: Local directory containing the files
        pattern: Pattern to match
        ext: Format of the data
        rename: In case some human fingers have renamed things
        date_type: Specifies which parser to use

        Returns:
        --------
        self.existing_dates: List of datetime objects of the dates which exist
                             in the local directory. Prevents downloading
                             files which already exist.
        """

        # Note, don't put any similar files in the folder which aren't
        # meant to be pattern matched over!
        all_files = glob.glob(file_location + '/*' + ext)

        if date_type == "Monthly":
            flat_dates = [self.parse_monthly_dates(x, pattern, ext,
                                                   rename=rename) for
                          x in all_files]

        elif date_type == "Daily":
            all_dates = [self.parse_daily_dates(x, pattern, ext, rename=rename)
                     for x in all_files]
            flat_dates = list(itertools.chain.from_iterable(all_dates))

        self.existing_dates = flat_dates

    def match_demand_urls(self):
        """ Parses the demand urls which are in a slightly unique format"""

        self.url_dates = [self.parse_demand_date(x) for
                          x in self.pattern_files]
        self.url_base = {self.parse_demand_date(x): x['href'] for
                         x in self.pattern_files}


    def parse_demand_date(self, x):
        """ Nodal Demand is referenced by time of file creation, not
        the data itself. This is a major pain in the proverbial and
        requires some different matching behaviour
        """
        string_rep = " ".join([y for y in x.text.split(' ') if y != ''])
        return datetime.datetime.strptime(string_rep, "%d %B %Y")


    def hit_historic_nodal_demand(self):
        """ Historic nodal demand has different structure"""

        self.get_links(self.url)
        month_links = [x for x in self.all_links if "demand_pages" in x]
        all_urls = [os.path.join(self.base_url, 'comitFta', x['href']) for
                    x in month_links]
        return all_urls



    def set_parameters(self, seed):
        """ Parse a configuration file for the seed (loaded as a dictionary
        from JSON) and update a number of parameters. Internally updates
        class parameters.

        Parameters:
        -----------
        seed: A string containing the dictionary key. Must match one of the
              seeds in the configuration file.

        Returns:
        --------
        self.file_location: File Save Location
        self.url: URL to Scrape
        self.pattern: Pattern Matching
        self.date_type: How the Data files are structured (e.g. Daily vs
                        Monthly, changing parsing behaviour.)
        self.match_type: Either 'href' or 'text', what to match the pattern on
        self.ext: file extension
        self.base_url: urls are parsed as relative for downloading relative to
                       the base url
        self.temp_loc: A temporary location to save the data to, for extraction
                       etc
        self.cext: optional, additional compression
        self.rename: optional, some data sources has human fingers on it...

        """

        # These parameters must be passed or KeyErrors exist
        # They constitute the base of a seed
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

        # Quick check to ensure the directory exists.
        if not os.path.isdir(self.file_location):
            os.mkdir(self.file_location)


    def scrape_seed(self, seed):
        """ Using a Seed which refers to a series of configuration values
        (Loaded from a config.json file) scrape a particular website.

        This class will parse the CONFIG file to generate a series of
        parameters and then passes these to a separate function to download
        all of the required files.

        It should work for both WITS and EMI scrapers at the moment using
        different seed patterns. In addition it will log some of this output
        to Screen (this needs to be changed to a separate, persistent, logging
        file).

        Each Seed contains metadata, adding a new Scraper can be a simple
        case of adding a new Seed in certain situations. This does depend on
        a site by site basis.

        Parameters:
        -----------
        seed: A string containing the dictionary key. Must match one of the
              seeds in the configuration file.


        Returns:
        --------
        This function will save data files according to the particular seed
        values passed. It has support for extraction, XML conversion and
        hitting multiple URLS.
        """

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


    def hit_all_seeds(self):
        """ Hits all of the relevant (recent) Seeds"""

        seeds = [x for x in self.CONFIG.keys() if "EMI" in x or "WITS" in x]
        act_seeds = [x for x in seeds if "Historic" not in x]
        for s in act_seeds:
            self.scrape_seed(s)


if __name__ == '__main__':
    pass

