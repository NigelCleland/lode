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
        os.chdir(self.CONFIG['temporary_location'])

        call_signature = ['7z', 'e', fName]
        subprocess.call(call_signature, stdout=sys.stdout,
                        stderr=sys.stderr)

        os.remove(fName)

        # Restore the original directory
        os.chdir(cwd)

        return fName.replace(cext, '')


if __name__ == '__main__':
    pass

