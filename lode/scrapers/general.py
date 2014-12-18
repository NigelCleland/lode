import requests
from bs4 import BeautifulSoup
import urllib2
import os
import subprocess
import sys
import logging

# The XML parser for frequency keeping.
from xmlutils.xml2csv import xml2csv

# This stops a download which has timed out from bringing
# down everything.
# A consequence is that you may need to run the script multiple times.
import socket
socket.setdefaulttimeout(10)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler

handler = logging.FileHandler('scraping.log')
handler.setLevel(logging.INFO)

# create a logging format

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger

logger.addHandler(handler)


def find_all_links(url):

    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    return soup.findAll('a')


def download_file(url, location, failure_mode=False, overwrite=False):

    save_location = os.path.join(location, os.path.basename(url))

    # Can choose not to overwrite any existing files
    if not overwrite:
        if os.path.exists(save_location):
            logger.info('%s already exists' % save_location)
            return save_location

        logger.info('Downloading %s' % url)
        try:
            opened = urllib2.urlopen(url)
            with open(save_location, 'wb') as w:
                w.write(opened.read())
        except Exception, e:
            logger.error('Failed to download %s' % url, exc_info=True)
            save_location = None

            if failure_mode:
                logger.warn('Quitting Process as unable to download')
                raise
            else:
                logger.info('Continuing although unable to download')

    return save_location


def extract_csvz_file(fName, remove_original=False):

    # Directory switching to ensure the extraction works
    cwd = os.getcwd()
    os.chdir(os.path.dirname(fName))

    # Extract the file
    extract_call = ['7z', 'e', fName]
    logger.info('Attempting to extract %s' % fName)
    subprocess.call(extract_call, stdout=sys.stdout, stderr=sys.stderr)
    logger.info('Extraction should be completed')

    # Check to see if the file exits now.
    save_name = fName.replace('.Z', '')
    if os.path.exists(save_name):
        logger.info('%s exists as expected' % save_name)
    else:
        logger.warn("Expected file does not exist for %s" % save_name)

    if remove_original:
        logger.info('Removing original file %s' % fName)
        os.remove(fName)

    # Restore the original directory
    os.chdir(cwd)

    return save_name


def get_pattern_links(url, pattern, match_type='href'):

    links = find_all_links(url)

    if match_type == 'href':
        return [x for x in links if pattern in os.path.basename(x['href'])]
    else:
        return [x for x in links if pattern in x.text]


def xml_to_csv(fName, tag='Row'):

    output_name = fName.replace('.XML', '.csv')
    converter = xml2csv(fName, output_name, encoding='utf-8')
    converter.convert(tag=tag)

    return None


def list_differences(s1, s2):
    return list(set(s1).difference(set(s2)))


if __name__ == '__main__':
    pass
