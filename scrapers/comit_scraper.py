#!/usr/bin/python

import pandas as pd
from datetime import datetime, timedelta
import mechanize
import cookielib
from io import StringIO
import logging
import logging.handlers
import os
import simplejson
import argparse

# Do some wizardry to get the location of the config file
file_path = os.path.abspath(__file__)
module_path = os.path.split(os.path.split(file_path)[0])[0]
config_name = os.path.join(module_path, 'config.json')

log = logging.getLogger('comit')
log.setLevel(logging.INFO)  # print everything above INFO
formatter = logging.Formatter('|%(asctime)-6s|%(message)s|',
                                      '%Y-%m-%d %H:%M:%S')
logstream = logging.StreamHandler()
logstream.setFormatter(formatter)
log.addHandler(logstream)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('-u', '--comit_user', action="store", dest='comit_user',
                    help='comit hydro username')
parser.add_argument('-p', '--comit_pass', action="store", dest='comit_pass',
                    help='comit hydro password')
args = parser.parse_args()

class comit_scraper(object):
    """This class acts as an automatic downloader for the NIWA Comit hydro data
       Current mode of operation in to grab the whole dataset with each call.
       Required comit hydro password"""
    def __init__(self, comit_user, comit_pass):
        super(comit_scraper, self).__init__()
        self.refresh_config()
        self.comit_host = self.CONFIG['comit_base_url']
        self.comit_path = self.CONFIG['comit_data_folder']
        self.comit_user = comit_user
        self.comit_pass = comit_pass
        self.comit_site = self.comit_host + '''/comitweb/request_template.html'''
        self.inflows_names = {'csv': 'inflows.csv', 'pickle': 'inflows.pickle'}
        self.storage_names = {'csv': 'storage.csv', 'pickle': 'storage.pickle'}
        self.locations = range(1, 15)
        self.df_stored = None
        self.inflows = None
        self.br = None

    def refresh_config(self):
        """ This permits hot loading of the config file instead of linking
        it to only be initialised on startup
        """
        with open(config_name, 'rb') as f:
            self.CONFIG = simplejson.load(f)

        return self

    def date_parser(self, x):  # Date parser for comit data
        x = x.split(' ')[0]
        return datetime.date(datetime(int(x.split('/')[2]),
                                      int(x.split('/')[1]),
                                      int(x.split('/')[0])))

    def get_data(self, location, storage_inflows):  # Data scraper, hard coded form filler and parser for comit data
        self.enter_comit()  # enter comit for each data pass
        self.br.select_form('test')  # select form
        self.br['loca'] = [location]
        self.br['dura'] = ['365.25']
        self.br['dury'] = "1000"
        self.br['mfiy'] = "1932"
        self.br['mlay'] = "2013"
        self.br['efiy'] = "1932"
        self.br['elay'] = "2013"
        self.br['todo'] = ["3"]
        #br['quan'] = "3"  #!!This took ages to work out.
        #The name "quan" is used twice so we can't set the form as above
        if storage_inflows == 'storage':
            controls = self.br.form.controls  # Set QUANTITIES, either {"1":INFLOW, "2":CUSUM (GWh), "3":STORED (GWh)}
            controls[7].value = ["3"]  # STORED (GWh)
        if storage_inflows == 'inflows':
            controls = self.br.form.controls  # Set QUANTITIES, either {"1":INFLOW, "2":CUSUM (GWh), "3":STORED (GWh)}
            controls[7].value = ["1"]  # INFLOW
        self.br.set_all_readonly(False)
        self.br['Submit'] = 'Display'
        response = self.br.submit()
        link = [l for l in self.br.links()][-1]
        self.br.click_link(link)
        response = self.br.follow_link(link).read()
        name = response.split(', ')[0]
        bufferIO = StringIO()  # Open a string buffer object, write the POCP database to this then read_csv the data...
        bufferIO.write(unicode(response))
        bufferIO.seek(0)
        data = pd.read_csv(bufferIO, skiprows=[0, 1, 2, 3]).rename(columns={'Unnamed: 0': 'date'})
        del data['Unnamed: 3']
        data['date'] = data.date.map(lambda x: self.date_parser(x))
        data = data.set_index('date')
        #print data
        year = str((datetime.now() - timedelta(days=1)).year)
        year_inflow = year + ' inflow'
        year_stored = year + ' stored'
        if storage_inflows == 'inflows':
            return data[year_inflow], name
        if storage_inflows == 'storage':
            return data[year_stored], name

    def enter_comit(self):
        try:
            self.br = mechanize.Browser()    # Browser
            cj = cookielib.LWPCookieJar()    # Cookie Jar
            self.br.set_cookiejar(cj)        # Browser options
            self.br.set_handle_equiv(True)
            self.br.set_handle_gzip(False)
            self.br.set_handle_redirect(True)
            self.br.set_handle_referer(True)
            self.br.set_handle_robots(False)
            self.br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)  # Follows refresh 0 but not hangs on refresh > 0
            self.br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
            self.br.add_password(self.comit_host, self.comit_user, self.comit_pass)
            self.br.open(self.comit_host)
            self.br.open(self.comit_site)
        except:
            error_text = "Unable to log into comit Hydro"
            log.error(error_text.center(msg_len, '*'))

    def get_all_data(self):
        self.storage = {}
        self.inflows = {}
        start_text = 'Scraping comit Hydro @ ' + self.comit_site
        log.info(start_text.center(msg_len, ' '))
        for loci in self.locations:
            data_storage, name_storage = self.get_data(str(loci), 'storage')
            data_inflows, name_inflows = self.get_data(str(loci), 'inflows')
            self.storage[name_storage] = data_storage
            self.inflows[name_inflows] = data_inflows

    def df_the_data(self):
        df_storage = pd.DataFrame(self.storage)
        df_inflows = pd.DataFrame(self.inflows)
        df_storage = df_storage.rename(columns=dict(zip(df_storage.columns, df_storage.columns.map(lambda x: x.split(' ')[0]))))
        df_inflows = df_inflows.rename(columns=dict(zip(df_inflows.columns, df_inflows.columns.map(lambda x: x.split(' ')[0]))))
        self.df_storage = df_storage.shift(-1).ix[:-1, :]  # shifting time stamp to previous day
        self.df_inflows = df_inflows.shift(-1).ix[:-1, :]

    def to_pickle_and_csv(self):
        self.df_storage.to_pickle(self.comit_path + 'data/' + self.storage_names['pickle'])
        self.df_inflows.to_pickle(self.comit_path + 'data/' + self.inflows_names['pickle'])
        self.df_storage.to_csv(self.comit_path + 'data/' + self.storage_names['csv'])
        self.df_inflows.to_csv(self.comit_path + 'data/' + self.inflows_names['csv'])
        done_text = 'GOT comit Hydro data, saved to ' + self.comit_path + 'data/'
        log.info(done_text.center(msg_len, ' '))


#Start the programme
msg_len = 88
if __name__ == '__main__':
    cs = comit_scraper(args.comit_user, args.comit_pass)  # run instance
    cs.get_all_data()  # get all the data!
    cs.df_the_data()  # data frame the data
    cs.to_pickle_and_csv()
