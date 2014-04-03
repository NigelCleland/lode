#!/usr/bin/python

'''
ASX web scrapper - automatic monitoring of ASX futures market

Used to scrape ASX Electrtcity Hedge Market data.

Data, includes:

    * since 14/7/2009, Benmore and Otahuhu quarterly;
    * since 9/12/2013, Benmore and Otahuhu base load monthly;
    * since 9/12/2013, Benmore and Otahuhu peak load quarterly.

ASX uses a two letter identifier for the different products.  This is
used to split the htm table into its consituent sub-tables.  These include:

    - BB: 90 Day Bank Bill (100 minus yield % p.a)
    - TY: 3 Year Stock (100 minus yield % p.a)
    - TN: 10 Year Stock (100 minus yield % p.a)
    - ZO: NZ 30 Day OCR Interbank (RBNZ Interbank Overnight Cash)
    - EA: NZ Electricity Futures (Otahuhu)
    - EE: NZ Electricity Futures (Benmore)
    - EB: NZ Electricity Strip Futures (Otahuhu)
    - EF: NZ Electricity Strip Futures (Benmore)
    - ED: ASX NZ Base Load Calendar Month Electricity Futures (Otahuhu)*
    - EH: ASX NZ Base Load Calendar Month Electricity Futures (Benmore)*
    - EC: ASX NZ Peak Load Electricity (Otahuhu)*
    - EG: ASX NZ Peak Load Electricity (Benmore)*

*Products that started trading on the 9/12/2013.

The asx_grabber class contains methods and functions to download, munge and
append ASX data.  Running day-to-day, the class downloads yesterdays daily data
(all products above), saves the raw data to a htm file, munges data into
dataframes and then updates each products timeseries Panel object.

Running for the first time, the process *should*:

    * automatically download all htm tables that exist upto yesterdays date;
    * then, once downloaded, process those htm files, appending the data to
      time-series objects and saving pickle/csv and xls files for data vis.

As above, the class is separated into two different procedures:

    * downloader: checks htm files are not already downloaded, before
                  downloading new htm files from the ASX website;
    * datamunger: checks the last downloaded date, vs the previously appended
                  date.  If there are new downloads, opens previous appended
                  time-series data and appends new data, saves to pickle/csv
                  and xls files.

This separation of downloading the data and munging improves performance and
testing of this code, simplifies the original code a little, and allows the
building of time-series data from scratch, or simple appends on a daily (or
more) basis.

Currently run daily at 9:05am (NZT), with the following crontab:
5 9 * * * /usr/bin/python /home/dave/python/asx/asx_daily.py >>
          /home/dave/python/asx/asx_daily.log 2>&1

Note: during testing we can revert data dir back with git:
   i.e., git checkout #githash_here -- data

DJ Hume 8 Jan, 2014.
'''
import pandas as pd
import numpy as np
import matplotlib.mlab as mp
from bs4 import BeautifulSoup
import mechanize
import os
from datetime import date, datetime, timedelta
import logging
import logging.handlers
import simplejson

# Do some wizardry to get the location of the config file
file_path = os.path.abspath(__file__)
module_path = os.path.split(os.path.split(file_path)[0])[0]
config_name = os.path.join(module_path, 'config.json')

log = logging.getLogger('asx')
log.setLevel(logging.INFO)  # print everything above INFO
formatter = logging.Formatter('|%(asctime)-6s|%(message)s|',
                                      '%Y-%m-%d %H:%M:%S')
logstream = logging.StreamHandler()
logstream.setFormatter(formatter)
log.addHandler(logstream)


class asx_scraper(object):
    '''This class acts as an automatic downloader for ASX hedge data'''
    def __init__(self):
        super(asx_scraper, self).__init__()
        self.refresh_config()
        self.asx_path = self.CONFIG['asx_data_folder']
        self.htm_dir = os.path.join(self.asx_path,'htm')  # Local downloads dir
        self.data_dir = os.path.join(self.asx_path, 'data')  # time series pickled panels
        self.url_head = self.CONFIG['asx_base_url']
        self.url_file_head = 'EODWebMarketSummary'
        self.asx_dirs = {'futures/total': 'ZFT'}  # ,'options/total':'ZOT'}
        self.idxer = {'BB': 'Q', 'TY': 'Q', 'TN': 'Q', 'ZO': 'M',
                      'EA': 'Q', 'EB': 'A', 'EC': 'Q', 'ED': 'M',
                      'EE': 'Q', 'EF': 'A', 'EG': 'Q', 'EH': 'M'}
        self.codes = {'BB': 'BB_90D', 'TY': 'TY_3YR', 'TN': 'TN_10Y',
                      'ZO': 'ZO_OCR', 'EA': 'EA_OTA', 'EB': 'EB_OTA',
                      'EC': 'EC_OTA', 'ED': 'ED_OTA', 'EE': 'EE_BEN',
                      'EF': 'EF_BEN', 'EG': 'EG_BEN', 'EH': 'EH_BEN'}
        self.cdict = {'Op Int': '_OPT', 'Volume': '_VOL', 'Sett': '_SET', 'all': ''}
        self.sd1 = date(2009, 7, 13)  # start of trading: EA/EB/EE/EF
        self.sd2 = date(2013, 12, 8)  # start of trading: EC/ED/EG/EH
        self.start_dates = {'BB': self.sd1, 'TY': self.sd1, 'TN': self.sd1,
                            'ZO': self.sd1, 'EA': self.sd1, 'EB': self.sd1,
                            'EC': self.sd2, 'ED': self.sd2, 'EE': self.sd1,
                            'EF': self.sd1, 'EG': self.sd2, 'EH': self.sd2}
        self.date = datetime.date(datetime.today() - timedelta(days=1))
        self.br = mechanize.Browser()  # Browser
        self.br.set_handle_refresh(
            mechanize._http.HTTPRefreshProcessor(), max_time=1)
        self.br.addheaders = [
            ('User-agent',
             'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
        self.asx_htm_data = None
        self.allasxdata = None
        self.last_htm_date = None
        self.last_pickle_date = None
        self.htm_got_data = False

    def refresh_config(self):
        """ This permits hot loading of the config file instead of linking
        it to only be initialised on startup
        """
        with open(config_name, 'rb') as f:
            self.CONFIG = simplejson.load(f)

        return self

    def fn(self, column, f_type, directory):
        '''Return filename dictionary with given inputs'''
        return {name: os.path.join(directory, "".join([i, self.cdict[column], '.', f_type]))
                for name, i in self.codes.iteritems()}

    def downloader(self):
        ''' Download and save all ASX files for the date required '''
        self.get_last_htm_date()  # get last_htm_date htm file was saved
        while self.last_htm_date < self.date:  # loop through days since
            self.last_htm_date = self.last_htm_date + timedelta(days=1)
            if self.last_htm_date.weekday() < 5:  # if Monday to Friday
                for dirs, tails in self.asx_dirs.iteritems():  # download, save
                    self.get_asx(dirs, tails, self.last_htm_date, 'web')

    def get_last_htm_date(self):
        ''' Return the date of the last htm save file '''
        x = []
        d = os.path.join(self.htm_dir, self.asx_dirs.keys()[0])
        if os.path.isdir(d):
            for filename in os.listdir(os.path.join(self.htm_dir, self.asx_dirs.keys()[0])):
                if 'Summary' in filename:
                    date_str = filename.split('.')[0].split('Summary')[1][0:6]
                    x.append(date(int('20' + date_str[0:2]),
                                  int(date_str[2:4]),
                                  int(date_str[4:6])))
            self.last_htm_date = np.sort(x)[-1]  # Return the last save date
            info_text = 'htm files last saved: %s' % str(self.last_htm_date)
            log.info(info_text)
        else:  # set date to day before start of market
            os.makedirs(d)
            self.last_htm_date = self.sd1
            info_text = 'No htm files found, start at %s' % str(self.sd1)
            log.info(info_text)

    def get_asx(self, dirs, tails, last_date, weblocal):
        ''' Note: Used in downloader and datamunger...
            Build filename string;
            Return self.asx_htm_data: save to disk if weblocal="web"'''
        os.chdir(os.path.join(self.htm_dir, dirs))
        url_file_date = str(last_date.year)[2:] + \
            "%02d" % (last_date.month) + \
            "%02d" % (last_date.day)  # YYMMDD format
        url_file = self.url_file_head + url_file_date + tails + '.htm'
        if weblocal == 'web':  # download and save to disk
            url = self.url_head + url_file  # add the url head
            try:
                r = self.br.open(url)  # open sfe website
                self.asx_htm_data = r.read()
                asx_to_disk = open(url_file, 'wb')  # write htm file to disk
                asx_to_disk.write(self.asx_htm_data)
                asx_to_disk.close()
                info_text = 'Download of %s to %s successful' % \
                    (url, self.htm_dir + dirs)
                log.info(info_text)
            except mechanize.HTTPError, error_text:
                log.error(error_text)
        if weblocal == 'local':  # read htm from local dir
            url = os.path.join(self.htm_dir, dirs, url_file)
            if os.path.isfile(url):
                try:
                    url_file_size = os.path.getsize(url)
                    if url_file_size > 5000:  # do we have data?
                        r = self.br.open('file://' + url)  # open local file
                        self.asx_htm_data = r.read()
                except mechanize.HTTPError, error_text:
                    log.error(error_text)

    def datamunger(self):
        ''' Update the pickle time-series files by looping through
        those htm files not already previously appended.  Normally, run daily,
        this will update the pickle Panel objects with just yesterdays htm
        data.  It may occur from time-to-time, during unplanned outages or
        other times, i.e., changes to the asx website tables, when we may be
        required to catch up to the htm file downloads.  We now handle
        this situation by effectively downloading htm files separately from the
        munging of the data'''
        if not os.path.isdir(self.data_dir):
            os.makedirs(self.data_dir)

        self.get_last_pickle_date()  # get last saved pickle date (Otahuhu)
        while self.last_pickle_date < self.last_htm_date:  # catch up
            self.last_pickle_date = self.last_pickle_date + timedelta(days=1)
            if self.last_pickle_date.weekday() < 5:  # if Monday to Friday
                for dirs, tails in self.asx_dirs.iteritems():
                    self.get_asx(dirs, tails, self.last_pickle_date, 'local')
                    self.get_asx_table()  # asx_htm_data into dict allasxdata
                    self.update_pickles()  # update ts panel objects

    def get_asx_table(self):
        soup = BeautifulSoup(self.asx_htm_data)
        body = soup.html.body
        tables = body.findAll('table')
        self.scrape_asx_table(tables[1])  # 3 tables; middle one has the data

    def scrape_asx_table(self, asx_table):
        '''Scrape the table in the htm file.
           This consists of 8/12 subtables and returns a dictionary
           of all asx data in dataframes (not a panel, yet...)
           Dataframes period indexed from self.idxer'''
        rows = asx_table.findAll('tr')  # get table rows
        all_data = []
        for row in rows:  # pass the rows
            entries = row.findAll('td')  # get a list of table data
            row_data = []
            for entry in entries:
                etext = entry.text.replace('-', '').replace(',', '')
                row_data.append(etext)
                all_data.append(row_data)
        colnames = filter(lambda d: d[0] == 'Expiry', all_data)  # filter data
        colnames = colnames[0]  # colnames = ['Expiry','Open','High', etc...
        ddd = filter(lambda d: d[0] != '' and
                     d[0] != 'Expiry' and
                     d[0] != 'Click here to view settlement price and volume graph', all_data)
        df_names = []  # get list of dataframes from table
        for d in ddd:
            if len(d) < 2:
                df_names.append(d[0][:2])
        xx = list(mp.find(map(lambda d: d[0][:2] in df_names, ddd)))
        xx.append(len(ddd))  # append the total length
        splits = {}  # split table data to 8/12 tables, prob a better way...
        ii = 0  # loop through the sub-tables
        for split in np.arange(len(xx) - 1):
            splits[ddd[xx[ii]][0][:2]] = ddd[(xx[ii] + 1):xx[ii + 1]]  # yuck.
            ii += 1
        self.allasxdata = {}  # create empty dict of dataframes
        for df_name, data in splits.iteritems():
            df_dict = {}
            for name, col in zip(colnames, zip(*data)):
                try:
                    df_dict[name] = np.array(col, dtype=float)
                except:
                    df_dict[name] = np.array(col)
            df = pd.DataFrame(df_dict, columns=colnames).replace(u'', 'nan')
            df = df.drop_duplicates()
            df.set_index('Expiry', drop=True, inplace=True)
            df.index = pd.period_range(df.index[0], df.index[-1],
                                       freq=self.idxer[df_name])
            df = df.applymap(lambda x: float(x))
            self.allasxdata[df_name] = df

    def get_last_pickle_date(self):
        ''' Get the last date that data was appended to time-series Panel
        objects. We will use Otahuhu Quarterly panel object for this and assume
        all other products are similar - should be ok...'''
        try:
            with open(self.fn('all', 'pickle', self.data_dir)['EA']):
                ea_ota = pd.read_pickle(self.fn('all', 'pickle', self.data_dir)['EA'])
                self.last_pickle_date = ea_ota.items[-1]
                info_text = 'Pickles last updated: %s' % str(self.last_pickle_date)
                log.info(info_text)
        except:
            info_text = 'No local pickles found, start at %s' % str(self.sd1)
            log.info(info_text)
            self.last_pickle_date = self.sd1

    def update_pickles(self, csv=True, xls=False):

        def panel_joiner(old_panel, new_panel):
            '''if we don't already have data for this day (useful when
            testing), then join new data to old data. Outer is the union
            of the indexes so when new rows appears for a new quaters,
            months, strips, etc, NaNs full the remainder of the df'''
            if self.last_pickle_date not in old_panel.items:
                return old_panel.join(new_panel, how='outer')

        for name, df in self.allasxdata.iteritems():
            if not df.empty:
                panel_new = pd.Panel({self.last_pickle_date: df})
                if self.last_pickle_date == (self.start_dates[name] +
                                             timedelta(days=1)):
                    pd.to_pickle(panel_new, self.fn('all', 'pickle', self.data_dir)[name])
                    info_text = 'New data found: %s on %s' % \
                        (name, str(self.last_pickle_date))
                    log.info(info_text)
                else:
                    panel_old = pd.read_pickle(self.fn('all', 'pickle', self.data_dir)[name])
                    panel = panel_joiner(panel_old, panel_new)
                    pd.to_pickle(panel, self.fn('all', 'pickle', self.data_dir)[name])
                    if csv:  # then dump to flat files
                        flats = self.cdict.keys()[:]
                        flats.remove('all')
                        for c in flats:  # pick and spit flat files
                            df = panel.xs(c, axis=2).T
                            if csv:
                                df.to_csv(self.fn(c, 'csv', self.data_dir)[name], index_label='date')  # to csv
                    if xls:
                        panel.to_excel(self.fn('all', 'xls', self.data_dir)[name])  # slow!
                        try:  # Now attempt to copy to P drive
                            panel.to_excel(self.fn('all', 'xls', self.asx_path_P)[name])
                            info_text = self.fn('all', 'xls', self.data_dir)[name] + ' --> ' + self.fn('all', 'xls', self.asx_path_P)[name]
                            log.info(info_text)
                        except:
                            info_text = 'Failed to access %s' % self.asx_path_P
                            log.info(info_text)

        info_text = 'Updated all data: %s' % str(self.last_pickle_date)
        log.info(info_text)


if __name__ == '__main__':
    asx = asx_scraper()
    asx.downloader()  # download htm files up until yesterday
    asx.datamunger()  # append data from new files to ts panel objects
