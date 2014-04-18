import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
import pandas as pd


class TempScraper(object):
    def __init__(self):
        self.key_url = "http://www.weatherbase.com/weather/weatherhourly.php3?s=596460&date=%s&cityname=%s&units=metric"

        self.location_dict = {"Newmarket-Auckland" : "Auckland+-+Newmarket%2C+Auckland%2C+New+Zealand"}

    def query_page(self, location, date):
        try:
            dtstr = date.strftime("%Y-%m-%d")
        except:
            dtstr = parse(date, dayfirst=True).strftime("%Y-%m-%d")

        loc = self.location_dict[location]
        self.url = self.key_url % (dtstr, loc)

        try:
            self.get_hourly_table()
            self.get_rows()
            df = self.create_df()
            df["Trading_Date"] = date
            df["Location"] = location

            # Parse to get better formatting
            df = self.parse_time(df)
            df = self.parse_temperatures(df)
            df = self.parse_humidity(df)
            df = self.parse_pressure(df)
            df = self.parse_visibility(df)
            df = self.parse_wind_speed(df)

            return df
        except:
            print "Data does not exist for %s, %s" % (loc, dtstr)



    def get_hourly_table(self):
        self.req = requests.get(self.url)
        self.soup = BeautifulSoup(self.req.text)
        self.tables = self.soup.findAll('table')

        self.hourly_table = self.tables[14]


    def get_rows(self):
        tab_rows = self.hourly_table.findAll('tr')
        all_rows = [[v.text for v in r.contents] for r in tab_rows]
        self.headers = all_rows[0]
        self.rows = all_rows[1:]

    def create_df(self):

        self.df = pd.DataFrame(self.rows, columns=self.headers)
        return self.df

    def query_to_csv(self, location, date, fName):
        df = self.query_page(location, date)
        if isinstance(df, pd.DataFrame):
            df.to_csv(fName, header=True, index=False, encoding='utf-8')

    def query_range_to_folder(self, locations, begin_date, end_date, folder_location):

        dates = pd.date_range(begin_date, end_date)
        for loc in locations:
            for d in dates:
                print "Scraping %s for %s" % (loc, d.strftime("%Y-%m-%d"))
                fName = folder_location + "/%s_%s.csv" % (loc.replace('-', '_'), d.strftime("%Y%m%d"))
                self.query_to_csv(loc, d, fName)


    def parse_time(self, df):
        df["Local Time"] = df["Local Time"].apply(lambda x: parse(x).strftime("%H:%M"))
        return df

    def tf_parse(self, x, n=2):
        if len(x) == 0:
            return ""
        else:
            return float(x[:-n])

    def parse_temperatures(self, df):
        df["Temperature"] = df["Temperature"].apply(self.tf_parse, n=2)
        df["Dewpoint"] = df["Dewpoint"].apply(self.tf_parse, n=2)
        return df

    def parse_humidity(self, df):
        df["Humidity"] = df["Humidity"].apply(self.tf_parse, n=1)
        return df

    def parse_pressure(self, df):
        df["Barometer"] = df["Barometer"].apply(self.tf_parse, n=3)
        return df

    def parse_visibility(self, df):
        df["Visibility"] = df["Visibility"].apply(self.tf_parse, n=2)
        return df

    def parse_wind_speed(self, df):
        df["Wind Speed"] = df["Wind Speed"].apply(self.tf_parse, n=4)
        df["Gust Speed"] = df["Gust Speed"].apply(self.tf_parse, n=4)
        return df
