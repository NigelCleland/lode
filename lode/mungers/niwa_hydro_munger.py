#!/usr/bin/python
import pandas as pd
import os

def parse_niwa_file(fName):
    """
    Parse NIWA's shitty Hydro format to get some useful information and
    filenames out of it. If they ever change the ordering this will need to be
    redone as a lot is hard coded
    """
    # Lake is identified in the first row
    # What kind of storage it is is in the fifth
    with open(fName, 'rb') as f:
        lake = f.readline().split(' ')[0]
        for i in xrange(0,3):
            f.readline()
        column_info = f.readline().split(',')[-2].strip().split(' ')[-1]

    # Dictionaries for the information
    information_types = {"stored": "Lake Levels",
                         "inflow": "Inflows",
                         "cusum": "Cusum"}
    units = {"stored": "GWh",
             "inflow": "GWh/d",
             "cusum": "GWh"}

    columns = ["Trading_Date", "Average Inflow", information_types[column_info], "None"]

    # Load the data, include extra columns with meta information
    raw_data = pd.read_csv(fName, skiprows=5, names=columns)
    raw_data["Lake"] = lake
    raw_data["Unit"] = units[column_info]

    # Drop the unneeded arrays
    result = raw_data.drop(["None", "Average Inflow"], axis=1)

    # Convert the dates
    result["Trading_Date"] = pd.to_datetime(result["Trading_Date"], dayfirst=True)

    # Get the first and last day of the selected series
    begin_period = result["Trading_Date"].min().strftime("%Y%m%d")
    end_period = result["Trading_Date"].max().strftime("%Y%m%d")

    # Construct a new file with useful information
    final_fName = "_".join([lake, information_types[column_info], begin_period, end_period]).replace(' ', '_') + '.csv'

    # Save the modified DataFrame to a csv file
    result.to_csv(final_fName, header=True, index=False)

