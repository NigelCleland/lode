""" Automate the manipulation of grid import and grid export data to a
useful state. E.g. consistent naming schema and values with other units.
"""

import pandas as pd


def convert_grid_values(filename):

    # Load Data
    raw_data = pd.read_csv(filename)

    # Rename Columns, convert to upper case first as cases can differ...
    raw_data = raw_data.rename(columns={x: x.upper() for x in
                                        raw_data.columns})

    column_rename = {"POC": "Grid_Point", "NWK_CODE": "Distribution_Network",
                     "GENERATION_TYPE": "Generation_Type", "TRADER": "Company",
                     "UNIT_MEASURE": "Unit_Measure",
                     "FLOW_DIRECTION": "Flow_Direction", "STATUS": "Status",
                     "TRADING_DATE": "Trading_Date"}

    raw_data = raw_data.rename(columns=column_rename)

    # get all non trading period columns
    group_columns = [x for x in raw_data.columns if "TP" not in x]

    # Rename the TP Columns
    TP_Cols = {x: int(x[2:]) for x in raw_data.columns if "TP" in x}
    raw_data = raw_data.rename(columns=TP_Cols)

    # Change the unit measure
    raw_data["Unit_Measure"] = "MW"

    # Group and Stack the columns
    grouped = raw_data.groupby(group_columns).sum().stack()

    # Rename the Index
    index_names = list(grouped.index.names)
    index_names[-1] = "Trading_Period"
    grouped.index.names = index_names

    # Conver to MW from kWh
    conv_ratio = 1. / 500.  # 1 KWh = 0.002 MW average
    grouped = grouped * conv_ratio

    # Name the series
    grouped.name = "Average_Output"

    # Reset the Index to restore the columns
    final_result = grouped.reset_index()

    return final_result
