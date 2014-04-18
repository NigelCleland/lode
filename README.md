# Lode

A single repository for setting up data analytical toolkits for
the New Zealand Electricity Market using Python.

Lode handles the Scraping and setting up some sharded Databases
as well as providing a simplified query interface for querying
relevant data.

## Implemented Features

### Scrapers:

* WITS
* EMI
* COMIT (courtesy djhume)
* ASX (courtesy djhume)

### Databases with query interface:

* Energy Offers
* IL Offers
* Generator Reserve Offers

### Features in Planning:

The list of current features being worked on can be found at [Github Enhancement List](https://github.com/NigelCleland/lode/issues?labels=enhancement&page=1&state=open)



# Problem Statement

## The problem at hand:

* There are many different relevant data sources to electricity markets
* These range from technical information
    - Offer Data
    - Price Data
    - Demand Data
    - Transmission Data
    - Retail Market Information
    - Reserve Data
    - Frequency Keeping Data
    - vSPD GDX data
* For each of these points there is also rich metadata
    - Latitude and Longitude
    - Hydrology Information (Storage and Inflows)
    - Temperature and Humidity Information
    - Company Information
    - Technology Type Information
* Each of these sources of information is typically provided:
    - By a different provider
    - In a different format
    - With different indexing material
    - Is updated at different frequencies

What we want is to have an up to date data source which plays nice
with other sources of data. Furthermore, we want to maintain the
integrity of our primary data sources. But we also want to undertake
meaningful analysis which answers important questions.

## Data Flow Pipeline

So we are left with a pipeline of things which are needed.

1. Automated Collection of data
    - APIs
    - Scraping
    - Automated Downloads
2. Manipulation of this data (munging)
    - Cohesive Dates
    - Cohesive Location Identifiers
    - Rich Metadata (for example extended company/technology type information)
    - Simplified Merging
        + E.g. Ability to merge disparate data sources, such as offers and
        demand together in a simplified format.
        + Want a consistent series of indices. Perhaps DateTime, MarketNode
        which applies across all of the datasets
3. Analysis of Data and presentation of the results
    - Simplified formats to accomplish common functions
        + Hydro Risk Curves
        + Price Distributions
        + Offer Curves
        + Fan Curves
        + Demand Distributions
        + Others

## Data Sources:

The current proposed Data Sources

* EMI Dashboard - An EA maintained Dashboard, caveat it is new and may break
* FreeWITS - Consistent scraping access but a bit of a pain to work with
* NIWA Hydro - Proprietary
* Wunderground - Very few API Calls
* Weatherbase - May not be fair to hit there servers over and over for
temperature data
* Custom - Some custom compiled metadata, for example lat/log for different
market nodes.
* vSPD Data - Run vSPD and use the final output solution?

## Contributing

The above is a broad outline, a wish list so to speak.
The ultimate goal is to reduce redundancy especially in the data
manipulation and aggregation roles.
It is simply crazy that each person is maintaining their own sources
of data in a non consistent manner.

## Useful Resources

A significant amount of code has already been produced.
A key element of each of these pieces of code is that they do not
rely upon Databases. Perhaps the next step is to set up persistent
databases for each of the sources in order to maintain consistency.
For example:

[Tessen](https://www.github.com/NigelCleland/Tessen) is a module which
simplifies the creation of Fan Curves from generation and reserve
offer data.

[OfferPandas](https://www.github.com/NigelCleland/OfferPandas) is a module
which handles a significant amount of the pain of working with offer data.
It has support for richer metadata as well as working with different
formats in a simple manner.

[vSPUD](https://www.github.com/NigelCleland/vSPUD) is a module to make working
with multiple sets of vSPD final output data easier.

## Potential Database flow structure

- Automatically Scrape and Manipulate Raw Files
- Load Raw Files to a Database
    + PostGreSQL
    + Potential issues with Table Size?
- Have a number of queries created automatically for simple tasks
    + Use the power of SQL for data selection
    + Final manipulation occurs in python scientific and plotting stack

This structure would stop the need to rerun each of the munging steps at each
iteration. The caveat is that we are likely to be working on desktop
machines which can have limited RAM.


## Work To Do:

* Temperature Data
    - Source
    - Munging
* Hydrology Data
    - Source
    - Munging
* Common Munging Tasks
    - Frequency Keeping
    - Nodal Demand
    - Nodal Prices
    - Offer Data
    -

## License

MIT License, acknowledge where the work has come from, send the
contributions up stream so that we all benefit. Let me know if it's useful.

## Style Guide:

Where appropriate will try to form to PEP8:

* Prefered case for classes is CamelCase
* Preferse case for functions and variables is underscore_lower_style
* 80 char per line unless a good reason exists otherwise.
