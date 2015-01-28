from helpers import (check_required_range, check_optional_range,
                     multi_query, merge_meta)
import query_builders as qb
import warnings


def query_nodal_price(begin_date=None, end_date=None, dates=None,
                      begin_period=None, end_period=None, periods=None,
                      minimum_price=None, maximum_price=None, nodes="Major",
                      apply_meta=False, database=None):
    """
    Query the Nodal Price in a simplified fashion with all of the information
    held behind the scenes. This function is one of the primary interfaces
    with the database although the queries can be run directly as needed using
    other hidden functionality.

    Parameters:
    -----------
    begin_date: What begin point of a date range to query, string or datetime
    end_date: What end point of a date range to query, string or datetime
    dates: What dates to specifically query, string, list
    begin_period: Beginning point of a query range, int
    end_period: End point of a query range, int
    periods: What periods to query, defaults to all
    nodes: What nodes to query, defaults to the Major nodes
    minimum_price: Minimum price to query, useful for finding specific
                   instances of a priced situation over a wide number of time
                   instances
    maximum_price: Maximum price to query, e.g. use in combination with
                   minimum_price
    apply_meta: Whether to apply the associated meta information to the
                returned dataframe
    database: What database to connect to, leave None for the default setup

    Returns:
    --------
    A DataFrame containing all of the information with the specific filters
    applied

    """

    # Set Database to the default nodal prices one
    if database is None:
        database = 'nodal_database'

    # Core nodes are those of some importance to the market in some way
    # E.g. major population centre, generation site, HVDC etc
    if nodes == "Core":
        nodes = ("OTA2201", "BEN2201", "HAY2201", "HLY2201",
                 "MAN2201", "BPE2201", "PEN2201", "ISL2201")
    # Major nodes are the big three, Auckland, NI HVDC, SI HVDC
    elif nodes == "Major":
        nodes = ("OTA2201", "HAY2201", "BEN2201")

    # Check the dates
    check_required_range(dates, begin_date, end_date)
    check_optional_range(periods, begin_period, end_period)

    all_queries = qb.create_date_limited_sql("nodal_prices", dates=dates,
                                             begin_date=begin_date,
                                             end_date=end_date,
                                             date_col="Trading_date")

    completed_queries = []
    for sql in all_queries:
        if periods:
            sql += qb.add_equality_constraint('Trading_period', periods)

        if (begin_period and end_period):
            sql += qb.add_range_constraint('Trading_period',
                                           begin_period, end_period)

        if nodes:
            sql += qb.add_equality_constraint('Node', nodes)

        if minimum_price:
            sql += qb.add_minimum_constraint('Price', minimum_price)

        if maximum_price:
            sql += qb.add_maximum_constraint('Price', maximum_price)

        # Finish modifying the SQL so add a semicolon to end it
        sql += ';'

        # Add to the completed queries
        completed_queries.append(sql)

    # Query using the query to dataframe method and a generator
    prices = multi_query(database, completed_queries)

    if apply_meta:
        warnings.warn('Metadata may be incomplete')
        prices = merge_meta(prices, 'price')

    return prices


def query_nodal_demand(begin_date=None, end_date=None, dates=None,
                       begin_period=None, end_period=None, periods=None,
                       nodes=None, minimum_demand=None,
                       maximum_demand=None, apply_meta=False,
                       meta_group=None, meta_agg=None,
                       excl_nodes=None, database=None):
    """
    Query the nodal demand from the database at the GXP level. This results
    in very large DataFrames as there are approximately 450 nodes to query
    at any given point in time. Some values may be negative (for generation)
    and these can be excluded by setting a minimum demand or specifically via
    the various nodes.

    Meta information can also be applied but this may be incomplete at the
    current point in time. Use at your own risk.

    Parameters:
    -----------
    begin_date: What begin point of a date range to query, string or datetime
    end_date: What end point of a date range to query, string or datetime
    dates: What dates to specifically query, string, list
    begin_period: Beginning point of a query range, int
    end_period: End point of a query range, int
    periods: What periods to query, defaults to all
    nodes: What nodes to query, defaults to all
    minimum_demand: Minimum GXP demand figure, may wish to set to 0.
    maximum_demand: Maximum GXP demand figure
    apply_meta: Whether to add the meta information such as richer locations
    meta_group: Whether to group the nodes by any particular reference point
    meta_agg: Apply a meta aggregation function
    excl_nodes: What nodes to exclude, optionally set this to "Wind"
    database: Optional, what Database to connect to, leave this Node for the
              default, useful if you have changed the database names.

    Returns:
    --------
    DataFrame: Pandas Dataframe containing the query information
    """

    if database is None:
        database = 'nodal_database'

    # Helper function for the wind nodes as wind is a negative load at the
    # moment on the GXPs
    if excl_nodes == "Wind":
        excl_nodes = ("TWC2201", "WDV1101", "WWD1101", "WWD1102",
                      "WWD1103", "TWH0331")

    # Error checking on the dates and period range consistencies
    check_required_range(dates, begin_date, end_date)
    check_optional_range(periods, begin_period, end_period)

    # Set up the initial SQL queries with dates loaded in
    all_queries = qb.create_date_limited_sql("nodal_demand", dates=dates,
                                             begin_date=begin_date,
                                             end_date=end_date,
                                             date_col="Trading_date")

    completed_queries = []
    for sql in all_queries:
        if periods:
            sql += qb.add_equality_constraint('Trading_period', periods)

        if (begin_period and end_period):
            sql += qb.add_range_constraint('Trading_period',
                                           begin_period, end_period)

        if nodes:
            sql += qb.add_equality_constraint("Node", nodes)

        if minimum_demand:
            # Don't use zero values, instead use a very small float
            if minimum_demand == 0:
                minimum_demand = 0.0001
            sql += qb.add_minimum_constraint("Demand", minimum_demand)

        if maximum_demand:
            sql += qb.add_maximum_constraint("Demand", maximum_demand)

        # Exclude certain nodes
        if excl_nodes:
            sql += qb.add_exclusion_constraint("Node", excl_nodes)

        sql += ';'
        completed_queries.append(sql)

    demand = multi_query(database, completed_queries)

    if apply_meta:
        warnings.warn('Metadata may be incomplete')
        demand = merge_meta(demand, 'demand')

        if meta_group and meta_agg:
            grouped = demand.groupby(meta_group)
            aggregate = grouped.aggregate(meta_agg)

            return aggregate

    return demand


if __name__ == '__main__':
    pass
