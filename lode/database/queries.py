from helpers import check_required_range, check_optional_range
import query_builders as qb


def query_nodal_price(begin_date=None, end_date=None, dates=None,
                      begin_period=None, end_period=None, periods=None,
                      minimum_price=None, maximum_price=None,
                      apply_meta=False, database=None):
    """

    """

    # Set Database to the default nodal prices one
    if database is None:
        database = 'nodal_database'

    # Check the dates
    check_required_range(dates, begin_date, end_date)
    check_optional_range(periods, begin_period, end_period)

    all_queries = qb.create_date_limited_sql("nodal_prices", dates=dates,
                                             begin_date=begin_date,
                                             end_date=end_date,
                                             date_col="Trading_date")

    return all_queries


if __name__ == '__main__':
    pass
