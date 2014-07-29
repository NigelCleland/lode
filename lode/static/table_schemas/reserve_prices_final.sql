CREATE TABLE IF NOT EXISTS reserve_prices
(
reserve_prices_key serial primary key,
trading_date date,
trading_period int,
region varchar(255),
fir_price double precision,
sir_price double precision,
UNIQUE(trading_date, trading_period, region)
);
