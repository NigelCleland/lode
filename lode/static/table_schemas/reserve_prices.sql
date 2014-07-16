CREATE TABLE IF NOT EXISTS reserve_prices
(
reserve_prices_key serial primary key,
Period_start timestamp,
Period_end timestamp,
Trading_period int,
Region_ID varchar(2),
Region varchar(255),
FIR_price double precision,
SIR_price double precision,
UNIQUE(Period_start, Trading_period, Region)
);
