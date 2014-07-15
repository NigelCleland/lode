CREATE TABLE IF NOT EXISTS frequency_keeping_final_dispatch_%s
(
frequency_keeping_final_dispatch_key serial primary key,
OFFER_SUBMISSION_DATE timestamp,
TRADING_PERIOD_START_DATE date,
OFFER_PRICE double precision,
FREQUENCY_BAND_MW double precision,
ISLAND_NAME varchar(2),
PNODE_NAME varchar(13),
TRADER_NAME varchar(4),
TRADING_PERIOD int,
UNIQUE(TRADER_NAME, PNODE_NAME, TRADING_PERIOD_START_DATE, TRADING_PERIOD)
);
