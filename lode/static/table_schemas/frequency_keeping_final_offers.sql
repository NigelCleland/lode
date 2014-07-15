CREATE TABLE IF NOT EXISTS frequency_keeping_final_offers_%s
(
frequency_keeping_final_offers_key serial primary key,
SUBMISSION_DATE timestamp,
PNODE_NAME varchar(13),
TRADER_NAME varchar(4),
TRADING_PERIOD_START_DATE date,
MW double precision,
PRICE double precision,
MIN_MW double precision,
MAX_MW double precision,
SRC_OFFER_BLOCK_ID, int,
TRADING_PERIOD int,
UNIQUE(PNODE_NAME, TRADER_NAME, TRADING_PERIOD_START_DATE, TRADING_PERIOD)
);
