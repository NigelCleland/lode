CREATE TABLE IF NOT EXISTS lake_inflows
(
lake_inflows_key serial primary key,
Trading_Date date,
Inflows double precision,
Lake varchar(12),
Unit varchar(5),
UNIQUE(Trading_Date, Lake)
);
