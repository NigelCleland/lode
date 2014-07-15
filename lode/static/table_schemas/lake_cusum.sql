CREATE TABLE IF NOT EXISTS lake_cusum
(
lake_cusum_key serial primary key,
Trading_Date date,
Cusum double precision,
Lake varchar(12),
Unit varchar(3),
UNIQUE(Trading_Date, Lake)
);
