CREATE TABLE IF NOT EXISTS lake_levels
(
lake_levels_key serial primary key,
Trading_Date date,
Lake_Levels double precision,
Lake varchar(12),
Unit varchar(3),
UNIQUE(Trading_Date, Lake)
);
