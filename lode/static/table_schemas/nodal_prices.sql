CREATE TABLE IF NOT EXISTS nodal_prices_%s
(
nodal_prices_key serial primary key,
Trading_date date,
Trading_period int,
Node varchar(7),
Price double precision,
UNIQUE(Trading_date, Trading_period, Node)
);
