CREATE TABLE IF NOT EXISTS rich_demand_group_%s
(
rich_demand_group_key serial primary key,
trading_date date,
trading_period int,
time time,
island varchar(2),
load_area varchar(255),
island_name varchar(255),
Demand double precision,
UNIQUE(Trading_date, Trading_period, load_area)
);


