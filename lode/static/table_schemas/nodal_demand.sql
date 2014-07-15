CREATE TABLE IF NOT EXISTS nodal_demand_%s
(
nodal_demand_key serial primary key,
Trading_date date,
Trading_period int,
Time time,
Island varchar(2),
Node varchar(7),
Demand double precision,
UNIQUE(Trading_date, Trading_period, Node)
);
