CREATE TABLE IF NOT EXISTS hvdc_flows_%s
(
hvdc_flows_key serial primary key,
POC varchar(7),
NWK_Code varchar(4),
GENERATION_TYPE varchar(2),
TRADER varchar(4),
UNIT_MEASURE varchar(3),
FLOW_DIRECTION varchar(1),
STATUS varchar(1),
TRADING_DATE date,
TP1 double precision,
TP2 double precision,
TP3 double precision,
TP4 double precision,
TP5 double precision,
TP6 double precision,
TP7 double precision,
TP8 double precision,
TP9 double precision,
TP10 double precision,
TP11 double precision,
TP12 double precision,
TP13 double precision,
TP14 double precision,
TP15 double precision,
TP16 double precision,
TP17 double precision,
TP18 double precision,
TP19 double precision,
TP20 double precision,
TP21 double precision,
TP22 double precision,
TP23 double precision,
TP24 double precision,
TP25 double precision,
TP26 double precision,
TP27 double precision,
TP28 double precision,
TP29 double precision,
TP30 double precision,
TP31 double precision,
TP32 double precision,
TP33 double precision,
TP34 double precision,
TP35 double precision,
TP36 double precision,
TP37 double precision,
TP38 double precision,
TP39 double precision,
TP40 double precision,
TP41 double precision,
TP42 double precision,
TP43 double precision,
TP44 double precision,
TP45 double precision,
TP46 double precision,
TP47 double precision,
TP48 double precision,
TP49 double precision,
TP50 double precision,
UNIQUE(POC, GENERATION_TYPE, FLOW_DIRECTION, TRADER, TRADING_DATE)
);
