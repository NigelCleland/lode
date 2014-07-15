CREATE TABLE IF NOT EXISTS temperature_data_%s
(
temperature_data_key serial primary key,
Local_Time time,
Temperature int,
Dewpoint int,
Humidity int,
Barometer int,
Visibility int,
Wind Direction varchar(15),
Wind Speed double precision,
Gust Speed double precision,
Precipitation double precision,
Events varchar(255),
Conditions varchar(255),
Trading_Date date,
Location varchar(255),
UNIQUE(Trading_Date, Local_Time, Location)
);
