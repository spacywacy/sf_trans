CREATE TABLE traces_gpmode
(
epochtime numeric,
modetime timestamp,
vehicle integer,
bicycle integer,
foot integer,
still integer,
tilting integer,
unknownmode integer,
deviceid varchar(4)
);

CREATE INDEX deviceid_index
ON traces_gpmode (deviceid);

CREATE TABLE trips
(
tripid char(6),
departloc char(50),
arriveloc char(50),
departtime timestamp,
arrivetime timestamp,
note varchar(30),
mode char(3),
data_interval decimal,
insf char(3),
userid varchar(4),
departlat numeric,
departlon numeric,
arrivelat numeric,
arrivelon numeric,
PRIMARY KEY (tripid)
);

CREATE INDEX tripid_index
ON trips (tripid);

CREATE INDEX userid_index
ON trips (userid);

CREATE TABLE traces_location
(
alt numeric,
epochtime numeric,
speed numeric,
error numeric,
pointtime timestamp, 
deviceid varchar(4),
lat numeric,
lon numeric
);

CREATE INDEX deviceid_index_loc
ON traces_location (deviceid);


/*
COPY traces_gpmode FROM '/home/larry/code_push/sf_trans/mock_traces_gpmode.csv' DELIMITER ',' CSV HEADER;
COPY trips FROM '/home/larry/code_push/sf_trans/mock_trip.csv' DELIMITER ',' CSV HEADER;
*/

/*
pv /home/larry/code_push/sf_trans/sf_data/tqs.traces.gpmode.csv | psql -c "COPY traces_gpmode FROM STDIN DELIMITER ',' CSV HEADER;" sf_data
pv /home/larry/code_push/sf_trans/sf_data/trips.csv | psql -c "COPY trips FROM STDIN DELIMITER ',' CSV HEADER;" sf_data
pv /home/larry/code_push/sf_trans/sf_data/tqs.traces.location.csv | psql -c "COPY traces_location FROM STDIN DELIMITER ',' CSV HEADER;" sf_data
*/







































