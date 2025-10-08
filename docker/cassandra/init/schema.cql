CREATE KEYSPACE IF NOT EXISTS flights
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE flights;

CREATE TABLE IF NOT EXISTS flights_by_route_day (
  origin TEXT,
  dest   TEXT,
  fl_date DATE,
  dep_time INT,
  op_unique_carrier TEXT,
  op_carrier_fl_num INT,
  arr_delay INT,
  dep_delay INT,
  distance INT,
  cancelled TINYINT,
  diverted TINYINT,
  PRIMARY KEY ((origin, dest, fl_date), dep_time, op_unique_carrier, op_carrier_fl_num)
) WITH CLUSTERING ORDER BY (dep_time ASC);

CREATE TABLE IF NOT EXISTS flights_by_carrier_day (
  op_unique_carrier TEXT,
  fl_date DATE,
  dep_time INT,
  origin TEXT,
  dest   TEXT,
  op_carrier_fl_num INT,
  arr_delay INT,
  dep_delay INT,
  cancelled TINYINT,
  diverted TINYINT,
  PRIMARY KEY ((op_unique_carrier, fl_date), dep_time, origin, dest, op_carrier_fl_num)
) WITH CLUSTERING ORDER BY (dep_time ASC);
