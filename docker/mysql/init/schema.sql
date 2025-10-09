CREATE TABLE flights
(
    flight_id         INT AUTO_INCREMENT PRIMARY KEY,
    year              int,
    month             int,
    day_of_month      int,
    day_of_week       int,
    fl_date           datetime,
    op_unique_carrier varchar(10) NOT NULL,
    op_carrier_fl_num varchar(20),
    origin            varchar(10) NOT NULL,
    dest              varchar(10) NOT NULL,
    crs_dep_time      int,
    crs_arr_time      int,
    crs_elapsed_time  int,
    distance          int
);

CREATE TABLE flight_status
(
    flight_id       int PRIMARY KEY,
    performance_id  int,
    cancellation_id int,
    CONSTRAINT chk_one_filled CHECK (
        (performance_id IS NOT NULL AND cancellation_id IS NULL)
            OR
        (performance_id IS NULL AND cancellation_id IS NOT NULL)
        )
);

CREATE TABLE flights_performance
(
    flight_id           int PRIMARY KEY,
    dep_time            int,
    dep_delay           int,
    taxi_out            int,
    wheels_off          int,
    wheels_on           int,
    taxi_in             int,
    arr_time            int,
    arr_delay           int,
    actual_elapsed_time int,
    air_time            int,
    diverted            boolean,
    delay_id            int
);

CREATE TABLE flights_delayed
(
    flight_id           int PRIMARY KEY,
    carrier_delay       int,
    weather_delay       int,
    nas_delay           int,
    security_delay      int,
    late_aircraft_delay int
);

CREATE TABLE flights_cancelled
(
    flight_id         int PRIMARY KEY,
    cancellation_code varchar(10)
);


CREATE TABLE airline
(
    carrier_code varchar(10) PRIMARY KEY
);

CREATE TABLE airport
(
    airport_code varchar(10) PRIMARY KEY,
    city_name    varchar(100),
    state_name   varchar(50)
);

ALTER TABLE flights
    ADD FOREIGN KEY (op_unique_carrier) REFERENCES airline (carrier_code);

ALTER TABLE flights
    ADD FOREIGN KEY (origin) REFERENCES airport (airport_code);

ALTER TABLE flights
    ADD FOREIGN KEY (dest) REFERENCES airport (airport_code);

ALTER TABLE flight_status
    ADD FOREIGN KEY (flight_id) REFERENCES flights (flight_id);

ALTER TABLE flight_status
    ADD FOREIGN KEY (performance_id) REFERENCES flights_performance (flight_id);

ALTER TABLE flight_status
    ADD FOREIGN KEY (cancellation_id) REFERENCES flights_cancelled (flight_id);

ALTER TABLE flights_performance
    ADD FOREIGN KEY (flight_id) REFERENCES flights (flight_id);

ALTER TABLE flights_performance
    ADD FOREIGN KEY (delay_id) REFERENCES flights_delayed (flight_id);

ALTER TABLE flights_delayed
    ADD FOREIGN KEY (flight_id) REFERENCES flights_performance (flight_id);

ALTER TABLE flights_cancelled
    ADD FOREIGN KEY (flight_id) REFERENCES flights (flight_id);
