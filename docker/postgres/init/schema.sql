CREATE TABLE airline
(
    carrier_code VARCHAR(10) PRIMARY KEY
);

CREATE TABLE airport
(
    airport_code VARCHAR(10) PRIMARY KEY,
    city_name    VARCHAR(100),
    state_name   VARCHAR(50)
);

CREATE TABLE flights
(
    flight_id         SERIAL PRIMARY KEY,
    year              INT,
    month             INT,
    day_of_month      INT,
    day_of_week       INT,
    fl_date           TIMESTAMP,
    op_unique_carrier VARCHAR(10) NOT NULL,
    op_carrier_fl_num VARCHAR(20),
    origin            VARCHAR(10) NOT NULL,
    dest              VARCHAR(10) NOT NULL,
    crs_dep_time      INT,
    crs_arr_time      INT,
    crs_elapsed_time  INT,
    distance          INT
);

CREATE TABLE flights_performance
(
    flight_id           INT PRIMARY KEY,
    dep_time            INT,
    dep_delay           INT,
    taxi_out            INT,
    wheels_off          INT,
    wheels_on           INT,
    taxi_in             INT,
    arr_time            INT,
    arr_delay           INT,
    actual_elapsed_time INT,
    air_time            INT,
    diverted            BOOLEAN,
    delay_id            INT
);

CREATE TABLE flights_delayed
(
    flight_id           INT PRIMARY KEY,
    carrier_delay       INT,
    weather_delay       INT,
    nas_delay           INT,
    security_delay      INT,
    late_aircraft_delay INT
);

CREATE TABLE flights_cancelled
(
    flight_id         INT PRIMARY KEY,
    cancelled         BOOLEAN,
    cancellation_code VARCHAR(10)
);

CREATE TABLE flight_status
(
    flight_id       INT PRIMARY KEY,
    performance_id  INT,
    cancellation_id INT,
    CONSTRAINT chk_one_filled CHECK (
        (performance_id IS NOT NULL AND cancellation_id IS NULL)
            OR
        (performance_id IS NULL AND cancellation_id IS NOT NULL)
        )
);

ALTER TABLE flights
    ADD CONSTRAINT fk_airline FOREIGN KEY (op_unique_carrier) REFERENCES airline (carrier_code);

ALTER TABLE flights
    ADD CONSTRAINT fk_origin FOREIGN KEY (origin) REFERENCES airport (airport_code);

ALTER TABLE flights
    ADD CONSTRAINT fk_dest FOREIGN KEY (dest) REFERENCES airport (airport_code);

ALTER TABLE flights_performance
    ADD CONSTRAINT fk_perf_flight FOREIGN KEY (flight_id) REFERENCES flights (flight_id);

ALTER TABLE flights_performance
    ADD CONSTRAINT fk_delay FOREIGN KEY (delay_id) REFERENCES flights_delayed (flight_id);

ALTER TABLE flights_delayed
    ADD CONSTRAINT fk_delayed_perf FOREIGN KEY (flight_id) REFERENCES flights_performance (flight_id);

ALTER TABLE flights_cancelled
    ADD CONSTRAINT fk_cancelled_flight FOREIGN KEY (flight_id) REFERENCES flights (flight_id);

ALTER TABLE flight_status
    ADD CONSTRAINT fk_status_flight FOREIGN KEY (flight_id) REFERENCES flights (flight_id);

ALTER TABLE flight_status
    ADD CONSTRAINT fk_status_perf FOREIGN KEY (performance_id) REFERENCES flights_performance (flight_id);

ALTER TABLE flight_status
    ADD CONSTRAINT fk_status_cancel FOREIGN KEY (cancellation_id) REFERENCES flights_cancelled (flight_id);
