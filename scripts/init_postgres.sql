-- DROP SCHEMA IF EXISTS analytics CASCADE;
CREATE SCHEMA IF NOT EXISTS analytics;
-- 1. Dimension: Airlines
CREATE TABLE IF NOT EXISTS analytics.dim_airlines (
    airline_id SERIAL PRIMARY KEY,
    airline_name VARCHAR(100) UNIQUE
);
-- 2. Dimension: Airports (Source/Destination)
CREATE TABLE IF NOT EXISTS analytics.dim_airports (
    airport_id SERIAL PRIMARY KEY,
    airport_code VARCHAR(10) UNIQUE,
    airport_name VARCHAR(255)
);
-- 3. Dimension: Date (for time-based analysis)
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    seasonality VARCHAR(50)
);
-- 4. Fact Table: Flights
CREATE TABLE IF NOT EXISTS analytics.fact_flights (
    flight_id SERIAL PRIMARY KEY,
    airline_id INT REFERENCES analytics.dim_airlines(airline_id),
    source_airport_id INT REFERENCES analytics.dim_airports(airport_id),
    destination_airport_id INT REFERENCES analytics.dim_airports(airport_id),
    departure_date_id DATE REFERENCES analytics.dim_date(date_id),
    aircraft_type VARCHAR(100),
    class VARCHAR(50),
    stopovers VARCHAR(50),
    booking_source VARCHAR(100),
    duration_hours DECIMAL(10, 2),
    days_before_departure INT,
    base_fare DECIMAL(12, 2),
    tax_surcharge DECIMAL(12, 2),
    total_fare DECIMAL(12, 2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);