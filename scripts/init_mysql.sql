CREATE DATABASE IF NOT EXISTS staging_flight_data;
USE staging_flight_data;
CREATE TABLE IF NOT EXISTS raw_flight_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(100),
    destination_code VARCHAR(10),
    destination_name VARCHAR(100),
    departure_datetime VARCHAR(50),
    arrival_datetime VARCHAR(50),
    duration_hours DECIMAL(10, 4),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare DECIMAL(12, 2),
    tax_surcharge DECIMAL(12, 2),
    total_fare DECIMAL(12, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS processed_hashes (
    row_hash CHAR(32) PRIMARY KEY,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);