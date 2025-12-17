-- Create basic tables for weather data

CREATE SCHEMA IF NOT EXISTS raw;

-- Postal codes
CREATE TABLE IF NOT EXISTS raw.postal_codes (
    plz VARCHAR(5) PRIMARY KEY,
    name VARCHAR(255)
);

-- Weather observations
CREATE TABLE IF NOT EXISTS raw.weather_observations (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    temperature NUMERIC,
    humidity NUMERIC
);

-- TODO: Add more fields as needed
