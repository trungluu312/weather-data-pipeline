# Weather Data Transformation Pipeline

This document explains how we turn raw weather data from stations into useful insights, organized by postal codes.

## Context
The goal of this pipeline is to take raw weather measurements from various stations and "map" them to specific postal codes. Since weather stations aren't perfectly aligned with postal code boundaries, we use spatial math to find the representative weather for every area.

---

## Staging - Intermediate - Marts approach

### Staging Layer
*Files: `stg_stations.sql`, `stg_observations.sql`, `stg_forecasts.sql`, `stg_postal_codes.sql`*

We take the raw data exactly as it comes from the sources (APIs and files) and give the columns clear, consistent names, select only required columns.
*   **Station Data**: We convert latitude and longitude into physical "points" on a map.
*   **Postal Codes**: We load the geographic shapes of every postal code area.

### Intermediate Layer
*Files: `int_station_location_map.sql`, `int_cleaned_observations.sql`, `int_cleaned_forecasts.sql`*

This is step to clean up and manipulate data:
*   **int_station_location_map**: Model to calculate the distance between every postal code and every weather station. The **top 10 nearest stations** for each postal code (within a 15km radius) are then picked. In addition, only stations that have at least some valid temperature records in our system are considered (some stations are available from the sources endpoint, but have no data)
*   **int_cleaned_observations** & **int_cleaned_forecasts**: Here "impossible" weather data (like a temperature of 100°C or -100°C) are thrown out to ensure our reports are accurate, this is also the step where stations data are matched with postal codes. In addition, only the latest forecast for any given hour is kept to ensure freshest data.

### Marts Layer
*Files: `mart_hourly_observations_pcode.sql`, `mart_hourly_forecasts_pcode.sql`*

This is the data ready for use by business analysts or ML models:
*   If a station was offline for an hour, we use the "Nearest Neighbor" logic to get data from the next closest station. If there's still a gap, we look at the previous hour's weather to provide a smart estimate (temporal fallback).
*   A clean table where you can simply look up a **Postal Code** and a **Time** to see exactly what the weather was (or will be).

---

## 🛠️ How to run it
You can refresh this entire logic by running:
```bash
make transform
```
