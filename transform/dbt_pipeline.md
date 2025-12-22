# Weather Data Transformation Pipeline 🌩️

This document explains how we turn raw weather data into useful insights, organized by postal codes.

## 🗺️ The Big Picture
The goal of this pipeline is to take raw weather measurements from various stations and "map" them to specific postal codes. Since weather stations aren't perfectly aligned with postal code boundaries, we use spatial math to find the representative weather for every area.

---

## 🏗️ The Three Layers of Transformation

We use a "Medallion Architecture" approach to organize our data flow:

### 1. Staging Layer (The "Clean-Up")
*Files: `stg_stations.sql`, `stg_observations.sql`, `stg_forecasts.sql`, `stg_postal_codes.sql`*

Think of this as **re-labeling the boxes**. We take the raw data exactly as it comes from the sources (APIs and files) and give the columns clear, consistent names.
*   **Station Data**: We convert latitude and longitude into physical "points" on a map.
*   **Postal Codes**: We load the geographic shapes of every postal code area.

### 2. Intermediate Layer (The "Brain")
*Files: `int_station_location_map.sql`, `int_cleaned_observations.sql`, `int_cleaned_forecasts.sql`*

This is where the complex logic happens:
*   **The Spatial Matchmaker**: We calculate the distance between every postal code and every weather station. We then pick the **top 10 nearest stations** for each postal code (within a 15km radius).
*   **Active Station Filter**: We only consider stations that have at least some valid temperature records in our system.
*   **Data Quality Filter**: We throw out "impossible" weather data (like a temperature of 100°C or -100°C) to ensure our reports are accurate.
*   **The Version Controller**: Weather forecasts change over time. We make sure we only keep the **latest, most accurate prediction** for any given hour.

### 3. Marts Layer (The "Final Product")
*Files: `mart_hourly_observations_pcode.sql`, `mart_hourly_forecasts_pcode.sql`*

This is the data ready for use by business analysts or ML models:
*   **Filling the Gaps**: If a station was offline for an hour, we use the "Nearest Neighbor" logic to get data from the next closest station. If there's still a gap, we look at the previous hour's weather to provide a smart estimate (temporal fallback).
*   **The Result**: A clean table where you can simply look up a **Postal Code** and a **Time** to see exactly what the weather was (or will be).

---

## 🔄 Data Journey Summary

1.  **Raw Data**: Messy tables with various column names and coordinates.
2.  **Staging**: Cleaned-up columns and formatted map coordinates.
3.  **Intermediate**: Logic applied to connect stations to locations and filter out bad data.
4.  **Marts**: Perfectly formatted hourly weather, ready for your dashboard or AI.

---

## 🛠️ How to run it
You can refresh this entire logic by running:
```bash
make transform
```
