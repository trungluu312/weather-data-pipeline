# 🌩️ Weather Data Pipeline

**Goal:** Provide clean, reliable hourly weather data (observations & forecasts) for any ZIP code in Berlin, ready for Machine Learning.

---

## 🚀 How to Run It
This project is fully containerized. You only need Docker.

### 1. Start from Scratch
Initialize the database and build the environment:
```bash
make reset-db
make build
```

### 2. Run the Pipeline
Download new data, process it, and update the tables:
```bash
make pipeline
```

### 3. Run Only Transformations
If you've already ingested data and only want to update the dbt models:
```bash
make transform
```

### 4. Generate Documentation
```bash
make dbt-docs
```

---

## � What Problem Does This Solve?

**The Challenge:** Weather data comes from "Stations" (points), but businesses operate in "Postal Codes" (areas). Stations also break down, leaving gaps in data that crash ML models.

**Our Solution:**
1.  **Ingest**: We download raw weather data from the BrightSky API and postal code shapes from GitHub.
2.  **Map**: We automatically find the 10 nearest active stations for every postal code.
3.  **Heal**: If a station is missing data, we fill the gap using:
    *   *Spatial Fallback*: Data from the next closest station.
    *   *Temporal Fallback*: Data from the previous hour (if no stations are online).

**Result**: A perfect, unbroken timeline of weather for every ZIP code.

---

## 🛠️ Tech Stack & Decisions
We chose tools that are **simple to start** but **ready to scale**.

| Tool | Why we picked it |
| :--- | :--- |
| **DuckDB** | Faster than Postgres for analytics, zero setup, runs on your laptop. |
| **dbt** | Handles the complex SQL logic (staging-intermediate-marts) and data testing. |
| **Prefect** | Orchestrates the Python scripts. |
| **Docker** | Containerizes the environment. |

---

## 🏭 Road to Production

If we needed to scale this, here is the plan:

**1. Move away from custom Scripts to managed Connectors**
*   *Current*: Custom Python scripts.
*   *Future*: **Airbyte**.

**2. Move away from File-Based to Cloud Warehouse**
*   *Current*: DuckDB (Local file).
*   *Future*: **BigQuery** or any other cloud warehouse. Separates storage from compute so we can crunch petabytes of data.

---

## 📂 Project Structure
*   `ingestion/`: Python scripts that talk to APIs.
*   `transform/`: dbt project where the SQL magic happens.
*   `orchestration/`: Prefect logic connecting ingestion and transformation.
*   `data/`: Where DuckDB stores the files (automatically ignored by git).
