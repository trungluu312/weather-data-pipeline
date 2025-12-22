# 🌩️ Weather Data Pipeline

**Goal:** Provide clean, reliable hourly weather data (observations & forecasts) for any ZIP code in Berlin (10xxx, 12xxx, 13xxx), ready for Machine Learning.

---

## 🚀 How to Run It
This project is fully containerized, find it in docker Hub

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
2.  **Map**: We automatically find the 10 nearest active stations for every postal code to ensure the minimum missing data.
3.  **Heal**: If a station is missing data, we fill the gap using:
    *   *Spatial Fallback*: Data from the next closest station.
    *   *Temporal Fallback*: Data from the previous hour (if no stations are online).

**Result**: Should provide an unbroken timeline of weather for every ZIP code.

---

## 🛠️ Tech Stack & Decisions
| Tool | Why I picked it |
| :--- | :--- |
| **DuckDB** | Embedded, faster to setup than Postgres, support SPATIAL extension that can handle geospatial data. |
| **dbt** | Handles the complex SQL logic (staging-intermediate-marts) and data testing. Industry standard for data transformation. |
| **Prefect** | Orchestrates the Python scripts, easier to setup than Airflow. |
| **Docker** | Containerizes the environment, makes it easy to share and deploy. |

---

## 🏭 Productionize?

If we needed to productionize this, here is the plan:

**1. Move away from custom Scripts to managed Connectors**
*   *Current*: Custom Python scripts.
*   *Future*: **Airbyte** or any managed connectors. Or can continue with custom scripts but need to setup a CI/CD pipeline.    

**2. Move away from File-Based to Cloud Warehouse**
*   *Current*: DuckDB (Local file).
*   *Future*: **BigQuery** or any other cloud warehouse. Separates storage from compute so we can crunch petabytes of data.

**3. Airflow**
*   *Current*: Prefect.
*   *Future*: Can continue with Prefect or switch to **Airflow** MWAA if current tech stack is AWS heavy.

**4. CI/CD**
*   *Current*: None.
*   *Future*: Can continue with None or switch to **GitHub Actions** or **GitLab CI**.

---

## 📂 Project Structure
*   `ingestion/`: Python scripts that talk to APIs.
*   `transform/`: dbt project where the SQL magic happens.
*   `orchestration/`: Prefect logic connecting ingestion and transformation.
*   `data/`: Where DuckDB stores the files (automatically ignored by git).
