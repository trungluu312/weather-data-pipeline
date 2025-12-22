from prefect import flow
from orchestration.tasks import (
    task_init_db,
    task_ingest_stations,
    task_ingest_postal_codes,
    task_ingest_observations,
    task_ingest_forecasts,
    task_transform_data
)

@flow(name="Weather Pipeline", log_prints=True)
def weather_pipeline_flow():
    """
    Main orchestration flow for the Weather Pipeline.
    
    Steps:
    0. Initialize Database (Idempotent)
    1. Ingest Stations (ensures we have latest metadata)
    2. Ingest Observations & Forecasts (Parallel)
    3. Run dbt transformations (Dependent on ingestion)
    """
    print("🚀 Starting Weather Pipeline Flow")
    
    # 0. Initialize DB (Auto-heal for fresh containers)
    task_init_db()

    # 1. Stations (Sequential, required for others)
    # We verify stations first to ensure referential integrity
    task_ingest_stations()
    task_ingest_postal_codes()
    
    # 2. Ingest Data (Parallel)
    # Prefect runs tasks in parallel by default if they don't depend on each other
    obs_future = task_ingest_observations.submit()
    fcst_future = task_ingest_forecasts.submit()
    
    # Wait for completion before transformation
    obs_future.wait()
    fcst_future.wait()
    
    # 3. Transform
    task_transform_data()
    
    print("✅ Weather Pipeline Flow Completed")

if __name__ == "__main__":
    # For local development/testing
    weather_pipeline_flow()
