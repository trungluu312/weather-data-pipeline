import logging
from prefect import task

from ingestion.stations import StationDiscovery
from ingestion.observations import ObservationsIngestion
from ingestion.forecasts import ForecastsIngestion
from ingestion.postal_codes import main as ingest_postal_codes
from common.init_db import init_database

@task(name="Initialize Database", log_prints=True)
def task_init_db():
    """Ensure database schema exists."""
    init_database()

@task(name="Ingest Postal Codes", log_prints=True)
def task_ingest_postal_codes():
    """Run postal code ingestion."""
    ingest_postal_codes()

@task(name="Ingest Stations", log_prints=True)
def task_ingest_stations():
    """Run station discovery."""
    discovery = StationDiscovery()
    discovery.run()

@task(name="Ingest Observations", log_prints=True)
def task_ingest_observations():
    """Run observations ingestion."""
    ingest = ObservationsIngestion()
    return ingest.run()

@task(name="Ingest Forecasts", log_prints=True)
def task_ingest_forecasts():
    """Run forecasts ingestion."""
    ingest = ForecastsIngestion()
    return ingest.run()

@task(name="Run dbt Transformations", log_prints=True)
def task_transform_data():
    """Run dbt pipeline via shell."""
    import subprocess
    import sys
    
    logger = logging.getLogger("dbt")
    
    print("Installing dbt dependencies...")
    subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "deps", "--profiles-dir", "."],
        cwd="./transform",
        check=True
    )

    print("Running dbt build...")
    result = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", "."],
        cwd="./transform",
        capture_output=True,
        text=True
    )
    
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
        
    if result.returncode != 0:
        raise Exception(f"dbt run failed with exit code {result.returncode}")
