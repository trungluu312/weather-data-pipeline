import logging
from prefect import task

from ingestion.stations import StationDiscovery
from ingestion.observations import ObservationsIngestion
from ingestion.forecasts import main as ingest_forecasts
from ingestion.postal_codes import main as ingest_postal_codes
from common.init_db import init_database

@task(name="Initialize Database")
def task_init_db():
    """Ensure database schema exists."""
    init_database()

@task(name="Ingest Postal Codes")
def task_ingest_postal_codes():
    """Run postal code ingestion."""
    ingest_postal_codes()

@task(name="Ingest Stations")
def task_ingest_stations():
    """Run station discovery."""
    discovery = StationDiscovery()
    discovery.run()

@task(name="Ingest Observations")
def task_ingest_observations():
    """Run observations ingestion."""
    ingest = ObservationsIngestion()
    ingest.run()

@task(name="Ingest Forecasts")
def task_ingest_forecasts():
    """Run forecasts ingestion."""
    ingest_forecasts()

@task(name="Run dbt Transformations")
def task_transform_data():
    """Run dbt pipeline via shell."""
    import subprocess
    import sys
    
    logger = logging.getLogger("dbt")
    
    # 1. Install dependencies (needed because volume mount overrides image packages)
    print("Installing dbt dependencies...")
    subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "deps", "--profiles-dir", "."],
        cwd="./transform",
        check=True
    )

    # 2. Run dbt build (which does dbt run + dbt test)
    print("Running dbt build...")
    result = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", "."],
        cwd="./transform",
        capture_output=True,
        text=True
    )
    
    # Log output so user can see what happened
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
        
    if result.returncode != 0:
        raise Exception(f"dbt run failed with exit code {result.returncode}")
