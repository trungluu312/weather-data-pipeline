"""
Weather station discovery and loading (Simplified).

Fetches all stations in the configured radius and loads them into DuckDB.
"""
import logging
import duckdb
import pandas as pd
from typing import List, Dict

from common.config import settings
from ingestion import brightsky_client


logger = logging.getLogger(__name__)


class StationDiscovery:
    """Discovers and loads weather stations into DuckDB."""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.duckdb_path
        logger.info(f"Initialized StationDiscovery | db: {self.db_path}")
    
    def run(self):
        """Execute the station discovery."""
        logger.info("Fetching stations from API...")
        
        try:
            stations = brightsky_client.get_sources(
                lat=settings.berlin_center_lat,
                lon=settings.berlin_center_lon,
                max_dist=settings.max_distance_m
            )
            logger.info(f"Found {len(stations)} stations")
            
            if not stations:
                logger.warning("No stations found.")
                return

            df = pd.DataFrame(stations)
            
            logger.info("Loading into DuckDB (raw.weather_stations)...")
            
            conn = duckdb.connect(self.db_path)
            try:
                conn.register('df_stations', df)
                
                conn.execute("DELETE FROM raw.weather_stations")
                
                conn.execute("INSERT INTO raw.weather_stations BY NAME SELECT * FROM df_stations")
                
                logger.info(f"✅ Stations loaded successfully: {len(df)} records")
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to ingest stations: {e}")
            raise

def main():
    discovery = StationDiscovery()
    discovery.run()

if __name__ == "__main__":
    main()