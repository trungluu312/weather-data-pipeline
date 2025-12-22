"""
Weather observations ingestion pipeline.

Fetches weather data from BrightSky API and loads it into DuckDB.
Refactored for simplicity and efficiency with explicit schema definition.
"""
import logging
import duckdb
import pandas as pd
from datetime import datetime, timedelta, timezone

from common.config import settings
from ingestion import brightsky_client

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)


class ObservationsIngestion:
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.duckdb_path
        logger.info(f"Initialized ObservationsIngestion | db: {self.db_path}")

    def run(self):
        logger.info("Starting Observations Ingestion...")
        
        conn = duckdb.connect(self.db_path)
        try:
            
            try:
                stations_df = conn.execute("""
                    SELECT id AS station_id, station_name, wmo_station_id 
                    FROM raw.weather_stations 
                    WHERE wmo_station_id IS NOT NULL
                """).fetchdf()
            except Exception as e:
                logger.error(f"Failed to query stations: {e}. Run stations.py first.")
                return

            stations = stations_df.to_dict('records')
            logger.info(f"Found {len(stations)} stations with WMO ID")
            
            total_records = 0

            for s in stations:
                station_name = s['station_name']
                wmo_id = s['wmo_station_id']
                station_id = s['station_id']

                # Determine time range
                start_date = self._get_start_date(conn, station_id)
                if not start_date:
                    start_date = datetime.now(timezone.utc) - timedelta(days=7)
                    logger.info(f"[{station_name}] Backfill (7 days)")
                else:
                    logger.debug(f"[{station_name}] Incremental from {start_date}")

                # API Request window (include current partial day)
                end_date_str = (datetime.now(timezone.utc)).isoformat()
                start_date_str = start_date.isoformat()

                try:
                    data = brightsky_client.get_weather(
                        wmo_station_id=wmo_id,
                        date=start_date_str,
                        last_date=end_date_str
                    )
                    
                    weather_list = data.get('weather', [])
                    if not weather_list:
                        continue

                    # Filter future records
                    now = datetime.now(timezone.utc)
                    valid_obs = [
                        w for w in weather_list 
                        if datetime.fromisoformat(w['timestamp'].replace('Z', '+00:00')) <= now
                    ]
                    
                    if not valid_obs:
                        continue

                    df_obs = pd.DataFrame(valid_obs)
                    df_obs['station_id'] = station_id 
                    df_obs['wmo_station_id'] = wmo_id 
                    
                    if 'fallback_source_ids' in df_obs.columns:
                         df_obs['fallback_source_ids'] = df_obs['fallback_source_ids'].astype(str)

                    conn.register('df_batch', df_obs)
                    conn.execute("INSERT INTO raw.weather_observations BY NAME SELECT * FROM df_batch")
                    
                    count = len(df_obs)
                    total_records += count
                    
                except Exception as e:
                    logger.error(f"Failed {station_name}: {e}")
                    continue
            
            logger.info(f"✅ Ingestion Complete. Total records ingested this run: {total_records}")

        finally:
            conn.close()

    def _get_start_date(self, conn, station_id):
        try:
            last_ts = conn.execute(
                "SELECT MAX(timestamp) FROM raw.weather_observations WHERE station_id = ?", 
                [station_id]
            ).fetchone()[0]
            
            if last_ts:
                return last_ts - timedelta(hours=1)
        except Exception:
            return None

def main():
    ObservationsIngestion().run()

if __name__ == "__main__":
    main()