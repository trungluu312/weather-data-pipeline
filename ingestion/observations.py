"""
Weather observations ingestion pipeline.
"""
import logging
import duckdb
import pandas as pd
from datetime import datetime, timedelta, timezone

from common.config import settings
from ingestion import brightsky_client


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
                    WHERE wmo_station_id IS NOT NULL AND last_record >= '2025-09-01'
                """).fetchdf()
            except Exception as e:
                logger.error(f"Failed to query stations: {e}")
                return

            stations = stations_df.to_dict('records')
            total_records = 0

            for s in stations:
                station_name = s['station_name']
                wmo_id = s['wmo_station_id']
                station_id = s['station_id']

                # 1. Determine constraints
                last_ts = self._get_last_timestamp(conn, station_id)
                # DuckDB returns naive datetime for TIMESTAMP, so we enforce UTC
                if last_ts and last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)
                
                now_utc = datetime.now(timezone.utc)
                
                if last_ts:
                    start_date = last_ts
                    mode = "Incremental"
                else:
                    start_date = now_utc - timedelta(days=settings.observation_lookback_days)
                    mode = "Backfill"

                try:
                    data = brightsky_client.get_weather(
                        wmo_station_id=wmo_id,
                        date=start_date.isoformat(),
                        last_date=now_utc.isoformat()
                    )
                    
                    weather_list = data.get('weather', [])
                    if not weather_list:
                        continue

                    df_obs = pd.DataFrame(weather_list)
                    
                    df_obs['timestamp'] = pd.to_datetime(df_obs['timestamp'])
                    
                    if last_ts:
                        df_obs = df_obs[df_obs['timestamp'] > last_ts]
                    
                    df_obs = df_obs[df_obs['timestamp'] <= now_utc]

                    if df_obs.empty:
                        continue
                    df_obs['station_id'] = station_id 
                    df_obs['wmo_station_id'] = wmo_id 
                    if 'fallback_source_ids' in df_obs.columns:
                         df_obs['fallback_source_ids'] = df_obs['fallback_source_ids'].astype(str)
                    conn.register('df_batch', df_obs)
                    conn.execute("INSERT INTO raw.weather_observations BY NAME SELECT * FROM df_batch")
                    conn.unregister('df_batch')
                    
                    count = len(df_obs)
                    total_records += count
                    logger.info(f"[{station_name}] {mode}: +{count} records")
                    
                except Exception as e:
                    logger.error(f"Failed {station_name}: {e}")
                    continue
            
            logger.info(f"✅ Ingestion Complete. Total new: {total_records}")
            return total_records

        finally:
            conn.close()

    def _get_last_timestamp(self, conn, station_id):
        try:
            res = conn.execute(
                "SELECT MAX(timestamp) FROM raw.weather_observations WHERE station_id = ?", 
                [station_id]
            ).fetchone()
            return res[0] if res else None
        except Exception:
            return None

def main():
    ObservationsIngestion().run()

if __name__ == "__main__":
    main()