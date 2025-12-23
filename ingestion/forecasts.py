"""
Weather forecasts ingestion pipeline.

Fetches forecasts for the next N days and overrides existing future records.
"""
import logging
import duckdb
import pandas as pd
from datetime import datetime, timedelta, timezone
from common.config import settings
from ingestion import brightsky_client

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

class ForecastsIngestion:
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.duckdb_path
        logger.info(f"Initialized ForecastsIngestion | db: {self.db_path}")

    def run(self):
        logger.info("Starting Forecast Ingestion...")
        conn = duckdb.connect(self.db_path)
        
        try:
            stations = conn.execute("""
                SELECT id AS station_id, wmo_station_id, station_name, lat, lon
                FROM raw.weather_stations 
                WHERE observation_type = 'forecast'
            """).fetchdf().to_dict('records')
            
            logger.info(f"Found {len(stations)} forecast stations")
            total_records = 0
            
            now_utc = datetime.now(timezone.utc)
            end_date = now_utc + timedelta(days=settings.forecast_horizon_days)
            
            for s in stations:
                station_id = s['station_id']
                wmo_id = s['wmo_station_id']
                name = s['station_name']
                
                try:
                    if wmo_id:
                        data = brightsky_client.get_weather(
                            wmo_station_id=wmo_id, 
                            date=now_utc.isoformat(), 
                            last_date=end_date.isoformat()
                        )
                    else:
                        data = brightsky_client.get_weather(
                            lat=s['lat'], 
                            lon=s['lon'], 
                            date=now_utc.isoformat(), 
                            last_date=end_date.isoformat()
                        )
                    
                    records = data.get('weather', [])
                    if not records:
                        continue

                    df = pd.DataFrame(records)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    
                    df = df[df['timestamp'] > now_utc]
                    
                    if df.empty:
                        continue

                    df['station_id'] = station_id
                    df['wmo_station_id'] = wmo_id
                    df['ingested_at'] = now_utc
                    df = df.rename(columns={'timestamp': 'forecast_timestamp'})
                    
                    conn.execute("DELETE FROM raw.weather_forecasts WHERE station_id = ? AND forecast_timestamp > ?", [station_id, now_utc])

                    conn.register('df_batch', df)
                    conn.execute("""
                        INSERT INTO raw.weather_forecasts (
                            id, station_id, wmo_station_id, forecast_timestamp,
                            source_id, cloud_cover, condition, dew_point, icon,
                            precipitation, pressure_msl, relative_humidity, sunshine,
                            temperature, visibility, wind_direction, wind_speed,
                            wind_gust_direction, wind_gust_speed, ingested_at
                        ) 
                        SELECT 
                            nextval('raw.weather_forecasts_id_seq'),
                            station_id, wmo_station_id, forecast_timestamp,
                            source_id, cloud_cover, condition, dew_point, icon,
                            precipitation, pressure_msl, relative_humidity, sunshine,
                            temperature, visibility, wind_direction, wind_speed,
                            wind_gust_direction, wind_gust_speed, ingested_at 
                        FROM df_batch
                    """)
                    conn.unregister('df_batch')
                    
                    count = len(df)
                    total_records += count
                    
                except Exception as e:
                    logger.error(f"Failed {name}: {e}")
                    continue
            
            logger.info(f"✅ Ingestion Complete. Total new forecasts: {total_records}")
            return total_records
            
        finally:
            conn.close()

def main():
    ForecastsIngestion().run()

if __name__ == "__main__":
    main()