"""
Weather forecasts ingestion pipeline (Bare Minimum).

Fetches 10-day forecasts for all stations and stores them with versioning (issue_timestamp).
"""
import logging
import duckdb
import pandas as pd
from datetime import datetime, timedelta, timezone
from common.config import settings
from ingestion import brightsky_client

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

def get_forecast_stations(conn):
    return conn.execute("""
        SELECT id, wmo_station_id, station_name, lat, lon
        FROM raw.weather_stations 
        WHERE observation_type = 'forecast'
    """).fetchall()

def main():
    logger.info("Starting Forecast Ingestion...")
    
    conn = duckdb.connect(settings.duckdb_path)
    
    try:
        stations = get_forecast_stations(conn)
        logger.info(f"Found {len(stations)} forecast stations")
        
        issue_ts = datetime.now(timezone.utc)
        start_date = issue_ts.strftime('%Y-%m-%d')
        end_date = (issue_ts + timedelta(days=settings.forecast_horizon_days)).strftime('%Y-%m-%d')
        
        for station_id, wmo_id, name, lat, lon in stations:
            logger.info(f"Fetching forecasts for {name} (WMO: {wmo_id}, Lat/Lon: {lat}/{lon})...")
            try:
                if wmo_id:
                     data = brightsky_client.get_weather(wmo_station_id=wmo_id, date=start_date, last_date=end_date)
                else:
                     data = brightsky_client.get_weather(lat=lat, lon=lon, date=start_date, last_date=end_date)
                
                records = data.get('weather', [])
                
                if not records:
                    continue

                # Filter future only
                df = pd.DataFrame(records)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df[df['timestamp'] > issue_ts]
                
                if df.empty:
                    continue

                df['station_id'] = station_id
                df['wmo_station_id'] = wmo_id 
                
                # Register for DuckDB
                conn.register('df_forecasts', df)
                
                # Insert with Sequence and Ignore Duplicates (if rerun same hour)
                # raw.weather_forecasts has UNIQUE(station_id, forecast_timestamp, issue_timestamp)
                # We use INSERT OR IGNORE to skip if we already ingested this version
                
                conn.execute("""
                    INSERT OR IGNORE INTO raw.weather_forecasts (
                        id, station_id, wmo_station_id, forecast_timestamp,
                        source_id, cloud_cover, condition, dew_point, icon,
                        precipitation, pressure_msl, relative_humidity, sunshine,
                        temperature, visibility, wind_direction, wind_speed,
                        wind_gust_direction, wind_gust_speed
                    )
                    SELECT 
                        nextval('raw.weather_forecasts_id_seq'),
                        station_id,
                        wmo_station_id,
                        timestamp,
                        source_id,
                        cloud_cover,
                        condition,
                        dew_point,
                        icon,
                        precipitation,
                        pressure_msl,
                        relative_humidity,
                        sunshine,
                        temperature,
                        visibility,
                        wind_direction,
                        wind_speed,
                        wind_gust_direction,
                        wind_gust_speed
                    FROM df_forecasts
                """)
                
                conn.unregister('df_forecasts')
                
            except Exception as e:
                logger.error(f"Failed station {name}: {e}")
                continue
                
        logger.info("✅ Forecast ingestion complete.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()