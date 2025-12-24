"""Initialize DuckDB database with schemas and extensions."""
import duckdb
from pathlib import Path


from common.config import settings

def init_database(db_path: str = None):
    """Initialize DuckDB with required extensions and schemas."""
    if db_path is None:
        db_path = settings.duckdb_path
    
    print(f"Initializing DuckDB at {db_path}")
    
    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    try:
        # Install and load spatial extension
        print("Installing spatial extension...")
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        
        # Install httpfs for remote file access (useful for postal codes)
        print("Installing httpfs extension...")
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        
        # Create schemas
        print("Creating schemas...")
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        
        # Create raw tables
        print("Creating raw tables...")
        
        # Postal codes table with geometry
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.postal_codes (
                plz VARCHAR(5) PRIMARY KEY,
                geometry GEOMETRY,
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Weather stations table with point geometry
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.weather_stations (
                id VARCHAR PRIMARY KEY,
                dwd_station_id VARCHAR,
                wmo_station_id VARCHAR,
                station_name VARCHAR,
                observation_type VARCHAR,
                lat DOUBLE,
                lon DOUBLE,
                height DOUBLE,
                location GEOMETRY,
                first_record TIMESTAMP,
                last_record TIMESTAMP,
                distance DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Weather observations table (Explicit Schema)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.weather_observations (
                timestamp TIMESTAMP,
                station_id VARCHAR,
                wmo_station_id VARCHAR,
                source_id BIGINT,
                precipitation DOUBLE,
                pressure_msl DOUBLE,
                sunshine DOUBLE,
                temperature DOUBLE,
                wind_direction BIGINT,
                wind_speed DOUBLE,
                cloud_cover BIGINT,
                dew_point DOUBLE,
                relative_humidity BIGINT,
                visibility BIGINT,
                wind_gust_direction BIGINT,
                wind_gust_speed DOUBLE,
                condition VARCHAR,
                precipitation_probability BIGINT,
                precipitation_probability_6h BIGINT,
                solar DOUBLE,
                icon VARCHAR,
                fallback_source_ids VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Weather forecasts table with version tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.weather_forecasts (
                id INTEGER PRIMARY KEY,
                station_id VARCHAR NOT NULL,
                wmo_station_id VARCHAR,
                forecast_timestamp TIMESTAMP NOT NULL,
                source_id INTEGER,
                cloud_cover DOUBLE,
                condition VARCHAR,
                dew_point DOUBLE,
                icon VARCHAR,
                precipitation DOUBLE,
                pressure_msl DOUBLE,
                relative_humidity DOUBLE,
                sunshine DOUBLE,
                temperature DOUBLE,
                visibility DOUBLE,
                wind_direction DOUBLE,
                wind_speed DOUBLE,
                wind_gust_direction DOUBLE,
                wind_gust_speed DOUBLE,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create sequences for auto-incrementing IDs

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS raw.weather_forecasts_id_seq START 1
        """)
        
        print("✅ Database initialized successfully!")
        
        # Show created objects
        print("\nCreated schemas:")
        result = conn.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw', 'staging', 'analytics')").fetchall()
        for row in result:
            print(f"  - {row[0]}")
        
        print("\nCreated tables in raw schema:")
        result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'").fetchall()
        for row in result:
            print(f"  - raw.{row[0]}")
        
        print("\nInstalled extensions:")
        result = conn.execute("SELECT extension_name, installed FROM duckdb_extensions() WHERE installed = true").fetchall()
        for row in result:
            print(f"  - {row[0]}")
        
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        raise
    finally:
        conn.close()
    
    print(f"\n✅ DuckDB ready at {db_path}")


if __name__ == "__main__":
    init_database()
