import os
import logging
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    duckdb_path: str = os.getenv("DUCKDB_PATH", "data/weather_pipeline.db")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Ensure absolute path based on project root
        if not os.path.isabs(self.duckdb_path):
            root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.duckdb_path = os.path.join(root_dir, self.duckdb_path)

    brightsky_api_url: str = "https://api.brightsky.dev"
    log_level: str = "INFO"
    
    # API Config
    api_rate_limit: int = 10
    
    # Spatial Config
    berlin_center_lat: float = 52.52
    berlin_center_lon: float = 13.40
    max_distance_m: int = 50000
    
    # Pipeline Config
    observation_lookback_days: int = 30
    forecast_horizon_days: int = 10
    
    # Schedule
    observation_schedule: str = "0 * * * *"
    forecast_schedule: str = "0 * * * *"
    
    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
