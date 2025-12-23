from orchestration.flow import weather_pipeline_flow
from prefect import serve

if __name__ == "__main__":
    print("Starting hourly automated pipeline... Press Ctrl+C to stop.")
    weather_pipeline_flow.serve(
        name="weather-pipeline-hourly",
        cron="*/15 * * * *",  # Run every 15 minutes
        tags=["production", "weather"],
        description="Hourly ingestion and transformation of weather data."
    )
