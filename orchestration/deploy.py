from orchestration.flow import weather_pipeline_flow
from prefect import serve

if __name__ == "__main__":
    print("Starting hourly automated pipeline... Press Ctrl+C to stop.")
    weather_pipeline_flow.serve(
        name="weather-pipeline-hourly",
        cron="*/3 * * * *",  # Run every 3 minutes
        tags=["production", "weather"],
        description="Hourly ingestion and transformation of weather data."
    )
