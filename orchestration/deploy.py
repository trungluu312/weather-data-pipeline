from orchestration.flow import weather_pipeline_flow
from prefect import serve

if __name__ == "__main__":
    # Create a deployment that runs hourly
    deployment = weather_pipeline_flow.to_deployment(
        name="weather-pipeline-hourly",
        cron="*/3 * * * *",  # Run every 3 minutes
        tags=["production", "weather"],
        description="Hourly ingestion and transformation of weather data."
    )
    
    print("Serving deployment... Press Ctrl+C to stop.")
    serve(deployment)
