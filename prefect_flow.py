from pathlib import Path

from prefect import flow, get_run_logger, task

from scraper import run_pipeline


# This is the project root on your local machine, used only for clearer logs.
PROJECT_ROOT = Path(__file__).resolve().parent


@task(name="refresh_weather_data", retries=2, retry_delay_seconds=60)
def refresh_weather_data():
    """Run the existing weather ETL and fail the flow if the pipeline fails."""

    # Get a Prefect logger so run details appear in the Prefect UI.
    logger = get_run_logger()
    # Log where the flow is running from to make debugging easier.
    logger.info("Running weather pipeline from %s", PROJECT_ROOT)
    # Reuse the existing ETL logic and force orchestration-visible failures.
    exit_code = run_pipeline(raise_on_failure=True)
    # Log a simple success message after the pipeline completes.
    logger.info("Weather pipeline finished successfully with exit code %s", exit_code)


@flow(name="dhaka-weather-prefect-flow", log_prints=True)
def dhaka_weather_prefect_flow():
    """Prefect flow wrapper around the Dhaka weather ETL pipeline."""

    # Trigger the ETL task so Prefect can schedule, retry, and observe it.
    refresh_weather_data()


# Allow the flow to be run directly during local testing.
if __name__ == "__main__":
    dhaka_weather_prefect_flow()
