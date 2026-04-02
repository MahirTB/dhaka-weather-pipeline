import os
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

from scraper import run_pipeline


# This is the project root on your local machine, used only for clearer logs.
PROJECT_ROOT = Path(__file__).resolve().parent

# These mappings connect Prefect Secret block names to the environment variables
# that scraper.py already uses for S3 uploads.
PREFECT_SECRET_ENV_MAPPING = {
    "AWS_ACCESS_KEY_ID": "aws-access-key-id",
    "AWS_SECRET_ACCESS_KEY": "aws-secret-access-key",
    "AWS_DEFAULT_REGION": "aws-default-region",
    "AWS_S3_BUCKET": "aws-s3-bucket",
    "AWS_S3_CURRENT_KEY": "aws-s3-current-key",
    "AWS_S3_FORECAST_KEY": "aws-s3-forecast-key",
    "AWS_S3_DAILY_KEY": "aws-s3-daily-key",
    "AWS_S3_HISTORICAL_KEY": "aws-s3-historical-key",
}


def load_prefect_secrets_into_env(logger):
    """Load Prefect Secret blocks into environment variables when available."""

    # Try to load each expected block one by one so missing optional values do
    # not prevent the whole flow from running.
    for env_name, block_name in PREFECT_SECRET_ENV_MAPPING.items():
        # Skip loading when the variable is already present in the environment.
        if os.getenv(env_name):
            continue

        try:
            # Read the secret value from Prefect Cloud and place it into the
            # process environment so scraper.py can use it unchanged.
            os.environ[env_name] = Secret.load(block_name).get()
            logger.info("Loaded Prefect secret for %s", env_name)
        except Exception:
            # Keep going if a block does not exist. Some values, like custom S3
            # object keys, are optional.
            logger.info("Prefect secret block '%s' not found; skipping", block_name)


@task(name="refresh_weather_data", retries=2, retry_delay_seconds=60)
def refresh_weather_data():
    """Run the existing weather ETL and fail the flow if the pipeline fails."""

    # Get a Prefect logger so run details appear in the Prefect UI.
    logger = get_run_logger()
    # Log where the flow is running from to make debugging easier.
    logger.info("Running weather pipeline from %s", PROJECT_ROOT)
    # Load AWS-related secrets from Prefect Cloud so the ETL can upload to S3
    # without storing credentials in GitHub or source files.
    load_prefect_secrets_into_env(logger)
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
