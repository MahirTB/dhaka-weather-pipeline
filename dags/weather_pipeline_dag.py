from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import sys

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# The whole project is mounted into the Airflow containers at /opt/airflow/project.
# We point directly there so the DAG can always find scraper.py reliably.
PROJECT_ROOT = Path("/opt/airflow/project")
SCRAPER_PATH = PROJECT_ROOT / "scraper.py"


def run_weather_pipeline():
    """Run the weather ETL script from Airflow."""

    # Execute the existing scraper script so the DAG refreshes the same pipeline
    # that you already run manually during development.
    subprocess.run(
        [sys.executable, str(SCRAPER_PATH)],
        check=True,
        cwd=PROJECT_ROOT,
    )


default_args = {
    "owner": "weather-pipeline",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="dhaka_weather_hourly_pipeline",
    default_args=default_args,
    description="Refresh Dhaka weather data every hour",
    schedule="0 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["weather", "streamlit", "s3"],
) as dag:
    refresh_weather_data = PythonOperator(
        task_id="refresh_weather_data",
        python_callable=run_weather_pipeline,
    )
