# Automated Weather Pipeline

This project fetches Dhaka weather data from Open-Meteo, transforms it into clean tabular datasets, saves the results as Parquet files, optionally uploads them to Amazon S3, and exposes a Streamlit dashboard on top of the cleaned data.

## Setup

Activate the virtual environment and install the main Python packages:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run The Pipeline

Run the ETL script to fetch weather data, clean it, and save parquet outputs:

```powershell
python scraper.py
```

This creates:

- `data/weather_current_clean.parquet`
- `data/weather_forecast_clean.parquet`
- `data/weather_daily_clean.parquet`
- `data/weather_historical_hourly.parquet`

## Optional S3 Upload

Set these environment variables before running the pipeline:

```powershell
$env:AWS_ACCESS_KEY_ID="your-access-key"
$env:AWS_SECRET_ACCESS_KEY="your-secret-key"
$env:AWS_DEFAULT_REGION="ap-southeast-1"
$env:AWS_S3_BUCKET="your-bucket-name"
```

Optional object keys:

```powershell
$env:AWS_S3_CURRENT_KEY="weather/weather_current_clean.parquet"
$env:AWS_S3_FORECAST_KEY="weather/weather_forecast_clean.parquet"
$env:AWS_S3_DAILY_KEY="weather/weather_daily_clean.parquet"
$env:AWS_S3_HISTORICAL_KEY="weather/weather_historical_hourly.parquet"
```

## Run The Dashboard

For local development with local parquet files:

```powershell
streamlit run app.py
```

For hosted use with data read directly from S3, also set:

```powershell
$env:STREAMLIT_USE_S3="true"
```

The app will then read the latest parquet files from your S3 bucket instead of local disk.

## Hourly Refresh With Airflow

This repo now includes an hourly Airflow DAG at `dags/weather_pipeline_dag.py`.

Install Airflow separately:

```powershell
pip install -r requirements-airflow.txt
```

The DAG schedule is:

```text
0 * * * *
```

That means the pipeline refreshes at the start of every hour.

What the DAG does:

1. Runs `scraper.py`
2. Regenerates the parquet outputs
3. Uploads fresh files to S3 if `AWS_S3_BUCKET` is configured

## Airflow With Docker

Using Docker is a good option for this project because it keeps Airflow isolated from your local Python environment, which helps avoid the package conflicts that often happen with direct `pip install apache-airflow`.

This repo now includes:

- `Dockerfile.airflow`
- `docker-compose.airflow.yml`

### Start Airflow In Docker

1. Make sure Docker Desktop is running.
2. Create the runtime folders:

```powershell
mkdir dags, logs, plugins
```

3. Initialize Airflow:

```powershell
docker compose -f docker-compose.airflow.yml up airflow-init
```

4. Start the Airflow services:

```powershell
docker compose -f docker-compose.airflow.yml up -d
```

5. Open Airflow:

```text
http://localhost:8080
```

Default login:

- Username: `airflow`
- Password: `airflow`

### Pause The Airflow DAG

If you switch to Prefect for scheduling, pause the Airflow DAG so both tools do not
run the same pipeline at the same time.

In the Airflow UI:

1. Open `Dags`
2. Find `dhaka_weather_hourly_pipeline`
3. Turn the toggle off so the DAG is paused

Or from the CLI inside Docker:

```powershell
docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags pause dhaka_weather_hourly_pipeline
```

### Stop Airflow

```powershell
docker compose -f docker-compose.airflow.yml down
```

### What This Docker Setup Does

- Runs Airflow with `LocalExecutor`
- Uses PostgreSQL for the Airflow metadata database
- Mounts your local `dags/` folder into the Airflow container
- Mounts the full project so the DAG can run `scraper.py`
- Reuses your existing `.env` file for AWS and project settings

### Important Note

This Docker setup is a good local development and portfolio setup. The official Airflow docs describe Docker Compose as a quick-start approach rather than a production-grade deployment. For serious production use, Airflow recommends Kubernetes with the official Helm chart.

## Public Hosting Options

### Option 1: Streamlit Community Cloud

Best for:

- portfolio projects
- recruiter demos
- easiest public deployment

Recommended setup:

1. Push this repo to GitHub
2. Deploy `app.py` on Streamlit Community Cloud
3. Add the required AWS environment variables in the Streamlit app settings
4. Set `STREAMLIT_USE_S3=true`
5. Run Airflow on another machine or server to keep S3 updated hourly

### Option 2: EC2 / VM Hosting

Best for:

- full control
- running both Airflow and Streamlit yourself

Typical setup:

1. Run Airflow on the server
2. Run Streamlit on the same server or another server
3. Keep data in S3 or on shared disk
4. Expose Streamlit behind a reverse proxy like Nginx

## Practical Architecture

Recommended production-style flow:

1. Airflow runs hourly
2. `scraper.py` refreshes the weather datasets
3. New parquet files are uploaded to S3
4. Streamlit reads the latest parquet files from S3
5. Public users see updated weather data when they load the dashboard

## Prefect Orchestration

This repo also includes a Prefect flow at `prefect_flow.py`. It reuses the same
weather ETL logic from `scraper.py`, so you do not need to maintain two separate
pipelines.

### Install Prefect

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Test The Flow Locally

```powershell
python prefect_flow.py
```

### Deploy With A 2-Hour Schedule

Log in to Prefect, then deploy the flow with a 2-hour cron schedule:

```powershell
prefect cloud login
prefect deploy prefect_flow.py:dhaka_weather_prefect_flow --name "dhaka-weather-every-2-hours" --cron "0 */2 * * *"
```

That schedule runs at:

- 12:00 AM
- 2:00 AM
- 4:00 AM
- and so on every 2 hours

### Why This Setup Helps

- Prefect handles scheduling, retries, logs, and run history
- `scraper.py` remains the single source of truth for the ETL logic
- Airflow can stay in the repo as a local showcase, while Prefect becomes the live scheduler
