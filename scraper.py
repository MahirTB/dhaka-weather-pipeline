import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv


# This is the forecast API URL with the exact fields we want to collect.
API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=23.81"
    "&longitude=90.41"
    "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
    "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    "&daily=weather_code,temperature_2m_max,temperature_2m_min"
    "&timezone=Asia%2FDhaka"
)

# This is the historical archive endpoint for actual past hourly weather data.
ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"

# These paths define where our parquet files will be stored locally.
DATA_DIR = Path("data")
FORECAST_PARQUET_PATH = DATA_DIR / "weather_forecast_clean.parquet"
CURRENT_PARQUET_PATH = DATA_DIR / "weather_current_clean.parquet"
DAILY_PARQUET_PATH = DATA_DIR / "weather_daily_clean.parquet"
HISTORICAL_PARQUET_PATH = DATA_DIR / "weather_historical_hourly.parquet"

# Load environment variables from the local .env file.
load_dotenv()


def get_dhaka_timezone():
    """Return the Dhaka timezone, with a UTC+6 fallback if tzdata is missing."""

    try:
        # Try to use the real Dhaka timezone name.
        return ZoneInfo("Asia/Dhaka")
    except ZoneInfoNotFoundError:
        # Fall back to a fixed UTC+06 timezone if the system lacks timezone data.
        return timezone(timedelta(hours=6), name="UTC+06")


def get_weather_code_description(weather_code):
    """Map Open-Meteo weather codes to short human-readable labels."""

    # This lookup keeps the dashboard copy friendly and readable.
    weather_code_map = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        56: "Light freezing drizzle",
        57: "Dense freezing drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        66: "Light freezing rain",
        67: "Heavy freezing rain",
        71: "Slight snow fall",
        73: "Moderate snow fall",
        75: "Heavy snow fall",
        77: "Snow grains",
        80: "Slight rain showers",
        81: "Moderate rain showers",
        82: "Violent rain showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm with hail",
        99: "Heavy thunderstorm with hail",
    }
    return weather_code_map.get(weather_code, "Unknown")


def build_daily_weather_summary(weather_code, temp_max, temp_min):
    """Create a friendlier daily summary from the weather code and temperatures."""

    # Start from the raw weather code description so we keep the underlying signal.
    base_description = get_weather_code_description(weather_code).lower()

    # Build a temperature tone so the description feels more natural for Dhaka.
    if temp_max is not None and temp_max >= 36:
        temp_tone = "Very warm"
    elif temp_max is not None and temp_max >= 33:
        temp_tone = "Hot"
    elif temp_max is not None and temp_max >= 29:
        temp_tone = "Warm"
    else:
        temp_tone = "Mild"

    # Rewrite a few common weather states into more dashboard-friendly phrases.
    if weather_code == 0:
        condition_text = "sunny and clear"
    elif weather_code == 1:
        condition_text = "mostly clear"
    elif weather_code == 2:
        condition_text = "partly cloudy"
    elif weather_code == 3:
        condition_text = "mostly cloudy"
    elif weather_code in {45, 48}:
        condition_text = "foggy"
    elif weather_code in {51, 53, 55, 56, 57}:
        condition_text = "drizzly"
    elif weather_code in {61, 63, 65, 66, 67, 80, 81, 82}:
        condition_text = "rainy"
    elif weather_code in {95, 96, 99}:
        condition_text = "stormy"
    else:
        condition_text = base_description

    # Return a short, more polished sentence fragment for the daily card.
    return f"{temp_tone} and {condition_text}".capitalize()


def build_current_weather_summary(weather_code, temperature):
    """Create a friendlier current-conditions summary."""

    # Build a temperature tone for the current weather card.
    if temperature is not None and temperature >= 36:
        temp_tone = "Very warm"
    elif temperature is not None and temperature >= 33:
        temp_tone = "Hot"
    elif temperature is not None and temperature >= 29:
        temp_tone = "Warm"
    else:
        temp_tone = "Pleasant"

    # Rewrite common current weather codes into cleaner phrases.
    if weather_code == 0:
        condition_text = "sunny skies"
    elif weather_code == 1:
        condition_text = "mostly clear skies"
    elif weather_code == 2:
        condition_text = "partly cloudy skies"
    elif weather_code == 3:
        condition_text = "overcast skies"
    elif weather_code in {45, 48}:
        condition_text = "foggy conditions"
    elif weather_code in {51, 53, 55, 56, 57}:
        condition_text = "light drizzle"
    elif weather_code in {61, 63, 65, 66, 67, 80, 81, 82}:
        condition_text = "rain showers"
    elif weather_code in {95, 96, 99}:
        condition_text = "thunderstorm conditions"
    else:
        condition_text = get_weather_code_description(weather_code).lower()

    # Return a more natural current weather phrase for the hero card.
    return f"{temp_tone} with {condition_text}"


def fetch_weather():
    """Call the forecast API and return the JSON response as a dictionary."""

    # Send a GET request to the forecast API.
    response = requests.get(API_URL, timeout=30)
    # Stop with an error if the API returns an unsuccessful status code.
    response.raise_for_status()
    # Convert the JSON response into a Python dictionary.
    return response.json()


def fetch_historical_weather(start_date, end_date):
    """Call the historical archive API for actual past hourly weather."""

    # Build query parameters for the historical API.
    params = {
        "latitude": 23.81,
        "longitude": 90.41,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "Asia/Dhaka",
    }
    # Send a GET request to the historical API.
    response = requests.get(ARCHIVE_API_URL, params=params, timeout=30)
    # Stop with an error if the API returns an unsuccessful status code.
    response.raise_for_status()
    # Convert the JSON response into a Python dictionary.
    return response.json()


def convert_api_time_to_dhaka(time_value, api_offset_seconds, dhaka_tz):
    """Convert one forecast API timestamp into Dhaka local time."""

    # Build a timezone object from the forecast API's UTC offset.
    api_tz = timezone(timedelta(seconds=api_offset_seconds))
    # Parse the timestamp string and attach the API timezone to it.
    parsed_time = datetime.fromisoformat(time_value).replace(tzinfo=api_tz)
    # Convert the timestamp into Dhaka time.
    return parsed_time.astimezone(dhaka_tz)


def parse_historical_time_to_dhaka(time_value, dhaka_tz):
    """Convert one historical API timestamp into a Dhaka-aware datetime."""

    # Parse the timestamp and attach the Dhaka timezone because the archive API
    # returns local-time values when timezone=Asia/Dhaka is requested.
    return datetime.fromisoformat(time_value).replace(tzinfo=dhaka_tz)


def transform_hourly_data(data, extracted_at, dhaka_tz):
    """Turn raw forecast API data into a clean hourly forecast DataFrame."""

    # Read metadata from the top level of the forecast API response.
    api_offset_seconds = data.get("utc_offset_seconds", 0)
    latitude = data.get("latitude")
    longitude = data.get("longitude")
    timezone_name = data.get("timezone", "unknown")
    # Read the nested hourly section from the forecast API response.
    hourly = data.get("hourly", {})

    # Build a list of row dictionaries before converting it into a DataFrame.
    records = []
    for time_value, temp, humidity, wind_speed in zip(
        hourly.get("time", []),
        hourly.get("temperature_2m", []),
        hourly.get("relative_humidity_2m", []),
        hourly.get("wind_speed_10m", []),
    ):
        # Convert each forecast timestamp into Dhaka time.
        forecast_time_dhaka = convert_api_time_to_dhaka(
            time_value, api_offset_seconds, dhaka_tz
        )

        # Store one clean row for this forecast hour.
        records.append(
            {
                "extracted_at": extracted_at,
                "location_name": "Dhaka",
                "latitude": latitude,
                "longitude": longitude,
                "source_timezone": timezone_name,
                "forecast_time_dhaka": forecast_time_dhaka,
                "forecast_date": forecast_time_dhaka.date().isoformat(),
                "forecast_hour": forecast_time_dhaka.hour,
                "temperature_2m": temp,
                "relative_humidity_2m": humidity,
                "wind_speed_10m": wind_speed,
            }
        )

    # Convert all collected rows into a pandas DataFrame.
    forecast_df = pd.DataFrame(records)
    if not forecast_df.empty:
        # Mark rows that happen strictly after the extraction time.
        forecast_df["is_upcoming"] = forecast_df["forecast_time_dhaka"] > extracted_at
        # Calculate how many hours away each forecast row is from extraction time.
        forecast_df["hours_from_extraction"] = (
            forecast_df["forecast_time_dhaka"] - extracted_at
        ).dt.total_seconds() / 3600
        # Round the hour difference so the output is easier to read.
        forecast_df["hours_from_extraction"] = forecast_df[
            "hours_from_extraction"
        ].round(2)

    return forecast_df


def transform_current_data(data, extracted_at):
    """Turn raw current-weather data into a one-row DataFrame."""

    # Read the nested current section from the forecast API response.
    current = data.get("current", {})
    # Create a single-row DataFrame containing the latest weather snapshot.
    return pd.DataFrame(
        [
            {
                "extracted_at": extracted_at,
                "location_name": "Dhaka",
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "temperature_2m": current.get("temperature_2m"),
                "relative_humidity_2m": current.get("relative_humidity_2m"),
                "wind_speed_10m": current.get("wind_speed_10m"),
                "weather_code": current.get("weather_code"),
                "weather_summary": build_current_weather_summary(
                    current.get("weather_code"),
                    current.get("temperature_2m"),
                ),
            }
        ]
    )


def transform_daily_data(data, extracted_at):
    """Turn raw daily forecast API data into a clean daily forecast DataFrame."""

    # Read metadata from the top level of the forecast API response.
    latitude = data.get("latitude")
    longitude = data.get("longitude")
    timezone_name = data.get("timezone", "unknown")
    # Read the nested daily section from the forecast API response.
    daily = data.get("daily", {})

    # Build one row per forecast day.
    records = []
    for forecast_date, weather_code, temp_max, temp_min in zip(
        daily.get("time", []),
        daily.get("weather_code", []),
        daily.get("temperature_2m_max", []),
        daily.get("temperature_2m_min", []),
    ):
        records.append(
            {
                "extracted_at": extracted_at,
                "location_name": "Dhaka",
                "latitude": latitude,
                "longitude": longitude,
                "source_timezone": timezone_name,
                "forecast_date": forecast_date,
                "weather_code": weather_code,
                "weather_summary": build_daily_weather_summary(
                    weather_code, temp_max, temp_min
                ),
                "temperature_2m_max": temp_max,
                "temperature_2m_min": temp_min,
            }
        )

    # Convert the collected daily rows into a DataFrame.
    return pd.DataFrame(records)


def transform_historical_data(data, extracted_at, dhaka_tz):
    """Turn raw archive API data into a clean historical hourly DataFrame."""

    # Read metadata from the top level of the archive API response.
    latitude = data.get("latitude")
    longitude = data.get("longitude")
    timezone_name = data.get("timezone", "unknown")
    # Read the nested hourly section from the archive API response.
    hourly = data.get("hourly", {})

    # Build a list of row dictionaries before converting it into a DataFrame.
    records = []
    for time_value, temp, humidity, wind_speed in zip(
        hourly.get("time", []),
        hourly.get("temperature_2m", []),
        hourly.get("relative_humidity_2m", []),
        hourly.get("wind_speed_10m", []),
    ):
        # Convert each historical timestamp into a timezone-aware Dhaka datetime.
        observed_time_dhaka = parse_historical_time_to_dhaka(time_value, dhaka_tz)

        # Store one clean row for this observed historical hour.
        records.append(
            {
                "extracted_at": extracted_at,
                "location_name": "Dhaka",
                "latitude": latitude,
                "longitude": longitude,
                "source_timezone": timezone_name,
                "observed_time_dhaka": observed_time_dhaka,
                "observed_date": observed_time_dhaka.date().isoformat(),
                "observed_hour": observed_time_dhaka.hour,
                "temperature_2m": temp,
                "relative_humidity_2m": humidity,
                "wind_speed_10m": wind_speed,
            }
        )

    # Convert all collected rows into a pandas DataFrame.
    return pd.DataFrame(records)


def save_parquet(dataframe, output_path):
    """Save a DataFrame to a parquet file."""

    # Create the destination folder if it does not already exist.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Write the DataFrame to parquet format without the pandas index column.
    dataframe.to_parquet(output_path, index=False)


def append_parquet_history(new_dataframe, output_path, subset_columns):
    """Append new rows to a history parquet file and remove duplicates."""

    # Start with the new data as the base DataFrame.
    combined_df = new_dataframe.copy()

    # If a history file already exists, load it and combine both datasets.
    if output_path.exists():
        existing_df = pd.read_parquet(output_path)
        combined_df = pd.concat([existing_df, new_dataframe], ignore_index=True)
    else:
        # In managed runtimes like Prefect Cloud, local files are ephemeral. If
        # the local history file does not exist, try to pull the previously
        # stored history parquet from S3 before appending the new batch.
        bucket_name = os.getenv("AWS_S3_BUCKET")
        historical_key = os.getenv(
            "AWS_S3_HISTORICAL_KEY", "weather/weather_historical_hourly.parquet"
        )
        if bucket_name:
            s3_history_path = f"s3://{bucket_name}/{historical_key}"
            try:
                existing_df = pd.read_parquet(s3_history_path)
                combined_df = pd.concat(
                    [existing_df, new_dataframe], ignore_index=True
                )
                print(f"Loaded existing historical data from {s3_history_path}")
            except Exception:
                # Keep going when the historical file does not exist yet or S3
                # is temporarily unavailable. In that case we simply start a new
                # history file from the current batch.
                print(
                    "No existing historical parquet found in S3. "
                    "Starting history from the current batch."
                )

    # Remove duplicate rows based on the chosen business key columns.
    combined_df = combined_df.drop_duplicates(subset=subset_columns, keep="last")
    # Sort the history by observed time so later analysis is easier.
    combined_df = combined_df.sort_values(subset_columns).reset_index(drop=True)
    # Save the combined history back to the parquet file.
    save_parquet(combined_df, output_path)
    return combined_df


def upload_to_s3(local_path, bucket_name, object_key):
    """Upload one local file to Amazon S3."""

    # Create an S3 client using AWS credentials from environment variables.
    s3_client = boto3.client("s3")
    # Upload the file to the chosen bucket and object key.
    s3_client.upload_file(str(local_path), bucket_name, object_key)


def maybe_upload_outputs():
    """Upload parquet files to S3 if the bucket name is configured."""

    # Read S3 settings from environment variables.
    bucket_name = os.getenv("AWS_S3_BUCKET")
    forecast_key = os.getenv(
        "AWS_S3_FORECAST_KEY", "weather/weather_forecast_clean.parquet"
    )
    current_key = os.getenv(
        "AWS_S3_CURRENT_KEY", "weather/weather_current_clean.parquet"
    )
    daily_key = os.getenv("AWS_S3_DAILY_KEY", "weather/weather_daily_clean.parquet")
    historical_key = os.getenv(
        "AWS_S3_HISTORICAL_KEY", "weather/weather_historical_hourly.parquet"
    )

    # Skip uploads when no S3 bucket has been configured.
    if not bucket_name:
        print("S3 upload skipped. Set AWS_S3_BUCKET to enable uploads.")
        return

    # Upload the forecast parquet file to S3.
    upload_to_s3(FORECAST_PARQUET_PATH, bucket_name, forecast_key)
    # Upload the current weather parquet file to S3.
    upload_to_s3(CURRENT_PARQUET_PATH, bucket_name, current_key)
    # Upload the daily forecast parquet file to S3.
    upload_to_s3(DAILY_PARQUET_PATH, bucket_name, daily_key)
    # Upload the historical hourly parquet file to S3.
    upload_to_s3(HISTORICAL_PARQUET_PATH, bucket_name, historical_key)
    # Print where each file was uploaded.
    print(f"Uploaded forecast parquet to s3://{bucket_name}/{forecast_key}")
    print(f"Uploaded current parquet to s3://{bucket_name}/{current_key}")
    print(f"Uploaded daily parquet to s3://{bucket_name}/{daily_key}")
    print(f"Uploaded historical parquet to s3://{bucket_name}/{historical_key}")


def print_preview(current_df, forecast_df, daily_df, historical_df):
    """Print a small terminal preview of the cleaned data."""

    # Show when the extraction happened.
    print(
        "Extracted at: "
        f"{current_df.loc[0, 'extracted_at'].strftime('%Y-%m-%d %I:%M:%S %p %Z')}"
    )
    # Show the latest current weather values.
    print("Current weather")
    print(f"Temperature: {current_df.loc[0, 'temperature_2m']}")
    print(f"Wind speed: {current_df.loc[0, 'wind_speed_10m']}")
    print(f"Humidity: {current_df.loc[0, 'relative_humidity_2m']}")
    print(f"Condition: {current_df.loc[0, 'weather_summary']}")

    # Show the next 8 hourly forecast rows after extraction time.
    print("\nNext 8 hourly forecast records from the next hour onward")
    next_eight = forecast_df[forecast_df["is_upcoming"]].head(8)
    for row in next_eight.itertuples(index=False):
        print(
            f"{row.forecast_time_dhaka.strftime('%Y-%m-%d %I:%M %p %Z')} | "
            f"temp={row.temperature_2m} | "
            f"humidity={row.relative_humidity_2m} | "
            f"wind_speed={row.wind_speed_10m}"
        )

    # Show the next 3 daily forecast rows.
    print("\nNext 3 daily forecast records")
    for row in daily_df.head(3).itertuples(index=False):
        print(
            f"{row.forecast_date} | "
            f"max={row.temperature_2m_max} | "
            f"min={row.temperature_2m_min} | "
            f"condition={row.weather_summary}"
        )

    # Show how many historical hourly rows are now stored.
    print(f"\nHistorical hourly rows stored: {len(historical_df)}")


def run_pipeline(raise_on_failure=False):
    """Run the full extract, transform, save, and optional upload pipeline."""

    # Load the timezone we want to use for all Dhaka timestamps.
    dhaka_tz = get_dhaka_timezone()
    # Record the moment we started extracting data.
    extracted_at = datetime.now(dhaka_tz)
    # Define the history window we want to refresh on every run.
    history_start_date = (extracted_at.date() - timedelta(days=1)).isoformat()
    history_end_date = extracted_at.date().isoformat()

    try:
        # Fetch the raw forecast payload from the API.
        raw_forecast_data = fetch_weather()
        # Fetch yesterday and today from the historical archive API.
        raw_historical_data = fetch_historical_weather(
            history_start_date, history_end_date
        )
    except requests.RequestException as exc:
        # Print a friendly message and stop if an API call fails.
        print(f"Weather API request failed: {exc}")
        if raise_on_failure:
            raise
        return 1

    # Transform the raw payload into a clean one-row current table.
    current_df = transform_current_data(raw_forecast_data, extracted_at)
    # Transform the raw payload into a clean hourly forecast table.
    forecast_df = transform_hourly_data(raw_forecast_data, extracted_at, dhaka_tz)
    # Transform the raw payload into a clean daily forecast table.
    daily_df = transform_daily_data(raw_forecast_data, extracted_at)
    # Transform the raw archive payload into a clean historical hourly table.
    historical_batch_df = transform_historical_data(
        raw_historical_data, extracted_at, dhaka_tz
    )

    # Save the cleaned current weather table as parquet.
    save_parquet(current_df, CURRENT_PARQUET_PATH)
    # Save the cleaned forecast table as parquet.
    save_parquet(forecast_df, FORECAST_PARQUET_PATH)
    # Save the cleaned daily forecast table as parquet.
    save_parquet(daily_df, DAILY_PARQUET_PATH)
    # Append the historical hourly data into a growing parquet history file.
    historical_df = append_parquet_history(
        historical_batch_df,
        HISTORICAL_PARQUET_PATH,
        ["location_name", "observed_time_dhaka"],
    )

    # Print a preview in the terminal so we can inspect the cleaned output.
    print_preview(current_df, forecast_df, daily_df, historical_df)
    print(f"\nSaved current data to {CURRENT_PARQUET_PATH}")
    print(f"Saved forecast data to {FORECAST_PARQUET_PATH}")
    print(f"Saved daily data to {DAILY_PARQUET_PATH}")
    print(f"Saved historical hourly data to {HISTORICAL_PARQUET_PATH}")

    # Upload parquet files to S3 if the user configured an S3 bucket.
    maybe_upload_outputs()
    return 0


def main():
    """Run the pipeline in script mode and return a process exit code."""

    return run_pipeline(raise_on_failure=False)


# Run the pipeline only when this file is executed directly.
if __name__ == "__main__":
    raise SystemExit(main())
