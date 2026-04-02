from prefect.runner.storage import GitRepository

from prefect_flow import dhaka_weather_prefect_flow


# This is the public GitHub repo that Prefect Managed will pull code from.
REPOSITORY_URL = "https://github.com/MahirTB/dhaka-weather-pipeline.git"

# These are the Python packages the managed Prefect runtime must install
# before it can import scraper.py and run the ETL successfully.
RUNTIME_PACKAGES = [
    "boto3",
    "pandas",
    "plotly",
    "python-dotenv",
    "pyarrow",
    "requests",
    "s3fs",
]


if __name__ == "__main__":
    # Tell Prefect to load the flow code from the GitHub repo instead of the
    # local machine, which is required for managed execution.
    source = GitRepository(url=REPOSITORY_URL, branch="main")

    # Create or update the managed Prefect deployment with a 2-hour schedule.
    dhaka_weather_prefect_flow.from_source(
        source=source,
        entrypoint="prefect_flow.py:dhaka_weather_prefect_flow",
    ).deploy(
        name="dhaka-weather-every-2-hours",
        work_pool_name="dhaka_weather_pool",
        cron="0 */2 * * *",
        build=False,
        push=False,
        job_variables={
            # Install all required runtime packages in the managed worker.
            "pip_packages": RUNTIME_PACKAGES,
        },
    )
