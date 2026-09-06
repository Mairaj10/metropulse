from dagster import (
    asset,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    RetryPolicy,
)

from src.ingest_realtime import run_realtime_ingestion

import subprocess


@asset(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=30,
    )
)
def gtfs_rt_stop_updates():
    run_realtime_ingestion()


@asset(deps=[gtfs_rt_stop_updates])
def gtfs_rt_source_freshness():
    subprocess.run(
        [
            "dbt",
            "source",
            "freshness",
            "--select",
            "source:raw.gtfs_rt_stop_updates",
        ],
        cwd="metropulse_dbt",
        check=True,
    )


@asset(deps=[gtfs_rt_source_freshness])
def dbt_transformations():
    subprocess.run(
        ["dbt", "build"],
        cwd="metropulse_dbt",
        check=True,
    )


# Manual full end-to-end job.
# Useful when we want to test the entire pipeline in one run.
metropulse_pipeline_job = define_asset_job(
    name="metropulse_pipeline_job",
    selection=[
        gtfs_rt_stop_updates,
        gtfs_rt_source_freshness,
        dbt_transformations,
    ],
)


# Frequent job whose only responsibility is preserving realtime history.
realtime_ingestion_job = define_asset_job(
    name="realtime_ingestion_job",
    selection=[
        gtfs_rt_stop_updates,
    ],
)


# Processes the realtime history already captured in RAW.
dbt_refresh_job = define_asset_job(
    name="dbt_refresh_job",
    selection=[
        gtfs_rt_source_freshness,
        dbt_transformations,
    ],
)


# Capture a new MTA snapshot every 5 minutes.
realtime_ingestion_schedule = ScheduleDefinition(
    job=realtime_ingestion_job,
    cron_schedule="*/5 * * * *",
)


# Run freshness + dbt every 15 minutes,
# a few minutes after an ingestion boundary.
dbt_refresh_schedule = ScheduleDefinition(
    job=dbt_refresh_job,
    cron_schedule="3,18,33,48 * * * *",
)


defs = Definitions(
    assets=[
        gtfs_rt_stop_updates,
        gtfs_rt_source_freshness,
        dbt_transformations,
    ],
    jobs=[
        metropulse_pipeline_job,
        realtime_ingestion_job,
        dbt_refresh_job,
    ],
    schedules=[
        realtime_ingestion_schedule,
        dbt_refresh_schedule,
    ],
)