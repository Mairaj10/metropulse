from dagster import (
    asset,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    RetryPolicy
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


metropulse_pipeline_job = define_asset_job(
    name="metropulse_pipeline_job",
    selection=[
        gtfs_rt_stop_updates,
        gtfs_rt_source_freshness,
        dbt_transformations,
    ],
)


metropulse_pipeline_schedule = ScheduleDefinition(
    job=metropulse_pipeline_job,
    cron_schedule="*/5 * * * *",
)


defs = Definitions(
    assets=[
        gtfs_rt_stop_updates,
        gtfs_rt_source_freshness,
        dbt_transformations,
    ],
    jobs=[metropulse_pipeline_job],
    schedules=[metropulse_pipeline_schedule],
)