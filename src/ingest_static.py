import csv
import os
from datetime import datetime, timezone

import snowflake.connector
from dotenv import load_dotenv


STATIC_FOLDER = "data/raw/static/gtfs_subway"


def connect_to_snowflake():
    load_dotenv()

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    return connection


def load_file(cursor, file_name, table_name, columns, loaded_at):
    file_path = f"{STATIC_FOLDER}/{file_name}"

    print(f"Loading {file_name}...")

    cursor.execute(f"TRUNCATE TABLE {table_name}")

    column_names = ", ".join(["LOADED_AT_UTC"] + columns)

    placeholders = ", ".join(["%s"] * (len(columns) + 1))

    insert_sql = f"""
        INSERT INTO {table_name} ({column_names})
        VALUES ({placeholders})
    """

    rows_loaded = 0
    batch = []

    with open(file_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for record in reader:
            row = [loaded_at]

            for column in columns:
                row.append(record[column])

            batch.append(tuple(row))

            if len(batch) == 5000:
                cursor.executemany(insert_sql, batch)
                rows_loaded += len(batch)
                batch = []

        if batch:
            cursor.executemany(insert_sql, batch)
            rows_loaded += len(batch)

    print(f"Loaded {rows_loaded} rows into {table_name}")


loaded_at = datetime.now(timezone.utc).isoformat()

connection = connect_to_snowflake()
cursor = connection.cursor()


load_file(
    cursor,
    "routes.txt",
    "GTFS_ROUTES",
    [
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_desc",
        "route_type",
        "route_url",
        "route_color",
        "route_text_color",
        "route_sort_order",
    ],
    loaded_at,
)


load_file(
    cursor,
    "trips.txt",
    "GTFS_TRIPS",
    [
        "route_id",
        "trip_id",
        "service_id",
        "trip_headsign",
        "direction_id",
        "shape_id",
    ],
    loaded_at,
)


load_file(
    cursor,
    "stops.txt",
    "GTFS_STOPS",
    [
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "location_type",
        "parent_station",
    ],
    loaded_at,
)


load_file(
    cursor,
    "stop_times.txt",
    "GTFS_STOP_TIMES",
    [
        "trip_id",
        "stop_id",
        "arrival_time",
        "departure_time",
        "stop_sequence",
    ],
    loaded_at,
)

load_file(
    cursor,
    "calendar.txt",
    "GTFS_CALENDAR",
    [
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ],
    loaded_at,
)


load_file(
    cursor,
    "calendar_dates.txt",
    "GTFS_CALENDAR_DATES",
    [
        "service_id",
        "date",
        "exception_type",
    ],
    loaded_at,
)


connection.commit()

cursor.close()
connection.close()

print("Static GTFS load finished.")