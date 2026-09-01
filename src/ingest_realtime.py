from datetime import datetime, timezone
import os

import requests
import snowflake.connector
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2


URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace"


def fetch_mta_feed():
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    return feed


def extract_stop_updates(feed, ingested_at):
    records = []

    for entity in feed.entity:

        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip = trip_update.trip

        for stop_update in trip_update.stop_time_update:

            record = {
                "ingested_at_utc": ingested_at,
                "feed_generated_at_epoch": feed.header.timestamp,
                "entity_id": entity.id,
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "start_date": trip.start_date,
                "start_time": trip.start_time,
                "trip_update_timestamp_epoch": (
                    trip_update.timestamp
                    if trip_update.HasField("timestamp")
                    else None
                ),
                "stop_id": stop_update.stop_id,
                "stop_sequence": (
                    stop_update.stop_sequence
                    if stop_update.HasField("stop_sequence")
                    else None
                ),
                "arrival_time_epoch": (
                    stop_update.arrival.time
                    if stop_update.HasField("arrival")
                    else None
                ),
                "departure_time_epoch": (
                    stop_update.departure.time
                    if stop_update.HasField("departure")
                    else None
                ),
            }

            records.append(record)

    return records


def load_to_snowflake(records):
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

    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO GTFS_RT_STOP_UPDATES (
            INGESTED_AT_UTC,
            FEED_GENERATED_AT_EPOCH,
            ENTITY_ID,
            TRIP_ID,
            ROUTE_ID,
            START_DATE,
            START_TIME,
            TRIP_UPDATE_TIMESTAMP_EPOCH,
            STOP_ID,
            STOP_SEQUENCE,
            ARRIVAL_TIME_EPOCH,
            DEPARTURE_TIME_EPOCH
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = []

    for record in records:
        row = (
            record["ingested_at_utc"],
            record["feed_generated_at_epoch"],
            record["entity_id"],
            record["trip_id"],
            record["route_id"],
            record["start_date"],
            record["start_time"],
            record["trip_update_timestamp_epoch"],
            record["stop_id"],
            record["stop_sequence"],
            record["arrival_time_epoch"],
            record["departure_time_epoch"],
        )

        rows.append(row)

    cursor.executemany(insert_sql, rows)

    connection.commit()

    cursor.close()
    connection.close()

    print("Rows loaded to Snowflake:", len(rows))


ingested_at = datetime.now(timezone.utc)

feed = fetch_mta_feed()

records = extract_stop_updates(feed, ingested_at)

print("Entities received:", len(feed.entity))
print("Stop-update rows:", len(records))

load_to_snowflake(records)