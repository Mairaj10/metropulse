# MetroPulse Project Log

## Getting the data into Snowflake

Started by loading both the static GTFS files and the MTA realtime feed into Snowflake.

Python is basically just handling the ingestion side for now. dbt will handle the actual cleaning and modeling.

The main raw tables so far are routes, trips, stops, stop times, realtime stop updates, and later the service calendar tables.

---

## Figuring out the grain

One thing I’m trying to be more careful about in this project is understanding what one row actually represents before doing joins.

So far:

- trips = one scheduled trip
- stop_times = one stop occurrence inside a scheduled trip
- realtime stop updates = one prediction for one trip/stop at the time I ingested the feed

I also started writing dbt tests around these grains instead of just assuming they’re unique.

---

## GTFS times are weird

I found out GTFS scheduled times can go past 24:00:00.

Some of the subway times go up to around 28:xx:xx.

So I didn’t convert those fields into a normal SQL TIME datatype in staging. I’m keeping them as strings for now and will deal with them properly later when building scheduled timestamps.

---

## Realtime trip IDs don’t match static trip IDs

This was the first annoying/interesting problem.

A realtime trip ID looks something like:

`057550_A..N54R`

while the static GTFS trip ID looks more like:

`BSP26GEN-A055-Sunday-00_057550_A..N54R`

At first I tried matching them using the ending part of the static ID.

That looked like it worked, but when I checked the grain it caused fan-out.

From 2,664 realtime rows:

- 2,129 matched one static trip
- 60 matched more than one
- 475 didn’t match at all

So matching only on the short trip ID clearly isn’t enough.

The static ID has extra schedule/service context that the realtime ID is missing.

---

## Adding calendar data

Because of the trip matching problem, I ended up loading `calendar.txt` and `calendar_dates.txt`.

I didn’t originally plan to use them this early, but now there’s an actual reason for them.

The idea is to use the realtime trip `start_date` together with the service calendar to work out which static scheduled trip is actually active on that date.

This should help get rid of the duplicate matches from the earlier join.

---

## First intermediate model: finding the active service for a realtime date

Staging felt pretty straightforward because it was mostly cleaning and casting columns.

The first intermediate model was much harder because I had to understand the actual GTFS scheduling logic.

What I figured out:

- Realtime gives a service date for the trip.
- The static calendar has schedule rules that are valid over a date range.
- I first checked which schedule definitions were valid for the realtime date.
- Then I used the weekday of that date to check whether each service normally runs that day.
- `calendar_dates` handles special exceptions that can add or remove a service for one exact date.
- The final model keeps only the active services.

The grain of the model is:

`service_date + service_id`

One thing I learned here is that intermediate models are less about cleaning columns and more about combining different pieces of data to derive new meaning.

This model took me a while to understand, but once I broke it into smaller questions, the logic became much clearer.

---

## Matching realtime predictions to scheduled times

Built an intermediate model to put the realtime predicted arrival next to the scheduled GTFS arrival for the same trip and stop.

The tricky part was that realtime was already UTC, while GTFS scheduled times were New York local time and can go past 24:00.

I used `TIMESTAMP_TZ_FROM_PARTS()` to build the scheduled timestamp properly and then converted it to UTC.

The grain is:

`realtime_trip_id + stop_id + ingested_at_utc`

The grain test passed.

I’m keeping delay calculations for the mart layer.