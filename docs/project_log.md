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

---

## First marts

Built the first fact table, `fct_stop_predictions`, and dimensions for stops, routes, and trips.

The fact keeps the same grain as the prediction comparison model:

`realtime_trip_id + stop_id + ingested_at_utc`

This is where I added the first actual metric, `predicted_delay_seconds`.

The marts ended up being much simpler than the intermediate models because most of the messy joining and matching had already been handled earlier.

I also used a table materialization for the fact instead of the default view and added relationship tests between the fact and dimensions.

---

## Making the fact incremental

Changed `fct_stop_predictions` to an incremental model.

I’m using `ingested_at_utc` to figure out what’s new, but I also look back one hour in case some rows arrive late.

The merge key is:

`realtime_trip_id + stop_id + ingested_at_utc`

While testing it, I got a duplicate-row merge error.

The issue was actually coming from upstream. The same realtime trip was showing slightly different `start_time` values across captures, and because `start_time` was inside a `SELECT DISTINCT`, the same trip ended up appearing twice.

That then doubled some prediction rows downstream.

I removed `start_time` from the trip-level intermediate models, rebuilt everything downstream, and the grain tests passed again.

After that, rerunning with no new data kept the fact count the same, and adding a new realtime capture increased it like expected.

---

## First scheduled Dagster run

Got the realtime ingestion running through Dagster.

At first I was manually clicking Materialize, which proved Dagster could call my existing Python ingestion function.

Then I added:

- an asset
- a job
- a schedule that runs every 5 minutes

The schedule fired automatically at 12:50 and again at 12:55 without me clicking anything.

The flow right now is:

MTA API

→ Python ingestion

→ Snowflake RAW

Dagster is not doing the ingestion itself. My Python function still does that. Dagster is just coordinating when it runs and keeping track of the run.

Also learned that local Dagster only works while `dagster dev` is running and my laptop is awake. If the laptop sleeps, the local scheduler stops.

Next step is to have Dagster trigger dbt after ingestion so the whole pipeline can run automatically.

---

## Dagster exposed real dbt issues

When I first ran `dbt build` through Dagster, the pipeline failed.

The first issue was a grain problem in `int_realtime_trip_matches`. One realtime trip was joining to multiple active services on the same date, which created duplicate candidate rows.

I changed the logic so static trips are first tied to the dates their services are actually active, and only then do realtime trips try to match against those valid candidates.

That fixed the fan-out instead of hiding it by changing the grain.

The second issue was a stop relationship test. Realtime had some stop IDs that did not exist in the static GTFS snapshot.

I confirmed they were missing from RAW static stops and stop_times too, and they were not reaching the final comparison/fact models.

I changed that relationship test to a warning instead of failing the whole pipeline.

I also changed the `service_id` not-null test so it only applies to matched trips, since unmatched trips can legitimately have no static `service_id`.

After all of that, the full `dbt build` passed.

---

## First automated full pipeline run

The full Dagster schedule finally ran by itself.

I turned on the five-minute schedule and waited for the next clock boundary instead of manually clicking Materialize.

At 15:30 Dagster automatically launched `metropulse_pipeline_job`.

The order worked as expected:

gtfs_rt_stop_updates

→ dbt_transformations

The realtime ingestion completed first, then Dagster ran `dbt build`, and the whole job finished successfully.

This is different from the earlier manual tests because Dagster is now actually coordinating the pipeline on a schedule.

So at this point the local MetroPulse pipeline can automatically ingest a new realtime snapshot and then run the downstream dbt transformations without me manually triggering each step.

---

## Added source freshness to Dagster

I added the realtime source freshness check into the Dagster pipeline between ingestion and dbt.

The order is now:

gtfs_rt_stop_updates

→ gtfs_rt_source_freshness

→ dbt_transformations

I manually materialized all three assets and they all succeeded.

This means Dagster now checks that the RAW realtime source is recent before running the downstream dbt models.

I also learned that defining freshness in dbt does not mean `dbt build` automatically runs it.

Source freshness is a separate dbt command, which is why it became its own Dagster step.

---

## Making realtime ingestion safer

Before adding retries in Dagster, I realized there was a possible problem if a Snowflake load failed halfway through.

If some rows from an MTA snapshot were saved and then the connection failed, a retry could see that snapshot already existed and skip it even though only part of it had loaded.

So I changed the Snowflake load to work as one transaction.

Now:

- if the whole snapshot loads successfully, it commits
- if something fails during the load, it rolls everything back
- the error is still raised so Dagster can see that the ingestion failed

After that I added a Dagster retry policy to the realtime ingestion asset.

Dagster can now retry the ingestion up to two times after a temporary failure, with a short wait between attempts.

The main thing I learned here is that retries are safer when the thing being retried can fail cleanly instead of leaving half-finished data behind.

---

## Finally checked the dbt lineage

Cleaned up the dbt docs and generated the docs site.

Seeing the lineage graph was actually nice because the project is getting big enough now that I don’t want to keep the whole thing in my head.

The dbt flow looks clean now:

RAW → staging → intermediate → marts

Also made it clearer to me that Dagster is showing the bigger pipeline steps, while dbt gives me the detailed model lineage.

---

## Started building analytics marts

Started building the tables that will eventually feed the dashboard.

First one was `agg_route_delay_summary`.

I ended up using the predicted arrival date/hour in New York time instead of the ingestion time, because ingestion time is just when I happened to capture the feed.

Then built `agg_stop_route_delay_summary` so I can look at predicted delay by stop/platform and route too.

I added one more, `agg_stop_delay_summary`, because I also want to be able to rank stops overall across routes.

I didn’t want to try rolling up the route-level medians later since those metrics don’t aggregate cleanly.

So the analytics layer now gives me route-level, route-at-stop, and overall stop-level views.

For now I’m using ±60 seconds as my own “roughly on time” range.

This is just a MetroPulse rule, not an MTA definition.

---

## Macros finally made sense

While building the second analytics mart I noticed I was copying the exact same delay metrics again.

That felt like an actual reason to use a macro.

Made `predicted_delay_metrics` and moved the repeated avg/median/early/late calculations there.

Now all three analytics marts use the same metric logic without copying it around.

I also ran `dbt compile` and saw the macro expand back into normal SQL, which made Jinja click a lot more for me.

Before this I was already using `ref()` and `is_incremental()`, but mostly without thinking about them as Jinja.

---

## Cleaned up all the repeated grain tests

I checked the `tests` folder and realized basically every singular test I had written was doing the same thing:

`GROUP BY grain columns`

`HAVING COUNT(*) > 1`

There were 10 of them.

So I made a reusable custom generic test called `unique_grain`.

I tested it beside the old singular tests first, then slowly replaced the staging, intermediate, and mart versions.

Now the grain itself is declared in YAML and the actual duplicate-checking logic only exists once.

All 10 old singular grain test files are gone now.

Way cleaner than having basically the same SQL repeated everywhere.

---

## Made the delay rule configurable

The analytics marts were all using 60 seconds as the cutoff between early, roughly on time, and late.

Instead of leaving that value buried inside the macro, I moved it into a dbt var called:

`delay_tolerance_seconds`

The default is still 60 seconds, but the macro now reads the value from the project config instead of hardcoding it.

I also tested overriding it from the command line with 90 seconds.

When I compiled the model again, dbt generated:

`> 90`

`< -90`

`BETWEEN -90 AND 90`

without me changing the SQL.

That made vars make more sense to me.

They’re basically configurable project values that Jinja can pull into the SQL.

---

## Added a seed for the dashboard scope

The realtime feed can contain routes outside the A/C/E scope I actually want for the dashboard.

I didn’t want to hardcode:

`IN ('A', 'C', 'E')`

inside every analytics mart.

So I made a small dbt seed called `dashboard_route_scope`.

Right now it contains:

A

C

E

All three analytics marts join to that seed before doing their aggregation.

I also added tests on the seed so null, duplicate, or invalid route IDs get caught.

This feels like a much cleaner place to keep a small piece of reference data that belongs to the project rather than to the MTA.

---

## Added contracts to the dashboard marts

The three analytics marts are starting to feel more like the stable layer the dashboard will depend on, so I added dbt model contracts to them.

The YAML now declares the expected columns and data types for each mart.

To make sure I actually understood what the contract was doing, I temporarily changed `prediction_count` in the contract from a number to a varchar.

dbt blocked the model and showed a data type mismatch.

Changed it back to the correct numeric type and the model built normally again.

That made the difference pretty clear to me:

tests check the data,

contracts protect the shape of the model.

I’m keeping contracts on the dashboard-facing marts for now instead of putting them everywhere just because I can.