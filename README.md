# NYC Taxi End-to-End Analytics Pipeline — Process Documentation

This document explains how the pipeline in this project works, from raw data download to analytics-ready tables in Snowflake.

## Overview

The project is an end-to-end ELT pipeline built with:

| Component | Role |
|---|---|
| **Apache Airflow** (Astro Runtime 3.3, via Astronomer CLI) | Orchestrates the whole pipeline |
| **Snowflake** | Data warehouse (database `NEW_YORK_TAXI`, schema `NYC_TAXI`) |
| **dbt** (dbt-snowflake, run through **Astronomer Cosmos**) | Transforms raw data into clean and aggregated models |
| **NYC TLC trip data** | Source data: yellow, green, and FHV trips for all 12 months of 2025, plus the taxi zone lookup |

The single DAG, `nyc_taxi_pipeline` (defined in [dags/load_nyc_taxi.py](dags/load_nyc_taxi.py)), runs three stages in order:

```
download  →  load_snowflake  →  dbt_models (Cosmos task group)
```

---

## Stage 1: Download raw data (`download` task)

- Downloads 36 parquet files (12 months × 3 trip types: `yellow`, `green`, `fhv`) plus `taxi_zone_lookup.csv`.
- Primary source is a GCS bucket (`msca-bdp-data-open/final_project_taxi`); if a file fails there, it falls back to the official NYC TLC CloudFront URL (`d37ci6vzurychx.cloudfront.net`).
- Files land in `include/data/{yellow,green,fhv,zones}/` inside the Airflow container (mounted from [include/data/](include/data/)).
- Downloads are **idempotent**: a file that already exists is skipped, and downloads write to a `.tmp` file first, then rename, so a partial download never masquerades as a complete file.

## Stage 2: Load into Snowflake (`load_snowflake` task)

Using the Airflow Snowflake connection (conn id `snowflake`), the task:

1. Switches to schema `new_york_taxi.nyc_taxi`.
2. Creates (if needed) a parquet file format, a CSV file format (skip header), and an internal stage `raw_stage`.
3. Creates the raw tables with explicit DDL:
   - `raw_yellow` — yellow taxi columns (`tpep_*` timestamps, fares, surcharges, etc.)
   - `raw_green` — green taxi columns (`lpep_*` timestamps, includes `ehail_fee`, `trip_type`)
   - `raw_fhv` — for-hire vehicle columns (dispatching base, `sr_flag`)
   - `raw_zones` — `locationid`, `borough`, `zone`, `service_zone`
4. `PUT`s each local file into the stage and `COPY INTO`s the raw tables. Parquet loads use `match_by_column_name = case_insensitive`, so column order in the files doesn't matter.
5. Prints a row count for each raw table as a sanity check.

## Stage 3: Transform with dbt (`dbt_models` task group)

The dbt project lives in [dags/dbt/nyc_taxi/](dags/dbt/nyc_taxi/) and is executed by **Astronomer Cosmos** (`DbtTaskGroup`), which turns every dbt model and test into its own Airflow task with correct dependencies. The dbt profile is generated automatically from the same `snowflake` Airflow connection, targeting database `new_york_taxi`, schema `nyc_taxi`. dbt runs from a dedicated virtualenv baked into the image (`/usr/local/airflow/dbt_venv/bin/dbt` — see the [Dockerfile](Dockerfile)).

All models are materialized as **tables** (set in [dbt_project.yml](dags/dbt/nyc_taxi/dbt_project.yml)).

### Model lineage

```
raw_yellow ─┐
raw_green ──┼─► unified_trips ─► trips_final ─┬─► agg_daily_volume
raw_fhv ────┘                        ▲        ├─► agg_hourly_demand
                                     │        ├─► agg_zone_stats
raw_zones ───────────────────────────┘        └─► agg_trip_type_stats
```

### 1. `unified_trips` — standardize and clean

[unified_trips.sql](dags/dbt/nyc_taxi/models/unified_trips.sql) unions the three raw trip tables into one schema:

- Renames source-specific columns to common names (`tpep_pickup_datetime` / `lpep_pickup_datetime` → `pickup_datetime`, `pulocationid` → `pickup_location_id`, etc.), padding missing columns with typed NULLs (e.g., FHV has no fare data; yellow has no `ehail_fee`).
- Adds a `trip_type` discriminator (`'yellow'`, `'green'`, `'fhv'`).
- Applies data-quality filters per source:
  - Pickup **and** dropoff must fall within calendar year 2025.
  - `pickup_location_id` and `dropoff_location_id` must be non-null.
  - Dropoff must be after pickup, and trip duration ≤ 6 hours (360 minutes).
  - Yellow/green only: `trip_distance > 0`, `fare_amount > 0`, and tips non-negative.

### 2. `trips_final` — enrich

[trips_final.sql](dags/dbt/nyc_taxi/models/trips_final.sql) joins `unified_trips` to `raw_zones` (twice — pickup and dropoff) and derives analysis columns:

- **Calendar features**: `pickup_date`, year, month, month name, day-of-week number (1 = Sunday, matching BigQuery convention) and name, `pickup_hour`, `day_type` (weekday/weekend).
- **Geography**: pickup/dropoff zone and borough, plus `route` (`"Zone A -> Zone B"`) and `borough_route`.
- **Derived metrics**: `trip_duration_minutes`, `tip_rate` (tip ÷ fare), `fare_per_mile`, `fare_per_minute` — each guarded against division by zero.

### 3. Aggregation marts

Four small reporting tables built from `trips_final`:

| Model | Grain | Key metrics |
|---|---|---|
| [agg_daily_volume](dags/dbt/nyc_taxi/models/agg_daily_volume.sql) | day × trip type | daily trips, avg fare, avg duration |
| [agg_hourly_demand](dags/dbt/nyc_taxi/models/agg_hourly_demand.sql) | hour × trip type × day type | trips, avg duration, avg fare |
| [agg_zone_stats](dags/dbt/nyc_taxi/models/agg_zone_stats.sql) | pickup zone | pickups, dropoffs, total activity, net surplus (pickups − dropoffs), avg fare from zone |
| [agg_trip_type_stats](dags/dbt/nyc_taxi/models/agg_trip_type_stats.sql) | trip type | total trips, % share, avg fare/tip/tip rate/distance/duration/fare-per-mile |

## Data quality testing

Cosmos runs dbt tests as Airflow tasks alongside the models.

**Generic tests** (in [sources.yml](dags/dbt/nyc_taxi/models/sources.yml) and [schema.yml](dags/dbt/nyc_taxi/models/schema.yml)):
- Source pickup timestamps are `not_null`; `raw_zones.locationid` is `unique` + `not_null`.
- `unified_trips`: `trip_type` limited to the three accepted values; timestamps and location IDs `not_null`.
- `trips_final`: zones `not_null`; `day_type` limited to weekday/weekend.

**Singular tests** (in [tests/](dags/dbt/nyc_taxi/tests/)):
- [assert_raw_tables_not_empty.sql](dags/dbt/nyc_taxi/tests/assert_raw_tables_not_empty.sql) — every raw table has rows.
- [assert_pickups_in_2025.sql](dags/dbt/nyc_taxi/tests/assert_pickups_in_2025.sql) — no trips outside 2025 survive the filters.
- [assert_duration_positive_max_6h.sql](dags/dbt/nyc_taxi/tests/assert_duration_positive_max_6h.sql) — durations are positive and ≤ 6 hours.
- [assert_yellow_green_fares_positive.sql](dags/dbt/nyc_taxi/tests/assert_yellow_green_fares_positive.sql) — yellow/green fares are positive.

## How to run it

1. **Prerequisites**: Docker, the [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/overview), and a Snowflake account with database `NEW_YORK_TAXI` and schema `NYC_TAXI` created.
2. **Start Airflow locally** from the `airflow-dbt/` directory:
   ```bash
   astro dev start
   ```
   This builds the image (Astro Runtime + a `dbt_venv` with `dbt-snowflake`) and opens the Airflow UI at http://localhost:8080.
3. **Create the Snowflake connection** in the Airflow UI (Admin → Connections): connection id `snowflake`, type Snowflake, with your account, user, password, warehouse, and role. Both the load task and the Cosmos-generated dbt profile use this one connection.
4. **Trigger the DAG** `nyc_taxi_pipeline` from the UI (it has no schedule — manual trigger only).
5. **Verify**: the Cosmos task group shows each model and test as its own task; the final tables (`unified_trips`, `trips_final`, and the four `agg_*` marts) appear in `NEW_YORK_TAXI.NYC_TAXI`.

## Design notes

- **ELT, not ETL**: raw files are loaded into Snowflake unmodified; all cleaning and shaping happens in-warehouse via dbt, so raw data stays available for reprocessing.
- **Idempotency**: downloads skip existing files, and all Snowflake DDL uses `IF NOT EXISTS`. (Note: re-running `load_snowflake` will re-`COPY` staged files; Snowflake's load-history deduplication prevents reloading the same files into the same table.)
- **One connection, two consumers**: Cosmos's `SnowflakeUserPasswordProfileMapping` derives the dbt profile from the Airflow connection, so credentials are managed in one place.
- **Test-gated lineage**: because Cosmos maps dbt tests into the DAG, downstream models only build after upstream models pass their tests.
