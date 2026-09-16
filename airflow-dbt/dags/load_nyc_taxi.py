import pathlib
import urllib.request

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DATA_DIR = pathlib.Path("/usr/local/airflow/include/data")
GCS = "https://storage.googleapis.com/msca-bdp-data-open/final_project_taxi"
TLC = "https://d37ci6vzurychx.cloudfront.net"
MONTHS = [f"2025-{m:02d}" for m in range(1, 13)]
TRIP_TYPES = ["yellow", "green", "fhv"]

RAW_DDL = {
    "raw_yellow": """
        vendorid int, tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz,
        passenger_count int, trip_distance float, ratecodeid int, store_and_fwd_flag varchar,
        pulocationid int, dolocationid int, payment_type int, fare_amount float, extra float,
        mta_tax float, tip_amount float, tolls_amount float, improvement_surcharge float,
        total_amount float, congestion_surcharge float, airport_fee float, cbd_congestion_fee float""",
    "raw_green": """
        vendorid int, lpep_pickup_datetime timestamp_ntz, lpep_dropoff_datetime timestamp_ntz,
        store_and_fwd_flag varchar, ratecodeid int, pulocationid int, dolocationid int,
        passenger_count int, trip_distance float, fare_amount float, extra float, mta_tax float,
        tip_amount float, tolls_amount float, ehail_fee float, improvement_surcharge float,
        total_amount float, payment_type int, trip_type int, congestion_surcharge float,
        cbd_congestion_fee float""",
    "raw_fhv": """
        dispatching_base_num varchar, pickup_datetime timestamp_ntz, dropoff_datetime timestamp_ntz,
        pulocationid int, dolocationid int, sr_flag int, affiliated_base_number varchar""",
}


def fetch(url, fallback, dest):
    if dest.exists():
        return "skipped"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    try:
        urllib.request.urlretrieve(url, tmp)
    except Exception:
        urllib.request.urlretrieve(fallback, tmp)
    tmp.rename(dest)
    return "downloaded"


@dag(schedule=None, catchup=False, tags=["nyc_taxi"])
def nyc_taxi_pipeline():

    @task
    def download():
        for t in TRIP_TYPES:
            for m in MONTHS:
                name = f"{t}_tripdata_{m}.parquet"
                fetch(f"{GCS}/{t}/{name}", f"{TLC}/trip-data/{name}", DATA_DIR / t / name)
        fetch(f"{GCS}/taxi_zone_lookup.csv", f"{TLC}/misc/taxi_zone_lookup.csv",
              DATA_DIR / "zones" / "taxi_zone_lookup.csv")

    @task
    def load_snowflake():
        cur = SnowflakeHook("snowflake").get_conn().cursor()
        cur.execute("use schema new_york_taxi.nyc_taxi")
        cur.execute("create file format if not exists pq_format type = parquet")
        cur.execute("create file format if not exists csv_format type = csv skip_header = 1")
        cur.execute("create stage if not exists raw_stage")

        for table, cols in RAW_DDL.items():
            cur.execute(f"create table if not exists {table} ({cols})")
        cur.execute("""create table if not exists raw_zones
                       (locationid int, borough varchar, zone varchar, service_zone varchar)""")

        for t in TRIP_TYPES:
            cur.execute(f"put file://{DATA_DIR}/{t}/*.parquet @raw_stage/{t}/ auto_compress = false")
            cur.execute(f"""copy into raw_{t} from @raw_stage/{t}/
                            file_format = pq_format match_by_column_name = case_insensitive""")

        cur.execute(f"put file://{DATA_DIR}/zones/*.csv @raw_stage/zones/ auto_compress = false")
        cur.execute("copy into raw_zones from @raw_stage/zones/ file_format = csv_format")

        for t in ["raw_yellow", "raw_green", "raw_fhv", "raw_zones"]:
            cur.execute(f"select count(*) from {t}")
            print(t, cur.fetchone()[0])

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/nyc_taxi"),
        profile_config=ProfileConfig(
            profile_name="nyc_taxi",
            target_name="dev",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake",
                profile_args={"database": "new_york_taxi", "schema": "nyc_taxi"},
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        ),
    )

    download() >> load_snowflake() >> dbt_models


nyc_taxi_pipeline()
