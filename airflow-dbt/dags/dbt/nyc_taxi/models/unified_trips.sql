with yellow as (

    select
        'yellow' as trip_type,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        passenger_count,
        trip_distance as trip_distance_miles,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        ratecodeid as rate_code_id,
        store_and_fwd_flag,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        extra,
        mta_tax,
        tolls_amount,
        improvement_surcharge,
        cast(null as float) as ehail_fee,
        cast(null as varchar) as dispatching_base_num,
        cast(null as varchar) as affiliated_base_number,
        cast(null as int) as sr_flag
    from {{ source('raw', 'raw_yellow') }}
    where tpep_pickup_datetime >= '2025-01-01' and tpep_pickup_datetime < '2026-01-01'
      and tpep_dropoff_datetime >= '2025-01-01' and tpep_dropoff_datetime < '2026-01-01'
      and pulocationid is not null
      and dolocationid is not null
      and trip_distance > 0
      and fare_amount > 0
      and tpep_dropoff_datetime > tpep_pickup_datetime
      and datediff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) <= 360
      and (tip_amount is null or tip_amount >= 0)

),

green as (

    select
        'green' as trip_type,
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        passenger_count,
        trip_distance as trip_distance_miles,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        ratecodeid as rate_code_id,
        store_and_fwd_flag,
        congestion_surcharge,
        cast(null as float) as airport_fee,
        cbd_congestion_fee,
        extra,
        mta_tax,
        tolls_amount,
        improvement_surcharge,
        ehail_fee,
        cast(null as varchar) as dispatching_base_num,
        cast(null as varchar) as affiliated_base_number,
        cast(null as int) as sr_flag
    from {{ source('raw', 'raw_green') }}
    where lpep_pickup_datetime >= '2025-01-01' and lpep_pickup_datetime < '2026-01-01'
      and lpep_dropoff_datetime >= '2025-01-01' and lpep_dropoff_datetime < '2026-01-01'
      and pulocationid is not null
      and dolocationid is not null
      and trip_distance > 0
      and fare_amount > 0
      and lpep_dropoff_datetime > lpep_pickup_datetime
      and datediff(minute, lpep_pickup_datetime, lpep_dropoff_datetime) <= 360
      and (tip_amount is null or tip_amount >= 0)

),

fhv as (

    select
        'fhv' as trip_type,
        pickup_datetime,
        dropoff_datetime,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        cast(null as int) as passenger_count,
        cast(null as float) as trip_distance_miles,
        cast(null as float) as fare_amount,
        cast(null as float) as tip_amount,
        cast(null as float) as total_amount,
        cast(null as int) as payment_type,
        cast(null as int) as rate_code_id,
        cast(null as varchar) as store_and_fwd_flag,
        cast(null as float) as congestion_surcharge,
        cast(null as float) as airport_fee,
        cast(null as float) as cbd_congestion_fee,
        cast(null as float) as extra,
        cast(null as float) as mta_tax,
        cast(null as float) as tolls_amount,
        cast(null as float) as improvement_surcharge,
        cast(null as float) as ehail_fee,
        dispatching_base_num,
        affiliated_base_number,
        sr_flag
    from {{ source('raw', 'raw_fhv') }}
    where pickup_datetime >= '2025-01-01' and pickup_datetime < '2026-01-01'
      and dropoff_datetime >= '2025-01-01' and dropoff_datetime < '2026-01-01'
      and pulocationid is not null
      and dolocationid is not null
      and dropoff_datetime > pickup_datetime
      and datediff(minute, pickup_datetime, dropoff_datetime) <= 360

)

select * from yellow
union all
select * from green
union all
select * from fhv
