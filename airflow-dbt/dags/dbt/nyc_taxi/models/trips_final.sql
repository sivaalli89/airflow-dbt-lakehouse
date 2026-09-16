select
    t.trip_type,
    t.pickup_datetime,
    t.dropoff_datetime,
    cast(t.pickup_datetime as date) as pickup_date,
    year(t.pickup_datetime) as pickup_year,
    month(t.pickup_datetime) as pickup_month,
    to_char(t.pickup_datetime, 'MMMM') as pickup_month_name,
    -- 1 = Sunday ... 7 = Saturday (matches BigQuery)
    dayofweekiso(t.pickup_datetime) % 7 + 1 as pickup_dayofweek,
    decode(to_char(t.pickup_datetime, 'DY'),
        'Mon', 'Monday', 'Tue', 'Tuesday', 'Wed', 'Wednesday', 'Thu', 'Thursday',
        'Fri', 'Friday', 'Sat', 'Saturday', 'Sun', 'Sunday') as pickup_dayofweek_name,
    hour(t.pickup_datetime) as pickup_hour,
    case when dayofweekiso(t.pickup_datetime) in (6, 7) then 'weekend' else 'weekday' end as day_type,
    datediff(minute, t.pickup_datetime, t.dropoff_datetime) as trip_duration_minutes,
    t.pickup_location_id,
    t.dropoff_location_id,
    pu.zone as pickup_zone,
    pu.borough as pickup_borough,
    dz.zone as dropoff_zone,
    dz.borough as dropoff_borough,
    pu.zone || ' -> ' || dz.zone as route,
    pu.borough || ' -> ' || dz.borough as borough_route,
    t.passenger_count,
    t.trip_distance_miles,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.payment_type,
    t.rate_code_id,
    t.congestion_surcharge,
    t.airport_fee,
    case when t.fare_amount > 0
         then round(t.tip_amount / t.fare_amount, 4) end as tip_rate,
    case when t.trip_distance_miles > 0
         then round(t.fare_amount / t.trip_distance_miles, 2) end as fare_per_mile,
    case when datediff(minute, t.pickup_datetime, t.dropoff_datetime) > 0
         then round(t.fare_amount / datediff(minute, t.pickup_datetime, t.dropoff_datetime), 2) end as fare_per_minute,
    t.dispatching_base_num,
    t.sr_flag,
    t.store_and_fwd_flag
from {{ ref('unified_trips') }} t
left join {{ source('raw', 'raw_zones') }} pu on t.pickup_location_id = pu.locationid
left join {{ source('raw', 'raw_zones') }} dz on t.dropoff_location_id = dz.locationid
