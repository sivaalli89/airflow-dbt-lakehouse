select
    trip_type,
    count(*) as total_trips,
    round(count(*) * 100.0 / sum(count(*)) over (), 2) as pct_share,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(tip_amount), 2) as avg_tip,
    round(avg(tip_rate), 4) as avg_tip_rate,
    round(avg(trip_distance_miles), 2) as avg_distance_miles,
    round(avg(trip_duration_minutes), 1) as avg_duration_minutes,
    round(avg(fare_per_mile), 2) as avg_fare_per_mile
from {{ ref('trips_final') }}
group by trip_type
order by total_trips desc
