select
    pickup_hour,
    trip_type,
    day_type,
    count(*) as trips,
    round(avg(trip_duration_minutes), 1) as avg_duration_min,
    round(avg(fare_amount), 2) as avg_fare
from {{ ref('trips_final') }}
group by pickup_hour, trip_type, day_type
order by pickup_hour, trip_type, day_type
