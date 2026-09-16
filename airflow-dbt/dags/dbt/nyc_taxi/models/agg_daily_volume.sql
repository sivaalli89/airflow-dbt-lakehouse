select
    pickup_date,
    pickup_month,
    pickup_month_name,
    pickup_dayofweek_name,
    day_type,
    trip_type,
    count(*) as daily_trips,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_duration_minutes), 1) as avg_duration_min
from {{ ref('trips_final') }}
group by 1, 2, 3, 4, 5, 6
order by 1, 6
