with pickup_counts as (

    select
        pickup_zone as zone,
        pickup_borough as borough,
        count(*) as pickups,
        round(avg(fare_amount), 2) as avg_fare_from_zone
    from {{ ref('trips_final') }}
    where pickup_zone is not null
    group by 1, 2

),

dropoff_counts as (

    select
        dropoff_zone as zone,
        count(*) as dropoffs
    from {{ ref('trips_final') }}
    where dropoff_zone is not null
    group by 1

)

select
    p.zone,
    p.borough,
    p.pickups,
    d.dropoffs,
    p.pickups + d.dropoffs as total_activity,
    p.pickups - d.dropoffs as net_surplus,
    p.avg_fare_from_zone
from pickup_counts p
join dropoff_counts d using (zone)
order by total_activity desc
