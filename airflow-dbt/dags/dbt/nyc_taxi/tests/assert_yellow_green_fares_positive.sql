select trip_type, fare_amount
from {{ ref('unified_trips') }}
where trip_type in ('yellow', 'green')
  and (fare_amount is null or fare_amount <= 0)
