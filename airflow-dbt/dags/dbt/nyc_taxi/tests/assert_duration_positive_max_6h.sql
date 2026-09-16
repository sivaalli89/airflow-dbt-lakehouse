select pickup_datetime, dropoff_datetime
from {{ ref('unified_trips') }}
where dropoff_datetime <= pickup_datetime
   or datediff(minute, pickup_datetime, dropoff_datetime) > 360
