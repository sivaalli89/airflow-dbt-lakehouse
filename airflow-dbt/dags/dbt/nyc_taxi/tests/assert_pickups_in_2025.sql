select pickup_datetime
from {{ ref('unified_trips') }}
where pickup_datetime < '2025-01-01' or pickup_datetime >= '2026-01-01'
