with counts as (
    select 'raw_yellow' as tbl, count(*) as n from {{ source('raw', 'raw_yellow') }}
    union all
    select 'raw_green', count(*) from {{ source('raw', 'raw_green') }}
    union all
    select 'raw_fhv', count(*) from {{ source('raw', 'raw_fhv') }}
    union all
    select 'raw_zones', count(*) from {{ source('raw', 'raw_zones') }}
)
select * from counts where n = 0
