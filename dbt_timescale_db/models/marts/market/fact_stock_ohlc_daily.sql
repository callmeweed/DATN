with source as (
    select * from {{ ref('int_stock_ohlc_dedup')}}
)
select * from source