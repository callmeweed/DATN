{{
    config(
    materialized = 'table',
    unique_key = 'unique_id',
    sort = [
        'symbol',
        'txn_date'
    ], sort_type = 'interleaved'
    )
}}

with source as (
      select * from {{ source('market', 'stock_ohlc_days') }}
),
renamed as (
    select
    -- ids

    -- floats
        {{ adapter.quote("volume") }}::float as match_quantity,
        {{ adapter.quote("low") }}::float as low_price,
        {{ adapter.quote("high") }}::float as high_price,
        {{ adapter.quote("open") }}::float as open_price,
        {{ adapter.quote("close") }}::float as close_price,

    -- timestamps
        (time::timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Saigon')::date as txn_date,
        {{ adapter.quote("last_updated") }}, -- timestamp update lại giá khi có sự kiện quyền

    -- strings
        {{ adapter.quote("symbol") }}::text as symbol

    from source
)
select {{ dbt_utils.generate_surrogate_key(['symbol','txn_date']) }} AS unique_id,
renamed.* 
from renamed