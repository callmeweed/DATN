{{
    config(
        tags=['wifeed']
    )
}}

with source as (
      select * from {{ source('wifeed', 'wifeed_danh_sach_ma_chung_khoan_raw') }}
),
renamed as (
    select
        {{ adapter.quote("san") }}::text,
        {{ adapter.quote("code") }}::text,
        {{ adapter.quote("loaidn") }}::int,
        {{ adapter.quote("fullname_vi") }}::text,
        {{ adapter.quote("indexed_timestamp_") }}::timestamp,
        {{ adapter.quote("symbol_") }}::text,
        {{ adapter.quote("crawled_date") }}

    from source
)
select * from renamed
  