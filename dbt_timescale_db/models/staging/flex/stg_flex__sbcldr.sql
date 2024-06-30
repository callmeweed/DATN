{{
    config(
    materialized = 'view',
    unique_key = 'id',
    sort = [
        'sb_date',
        'calendar_type'
    ]
    )
}}

with source as (
      select * from {{ source('flex', 'sbcldr') }}
),
renamed as (
    select
    -- ids
        {{ adapter.quote("autoid") }}::int as id,
    -- timestamps
        {{ adapter.quote("sbdate") }}::date as sb_date,
    -- strings
        {{ adapter.quote("holiday") }}::text as is_holiday,
        {{ adapter.quote("cldrtype") }}::text as calendar_type,
        {{ adapter.quote("sbbow") }}::text as is_fow,
        {{ adapter.quote("sbbom") }}::text as is_fom,
        {{ adapter.quote("sbboq") }}::text as is_foq,
        {{ adapter.quote("sbboy") }}::text as is_foy,
        {{ adapter.quote("sbeow") }}::text as is_eow,
        {{ adapter.quote("sbeom") }}::text as is_eom,
        {{ adapter.quote("sbeoq") }}::text as is_eoq,
        {{ adapter.quote("sbeoy") }}::text as is_eoy,
        {{ adapter.quote("sbbusday")}}::text as is_current_date

    from source
)
select * from renamed
  