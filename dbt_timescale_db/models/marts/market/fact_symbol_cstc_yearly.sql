with source as (
    select * from {{ ref('int_symbol_cstc_yearly') }}
)
select * from source