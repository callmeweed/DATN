with source as (

    select *
    from {{ ref('int_symbol_thong_tin_co_ban') }}
)

select * from source