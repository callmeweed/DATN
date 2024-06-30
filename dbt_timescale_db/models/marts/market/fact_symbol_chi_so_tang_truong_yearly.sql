with source as (
    select * from {{ ref('int_symbol_chi_so_tang_truong_yearly')}}
)

select * from source