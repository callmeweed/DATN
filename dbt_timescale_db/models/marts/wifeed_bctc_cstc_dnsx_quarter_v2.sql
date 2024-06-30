with source as (
      select * from {{ ref('wifeed_bctc_cstc_dnsx_quarter_v2_raw_snapshot') }}
),
renamed as (
    select
        *
        , row_number() over (partition by unique_id order by dbt_updated_at desc) as rnk
    from source

)
select * from renamed
where rnk = 1 and dbt_valid_to is null