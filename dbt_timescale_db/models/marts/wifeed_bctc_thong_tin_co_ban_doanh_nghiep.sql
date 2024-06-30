with source as (
      select * from {{ ref('wifeed_bctc_ttcb_doanh_nghiep_v2_raw_snapshot') }}
),
renamed as (
    select
        *
        , row_number() over (partition by mack order by dbt_updated_at desc) as rnk
    from source

)
select * from renamed
where rnk = 1 and dbt_valid_to is null
