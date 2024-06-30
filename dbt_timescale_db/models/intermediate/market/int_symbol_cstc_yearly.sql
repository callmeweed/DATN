with cstc_ngan_hang as (

    select
        mack,
        type,
        quy,
        nam,
        null::numeric as bienlaigop,
        roe,
        eps,
        pe,
        pb,
        null::numeric as ev_ebitda

    from {{ ref('wifeed_bctc_cstc_ngan_hang_year_v2') }}
),

cstc_san_xuat as (

    select
        mack,
        type,
        quy,
        nam,
        bienlaigop::numeric,
        roe,
        eps,
        pe,
        pb,
        ev_ebitda::numeric

    from {{ ref('wifeed_bctc_cstc_dnsx_year_v2') }}
),

cstc_chung_khoan as (

    select
        mack,
        type,
        quy,
        nam,
        null::numeric as bienlaigop,
        roe,
        eps,
        pe,
        pb,
        null::numeric as ev_ebitda

    from {{ ref('wifeed_bctc_cstc_chung_khoan_year_v2') }}
),

cstc_bao_hiem as (

    select
        mack,
        type,
        quy,
        nam,
        null::numeric as bienlaigop,
        roe,
        eps,
        pe,
        pb,
        null::numeric as ev_ebitda

    from {{ ref('wifeed_bctc_cstc_bao_hiem_year_v2') }}
),

final as (

    select * from cstc_bao_hiem
    union all
    select * from cstc_chung_khoan
    union all
    select * from cstc_san_xuat
    union all
    select * from cstc_ngan_hang
)

select * from final