with source as (

    select
        mack as symbol,
        ten,
        loai_hinh_cong_ty,
        san_niem_yet,

        smg,
        vonhoa,
        tong_tai_san

    from {{ ref('wifeed_bctc_thong_tin_co_ban_doanh_nghiep') }}
)

select * from source