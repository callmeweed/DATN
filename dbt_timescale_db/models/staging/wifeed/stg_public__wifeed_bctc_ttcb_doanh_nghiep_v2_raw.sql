{{
    config(
        tags=['wifeed']
    )
}}

with source as (
      select * from {{ source('wifeed', 'wifeed_bctc_ttcb_doanh_nghiep_v2_raw') }}
),
renamed as (
    select
        {{ adapter.quote("mack") }},
        {{ adapter.quote("ten") }},
        {{ adapter.quote("loai_hinh_cong_ty") }},
        {{ adapter.quote("san_niem_yet") }},
        {{ adapter.quote("gioithieu") }},
        {{ adapter.quote("ghichu") }},
        {{ adapter.quote("diachi") }},
        {{ adapter.quote("website") }},
        {{ adapter.quote("nganhcap1") }},
        {{ adapter.quote("nganhcap2") }},
        {{ adapter.quote("nganhcap3") }},
        {{ adapter.quote("nganhcap4") }},
        {{ adapter.quote("ngayniemyet") }},
        {{ adapter.quote("smg") }},
        {{ adapter.quote("volume_daily") }},
        {{ adapter.quote("vol_tb_15ngay") }},
        {{ adapter.quote("vonhoa") }},
        {{ adapter.quote("dif") }},
        {{ adapter.quote("dif_percent") }},
        {{ adapter.quote("tong_tai_san") }},
        {{ adapter.quote("soluongluuhanh") }},
        {{ adapter.quote("soluongniemyet") }},
        {{ adapter.quote("cophieuquy") }},
        {{ adapter.quote("logo") }},
        {{ adapter.quote("logo_dnse_cdn") }},
        {{ adapter.quote("created_at") }},
        {{ adapter.quote("updated_at") }},
        {{ adapter.quote("indexed_timestamp_") }},
        {{ adapter.quote("symbol_") }},
        {{ adapter.quote("crawled_date") }}


    from source
)
select * from renamed where san_niem_yet != 'DELISTING'  -- remove delisted companies
  