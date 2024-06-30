{{
    config(
        tags=['wifeed']
    )
}}

with source as (
      select * from {{ source('wifeed', 'wifeed_bctc_chi_so_tai_chinh_ttm_raw') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(["code", "nam","type","quy"]) }} AS unique_id,
        {{ adapter.quote("code") }},
        {{ adapter.quote("type") }},
        {{ adapter.quote("quy") }},
        {{ adapter.quote("nam") }},
        {{ adapter.quote("soluongluuhanh_ttm") }},
        {{ adapter.quote("eps_ttm") }},
        {{ adapter.quote("bookvalue_ttm") }},
        {{ adapter.quote("vonhoa_ttm") }},
        {{ adapter.quote("ev_ttm") }},
        {{ adapter.quote("fcff_ttm") }},
        {{ adapter.quote("bienlaigop_ttm") }},
        {{ adapter.quote("bienlaithuan_ttm") }},
        {{ adapter.quote("bienlaiebit_ttm") }},
        {{ adapter.quote("bienlaitruocthue_ttm") }},
        {{ adapter.quote("vongquaytaisan_ttm") }},
        {{ adapter.quote("vongquayphaithu_ttm") }},
        {{ adapter.quote("vongquaytonkho_ttm") }},
        {{ adapter.quote("vongquayphaitra_ttm") }},
        {{ adapter.quote("roa_ttm") }},
        {{ adapter.quote("roe_ttm") }},
        {{ adapter.quote("roic_ttm") }},
        {{ adapter.quote("dongtien_hdkd_lnthuan_ttm") }},
        {{ adapter.quote("tongno_tongtaisan_ttm") }},
        {{ adapter.quote("congnonganhan_tongtaisan_ttm") }},
        {{ adapter.quote("congnodaihan_tongtaisan_ttm") }},
        {{ adapter.quote("thanhtoan_hienhanh_ttm") }},
        {{ adapter.quote("thanhtoan_nhanh_ttm") }},
        {{ adapter.quote("thanhtoan_tienmat_ttm") }},
        {{ adapter.quote("novay_dongtienhdkd_ttm") }},
        {{ adapter.quote("ebit_laivay_ttm") }},
        {{ adapter.quote("pe_ttm") }},
        {{ adapter.quote("pb_ttm") }},
        {{ adapter.quote("ps_ttm") }},
        {{ adapter.quote("p_ocf_ttm") }},
        {{ adapter.quote("ev_ocf_ttm") }},
        {{ adapter.quote("ev_ebit_ttm") }},
        {{ adapter.quote("nim_ttm") }},
        {{ adapter.quote("cof_ttm") }},
        {{ adapter.quote("yea_ttm") }},
        {{ adapter.quote("cir_ttm") }},
        {{ adapter.quote("doanhthu_ttm") }},
        {{ adapter.quote("lairong_ttm") }},
        {{ adapter.quote("novay_ttm") }},
        {{ adapter.quote("tonkho_ttm") }},
        {{ adapter.quote("ocf_ttm") }},
        {{ adapter.quote("thunhaplaithuan_ttm") }},
        {{ adapter.quote("tongthunhaphoatdong_ttm") }},
        {{ adapter.quote("car_ttm") }},
        {{ adapter.quote("tt_thunhaplaithuan_ttm_yoy") }},
        {{ adapter.quote("tt_doanhthu_ttm_yoy") }},
        {{ adapter.quote("tt_lairong_ttm_yoy") }},
        {{ adapter.quote("tt_ocf_ttm_yoy") }},
        {{ adapter.quote("symbol_") }},
        {{ adapter.quote("indexed_timestamp_") }}

    from source
)
select * from renamed