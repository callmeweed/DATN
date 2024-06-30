{{
    config(
        tags=['wifeed']
    )
}}

with source as (
      select * from {{ source('wifeed', 'wifeed_bctc_chi_so_tai_chinh_quarter_raw') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(["code", "nam","type","quy"]) }} AS unique_id,
        {{ adapter.quote("code") }},
        {{ adapter.quote("type") }},
        {{ adapter.quote("quy") }},
        {{ adapter.quote("nam") }},
        {{ adapter.quote("soluongluuhanh_q")}},
        {{ adapter.quote("eps_q") }},
        {{ adapter.quote("fcff_q") }},
        {{ adapter.quote("bienlaigop_q") }},
        {{ adapter.quote("bienlaithuan_q") }},
        {{ adapter.quote("bienlaiebit_q") }},
        {{ adapter.quote("bienlaitruocthue_q") }},
        {{ adapter.quote("dongtien_hdkd_lnthuan_q") }},
        {{ adapter.quote("nim_q") }},
        {{ adapter.quote("cof_q") }},
        {{ adapter.quote("yea_q") }},
        {{ adapter.quote("cir_q") }},
        {{ adapter.quote("tyle_baonoxau_q") }},
        {{ adapter.quote("tyle_noxau_q") }},
        {{ adapter.quote("laiphiphaithu_tongtaisan_q") }},
        {{ adapter.quote("ldr_q") }},
        {{ adapter.quote("casa_q") }},
        {{ adapter.quote("noxau_q") }},
        {{ adapter.quote("tienguicuakhachhang_q") }},
        {{ adapter.quote("tongtaisan_q") }},
        {{ adapter.quote("vonchusohuu_q") }},
        {{ adapter.quote("doanhthuthuan_q") }},
        {{ adapter.quote("lnstctyme_q") }},
        {{ adapter.quote("phaithu_q") }},
        {{ adapter.quote("tt_huydong_qoq") }},
        {{ adapter.quote("tt_tindung_qoq") }},
        {{ adapter.quote("tt_noxau_qoq") }},
        {{ adapter.quote("tt_thunhaplaithuan_q_yoy") }},
        {{ adapter.quote("tt_doanhthu_q_yoy") }},
        {{ adapter.quote("tt_lairong_q_yoy") }},
        {{ adapter.quote("tt_taisan_qoq") }},
        {{ adapter.quote("tt_vonchu_qoq") }},
        {{ adapter.quote("tt_novay_qoq") }},
        {{ adapter.quote("tt_tonkho_qoq") }},
        {{ adapter.quote("tt_phaithu_qoq") }},
        {{ adapter.quote("ocf_q") }},
        {{ adapter.quote("thunhaplaithuan_q") }},
        {{ adapter.quote("tindung_q") }},
        {{ adapter.quote("symbol_") }},
        {{ adapter.quote("indexed_timestamp_") }}
        
    from source
)
select * from renamed
