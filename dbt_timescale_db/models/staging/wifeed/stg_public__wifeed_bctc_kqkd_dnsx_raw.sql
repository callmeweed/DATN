{{
    config(
        tags=['wifeed']
    )
}}

with source as (
      select * from {{ source('wifeed', 'wifeed_bctc_ket_qua_kinh_doanh_doanh_nghiep_san_xuat_raw') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(["code", "nam","type","quy"]) }} AS unique_id,
        {{ adapter.quote("code") }},
        {{ adapter.quote("type") }},
        {{ adapter.quote("quy") }},
        {{ adapter.quote("nam") }},
        {{ adapter.quote("donvikiemtoan") }},
        {{ adapter.quote("ykienkiemtoan") }},
        {{ adapter.quote("doanhthubanhangvacungcapdichvu") }},
        {{ adapter.quote("cackhoangiamtrudoanhthu") }},
        {{ adapter.quote("doanhthuthuanvebanhangvacungcapdichvu") }},
        {{ adapter.quote("giavonhangban") }},
        {{ adapter.quote("loinhuangopvebanhangvacungcapdichvu") }},
        {{ adapter.quote("doanhthuhoatdongtaichinh") }},
        {{ adapter.quote("chiphitaichinh") }},
        {{ adapter.quote("trongdochiphilaivay") }},
        {{ adapter.quote("phanlailohoaclotrongcongtyliendoanhlienket") }},
        {{ adapter.quote("chiphibanhang") }},
        {{ adapter.quote("chiphiquanlydoanhnghiep") }},
        {{ adapter.quote("loinhuanthuantuhoatdongkinhdoanh") }},
        {{ adapter.quote("thunhapkhac") }},
        {{ adapter.quote("chiphikhac") }},
        {{ adapter.quote("loinhuankhac") }},
        {{ adapter.quote("tongloinhuanketoantruocthue") }},
        {{ adapter.quote("chiphithuetndnhienhanh") }},
        {{ adapter.quote("chiphithuetndnhoanlai") }},
        {{ adapter.quote("loinhuansauthuethunhapdoanhnghiep") }},
        {{ adapter.quote("loiichcuacodongthieuso_bctn") }},
        {{ adapter.quote("loinhuansauthuecuacongtyme") }},
        {{ adapter.quote("laicobantrencophieu") }},
        {{ adapter.quote("laisuygiamtrencophieu") }},
        {{ adapter.quote("indexed_timestamp_") }},
        {{ adapter.quote("symbol_") }}

    from source
)
select * from renamed
  