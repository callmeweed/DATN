{{
    config(
        tags=['wifeed']
    )
}}

with source as (
      select * from {{ source('wifeed', 'wifeed_bctc_ket_qua_kinh_doanh_ngan_hang_raw') }}
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
        {{ adapter.quote("thunhaplaivacackhoanthunhaptuongtu") }},
        {{ adapter.quote("chiphilaivacacchiphituongtu") }},
        {{ adapter.quote("thunhaplaithuan") }},
        {{ adapter.quote("thunhaptuhoatdongdichvu") }},
        {{ adapter.quote("chiphihoatdongdichvu") }},
        {{ adapter.quote("laithuantuhoatdongdichvu") }},
        {{ adapter.quote("lailothuantuhoaydongkinhdoanhngoaihoivavang") }},
        {{ adapter.quote("lailothuantumuabanchungkhoankinhdoanh") }},
        {{ adapter.quote("lailothuantumuabanchungkhoandautu") }},
        {{ adapter.quote("thunhaptuhoatdongkhac") }},
        {{ adapter.quote("chiphihoatdongkhac") }},
        {{ adapter.quote("lailothuantuhoatdongkhac") }},
        {{ adapter.quote("thunhaptugopvonmuacophan") }},
        {{ adapter.quote("tongthunhaphoatdong") }},
        {{ adapter.quote("chiphihoatdong") }},
        {{ adapter.quote("loinhuanthuantuhdkdtruocchiphiduphongruirotindung") }},
        {{ adapter.quote("chiphiduphongruirotindung") }},
        {{ adapter.quote("tongloinhuantruocthue") }},
        {{ adapter.quote("chiphithuetndnhienhanh") }},
        {{ adapter.quote("chiphithuetndnhoanlai") }},
        {{ adapter.quote("chiphithuethunhapdoanhnghiep") }},
        {{ adapter.quote("loinhuansauthue") }},
        {{ adapter.quote("loiichcuacodongthieuso_pl") }},
        {{ adapter.quote("codongcuacongtyme") }},
        {{ adapter.quote("indexed_timestamp_") }},
        {{ adapter.quote("symbol_") }}

    from source
)
select * from renamed
  