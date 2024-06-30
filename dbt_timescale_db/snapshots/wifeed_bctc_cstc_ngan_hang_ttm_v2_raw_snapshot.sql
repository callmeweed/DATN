{% snapshot wifeed_bctc_cstc_ngan_hang_ttm_v2_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='unique_id',
            check_cols=['mack' ,'type' ,'quy' ,'nam' ,'roa' ,'roe' ,'nim' ,'cof' ,'yea' ,'cir' ,'tt_thunhaplai_yoy' ,'tt_chiphilai_yoy' ,'tt_thunhaplaithuan_yoy' ,'tt_lailotudichvu_yoy' ,'tt_lailotungoaihoivavang_yoy' ,'tt_lailomuabanckkd_yoy' ,'tt_lailomubanckdt_yoy' ,'tt_loinhuankhac_yoy' ,'tt_thunhaptugopvonmuacophan_yoy' ,'tt_tongthunhaphoatdong_yoy' ,'tt_tongchiphihoatdong_yoy' ,'tt_lntruocduphong_yoy' ,'tt_chiphiduphong_yoy' ,'tt_lntt_yoy' ,'tt_lnst_yoy' ,'tt_lnstme_yoy' ,'tt_thunhaplai_qoq' ,'tt_chiphilai_qoq' ,'tt_thunhaplaithuan_qoq' ,'tt_lailotudichvu_qoq' ,'tt_lailotungoaihoivavang_qoq' ,'tt_lailomuabanckkd_qoq' ,'tt_lailomubanckdt_qoq' ,'tt_loinhuankhac_qoq' ,'tt_thunhaptugopvonmuacophan_qoq' ,'tt_tongthunhaphoatdong_qoq' ,'tt_tongchiphihoatdong_qoq' ,'tt_lntruocduphong_qoq' ,'tt_chiphiduphong_qoq' ,'tt_lntt_qoq' ,'tt_lnst_qoq' ,'tt_lnstme_qoq' ,'tile_thunhaplaithuan' ,'tile_laithuantuhoatdongdichvu' ,'tile_loinhuantuhoatdongthanhtoan' ,'tile_loinhuantuhoatdongkinhdoanhbaohiem' ,'tile_loinhuantudichvuchungkhoan' ,'tile_loinhuantukinhdoanhvangvangoaihoi' ,'tile_lailotumuabanckdt' ,'tile_lailotumuabanckkd' ,'tile_loinhuankhac' ,'tile_thunhaptugopvonmuacophan' ,'tile_chiphiduphong' ,'tylethuesuat' ,'dongtien_hdkd_lnthuan' ,'dongtien_hdkd_tts' ,'dongtien_hdkd_vcsh' ,'lnst_ctyme' ,'lnst_ctyme_yoy' ,'lnst_ctyme_qoq' ,'lctt_hdkd' ,'lctt_hddt' ,'lctt_hdtc' ,'lntt' ,'lntt_yoy' ,'lntt_qoq' ,'lnst' ,'lnst_yoy' ,'lnst_qoq' ,'eps' ,'pe' ,'ep' ,'pe_dp' ,'peg' ,'peg_dc' ,'graham_1' ,'graham_2' ,'graham_3' ,'tl_chitracotucbangtien' ,'dongtien_hdkd_tmcp' ,'p_ocf' ],
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_cstc_ngan_hang_ttm_v2_raw') }}
{% endsnapshot %}

