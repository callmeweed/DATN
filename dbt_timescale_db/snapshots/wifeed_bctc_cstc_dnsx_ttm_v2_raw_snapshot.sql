{% snapshot wifeed_bctc_cstc_dnsx_ttm_v2_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='unique_id',
            check_cols=['mack' ,'type' ,'quy' ,'nam' ,'fcff' ,'fcfe' ,'ebit' ,'ebitda' ,'tylethuesuat' ,'nopat' ,'ccc' ,'bienlaigop' ,'bienlaisauthue' ,'bienlaiebit' ,'bienlaiebitda' ,'bienlaitruocthue' ,'vongquaytaisan' ,'vongquayphaithu' ,'vongquaytonkho' ,'vongquayphaitra' ,'roa' ,'roe' ,'roic' ,'dongtien_hdkd_lnthuan' ,'dongtien_hdkd_dtt' ,'dongtien_hdkd_tts' ,'dongtien_hdkd_vcsh' ,'khtscd' ,'capex' ,'tt_dtt_yoy' ,'tt_lng_yoy' ,'tt_ebit_yoy' ,'tt_ebitda_yoy' ,'tt_lntt_yoy' ,'tt_lnst_yoy' ,'lnst_yoy' ,'ln_hdkd_yoy' ,'ln_hdtc_yoy' ,'ln_ldlk_yoy' ,'ln_khac_yoy' ,'cpgiavon_yoy' ,'cptaichinh_yoy' ,'cpbh_yoy' ,'cpqldn_yoy' ,'lctt_hdkd_yoy' ,'lctt_hddt_yoy' ,'lctt_hdtc_yoy' ,'lctt_trongky_yoy' ,'khauhao_tscd_yoy' ,'capex_yoy' ,'dtt_qoq' ,'ln_gop_qoq' ,'ebit_qoq' ,'ebitda_qoq' ,'lntt_qoq' ,'lnst_ctyme_qoq' ,'lnst_qoq' ,'ln_hdkd_qoq' ,'ln_hdtc_qoq' ,'ln_ldlk_qoq' ,'ln_khac_qoq' ,'cpgiavon_qoq' ,'cptaichinh_qoq' ,'cpbanhang_qoq' ,'cpquanlydn_qoq' ,'lctt_hdkd_qoq' ,'lctt_hddt_qoq' ,'lctt_hdtc_qoq' ,'lctt_trongky_qoq' ,'khauhao_tscd_qoq' ,'capex_qoq' ,'dol' ,'dfl' ,'dtl' ,'ls_novay' ,'novay_ebitda' ,'novay_dongtienhdkd' ,'ebit_laivay' ,'dongtien_hdkd_novay' ,'dongtien_hdkd_laivay' ,'tile_chiphigiavon' ,'tile_chiphilaivay' ,'tile_chiphibanhang' ,'tile_chiphiquanly' ,'tile_loinhuanhoatdongkinhdoanhchinh' ,'tile_loinhuantaichinh' ,'tile_lailoldlk' ,'tile_loinhuankhac' ,'p_fcff' ,'p_fcfe' ,'lng_tmcp' ,'p_gp' ,'ev_ocf' ,'ev_ebit' ,'ev_ebitda' ,'ev_fcff' ,'ps' ,'rev_per_share' ,'lnst_ctyme' ,'lnst_ctyme_yoy' ,'lctt_hdkd' ,'lctt_hddt' ,'lctt_hdtc' ,'lntt' ,'lntt_yoy' ,'lnst' ,'eps' ,'pe' ,'ep' ,'pe_dp' ,'peg' ,'peg_dc' ,'graham_1' ,'graham_2' ,'graham_3' ,'tl_chitracotucbangtien' ,'dongtien_hdkd_tmcp' ,'p_ocf' ],
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_cstc_dnsx_ttm_v2_raw') }}
{% endsnapshot %}

