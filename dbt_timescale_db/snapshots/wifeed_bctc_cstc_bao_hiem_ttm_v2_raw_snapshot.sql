{% snapshot wifeed_bctc_cstc_bao_hiem_ttm_v2_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='unique_id',
            check_cols=['mack', 'type', 'quy', 'nam', 'tlbt_bhchayno', 'tlbt_bhnt', 'tlbt_bhpnt', 'tlbt_bhsk', 'tlbt_bhtauthuy', 'tlbt_bhtn', 'tlbt_bhtsvathiethai', 'tlbt_bhxecogioi', 'ttdthu_tm_baohiemhonhop_bhg', 'ttdthu_tm_baohiemkhac_bhg', 'ttdthu_tm_baohiemlienketchung_bhg', 'ttdthu_tm_bhchayno_bhg', 'ttdthu_tm_bhsuckhoe_bhg', 'ttdthu_tm_bhtaisanthiethai_bhg', 'ttdthu_tm_bhtauthuy_bhg', 'ttdthu_tm_bhtrachnhiem_bhg', 'ttdthu_tm_bhxecogioi_bhg', 'ttln_lailotucongtyliendoanhlienket', 'ttln_lngop', 'ttln_lnhdtaichinh', 'ttln_lnkhac', 'ttln_lntuhdkdkhac', 'tt_chiboithuongbaohiemgoc_yoy', 'tt_chiboithuongnhantaibaohiem_yoy', 'tt_chihoahongbaohiemgoc_yoy', 'tt_chiphibanhangbaohiemgoc_yoy', 'tt_chiphiquanlydn_yoy', 'tt_doanhthuphibaohiemthuan_yoy', 'tt_doanhthuthuan_yoy', 'tt_lailotucongtyliendoanhlienket_yoy', 'tt_lngop_yoy', 'tt_lnhdtaichinh_yoy', 'tt_lnkhac_yoy', 'tt_lntuhdkdkhac_yoy', 'tt_luuchuyentienthuantuhddautu_yoy', 'tt_luuchuyentienthuantuhdkd_yoy', 'tt_luuchuyentienthuantuhdtaichinh_yoy', 'tt_phibaohiemgoc_yoy', 'tt_phinhantaibaohiem_yoy', 'tt_phinhuongtaibaohiem_yoy', 'tt_tangduphongnghiepvubaohiemgoc_yoy','tt_thuhoahongnhuongtaibaohiem_yoy', 'tt_tm_baohiemnhantho_bh_yoy', 'tt_tm_baohiemnhantho_bhg_yoy', 'tt_tm_baohiemphinhantho_bh_yoy', 'tt_tm_baohiemphinhantho_bhg_yoy', 'tt_tm_bhchayno_bhg_yoy', 'tt_tm_bhchayno_cbt_yoy', 'tt_tm_bhsuckhoe_bhg_yoy', 'tt_tm_bhsuckhoe_cbt_yoy', 'tt_tm_bhtaisanthiethai_bhg_yoy', 'tt_tm_bhtaisanthiethai_cbt_yoy', 'tt_tm_bhtauthuy_bhg_yoy', 'tt_tm_bhtauthuy_cbt_yoy', 'tt_tm_bhtrachnhiem_bhg_yoy', 'tt_tm_bhtrachnhiem_cbt_yoy', 'tt_tm_bhxecogioi_bhg_yoy', 'tt_tm_bhxecogioi_cbt_yoy', 'tt_tongchiboithuong_yoy', 'tt_tongchiboithuongvatratienbaohiem_yoy', 'tt_tongchitructiephdkdbaohiem_yoy', 'tt_tonglnketoantruocthue_bs_yoy', 'tt_chiboithuongbaohiemgoc_qoq', 'tt_chiboithuongnhantaibaohiem_qoq', 'tt_chihoahongbaohiemgoc_qoq', 'tt_chiphibanhangbaohiemgoc_qoq', 'tt_chiphiquanlydn_qoq', 'tt_doanhthuphibaohiemthuan_qoq', 'tt_doanhthuthuan_qoq', 'tt_lailotucongtyliendoanhlienket_qoq', 'tt_lngop_qoq', 'tt_lnhdtaichinh_qoq', 'tt_lnkhac_qoq', 'tt_lntuhdkdkhac_qoq', 'tt_luuchuyentienthuantuhddautu_qoq', 'tt_luuchuyentienthuantuhdkd_qoq', 'tt_luuchuyentienthuantuhdtaichinh_qoq','tt_phibaohiemgoc_qoq', 'tt_phinhantaibaohiem_qoq', 'tt_phinhuongtaibaohiem_qoq', 'tt_tangduphongnghiepvubaohiemgoc_qoq', 'tt_thuhoahongnhuongtaibaohiem_qoq', 'tt_tm_baohiemnhantho_bh_qoq', 'tt_tm_baohiemnhantho_bhg_qoq', 'tt_tm_baohiemphinhantho_bh_qoq', 'tt_tm_baohiemphinhantho_bhg_qoq', 'tt_tm_bhchayno_bhg_qoq', 'tt_tm_bhchayno_cbt_qoq', 'tt_tm_bhsuckhoe_bhg_qoq', 'tt_tm_bhsuckhoe_cbt_qoq', 'tt_tm_bhtaisanthiethai_bhg_qoq', 'tt_tm_bhtaisanthiethai_cbt_qoq', 'tt_tm_bhtauthuy_bhg_qoq', 'tt_tm_bhtauthuy_cbt_qoq', 'tt_tm_bhtrachnhiem_bhg_qoq', 'tt_tm_bhtrachnhiem_cbt_qoq', 'tt_tm_bhxecogioi_bhg_qoq', 'tt_tm_bhxecogioi_cbt_qoq', 'tt_tongchiboithuong_qoq', 'tt_tongchiboithuongvatratienbaohiem_qoq', 'tt_tongchitructiephdkdbaohiem_qoq', 'tt_tonglnketoantruocthue_bs_qoq', 'tylethuesuat', 'roa', 'roe', 'dongtien_hdkd_lnthuan', 'dongtien_hdkd_tts', 'dongtien_hdkd_vcsh', 'lnst_ctyme', 'lnst_ctyme_yoy', 'lnst_ctyme_qoq', 'lctt_hdkd', 'lctt_hddt', 'lctt_hdtc', 'lntt', 'lntt_yoy', 'lntt_qoq', 'lnst', 'lnst_yoy', 'lnst_qoq', 'eps', 'pe', 'ep', 'pe_dp', 'peg', 'peg_dc', 'graham_1', 'graham_2', 'graham_3', 'tl_chitracotucbangtien', 'dongtien_hdkd_tmcp', 'p_ocf'],
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_cstc_bao_hiem_ttm_v2_raw') }}
{% endsnapshot %}
