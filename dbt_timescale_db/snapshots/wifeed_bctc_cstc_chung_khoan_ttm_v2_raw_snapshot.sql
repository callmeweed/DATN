{% snapshot wifeed_bctc_cstc_chung_khoan_ttm_v2_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='unique_id',
            check_cols=['mack','type','quy','nam','dtnhdt','dtchovay','dtdanhgialaifvtpl','dtbantsfvtpl','dtfvtpl','dthtm','dtafs','dttudoanhvanguonvon','doanhthu','chiphinhdt','chiphihoatdong','laitudoanhnguonvonchovaykyquy','lainganhangdt','laimoigioi','laituvantaichinh','tile_cpdphoannhaptstc','tile_chiphitudoanh','tile_chiphimoigioi','tile_chiphibaolanh','tile_chiphidautuck','tile_chiphidaugia','tile_chiphiluuky','tile_chiphituvan','tile_chiphikhac','tile_nganhangdt','tile_dtchovay','tile_tudoanhnguonvon','tile_moigioichungkhoan','dtnhdt_yoy','dtchovay_yoy','dttudoanhvanguonvon_yoy','doanhthuhoatdongmoigioick_yoy','cpdphoannhaptstc_yoy','cphoatdongtudoanh_yoy','cphoatdongmoigioick_yoy','cphoatdongbaolanhdailyphathanhck_yoy','cphoatdongtuvandautuck_yoy','cphoatdongdaugiauythac_yoy','cpnghiepvuluukyck_yoy','cphoatdongtuvantc_yoy','cphoatdongkhac_yoy','tonglnketoantruocthue_yoy','lnsauthue_yoy','laitudoanhnguonvonchovaykyquy_yoy','lainganhangdt_yoy','laimoigioi_yoy','laituvantaichinh_yoy','doanhthu_qoq','dtnhdt_qoq','dtchovay_qoq','dttudoanhvanguonvon_qoq','doanhthuhoatdongmoigioick_qoq','chiphihoatdong_qoq','cpdphoannhaptstc_qoq','cphoatdongtudoanh_qoq','cphoatdongmoigioick_qoq','cphoatdongbaolanhdailyphathanhck_qoq','cphoatdongtuvandautuck_qoq','cphoatdongdaugiauythac_qoq','cpnghiepvuluukyck_qoq','cphoatdongtuvantc_qoq','cphoatdongkhac_qoq','tonglnketoantruocthue_qoq','lnsauthue_qoq','laitudoanhnguonvonchovaykyquy_qoq','lainganhangdt_qoq','laimoigioi_qoq','laituvantaichinh_qoq','loinhuanhoatdong','chiphihoatdong_yoy','laitucackhoanchovayvaphaithu_yoy','doanhthu_yoy','loinhuanhoatdong_yoy','tile_chovaymargin_qoq','tile_chovayungtrc_qoq','laitucackhoanchovayvaphaithu_qoq','loinhuanhoatdong_qoq','tylethuesuat','roa','roe','dongtien_hdkd_lnthuan','dongtien_hdkd_tts','dongtien_hdkd_vcsh','lnst_ctyme','lnst_ctyme_yoy','lnst_ctyme_qoq','lctt_hdkd','lctt_hddt','lctt_hdtc','lntt','lntt_yoy','lntt_qoq','lnst','lnst_yoy','lnst_qoq','eps','pe','ep','pe_dp','peg','peg_dc','graham_1','graham_2','graham_3','tl_chitracotucbangtien','dongtien_hdkd_tmcp','p_ocf'],
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_cstc_chung_khoan_ttm_v2_raw') }}
{% endsnapshot %}

