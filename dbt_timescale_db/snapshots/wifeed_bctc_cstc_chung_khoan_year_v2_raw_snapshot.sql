{% snapshot wifeed_bctc_cstc_chung_khoan_year_v2_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='unique_id',
            check_cols=['mack', 'type', 'quy', 'nam', 'tilechovaykq', 'tsquanlytheocamket', 'dtnhdt', 'dtchovay', 'dtdanhgialaifvtpl', 'dtbantsfvtpl', 'dtfvtpl', 'dthtm', 'dtafs', 'dttudoanhvanguonvon', 'doanhthu', 'chiphinhdt', 'chiphihoatdong', 'laitudoanhnguonvonchovaykyquy', 'lainganhangdt', 'laimoigioi', 'laituvantaichinh', 'tile_cpdphoannhaptstc', 'tile_chiphitudoanh', 'tile_chiphimoigioi', 'tile_chiphibaolanh', 'tile_chiphidautuck', 'tile_chiphidaugia', 'tile_chiphiluuky', 'tile_chiphituvan', 'tile_chiphikhac', 'tile_nganhangdt', 'tile_dtchovay', 'tile_tudoanhnguonvon', 'tile_moigioichungkhoan', 'tile_cpniemyet', 'tile_cpchuaniemyet', 'tile_chungchiquy', 'tile_traiphieu', 'tile_tiengui', 'tile_chovaymargin', 'tile_chovayungtrc', 'tongcongtaisan_yoy', 'taisantcnganhan_yoy', 'tienvacackhoantuongduongtien_yoy', 'taisantcfvtpl_yoy', 'dautugiudenngaydaohanhtm_yoy', 'cackhoanchovay_yoy', 'cackhoantcsansangdebanafs_yoy', 'dpsuygiamtaisantc_yoy', 'tongcackhoanphaithunganhan_yoy', 'phaithucacdichvuctckcungcap_yoy', 'tratruocchonguoiban_yoy', 'cackhoanphaithukhac_yoy', 'taisannganhankhac_tong_yoy', 'taisantcdaihan_yoy', 'nonganhan_yoy', 'vayvanothuetcnganhan_yoy', 'traiphieuphathanhnganhan_yoy', 'nguoimuatratientruocnganhan_yoy', 'phaitrahoatdonggiaodichchungkhoan_yoy', 'cpphaitranganhan_yoy', 'nodaihan_yoy', 'traiphieuphathanhdaihan_yoy', 'vonchusohuu_tong_yoy', 'vongopcuachusohuu_yoy', 'tm_tienguicuandt_yoy', 'tm_taisantcniemyetdangkygdtaivsdcuactck_yoy', 'tm_taisantcdaluukytaivsdvachuagdcuactck_yoy', 'tm_taisantcchovecuactck_yoy', 'tm_taisantcchualuukytaivsdcuactck_yoy', 'tsquanlytheocamket_yoy', 'tm_phaitrandtvetienguigdcknhtmql_yoy', 'tm_cophieuniemyetfvtplhoply_yoy', 'tm_cophieuchuaniemyetfvtplhoply_yoy', 'tm_chungchiquyfvtplhoply_yoy', 'tm_traiphieufvtplhoply_yoy', 'tm_taisantckhacfvtplhoply_yoy', 'dtnhdt_yoy', 'dtchovay_yoy', 'dttudoanhvanguonvon_yoy', 'doanhthuhoatdongmoigioick_yoy', 'cpdphoannhaptstc_yoy', 'cphoatdongtudoanh_yoy', 'cphoatdongmoigioick_yoy', 'cphoatdongbaolanhdailyphathanhck_yoy', 'cphoatdongtuvandautuck_yoy', 'cphoatdongdaugiauythac_yoy', 'cpnghiepvuluukyck_yoy', 'cphoatdongtuvantc_yoy', 'cphoatdongkhac_yoy', 'tonglnketoantruocthue_yoy', 'lnsauthue_yoy', 'laitudoanhnguonvonchovaykyquy_yoy', 'lainganhangdt_yoy', 'laimoigioi_yoy', 'laituvantaichinh_yoy', 'tm_chovaynghiepvukyquymargin_yoy', 'tm_chovayungtruoctienbanckcuakhachhang_yoy', 'tile_tien', 'tile_fvtpl', 'tile_htm', 'tile_afs', 'tile_chovay', 'tile_phaithudichvu', 'loinhuanhoatdong', 'tstccuactckndt', 'chiphihoatdong_yoy', 'laitucackhoanchovayvaphaithu_yoy', 'doanhthu_yoy', 'loinhuanhoatdong_yoy', 'tm_taisantcchovecuandt_yoy', 'tm_taisantcchualuukytaivsdcuandt_yoy', 'tstccuactckndt_yoy', 'tm_taisantcdaluukyvsdchuagdcuandt_yoy', 'tm_taisantcniemyetdangkygdtaivsd_yoy', 'tile_phaithukhac', 'tylethuesuat', 'tongno_tongtaisan', 'tts_vonchu', 'roa', 'roe', 'dongtien_hdkd_lnthuan', 'dongtien_hdkd_tts', 'dongtien_hdkd_vcsh', 'vcsh_nguonvon', 'nophaitra_vcsh', 'lnst_ctyme', 'lnst_ctyme_yoy', 'vcsh', 'vcsh_yoy', 'lctt_hdkd', 'lctt_hddt', 'lctt_hdtc', 'lntt', 'lntt_yoy', 'nophaitra', 'nophaitra_yoy', 'tts', 'tts_yoy', 'lnst', 'lnst_yoy', 'chiaccotuc_tienmat', 'tile_chiacotuc_cp', 'tt_lnst_kehoach_yoy', 'tt_lnst_lnstkehoach_yoy', 'tyle_hoanthanh_dt', 'tyle_hoanthanh_lnst', 'vonhoa', 'eps', 'bookvalue', 'bvps', 'pe', 'ep', 'pb', 'tang_truong_kv', 'pe_dp', 'peg', 'peg_dc', 'graham_1', 'graham_2', 'graham_3', 'tysuatcotuc', 'tl_chitracotucbangtien', 'dongtien_hdkd_tmcp', 'bq_tysuatcotuc', 'p_ocf'],
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_cstc_chung_khoan_year_v2_raw') }}
{% endsnapshot %}

