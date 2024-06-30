{{
    config(
        tags=['wifeed']
    )
}}

with source as (
      select * from {{ source('wifeed', 'wifeed_bctc_chi_so_tai_chinh_chung_khoan_year_v2_raw') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(["mack", "nam","type","quy"]) }} AS unique_id,
        {{ adapter.quote("mack") }},
        {{ adapter.quote("type") }},
        {{ adapter.quote("quy") }},
        {{ adapter.quote("nam") }},
        {{ adapter.quote("tilechovaykq") }},
        {{ adapter.quote("tsquanlytheocamket") }},
        {{ adapter.quote("dtnhdt") }},
        {{ adapter.quote("dtchovay") }},
        {{ adapter.quote("dtdanhgialaifvtpl") }},
        {{ adapter.quote("dtbantsfvtpl") }},
        {{ adapter.quote("dtfvtpl") }},
        {{ adapter.quote("dthtm") }},
        {{ adapter.quote("dtafs") }},
        {{ adapter.quote("dttudoanhvanguonvon") }},
        {{ adapter.quote("doanhthu") }},
        {{ adapter.quote("chiphinhdt") }},
        {{ adapter.quote("chiphihoatdong") }},
        {{ adapter.quote("laitudoanhnguonvonchovaykyquy") }},
        {{ adapter.quote("lainganhangdt") }},
        {{ adapter.quote("laimoigioi") }},
        {{ adapter.quote("laituvantaichinh") }},
        {{ adapter.quote("tile_cpdphoannhaptstc") }},
        {{ adapter.quote("tile_chiphitudoanh") }},
        {{ adapter.quote("tile_chiphimoigioi") }},
        {{ adapter.quote("tile_chiphibaolanh") }},
        {{ adapter.quote("tile_chiphidautuck") }},
        {{ adapter.quote("tile_chiphidaugia") }},
        {{ adapter.quote("tile_chiphiluuky") }},
        {{ adapter.quote("tile_chiphituvan") }},
        {{ adapter.quote("tile_chiphikhac") }},
        {{ adapter.quote("tile_nganhangdt") }},
        {{ adapter.quote("tile_dtchovay") }},
        {{ adapter.quote("tile_tudoanhnguonvon") }},
        {{ adapter.quote("tile_moigioichungkhoan") }},
        {{ adapter.quote("tile_cpniemyet") }},
        {{ adapter.quote("tile_cpchuaniemyet") }},
        {{ adapter.quote("tile_chungchiquy") }},
        {{ adapter.quote("tile_traiphieu") }},
        {{ adapter.quote("tile_tiengui") }},
        {{ adapter.quote("tile_chovaymargin") }},
        {{ adapter.quote("tile_chovayungtrc") }},
        {{ adapter.quote("tongcongtaisan_yoy") }},
        {{ adapter.quote("taisantcnganhan_yoy") }},
        {{ adapter.quote("tienvacackhoantuongduongtien_yoy") }},
        {{ adapter.quote("taisantcfvtpl_yoy") }},
        {{ adapter.quote("dautugiudenngaydaohanhtm_yoy") }},
        {{ adapter.quote("cackhoanchovay_yoy") }},
        {{ adapter.quote("cackhoantcsansangdebanafs_yoy") }},
        {{ adapter.quote("dpsuygiamtaisantc_yoy") }},
        {{ adapter.quote("tongcackhoanphaithunganhan_yoy") }},
        {{ adapter.quote("phaithucacdichvuctckcungcap_yoy") }},
        {{ adapter.quote("tratruocchonguoiban_yoy") }},
        {{ adapter.quote("cackhoanphaithukhac_yoy") }},
        {{ adapter.quote("taisannganhankhac_tong_yoy") }},
        {{ adapter.quote("taisantcdaihan_yoy") }},
        {{ adapter.quote("nonganhan_yoy") }},
        {{ adapter.quote("vayvanothuetcnganhan_yoy") }},
        {{ adapter.quote("traiphieuphathanhnganhan_yoy") }},
        {{ adapter.quote("nguoimuatratientruocnganhan_yoy") }},
        {{ adapter.quote("phaitrahoatdonggiaodichchungkhoan_yoy") }},
        {{ adapter.quote("cpphaitranganhan_yoy") }},
        {{ adapter.quote("nodaihan_yoy") }},
        {{ adapter.quote("traiphieuphathanhdaihan_yoy") }},
        {{ adapter.quote("vonchusohuu_tong_yoy") }},
        {{ adapter.quote("vongopcuachusohuu_yoy") }},
        {{ adapter.quote("tm_tienguicuandt_yoy") }},
        {{ adapter.quote("tm_taisantcniemyetdangkygdtaivsdcuactck_yoy") }},
        {{ adapter.quote("tm_taisantcdaluukytaivsdvachuagdcuactck_yoy") }},
        {{ adapter.quote("tm_taisantcchovecuactck_yoy") }},
        {{ adapter.quote("tm_taisantcchualuukytaivsdcuactck_yoy") }},
        {{ adapter.quote("tsquanlytheocamket_yoy") }},
        {{ adapter.quote("tm_phaitrandtvetienguigdcknhtmql_yoy") }},
        {{ adapter.quote("tm_cophieuniemyetfvtplhoply_yoy") }},
        {{ adapter.quote("tm_cophieuchuaniemyetfvtplhoply_yoy") }},
        {{ adapter.quote("tm_chungchiquyfvtplhoply_yoy") }},
        {{ adapter.quote("tm_traiphieufvtplhoply_yoy") }},
        {{ adapter.quote("tm_taisantckhacfvtplhoply_yoy") }},
        {{ adapter.quote("dtnhdt_yoy") }},
        {{ adapter.quote("dtchovay_yoy") }},
        {{ adapter.quote("dttudoanhvanguonvon_yoy") }},
        {{ adapter.quote("doanhthuhoatdongmoigioick_yoy") }},
        {{ adapter.quote("cpdphoannhaptstc_yoy") }},
        {{ adapter.quote("cphoatdongtudoanh_yoy") }},
        {{ adapter.quote("cphoatdongmoigioick_yoy") }},
        {{ adapter.quote("cphoatdongbaolanhdailyphathanhck_yoy") }},
        {{ adapter.quote("cphoatdongtuvandautuck_yoy") }},
        {{ adapter.quote("cphoatdongdaugiauythac_yoy") }},
        {{ adapter.quote("cpnghiepvuluukyck_yoy") }},
        {{ adapter.quote("cphoatdongtuvantc_yoy") }},
        {{ adapter.quote("cphoatdongkhac_yoy") }},
        {{ adapter.quote("tonglnketoantruocthue_yoy") }},
        {{ adapter.quote("lnsauthue_yoy") }},
        {{ adapter.quote("laitudoanhnguonvonchovaykyquy_yoy") }},
        {{ adapter.quote("lainganhangdt_yoy") }},
        {{ adapter.quote("laimoigioi_yoy") }},
        {{ adapter.quote("laituvantaichinh_yoy") }},
        {{ adapter.quote("tm_chovaynghiepvukyquymargin_yoy") }},
        {{ adapter.quote("tm_chovayungtruoctienbanckcuakhachhang_yoy") }},
        {{ adapter.quote("tile_tien") }},
        {{ adapter.quote("tile_fvtpl") }},
        {{ adapter.quote("tile_htm") }},
        {{ adapter.quote("tile_afs") }},
        {{ adapter.quote("tile_chovay") }},
        {{ adapter.quote("tile_phaithudichvu") }},
        {{ adapter.quote("loinhuanhoatdong") }},
        {{ adapter.quote("tstccuactckndt") }},
        {{ adapter.quote("chiphihoatdong_yoy") }},
        {{ adapter.quote("laitucackhoanchovayvaphaithu_yoy") }},
        {{ adapter.quote("doanhthu_yoy") }},
        {{ adapter.quote("loinhuanhoatdong_yoy") }},
        {{ adapter.quote("tm_taisantcchovecuandt_yoy") }},
        {{ adapter.quote("tm_taisantcchualuukytaivsdcuandt_yoy") }},
        {{ adapter.quote("tstccuactckndt_yoy") }},
        {{ adapter.quote("tm_taisantcdaluukyvsdchuagdcuandt_yoy") }},
        {{ adapter.quote("tm_taisantcniemyetdangkygdtaivsd_yoy") }},
        {{ adapter.quote("tile_phaithukhac") }},
        {{ adapter.quote("tylethuesuat") }},
        {{ adapter.quote("tongno_tongtaisan") }},
        {{ adapter.quote("tts_vonchu") }},
        {{ adapter.quote("roa") }},
        {{ adapter.quote("roe") }},
        {{ adapter.quote("dongtien_hdkd_lnthuan") }},
        {{ adapter.quote("dongtien_hdkd_tts") }},
        {{ adapter.quote("dongtien_hdkd_vcsh") }},
        {{ adapter.quote("vcsh_nguonvon") }},
        {{ adapter.quote("nophaitra_vcsh") }},
        {{ adapter.quote("lnst_ctyme") }},
        {{ adapter.quote("lnst_ctyme_yoy") }},
        {{ adapter.quote("vcsh") }},
        {{ adapter.quote("vcsh_yoy") }},
        {{ adapter.quote("lctt_hdkd") }},
        {{ adapter.quote("lctt_hddt") }},
        {{ adapter.quote("lctt_hdtc") }},
        {{ adapter.quote("lntt") }},
        {{ adapter.quote("lntt_yoy") }},
        {{ adapter.quote("nophaitra") }},
        {{ adapter.quote("nophaitra_yoy") }},
        {{ adapter.quote("tts") }},
        {{ adapter.quote("tts_yoy") }},
        {{ adapter.quote("lnst") }},
        {{ adapter.quote("lnst_yoy") }},
        {{ adapter.quote("chiaccotuc_tienmat") }},
        {{ adapter.quote("tile_chiacotuc_cp") }},
        {{ adapter.quote("tt_lnst_kehoach_yoy") }},
        {{ adapter.quote("tt_lnst_lnstkehoach_yoy") }},
        {{ adapter.quote("tyle_hoanthanh_dt") }},
        {{ adapter.quote("tyle_hoanthanh_lnst") }},
        {{ adapter.quote("vonhoa") }},
        {{ adapter.quote("eps") }},
        {{ adapter.quote("bookvalue") }},
        {{ adapter.quote("bvps") }},
        {{ adapter.quote("pe") }},
        {{ adapter.quote("ep") }},
        {{ adapter.quote("pb") }},
        {{ adapter.quote("tang_truong_kv") }},
        {{ adapter.quote("pe_dp") }},
        {{ adapter.quote("peg") }},
        {{ adapter.quote("peg_dc") }},
        {{ adapter.quote("graham_1") }},
        {{ adapter.quote("graham_2") }},
        {{ adapter.quote("graham_3") }},
        {{ adapter.quote("tysuatcotuc") }},
        {{ adapter.quote("tl_chitracotucbangtien") }},
        {{ adapter.quote("dongtien_hdkd_tmcp") }},
        {{ adapter.quote("bq_tysuatcotuc") }},
        {{ adapter.quote("p_ocf") }},
        {{ adapter.quote("indexed_timestamp_") }},
        {{ adapter.quote("symbol_") }}

    from source
)
select * from renamed
  