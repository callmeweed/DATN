{% snapshot wifeed_bctc_cstc_quarter_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='unique_id',
            check_cols=[
             'soluongluuhanh_q','eps_q','fcff_q','bienlaigop_q','bienlaithuan_q','bienlaiebit_q',
             'bienlaitruocthue_q','dongtien_hdkd_lnthuan_q','nim_q','cof_q','yea_q','cir_q',
             'tyle_baonoxau_q','tyle_noxau_q','laiphiphaithu_tongtaisan_q','ldr_q','casa_q','noxau_q',
             'tienguicuakhachhang_q','tongtaisan_q','vonchusohuu_q','doanhthuthuan_q','lnstctyme_q','phaithu_q',
             'tt_huydong_qoq','tt_tindung_qoq','tt_noxau_qoq','tt_thunhaplaithuan_q_yoy','tt_doanhthu_q_yoy','tt_lairong_q_yoy',
             'tt_taisan_qoq','tt_vonchu_qoq','tt_novay_qoq','tt_tonkho_qoq','tt_phaithu_qoq','ocf_q',
             'thunhaplaithuan_q','tindung_q'] ,
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_cstc_quarter_raw') }}
{% endsnapshot %}