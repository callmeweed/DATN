{% snapshot wifeed_bctc_cstc_ttm_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='unique_id',
            check_cols=[
             'soluongluuhanh_ttm', 'eps_ttm', 'bookvalue_ttm', 'vonhoa_ttm', 'ev_ttm', 'fcff_ttm',
             'bienlaigop_ttm', 'bienlaithuan_ttm', 'bienlaiebit_ttm', 'bienlaitruocthue_ttm', 'vongquaytaisan_ttm', 'vongquayphaithu_ttm', 
             'vongquaytonkho_ttm', 'vongquayphaitra_ttm', 'roa_ttm', 'roe_ttm', 'roic_ttm', 'dongtien_hdkd_lnthuan_ttm', 
             'tongno_tongtaisan_ttm', 'congnonganhan_tongtaisan_ttm', 'congnodaihan_tongtaisan_ttm', 'thanhtoan_hienhanh_ttm', 'thanhtoan_nhanh_ttm', 'thanhtoan_tienmat_ttm', 
             'novay_dongtienhdkd_ttm', 'ebit_laivay_ttm', 'pe_ttm', 'pb_ttm', 'ps_ttm', 'p_ocf_ttm', 
             'ev_ocf_ttm', 'ev_ebit_ttm', 'nim_ttm', 'cof_ttm', 'yea_ttm', 'cir_ttm', 
             'doanhthu_ttm', 'lairong_ttm', 'novay_ttm', 'tonkho_ttm', 'ocf_ttm', 'thunhaplaithuan_ttm', 
             'tongthunhaphoatdong_ttm', 'car_ttm', 'tt_thunhaplaithuan_ttm_yoy', 'tt_doanhthu_ttm_yoy', 'tt_lairong_ttm_yoy', 'tt_ocf_ttm_yoy', 
            ]
,
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_cstc_ttm_raw') }}
{% endsnapshot %}