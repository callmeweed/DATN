{% snapshot wifeed_bctc_ttcb_doanh_nghiep_v2_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='mack',
            check_cols=[
                'ten', 'loai_hinh_cong_ty', 'san_niem_yet', 'gioithieu', 'ghichu', 'diachi', 'website', 'nganhcap1',
                'nganhcap2', 'nganhcap3', 'nganhcap4', 'ngayniemyet', 'smg', 'volume_daily', 'vol_tb_15ngay',
                'vonhoa', 'dif', 'dif_percent', 'tong_tai_san', 'soluongluuhanh', 'soluongniemyet', 'cophieuquy',
                'logo', 'logo_dnse_cdn'
            ],
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_ttcb_doanh_nghiep_v2_raw') }}
{% endsnapshot %}



