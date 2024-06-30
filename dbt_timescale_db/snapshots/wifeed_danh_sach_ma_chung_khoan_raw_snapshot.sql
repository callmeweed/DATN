{% snapshot wifeed_danh_sach_ma_chung_khoan_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='code',
            check_cols=[ "san", "loaidn", "fullname_vi"],
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_danh_sach_ma_chung_khoan_raw') }}
{% endsnapshot %}
