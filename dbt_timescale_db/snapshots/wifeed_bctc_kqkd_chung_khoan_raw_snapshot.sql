{% snapshot wifeed_bctc_kqkd_chung_khoan_raw_snapshot %}

    {{
        config(
            strategy='check',
            target_schema='snapshots',
            unique_key='unique_id',
            check_cols='all',
            invalidate_hard_deletes=True
        )
    }}

select *
from {{ ref('stg_public__wifeed_bctc_kqkd_chung_khoan_raw') }}
{% endsnapshot %}

