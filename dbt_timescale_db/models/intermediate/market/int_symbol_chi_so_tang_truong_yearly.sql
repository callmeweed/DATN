
WITH
full_year AS (--gen all year, starting from 2009 since for all symbol
    SELECT
        t1.year_number
        , t2.code as symbol
    FROM
    (SELECT
        distinct year_number
    FROM {{ref('dim_date')}}
    WHERE 1 = 1
    AND date >= '2009-12-01'
    AND date < current_date) t1
    , (SELECT DISTINCT code FROM {{ ref('wifeed_bctc_kqkd_doanh_nghiep_san_xuat') }}
        UNION
        SELECT DISTINCT code FROM {{ ref('wifeed_bctc_kqkd_ngan_hang') }}
        UNION
        SELECT DISTINCT code FROM {{ ref('wifeed_bctc_kqkd_chung_khoan') }}
        UNION
        SELECT DISTINCT code FROM {{ ref('wifeed_bctc_kqkd_bao_hiem') }}) t2
)
, symbol_sx AS (--lấy doanh thu, lợi nhuận các mã ngành sx theo quý
    SELECT
        t1.nam
        , t1.code as symbol
        , t1.doanhthuthuanvebanhangvacungcapdichvu AS doanh_thu
        , t1.loinhuansauthuecuacongtyme AS loi_nhuan
        , t2.eps
    FROM {{ ref('wifeed_bctc_kqkd_doanh_nghiep_san_xuat') }} t1
    LEFT JOIN {{ ref('wifeed_bctc_cstc_dnsx_year_v2') }} t2
        on t1.nam = t2.nam AND t1.code = t2.mack
    WHERE t1.type = 'quarter'
    AND t1.quy = 0
)
, symbol_bank AS (--lấy doanh thu, lợi nhuận các mã ngành NH theo quý
    SELECT
        t1.nam
        , t1.code as symbol
        , t1.tongthunhaphoatdong AS doanh_thu
        , t1.codongcuacongtyme AS loi_nhuan
        , t2.eps
    FROM {{ ref('wifeed_bctc_kqkd_ngan_hang') }} t1
    LEFT JOIN {{ ref('wifeed_bctc_cstc_ngan_hang_year_v2') }} t2
        on t1.nam = t2.nam AND t1.code = t2.mack
    WHERE t1.type = 'quarter'
    AND t1.quy = 0
)
, symbol_ck AS (--lấy doanh thu, lợi nhuận các mã ngành CK theo quý
    SELECT
        t1.nam
        , t1.code as symbol
        , t1.doanhthuhoatdong AS doanh_thu
        , t1.lnsauthuecuachusohuu AS loi_nhuan
        , t2.eps
    FROM {{ ref('wifeed_bctc_kqkd_chung_khoan') }} t1
    LEFT JOIN {{ ref('wifeed_bctc_cstc_chung_khoan_year_v2') }} t2
        on t1.nam = t2.nam AND t1.code = t2.mack
    WHERE t1.type = 'quarter'
    AND t1.quy = 0
)
, symbol_bh AS (--lấy doanh thu, lợi nhuận các mã ngành BH theo quý
    SELECT
        t1.nam
        , t1.code as symbol
        , t1.doanhthuthuan as doanh_thu
        , t1.lnstcuacongtyme AS loi_nhuan
        , t2.eps
    FROM {{ ref('wifeed_bctc_kqkd_bao_hiem') }} t1
    LEFT JOIN {{ ref('wifeed_bctc_cstc_bao_hiem_year_v2') }} t2
        on t1.nam = t2.nam AND t1.code = t2.mack
    WHERE t1.type = 'quarter'
    AND t1.quy = 0
)
, get_rev AS (
    SELECT 
        t.year_number
        , t.symbol
        , CASE 
        WHEN symbol_sx.doanh_thu IS NOT NULL THEN symbol_sx.doanh_thu
        WHEN symbol_bank.doanh_thu IS NOT NULL THEN symbol_bank.doanh_thu
        WHEN symbol_ck.doanh_thu IS NOT NULL THEN symbol_ck.doanh_thu
        WHEN symbol_bh.doanh_thu IS NOT NULL THEN symbol_bh.doanh_thu
        ELSE NULL END AS doanh_thu 
        , CASE 
        WHEN symbol_sx.loi_nhuan IS NOT NULL THEN symbol_sx.loi_nhuan
        WHEN symbol_bank.loi_nhuan IS NOT NULL THEN symbol_bank.loi_nhuan
        WHEN symbol_ck.loi_nhuan IS NOT NULL THEN symbol_ck.loi_nhuan
        WHEN symbol_bh.loi_nhuan IS NOT NULL THEN symbol_bh.loi_nhuan
        ELSE NULL END AS loi_nhuan
        , CASE
        WHEN symbol_sx.eps IS NOT NULL THEN symbol_sx.eps
        WHEN symbol_bank.eps IS NOT NULL THEN symbol_bank.eps
        WHEN symbol_ck.eps IS NOT NULL THEN symbol_ck.eps
        WHEN symbol_bh.eps IS NOT NULL THEN symbol_bh.eps
        ELSE NULL END AS eps
    FROM full_year t
    LEFT JOIN symbol_sx ON t.year_number = symbol_sx.nam and t.symbol = symbol_sx.symbol
    LEFT JOIN symbol_bank ON t.year_number = symbol_bank.nam and t.symbol = symbol_bank.symbol
    LEFT JOIN symbol_ck ON t.year_number = symbol_ck.nam and t.symbol = symbol_ck.symbol
    LEFT JOIN symbol_bh ON t.year_number = symbol_bh.nam and t.symbol = symbol_bh.symbol
)
, lag_data AS (
    SELECT
        *
        , LAG(doanh_thu, 1)  over (partition by symbol order by year_number) as doanh_thu_1_y_ago
        , LAG(doanh_thu, 5)  over (partition by symbol order by year_number) as doanh_thu_5_y_ago
        , LAG(loi_nhuan, 1)  over (partition by symbol order by year_number) as loi_nhuan_1_y_ago
        , LAG(loi_nhuan, 5)  over (partition by symbol order by year_number) as loi_nhuan_5_y_ago
        , LAG(eps, 1)  over (partition by symbol order by year_number) as eps_1_y_ago
        , LAG(eps, 5)  over (partition by symbol order by year_number) as eps_5_y_ago

    FROM get_rev
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['year_number', 'symbol']) }} AS unique_id
    , year_number
    , symbol
    , doanh_thu
    , doanh_thu_1_y_ago
    , doanh_thu_5_y_ago
    , CASE
    WHEN doanh_thu_1_y_ago IS NOT NULL AND doanh_thu_1_y_ago >= 0 THEN (doanh_thu/NULLIF(doanh_thu_1_y_ago,0)) - 1
    WHEN doanh_thu_1_y_ago IS NOT NULL AND doanh_thu_1_y_ago < 0 THEN 1 - (doanh_thu/NULLIF(doanh_thu_1_y_ago,0))
    ELSE NULL END AS ty_le_tang_truong_doanh_thu_1_y
    , CASE
    WHEN doanh_thu_5_y_ago IS NOT NULL AND doanh_thu_5_y_ago >= 0 THEN (doanh_thu/NULLIF(doanh_thu_5_y_ago,0)) - 1
    WHEN doanh_thu_5_y_ago IS NOT NULL AND doanh_thu_5_y_ago < 0 THEN 1 - (doanh_thu/NULLIF(doanh_thu_5_y_ago,0))
    ELSE NULL END AS ty_le_tang_truong_doanh_thu_5_y
    , loi_nhuan
    , loi_nhuan_1_y_ago
    , loi_nhuan_5_y_ago
    , CASE
    WHEN loi_nhuan_1_y_ago IS NOT NULL AND loi_nhuan_1_y_ago >= 0 THEN (loi_nhuan/NULLIF(loi_nhuan_1_y_ago,0)) - 1
    WHEN loi_nhuan_1_y_ago IS NOT NULL AND loi_nhuan_1_y_ago < 0 THEN 1 - (loi_nhuan/NULLIF(loi_nhuan_1_y_ago,0))
    ELSE NULL END AS ty_le_tang_truong_loi_nhuan_1_y
    , CASE
    WHEN loi_nhuan_5_y_ago IS NOT NULL AND loi_nhuan_5_y_ago >= 0 THEN (loi_nhuan/NULLIF(loi_nhuan_5_y_ago,0)) - 1
    WHEN loi_nhuan_5_y_ago IS NOT NULL AND loi_nhuan_5_y_ago < 0 THEN 1 - (loi_nhuan/NULLIF(loi_nhuan_5_y_ago,0))
    ELSE NULL END AS ty_le_tang_truong_loi_nhuan_5_y
    , eps
    , eps_1_y_ago
    , eps_5_y_ago
    , CASE
    WHEN eps_1_y_ago IS NOT NULL AND eps_1_y_ago >= 0 THEN (eps/NULLIF(eps_1_y_ago,0)) - 1
    WHEN eps_1_y_ago IS NOT NULL AND eps_1_y_ago < 0 THEN 1 - (eps/NULLIF(eps_1_y_ago,0))
    ELSE NULL END AS ty_le_tang_truong_eps_1_y
    , CASE
    WHEN eps_5_y_ago IS NOT NULL AND eps_5_y_ago >= 0 THEN (eps/NULLIF(eps_5_y_ago,0)) - 1
    WHEN eps_5_y_ago IS NOT NULL AND eps_5_y_ago < 0 THEN 1 - (eps/NULLIF(eps_5_y_ago,0))
    ELSE NULL END AS ty_le_tang_truong_eps_5_y
FROM lag_data




