
WITH
full_quarter AS (--gen all quarters, starting from Q4 2009 since for all symbol
    SELECT
        t1.qtr_number
        , t1.year_number
        , t1.quater_year_name AS quarter_year_name
        , t2.code as symbol
    FROM
    (SELECT
        qtr_number
        , year_number
        , quater_year_name
    FROM {{ref('dim_date')}}
    WHERE 1 = 1
    AND date >= '2009-12-01'
    AND date < current_date
    GROUP BY 1, 2, 3) t1
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
        nam
        , quy
        , code as symbol
        , doanhthuthuanvebanhangvacungcapdichvu AS doanh_thu
        , loinhuansauthuecuacongtyme AS loi_nhuan
    FROM {{ ref('wifeed_bctc_kqkd_doanh_nghiep_san_xuat') }}
    WHERE type = 'quarter'
    AND quy != 0
)
, symbol_bank AS (--lấy doanh thu, lợi nhuận các mã ngành NH theo quý
    SELECT
        nam
        , quy
        , code as symbol
        , tongthunhaphoatdong AS doanh_thu
        , codongcuacongtyme AS loi_nhuan
    FROM {{ ref('wifeed_bctc_kqkd_ngan_hang') }}
    WHERE type = 'quarter'
    AND quy != 0
)
, symbol_ck AS (--lấy doanh thu, lợi nhuận các mã ngành CK theo quý
    SELECT
        nam
        , quy
        , code as symbol
        , doanhthuhoatdong AS doanh_thu
        , lnsauthuecuachusohuu AS loi_nhuan
    FROM {{ ref('wifeed_bctc_kqkd_chung_khoan') }}
    WHERE type = 'quarter'
    AND quy != 0
)
, symbol_bh AS (--lấy doanh thu, lợi nhuận các mã ngành BH theo quý
    SELECT
        nam
        , quy
        , code as symbol
        , doanhthuthuan as doanh_thu
        , lnstcuacongtyme AS loi_nhuan
    FROM {{ ref('wifeed_bctc_kqkd_bao_hiem') }}
    WHERE type = 'quarter'
    AND quy != 0
)
, get_rev AS (
    SELECT 
        t.qtr_number
        , t.year_number
        , t.quarter_year_name
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
    FROM full_quarter t
    LEFT JOIN symbol_sx ON t.year_number = symbol_sx.nam AND t.qtr_number = symbol_sx.quy and t.symbol = symbol_sx.symbol
    LEFT JOIN symbol_bank ON t.year_number = symbol_bank.nam AND t.qtr_number = symbol_bank.quy and t.symbol = symbol_bank.symbol
    LEFT JOIN symbol_ck ON t.year_number = symbol_ck.nam AND t.qtr_number = symbol_ck.quy and t.symbol = symbol_ck.symbol
    LEFT JOIN symbol_bh ON t.year_number = symbol_bh.nam AND t.qtr_number = symbol_bh.quy and t.symbol = symbol_bh.symbol
)
, lag_data AS (
    SELECT
        *
        , LAG(doanh_thu, 1) over (partition by symbol order by year_number, qtr_number) as doanh_thu_1_qtr_ago
        , LAG(loi_nhuan, 1) over (partition by symbol order by year_number, qtr_number) as loi_nhuan_1_qtr_ago
        , LAG(doanh_thu, 2) over (partition by symbol order by year_number, qtr_number) as doanh_thu_2_qtr_ago
        , LAG(loi_nhuan, 2) over (partition by symbol order by year_number, qtr_number) as loi_nhuan_2_qtr_ago
    FROM get_rev
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['qtr_number', 'year_number', 'symbol']) }} AS unique_id
    , qtr_number
    , year_number
    , quarter_year_name
    , symbol
    , doanh_thu
    , doanh_thu_1_qtr_ago
    , doanh_thu_2_qtr_ago
    , CASE
    WHEN doanh_thu_1_qtr_ago IS NOT NULL AND doanh_thu_1_qtr_ago >= 0 THEN (doanh_thu/NULLIF(doanh_thu_1_qtr_ago,0)) - 1
    WHEN doanh_thu_1_qtr_ago IS NOT NULL AND doanh_thu_1_qtr_ago < 0 THEN 1 - (doanh_thu/NULLIF(doanh_thu_1_qtr_ago,0))
    ELSE NULL END AS ty_le_tang_truong_doanh_thu_1_qtr
    , CASE
    WHEN doanh_thu_2_qtr_ago IS NOT NULL AND doanh_thu_2_qtr_ago >= 0 THEN (doanh_thu/NULLIF(doanh_thu_2_qtr_ago,0)) - 1
    WHEN doanh_thu_2_qtr_ago IS NOT NULL AND doanh_thu_2_qtr_ago < 0 THEN 1 - (doanh_thu/NULLIF(doanh_thu_2_qtr_ago,0))
    ELSE NULL END AS ty_le_tang_truong_doanh_thu_2_qtr
    , loi_nhuan
    , loi_nhuan_1_qtr_ago
    , loi_nhuan_2_qtr_ago
    , CASE
    WHEN loi_nhuan_1_qtr_ago IS NOT NULL AND loi_nhuan_1_qtr_ago >= 0 THEN (loi_nhuan/NULLIF(loi_nhuan_1_qtr_ago,0)) - 1
    WHEN loi_nhuan_1_qtr_ago IS NOT NULL AND loi_nhuan_1_qtr_ago < 0 THEN 1 - (loi_nhuan/NULLIF(loi_nhuan_1_qtr_ago,0))
    ELSE NULL END AS ty_le_tang_truong_loi_nhuan_1_qtr
    , CASE
    WHEN loi_nhuan_2_qtr_ago IS NOT NULL AND loi_nhuan_2_qtr_ago >= 0 THEN (loi_nhuan/NULLIF(loi_nhuan_2_qtr_ago,0)) - 1
    WHEN loi_nhuan_2_qtr_ago IS NOT NULL AND loi_nhuan_2_qtr_ago < 0 THEN 1 - (loi_nhuan/NULLIF(loi_nhuan_2_qtr_ago,0))
    ELSE NULL END AS ty_le_tang_truong_loi_nhuan_2_qtr
FROM lag_data