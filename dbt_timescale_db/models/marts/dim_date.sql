{{
  config(
    materialized='table',
    unique_key = 'date_id',
    sort = [
        'date_id',
        'date',
        'is_eom'
    ]
  )
}}


WITH sequence AS (
    SELECT generate_series(0, 50 * 365) AS generated_number
),
date_series AS (
    SELECT '2010-01-01'::date + generated_number AS datum,
           generated_number AS seq
    FROM sequence
),
tb AS (
    SELECT CAST(seq + 1 AS INTEGER) AS date_id,
           datum AS date,
           datum + INTERVAL '00:00:00' AS earliest_time,
           datum + INTERVAL '23:59:59' AS latest_time,
           TO_CHAR(datum, 'DD/MM/YYYY')::CHAR(10) AS au_format_date,
           TO_CHAR(datum, 'MM/DD/YYYY')::CHAR(10) AS us_format_date,
           CAST(EXTRACT(YEAR FROM datum) AS SMALLINT) AS year_number,
           CAST(EXTRACT(WEEK FROM datum) AS SMALLINT) AS year_week_number,
           CAST(EXTRACT(DOY FROM datum) AS SMALLINT) AS year_day_number,
           CAST(TO_CHAR(datum + INTERVAL '6 months', 'yyyy') AS SMALLINT) AS au_fiscal_year_number,
           CAST(TO_CHAR(datum + INTERVAL '3 months', 'yyyy') AS SMALLINT) AS us_fiscal_year_number,
           (DATE_TRUNC('year', datum::date))::date AS first_day_of_year,
           (DATE_TRUNC('year', (datum + INTERVAL '1 year')::date) - INTERVAL '1 day')::date AS last_day_of_year,
           CAST(TO_CHAR(datum, 'Q') AS SMALLINT) AS qtr_number,
           CAST(TO_CHAR(datum + INTERVAL '6 months', 'Q') AS SMALLINT) AS au_fiscal_qtr_number,
           CAST(TO_CHAR(datum + INTERVAL '3 months', 'Q') AS SMALLINT) AS us_fiscal_qtr_number,
           (DATE_TRUNC('quarter', datum::date))::date AS first_day_of_quarter,
           (DATE_TRUNC('quarter', (datum + INTERVAL '3 months')::date) - INTERVAL '1 day')::date AS last_day_of_quarter,
           CAST(EXTRACT(MONTH FROM datum) AS SMALLINT) AS month_number,
           TO_CHAR(datum, 'MM-YYYY') AS month_name,
           CAST(EXTRACT(DAY FROM datum) AS SMALLINT) AS month_day_number,
           (DATE_TRUNC('month', datum::date))::date AS first_day_of_month,
           (DATE_TRUNC('month', (datum + INTERVAL '1 month')::date) - INTERVAL '1 day')::date AS last_day_of_month,
           CAST(TO_CHAR(datum, 'D') AS SMALLINT) AS week_day_number,
           (DATE_TRUNC('week', datum::date))::date AS first_day_of_week,
           (DATE_TRUNC('week', (datum + INTERVAL '1 week')::date) - INTERVAL '1 day')::date AS last_day_of_week,
           TO_CHAR(datum, 'Day') AS day_name,
           CASE
               WHEN TO_CHAR(datum, 'D') IN ('1', '7') THEN 0
               ELSE 1
           END AS is_weekday,
           CASE
               WHEN (date_trunc('month', datum::date) + INTERVAL '1 month - 1 day')::date = datum THEN 1
               ELSE 0
           END AS is_eom
    FROM date_series
    ORDER BY 1
),
d AS (
    SELECT
        tb.*,
        tb.year_number::TEXT AS year_name,
        'Q'||tb.qtr_number||'-'||tb.year_number AS quater_year_name,
        tb.month_number||'-'||tb.year_number AS month_year_name,
        'W'||tb.year_week_number AS week_year_name,
        sb.is_holiday
    FROM tb
    LEFT JOIN (SELECT sb_date, is_holiday FROM {{ref('stg_flex__sbcldr')}} WHERE calendar_type = '0') sb
    ON tb.date = sb.sb_date::date
),
end_of_month_trading_date AS (
    SELECT
        month_year_name,
        MAX(date) AS end_of_month_trading_date
    FROM d
    WHERE is_holiday = 'N'
    GROUP BY 1
)
SELECT
    d.*,
    CASE WHEN etd.end_of_month_trading_date IS NOT NULL THEN 1 ELSE 0 END AS is_eom_trading
FROM d
LEFT JOIN end_of_month_trading_date etd
ON etd.month_year_name = d.month_year_name AND etd.end_of_month_trading_date = d.date
