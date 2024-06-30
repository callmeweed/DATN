
with t as (
    select
        *
        , row_number() over(partition by txn_date, symbol order by last_updated desc) as row_num -- sort theo thời gian update giá (điều chỉnh giá khi có sự kiện quyền)
    from {{ ref('stg_market__stock_ohlc_days') }}
),
 symbol_daily as (
select
    unique_id
    , txn_date
    , row_number() over(partition by symbol order by txn_date) as rank_day_by_symbol
    , date_part('week', txn_date) as week_name
    , date_part('year', txn_date) as year_name
    , symbol
    , match_quantity
    , open_price
    , high_price
    , low_price
    , close_price
from t
where row_num = 1
),
std20_week as ( --- Mục tiêu cần std của 20 phiên giao dịch cuối cùng
    select
        a.txn_date,
        a.symbol,
        case
            when a.rank_week <= 20 then null
            else stddev_pop(a.close_price) over(partition by a.symbol order by a.txn_date rows between 20 preceding and 1 preceding) end as std_l20w
    from
        (select
            txn_date,
            symbol,
            close_price,
            dense_rank() over( order by year_name, week_name) rank_week,
            row_number() over(partition by year_name, week_name, symbol order by txn_date desc) as rank_day_in_week_by_symbol
        from
            symbol_daily
        ) as a
    where a.rank_day_in_week_by_symbol = 1   --- Phiên giao dịch cuối cùng trong tuần
),
all_metric as (
    select
        unique_id
        , symbol_daily.txn_date
        , rank_day_by_symbol
        , week_name
        , year_name
        , symbol_daily.symbol
        , match_quantity
        , open_price
        , high_price
        , low_price
        , close_price
        , min(close_price) over(partition by symbol_daily.symbol order by symbol_daily.txn_date rows between 29 preceding and current row) as min_close_price_l30d
        , case when rank_day_by_symbol < 20 then null else stddev_pop(symbol_daily.close_price) over(partition by symbol_daily.symbol order by symbol_daily.txn_date  rows between 19 preceding and current row) end as std_l20d
        , stdw.std_l20w
    from symbol_daily
    left join
        std20_week as stdw
    on date_trunc('week', symbol_daily.txn_date) = date_trunc('week', stdw.txn_date) and symbol_daily.symbol = stdw.symbol
)
    select
        *
    from all_metric