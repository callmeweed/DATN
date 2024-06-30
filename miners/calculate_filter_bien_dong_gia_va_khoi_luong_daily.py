import logging
from miners.miner_base import MinerBase
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from streams.data_stream_base import DataStreamBase
from storage.database_storage import DatabaseStorage
from common import config
from streams.stream_cfg import StreamCfg
from dateutil.relativedelta import relativedelta

pd.options.display.float_format = '{:.5f}'.format


class CalculateFilterBienDongGiaVaKhoiLuongDaily(MinerBase):
    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    input_cfg = [
        StreamCfg(signal_name='market.stock_ohlc_days',
                  same_table_name=True,
                  timestep=timedelta(days=1),
                  version=1,
                  timestamp_field='time',
                  symbol_field='symbol',
                  to_create=False,
                  storage_backend=DatabaseStorage),
        StreamCfg(signal_name='market.stock_infos_history',
                  same_table_name=True,
                  timestep=timedelta(days=1),
                  version=1,
                  timestamp_field='trading_time',
                  symbol_field='symbol',
                  to_create=False,
                  storage_backend=DatabaseStorage),
    ]

    # out put stream
    output_cfg = StreamCfg(
        signal_name='calculate_filter_bien_dong_gia_va_khoi_luong_daily',
        timestep=timedelta(days=1),
        timestamp_field=config.SYSTEM_TIMESTAMP_COL,
        version=1,
        symbol_field=config.SYSTEM_SYMBOL_COL,
        stream_fields=[
            ("symbol", "varchar(20)"),
            ("trading_time", "timestamp"),
            ("accumulated_val", "numeric"),
            ("accumulated_val_1w", "numeric"),
            ("accumulated_val_2w", "numeric"),
            ("accumulated_val_1m", "numeric"),

            ("close_price", "numeric"),
            ("sma_5", "numeric"),
            ("sma_10", "numeric"),
            ("sma_20", "numeric"),
            ("sma_50", "numeric"),
            ("sma_100", "numeric"),

            ("sma_5_tag", "varchar(40)"),
            ("sma_10_tag", "varchar(40)"),
            ("sma_20_tag", "varchar(40)"),
            ("sma_50_tag", "varchar(40)"),
            ("sma_100_tag", "varchar(40)"),

        ],
        storage_backend=DatabaseStorage
    )

    def __init__(self, target_symbols):
        output_stream = DataStreamBase.from_config(self.output_cfg)

        input_streams = self.init_input_streams(self.input_cfg)

        if target_symbols is None:
            list_symbol = list(
                input_streams[
                    "market.stock_ohlc_days"
                ].backend.get_distinct_symbol('symbol')[
                    'symbol'
                ]
            )
            target_symbols = list_symbol

        logging.info(f"Done init: {input_streams}")

        super().__init__(target_symbols, output_stream, input_streams)

    def get_inputs(self, timestamp):
        logging.info(f"Logging timestamp: {timestamp}")

        filter_query = f"AND length(symbol) = 3"
        stock_price_data = self.input_streams['market.stock_ohlc_days'].get_record_range_v2(
            included_min_timestamp=timestamp + relativedelta(days=-160),
            included_max_timestamp=datetime.strftime(timestamp, '%Y-%m-%dT23:59:59+07:00'),
            symbol_column='symbol',
            timestamp_column='time',
            target_symbols=self.target_symbols,
            filter_query=filter_query
        )
        stock_price_data['symbol_'] = stock_price_data['symbol']
        stock_price_data['txn_time'] = pd.to_datetime(stock_price_data['time']).dt.strftime('%Y-%m-%d')

        stock_accumulated_data = self.input_streams['market.stock_infos_history'].get_record_range_v2(
            included_min_timestamp=timestamp + relativedelta(days=-160),
            included_max_timestamp=datetime.strftime(timestamp, '%Y-%m-%dT23:59:59+07:00'),
            symbol_column='symbol',
            timestamp_column='trading_time',
            target_symbols=self.target_symbols,
            filter_query=filter_query
        )
        stock_accumulated_data['symbol_'] = stock_accumulated_data['symbol']
        stock_accumulated_data['txn_time'] = pd.to_datetime(stock_accumulated_data['trading_time']).dt.strftime('%Y-%m-%d')

        data_merge = pd.merge(stock_accumulated_data, stock_price_data, on=['symbol_', 'txn_time'], how='left')
        logging.info(f'min timestamp - {min(data_merge["txn_time"])} ==== max timestamp - {max(data_merge["txn_time"])}')
        return data_merge, stock_price_data, stock_accumulated_data

    def process_per_symbol(self, inputs, symbol, timestamp):
        logging.info(f"Processing symbol: {symbol} at timestamp: {timestamp}")
        data, stock_price_data, stock_accumulated_data = inputs

        if len(data) == 0:
            logging.info('Not enough data for symbol: {}'.format(symbol))
            return

        data.sort_values(by=['txn_time'], inplace=True, ascending=True)

        if datetime.strftime(timestamp, '%Y-%m-%d') != data['txn_time'].iloc[-1]:
            logging.info(f"Not enough data for symbol: {symbol} - timestamp: {datetime.strftime(timestamp, '%Y-%m-%d')}"
                         f" - timestamp_query: {data['txn_time'].iloc[-1]}")
            return

        data['accumulated_val'] = data['accumulated_val'].fillna(0)

        # accumulated_val 1 week, 2 week, 1 month ago.
        accumulated_val_1w = data['accumulated_val'].rolling(5).mean().iloc[-1]
        accumulated_val_2w = data['accumulated_val'].rolling(10).mean().iloc[-1]
        accumulated_val_1m = data['accumulated_val'].rolling(20).mean().iloc[-1]

        # sma 5, 10, 20, 50, 100
        sma_5 = data['close'].rolling(5).mean().iloc[-1]
        sma_10 = data['close'].rolling(10).mean().iloc[-1]
        sma_20 = data['close'].rolling(20).mean().iloc[-1]
        sma_50 = data['close'].rolling(50).mean().iloc[-1]
        sma_100 = data['close'].rolling(100).mean().iloc[-1]

        if np.isnan(sma_5):
            sma_5_tag = None
        else:
            sma_5_tag = 'Giá cắt lên SMA(5)' if data['close'].iloc[-1] > sma_5 else 'Giá cắt xuống SMA(5)'

        if np.isnan(sma_10):
            sma_10_tag = None
        else:
            sma_10_tag = 'Giá cắt lên SMA(10)' if data['close'].iloc[-1] > sma_10 else 'Giá cắt xuống SMA(10)'

        if np.isnan(sma_20):
            sma_20_tag = None
        else:
            sma_20_tag = 'Giá cắt lên SMA(20)' if data['close'].iloc[-1] > sma_20 else 'Giá cắt xuống SMA(20)'

        if np.isnan(sma_50):
            sma_50_tag = None
        else:
            sma_50_tag = 'Giá cắt lên SMA(50)' if data['close'].iloc[-1] > sma_50 else 'Giá cắt xuống SMA(50)'

        if np.isnan(sma_100):
            sma_100_tag = None
        else:
            sma_100_tag = 'Giá cắt lên SMA(100)' if data['close'].iloc[-1] > sma_100 else 'Giá cắt xuống SMA(100)'

        out_dict = {}
        out_dict[config.SYSTEM_SYMBOL_COL] = symbol
        out_dict[config.SYSTEM_TIMESTAMP_COL] = data['txn_time'].iloc[-1]
        out_dict['trading_time'] = data['txn_time'].iloc[-1]
        out_dict['symbol'] = symbol

        out_dict['accumulated_val'] = data['accumulated_val'].iloc[-1]
        out_dict['accumulated_val_1w'] = None if np.isnan(accumulated_val_1w) else accumulated_val_1w
        out_dict['accumulated_val_2w'] = None if np.isnan(accumulated_val_2w) else accumulated_val_2w
        out_dict['accumulated_val_1m'] = None if np.isnan(accumulated_val_1m) else accumulated_val_1m

        out_dict['close_price'] = None if np.isnan(data['close'].iloc[-1]) else data['close'].iloc[-1]
        out_dict['sma_5'] = None if np.isnan(sma_5) else sma_5
        out_dict['sma_10'] = None if np.isnan(sma_10) else sma_10
        out_dict['sma_20'] = None if np.isnan(sma_20) else sma_20
        out_dict['sma_50'] = None if np.isnan(sma_50) else sma_50
        out_dict['sma_100'] = None if np.isnan(sma_100) else sma_100

        out_dict['sma_5_tag'] = sma_5_tag
        out_dict['sma_10_tag'] = sma_10_tag
        out_dict['sma_20_tag'] = sma_20_tag
        out_dict['sma_50_tag'] = sma_50_tag
        out_dict['sma_100_tag'] = sma_100_tag

        return pd.DataFrame([out_dict])

# if __name__ ==  "__main__":
#     miner = CalculateFilterBienDongGiaVaKhoiLuongDaily(target_symbols=['VCB'])
#     runtime_date_format = datetime.strptime('2024-03-02 00:00:00', "%Y-%m-%d")
#     miner.process_and_save(timestamp=runtime_date_format)
