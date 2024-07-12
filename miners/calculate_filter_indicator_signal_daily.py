import logging
import talib as ta
from datetime import datetime
from .miner_base import MinerBase
from datetime import timedelta
from streams.data_stream_base import DataStreamBase
from storage.database_storage import DatabaseStorage
from dateutil.relativedelta import relativedelta

import warnings
import pandas as pd
from common import config
import numpy as np

from streams.stream_cfg import StreamCfg
warnings.simplefilter(action='ignore', category=FutureWarning)


class DnseIndicatorSignal(MinerBase):
    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    input_cfg = [
        StreamCfg(signal_name='public_market.fact_stock_ohlc_daily',
                  same_table_name=True,
                  timestep=timedelta(days=1),
                  version=1,
                  timestamp_field='txn_date',
                  symbol_field='symbol',
                  to_create=False,
                  storage_backend=DatabaseStorage),
        StreamCfg(signal_name='market.index_ohlc',
                  same_table_name=True,
                  timestep=timedelta(days=1),
                  version=1,
                  timestamp_field='time',
                  symbol_field='symbol',
                  to_create=False,
                  storage_backend=DatabaseStorage)
    ]

    output_cfg = StreamCfg(signal_name='indicator_signal',
                           timestep=timedelta(days=1),
                           version=1,
                           timestamp_field=config.SYSTEM_TIMESTAMP_COL,
                           symbol_field=config.SYSTEM_SYMBOL_COL,
                           storage_backend=DatabaseStorage,
                           stream_fields=[
                               ("rsi", "numeric"),
                               ("rsi_tag", "varchar(20)"),
                               ("macd", "numeric"),
                               ("macd_tag", "varchar(20)"),
                               ("macd_histogram", "numeric"),
                               ("macd_histogram_tag", "varchar(20)"),
                               ("bb_width", "numeric"),
                               ("bb_width_tag", "varchar(20)"),
                               ("symbol", "varchar(20)"),
                               ('close_price', "numeric")
                           ]
                           )

    def __init__(self, target_symbols):
        logging.info(f"Logging init: {target_symbols}")
        output_stream = DataStreamBase.from_config(self.output_cfg)

        input_streams = self.init_input_streams(self.input_cfg)
        logging.info(f"Logging input_streams: {input_streams}")

        if target_symbols is None:
            list_symbol = list(
                input_streams[
                    "public_market.fact_stock_ohlc_daily"
                ].backend.get_distinct_symbol('symbol')[
                    'symbol'
                ]
            )

            target_symbols = list_symbol
            list_index = list(
                input_streams[
                    "market.index_ohlc"
                ].backend.get_distinct_symbol('symbol')['symbol']
            )
            target_symbols.extend(list_index)

        logging.info(f"Done init: {input_streams}")

        super().__init__(target_symbols, output_stream, input_streams)

    def get_inputs(self, timestamp):
        logging.info(f"Logging timestamp: {timestamp}")
        stock_price_data = self.input_streams['public_market.fact_stock_ohlc_daily'].get_record_range_v2(
            included_min_timestamp=timestamp + relativedelta(days=-300),
            included_max_timestamp=datetime.strftime(timestamp, '%Y-%m-%d'),
            symbol_column='symbol',
            timestamp_column='txn_date',
            target_symbols=self.target_symbols,
            filter_query=None
        )

        index_price_data = self.input_streams['market.index_ohlc'].get_record_range_v2(
            included_min_timestamp=timestamp + relativedelta(days=-300),
            included_max_timestamp=datetime.strftime(timestamp, '%Y-%m-%d 23:59:59'),
            symbol_column='symbol',
            timestamp_column='time',
            target_symbols=self.target_symbols,
            filter_query="AND resolution = 'DAY'"
        )
        stock_price_data['symbol_'] = stock_price_data['symbol']
        index_price_data['symbol_'] = index_price_data['symbol']

        # convert colimn time to format yy-mm-dd 00:00:00
        index_price_data['time'] = index_price_data['time'].apply(lambda x: x.strftime('%Y-%m-%d 00:00:00'))
        print(index_price_data.loc[index_price_data['time'] == '2023-10-09 00:00:00'].to_string())
        return (stock_price_data,index_price_data)

    def process_per_symbol(self, inputs, symbol, timestamp):
        (stock_price_data,index_price_data) = inputs



        stock_price_data.sort_values(by=['txn_date'], inplace=True, ascending=True)
        index_price_data.sort_values(by=['time'], inplace=True, ascending=True)


        if symbol in ['HNX30','VNXALLSHARE','UPCOM','VN30','VNINDEX','HNX']:
            if len(index_price_data) < 14:
                logging.info('Not enough data for symbol: {}'.format(symbol))
                return
            else:
                close_price = index_price_data['close']

        else:
            if len(stock_price_data) < 14 :
                logging.info('Not enough data for symbol: {}'.format(symbol))
                return
            else:
                # get 3 type price of symbol
                close_price = stock_price_data['close_price']

        #get new price
        try:
            nearest_close_price = close_price.iloc[-1]
        except:
            print('EROR for symbol: {}'.format(symbol))
            return

        rsi_all = ta.RSI(close_price,timeperiod=14)
        rsi = rsi_all.iloc[-1]
        if rsi > 70:
            rsi_tag = 'mua'
        elif rsi < 30:
            rsi_tag = 'ban'
        else:
            rsi_tag = "trung_tinh"


        macd, signal, hist = ta.MACD(close_price, fastperiod=12, slowperiod=26, signalperiod=9)

        last_macd = macd.iloc[-1]
        last_signal = signal.iloc[-1]
        if last_macd > last_signal:
            macd_tag = "mua"
        elif last_macd < last_signal:
            macd_tag = "ban"
        else:
            macd_tag = "trung_tinh"

        macd_ytd = macd.iloc[-2]
        macd_histogram = last_macd - last_signal

        if macd_histogram > 0 and macd_ytd < last_macd:
            macd_histogram_tag = 'mua'
        elif macd_histogram < 0 and macd_ytd > last_macd:
            macd_histogram_tag = 'ban'
        else:
            macd_histogram_tag = 'trung_tinh'


        upper, middle, lower = ta.BBANDS(close_price, timeperiod=20, nbdevup=2, nbdevdn=2)


        bb_width = (upper.iloc[-1] - lower.iloc[-1]) / middle.iloc[-1]

        bb_width_ytd = (upper.iloc[-2] - lower.iloc[-2]) / middle.iloc[-2]
        if close_price.iloc[-2] < close_price.iloc[-1] and bb_width_ytd < bb_width:
            bb_width_tag = 'mua'
        elif close_price.iloc[-2] > close_price.iloc[-1] and bb_width_ytd > bb_width:
            bb_width_tag = 'ban'
        else:
            bb_width_tag = 'trung_tinh'



        out_dict = {}
        out_dict[config.SYSTEM_SYMBOL_COL] = symbol
        out_dict[config.SYSTEM_TIMESTAMP_COL] = timestamp
        out_dict['rsi'] = rsi
        out_dict['rsi_tag'] = rsi_tag
        out_dict['macd_histogram'] = macd_histogram
        out_dict['macd_histogram_tag'] = macd_histogram_tag
        out_dict['macd'] = last_macd
        out_dict['macd_tag'] = macd_tag
        out_dict['bb_width'] = bb_width
        out_dict['bb_width_tag'] = bb_width_tag
        out_dict['symbol'] = symbol

        out_dict['close_price'] = nearest_close_price


        return pd.DataFrame([out_dict])


# def main():
#     symbol = ['HPG']
#     time = datetime(2023 , 10 , 8)
#     obj = DnseIndicatorSignal(symbol)
#     obj.process_and_save(time)
#
# if __name__ == '__main__':
#     main()