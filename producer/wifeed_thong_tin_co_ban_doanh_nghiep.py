import logging

import numpy as np
import pandas as pd
from storage.connection_utils import PSQLHook
from producer.wifeed_api import WifeedApi

class WiFeedThongTinDoanhNghiep:
    def __init__(self):
        self.wifeed_api = WifeedApi()
    def get_thong_tin_doanh_nghiep_v2(self):
        list_stock_active = self.wifeed_api.get_all_code()

        df_output = pd.DataFrame()

        for code in list_stock_active:

            # data = []
            tmp_data = self.wifeed_api.get_thong_tin_co_ban_doanh_nghiep_v2(
                code=code
            )

            if tmp_data and len(tmp_data) > 0:
                df_data = pd.DataFrame(tmp_data)
                df_data = df_data.rename(columns=str.lower)
                df_data['crawled_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')

                df_output = pd.concat([df_output, df_data], ignore_index=True)
        if len(df_output.index) > 0:
            df_output['logo_dnse_cdn'] = None
            df_output = df_output.replace({np.nan: None})
            df_output = df_output.drop('donvikiemtoan', axis=1)
            df_output['symbol_'] = df_output['mack']
            df_output['indexed_timestamp_'] = df_output['crawled_date']

            psql_hooks = PSQLHook()
            psql_hooks.truncate_table(table_name='wifeed_bctc_ttcb_doanh_nghiep_v2_raw')
            psql_hooks.append(
                f"wifeed_bctc_ttcb_doanh_nghiep_v2_raw",
                df_output,
                replace=True,
                replace_index=['mack']
            )
    def get_danh_sach_ma_chung_khoan_raw(self):
        df_output = pd.DataFrame()
        tmp_data = self.wifeed_api.get_danh_sach_ma_chung_khoan()

        if tmp_data and len(tmp_data) > 0:
            df_output = pd.DataFrame(tmp_data)

        if len(df_output.index) > 0:
            df_output = df_output.replace({np.nan: None})
            df_output['symbol_'] = df_output['code']
            df_output['indexed_timestamp_'] = pd.to_datetime('today').strftime('%Y-%m-%d')
            df_output['crawled_date'] = pd.to_datetime("today").date()
            psql_hooks = PSQLHook()
            psql_hooks.truncate_table(table_name='wifeed_danh_sach_ma_chung_khoan_raw')
            psql_hooks.append(
                f"wifeed_danh_sach_ma_chung_khoan_raw",
                df_output,
                replace=True,
                replace_index=['code']
            )
