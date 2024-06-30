import datetime
import logging

import numpy as np
import pandas as pd
from airflow.models import Variable

import asyncio
import aiohttp

from pendulum import now
from datetime import timedelta
from common import utils, config
from storage.connection_utils import PSQLHook
from producer.wifeed_api import WifeedApi


def convert_to_snake_case(text):
    return text.lower().replace('-', '_')


class WiFeedChiSoTaiChinh:
    def __init__(self, list_symbol, year=None, quarter=None, company_type=None):
        self.list_symbol = list_symbol
        self.category = 'chi-so-tai-chinh'
        self.year = year
        self.quarter = quarter
        self.company_type = company_type
        self.wifeed_api = WifeedApi()
        if company_type:
            self.variable_key_name = f"LIST_{self.company_type.upper()}_HAVE_BCTC" if self.quarter != 0 else f"LIST_{self.company_type.upper()}_HAVE_BCTC_NAM"

    async def async_get_chi_so_tai_chinh(self):
        logging.info(f"List stock active: {self.list_symbol}")
        semaphore = asyncio.Semaphore(20)
        for type in ['quarter', 'ttm', 'year']:
            data = []
            if type == 'year' and (self.quarter != 0 and self.quarter != None):
                continue
            async with aiohttp.ClientSession() as session:
                tasks = [
                    self.wifeed_api.async_get_tai_chinh_doanh_nghiep(
                        session=session,
                        semaphore=semaphore,
                        type=type,
                        code=code,
                        category=self.category,
                        year=self.year,
                        quarter=self.quarter
                    ) for code in self.list_symbol
                ]
                responses = await asyncio.gather(*tasks)
                for tmp_data in responses:
                    if tmp_data and len(tmp_data) > 0:
                        data.extend(tmp_data)
            logging.info(f"Symbol list length: {len(self.list_symbol)}")
            logging.info(f"Crawled length: {len(data)}")
            if data and len(data) > 0:
                df_data = pd.DataFrame(data)
                df_data = df_data.rename(columns=str.lower)
                df_data['type'] = type
                df_data = df_data.replace({np.nan: None})

                df_data['symbol_'] = df_data['code']
                df_data["indexed_timestamp_"] = np.vectorize(utils.convert_date_to_first_quarter_month)(
                    df_data['nam'], df_data['quy'])

                psql_hooks = PSQLHook()
                psql_hooks.append(
                    f"wifeed_bctc_{convert_to_snake_case(self.category)}_{type}_raw",
                    df_data,
                    replace=True,
                    replace_index=['code', 'type', 'quy', 'nam']
                )

    async def async_get_chi_so_tai_chinh_v2(self):
        logging.info(f"List stock active: {self.list_symbol}")
        semaphore = asyncio.Semaphore(20)
        for type in ['quarter', 'ttm', 'year']:
            data = []
            if type == 'year' and (self.quarter != 0 and self.quarter != None):
                continue
            async with aiohttp.ClientSession() as session:
                tasks = [
                    self.wifeed_api.async_get_chi_so_tai_chinh_v2(
                        session=session,
                        semaphore=semaphore,
                        type=type,
                        code=code,
                        nam=self.year,
                        quy=self.quarter,
                    ) for code in self.list_symbol
                ]
                responses = await asyncio.gather(*tasks)
                for tmp_data in responses:
                    if tmp_data and len(tmp_data) > 0:
                        data.extend(tmp_data)
            logging.info(f"Symbol list length: {len(self.list_symbol)}")
            logging.info(f"Crawled length: {len(data)}")
            if data and len(data) > 0:
                df_data = pd.DataFrame(data)
                df_data = df_data.rename(columns=str.lower)
                df_data['type'] = type
                df_data = df_data.replace({np.nan: None})

                df_data['symbol_'] = df_data['mack']
                df_data["indexed_timestamp_"] = np.vectorize(utils.convert_date_to_first_quarter_month)(df_data['nam'],
                                                                                                        df_data['quy'])

                psql_hooks = PSQLHook()
                psql_hooks.append(
                    f"wifeed_bctc_{convert_to_snake_case(self.category)}_{self.company_type}_{type}_v2_raw",
                    df_data,
                    replace=True,
                    replace_index=['mack', 'type', 'quy', 'nam']
                )
