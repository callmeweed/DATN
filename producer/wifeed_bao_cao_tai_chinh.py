import logging

import numpy as np
import pandas as pd

import asyncio
import aiohttp

from common import utils
from storage.connection_utils import PSQLHook
from producer.wifeed_api import WifeedApi


def convert_to_snake_case(text):
    return text.lower().replace('-', '_')


class WiFeedBaoCaoTaiChinh:
    def __init__(self, sub_url, list_symbol, year, quarter, company_type=None):
        self.category = 'bctc'
        self.sub_url = sub_url
        self.list_symbol = list_symbol
        self.year = year
        self.quarter = quarter
        self.company_type = company_type
        self.wifeed_api = WifeedApi()

    async def async_get_bao_cao_tai_chinh(self):
        logging.info(f"List stock need get: {self.list_symbol}")
        semaphore = asyncio.Semaphore(20)
        for type in ['quarter', 'ttm']:
            data = []
            async with aiohttp.ClientSession() as session:
                tasks = [
                    self.wifeed_api.async_get_tai_chinh_doanh_nghiep(
                        session=session,
                        semaphore=semaphore,
                        type=type,
                        code=code,
                        category=self.category,
                        sub_url=self.sub_url,
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
                df_data["indexed_timestamp_"] = np.vectorize(utils.convert_date_to_first_quarter_month)(df_data['nam'], df_data['quy'])

                psql_hooks = PSQLHook()
                psql_hooks.append(
                    f"wifeed_bctc_{convert_to_snake_case(self.sub_url)}_{self.company_type}_raw",
                    df_data,
                    replace=True,
                    replace_index=['code', 'type', 'quy', 'nam']
                )
