from typing import List, Dict

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from pendulum import now

from common.utils import get_env_var
import logging
from requests.adapters import Retry

from storage.connection_utils import PSQLHook


def is_response_empty(response):
    return response.status_code == 200 and len(response.content) == 0


class WifeedApi:
    def __init__(self, pool_connections=10):
        self.session = requests.Session()
        self.domain = get_env_var('WIFEED_BASE_URL')
        self.api_key = get_env_var('WIFEED_API_KEY')

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = requests.adapters.HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=pool_connections,
            pool_maxsize=pool_connections
        )
        self.session.mount('https://', adapter)

    def get_all_code(self):
        entrade_path = get_env_var('ENTRADE_SERVICE')

        path = f'{entrade_path}/chart-api/symbols?type=stock'
        logging.info(f"Request to {path}")
        request = self.session.get(path)
        request.raise_for_status()
        symbols = request.json()['symbols']
        list_symbol = [symbol for symbol in symbols if len(symbol) == 3]

        return list_symbol

    def get_all_code_by_company_type(self, company_type):
        mapping_company_type = {
            'doanh_nghiep_san_xuat': 1,
            'ngan_hang': 2,
            'bao_hiem': 3,
            'chung_khoan': 4,
        }
        loaidn = mapping_company_type[company_type]
        psql_hooks = PSQLHook()
        str_query = f"""
            SELECT DISTINCT symbol_ FROM wifeed_danh_sach_ma_chung_khoan
            WHERE loaidn = {loaidn} AND san != 'DELISTING'
        """
        df = psql_hooks.execute_query_result_dataframe(str_query)
        return df

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def async_get_tai_chinh_doanh_nghiep(self, session, semaphore, code, category, type=None, sub_url=None, year=now().year,
                                   quarter=now().quarter - 1):
        """
        Get tai chinh doanh nghiep

        :param type: type of company: 'quarter' or 'ttm'
        :param code: company code
        :param category: category of api: bctc or chi-so-tai-chinh or ke-hoach-kinh-doanh
        :param sub_url: sub url only bctc in: can-doi-ke-toan, ket-qua-kinh-doanh, luu-chuyen-tien-te, thuyet-minh,
        :param year: year of financial report
        :param quarter: quarter of financial report
        :return:
        """
        path = f"{self.domain}/tai-chinh-doanh-nghiep/{category}"
        if category == "bctc":
            path = f"{self.domain}/tai-chinh-doanh-nghiep/{category}/{sub_url}"
        params = {
            "type": type,
            "code": code,
            "quy": quarter if quarter in [0, 1, 2, 3, 4] else "",
            "nam": year if year else "",
            "apikey": self.api_key
        }
        logging.info(f"Request to path {path} with params: {params}")
        async with semaphore:
            async with session.get(path, params=params) as request:
                if request.status != 200:
                    logging.info(f"Error {request.status}")
                    return []
                return await request.json()

    def get_thong_tin_co_ban_doanh_nghiep_v2(self, code):
        """
        Get thong tin co phieu

        :param code: company code
        :return:
        """
        path = f"{self.domain}/thong-tin-co-phieu/v2/thong-tin-co-ban"
        logging.info(f"Request to {path} with code: {code}")

        request = self.session.get(path, params={
            "code": code,
            "apikey": self.api_key
        })
        request.raise_for_status()
        return request.json()

    def get_danh_sach_ma_chung_khoan(self):
        """
        Get danh sach ma chung khoan

        :return:
        """
        path = f"{self.domain}/thong-tin-co-phieu/danh-sach-ma-chung-khoan"
        logging.info(f"Request to {path}")

        request = self.session.get(path, params={
            "apikey": self.api_key
        })
        request.raise_for_status()
        return request.json()['data']

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def async_get_chi_so_tai_chinh_v2(self, session, semaphore, code, type=None, nam=None, quy=None, from_date=None, to_date=None):
        """
        Get chi so tai chinh v2

        :param type: type of company: 'quarter' or 'ttm' or 'year' or 'daily'
        :param code: company code
        :param nam: year of financial report
        :param quy: quarter of financial report
        :return:
        """
        path = f"{self.domain}/tai-chinh-doanh-nghiep/v2/chi-so-tai-chinh"
        params = {
            "type": type,
            "code": code,
            "nam": nam if nam else "",
            "quy": quy if quy in [0, 1, 2, 3, 4] else "",
            "apikey": self.api_key
        }
        if type == 'daily' and from_date and to_date:
            params['from-date'] = from_date
            params['to-date'] = to_date
        logging.info(f"Request to path {path} with params: {params}")
        async with semaphore:
            async with session.get(path, params=params) as request:
                if request.status != 200:
                    logging.info(f"Error {request.status}")
                    return []
                return await request.json()
