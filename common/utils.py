import hashlib
import logging

import pandas as pd
from airflow.models import Variable

CATEGORY_WIFEED = {
    'doanh_nghiep_san_xuat': 'doanh_nghiep_san_xuat',
    'chung_khoan': 'chung_khoan',
    'bao_hiem': 'bao_hiem',
    'ngan_hang': 'ngan_hang',
}


def format_timedelta(t_delta, fmt):
    d = {"days": t_delta.days}
    d["hours"], rem = divmod(t_delta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


def generated_identified_name(signal_name, timestep, version):
    return "{}_{}_{}".format(signal_name, format_timedelta(timestep, '{days}d{hours}h{minutes}m'), version)


def get_env_var(key):
    try:
        value = Variable.get(key)
        return value
    except KeyError:
        logging.error(f"Cannot get key {key}")
        raise KeyError(f"Cannot get key {key}")


def convert_date_to_first_quarter_month(df_year_col, df_month_code_col):
    dict_month_code = {
        0: '01-01',
        1: '01-01',
        2: '04-01',
        3: '07-01',
        4: '10-01'
    }
    df_month_code_col = dict_month_code[df_month_code_col]
    df_year_col = str(df_year_col)
    date_str = df_year_col + '-' + df_month_code_col
    date_str = pd.to_datetime(date_str, format='%Y-%m-%d')
    return date_str


