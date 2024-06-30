import hashlib
import logging
import os
import sys
from datetime import datetime

import requests

import pandas as pd
from airflow.models import Variable
from airflow.operators.python import get_current_context
from pandas import DataFrame


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


def no_accent_vietnamese_col(df, col):
    s = df[col]
    s = s.replace(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', regex=True)
    s = s.replace(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', regex=True)
    s = s.replace(r'[èéẹẻẽêềếệểễ]', 'e', regex=True)
    s = s.replace(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', regex=True)
    s = s.replace(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', regex=True)
    s = s.replace(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', regex=True)
    s = s.replace(r'[ìíịỉĩ]', 'i', regex=True)
    s = s.replace(r'[ÌÍỊỈĨ]', 'I', regex=True)
    s = s.replace(r'[ùúụủũưừứựửữ]', 'u', regex=True)
    s = s.replace(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', regex=True)
    s = s.replace(r'[ỳýỵỷỹ]', 'y', regex=True)
    s = s.replace(r'[ỲÝỴỶỸ]', 'Y', regex=True)
    s = s.replace(r'[Đ]', 'D', regex=True)
    s = s.replace(r'[đ]', 'd', regex=True)
    df[col] = s
    return df


def convert_df_data_to_snake_case(df, col):
    s = df[col]
    s = s.str.lower()
    s = s.str.replace(' ', '_')
    df[col] = s
    return df


def get_exec_date() -> str:
    """
    get execution date
    """
    context = get_current_context()
    return context["data_interval_end"].strftime("%Y-%m-%d")


def gen_disclamer_smg():
    return '<i>*Nội dung bài viết trên chỉ mang tính chất tham khảo. Vui lòng đọc kỹ “Thông tin quan trọng cần biết” của trang web <a href="https://cdn.dnse.com.vn/dnse-assets/CRM/Dieu_khoan_Dieu_kien_DNSE_04082023.pdf">tại đây</a>'


def string_to_unique_bigint(input_string):
    # Hash the input string using SHA-256
    hash_object = hashlib.sha256(input_string.encode())
    hex_digest = hash_object.hexdigest()

    # Convert the first 16 characters of the hash to an integer
    int_value = int(hex_digest[:16], 16)

    # Ensure the value fits within a signed 64-bit integer range
    int_value %= sys.maxsize

    return int_value


def remove_noise_signal(input_df: DataFrame, signal_column: str = 'signal') -> DataFrame:
    """
    Remove noise signal
    :param input_df: input dataframe
    :param signal_column: signal column name
    :return:
    """

    df = input_df.reset_index()

    # Xóa nhiễu 2 ngày ngay sau đó ko có tín hiệu
    for i in range(len(df) - 1):
        # Kiểm tra xem giá trị của cột data là BUY hay không
        if df.loc[i, signal_column] == "BUY":
            # Cập nhật giá trị của cột data trong 2 ngày tiếp theo thành None
            for j in range(i + 1, i + 3):
                df.loc[j, signal_column] = None

    # Set gía trị pair = True cho các signal BUY
    df['pair'] = df[signal_column] == 'BUY'

    # Tìm list index các giá trị
    buy_positions = df[df[signal_column] == 'BUY'].index
    sell_positions = df[df[signal_column] == 'SELL'].index

    # Duyệt qua tất cả các vị trí của tín hiệu BUY
    for buy_position in buy_positions:

        # Tìm vị trí của tín hiệu SELL đầu tiên sau tín hiệu BUY
        sell_after_buy = sell_positions[sell_positions > buy_position].min()

        # Nếu tìm thấy, cập nhật cột 'pair' thành True cho dòng SELL đó
        if sell_after_buy is not None and buy_position != len(df) - 1:
            df.at[sell_after_buy, 'pair'] = True

        # Tìm vị trí của tín hiệu BUY sau tín hiệu BUY
        buy_between = buy_positions[(buy_positions > buy_position) & (buy_positions < sell_after_buy)].min()
        if buy_between is not None and type(buy_between) != float:
            df.at[buy_between, 'pair'] = False

    df = df.dropna(subset=['indexed_timestamp_'])

    if df[signal_column].values[-1] == 'BUY' and sell_positions[-1] < buy_positions[-2]:
        df.loc[df.index[-1], 'pair'] = False

    # df["signal"] = df["pair"].where(lambda x: x == False, None)
    df.loc[df['pair'] == False, signal_column] = None
    df = df.drop(columns=['pair'])
    return df


def generate_message_ensa(
        symbol,
        txn_hour,
        txn_date,
        bot_name,
        bot_version,
        text_duration,
        trade_duration,
        buy_price,
        tp_price,
        sl_price,
        win_rate_year,
        win_rate_half_year
) -> str:
    message = (f"Với mã {symbol}, chiến lược {bot_name} {bot_version} tại thời điểm {txn_hour} phiên {txn_date}, "
               f"khuyến nghị giá mua {buy_price}, giá chốt lời {tp_price}, giá cắt lỗ {sl_price}. Thời gian nắm giữ {text_duration} {trade_duration} phiên. "
               f"Kết quả backtest cho thấy, trong vòng 1 năm qua tỉ lệ thắng đạt {win_rate_year}%, nửa năm đạt {win_rate_half_year}%.")
    return message


def format_decimal_float(value):
    if value is None:
        return None
    return '{:.2f}'.format(value)


def round_to(n, precision):
    correction = 0.5 if n >= 0 else -0.5
    return int(n / precision + correction) * precision


def round_to_05(n):
    return round_to(n, 0.05)


def custom_round(number):
    """
    Custom output giá chứng khoán
    """
    if number < 10:
        return '{:.2f}'.format(round(number, 2))
    elif number <= 50:
        return '{:.2f}'.format(round_to_05(number))
    else:
        return '{:.1f}'.format(round(number, 1))

def get_next_trading_date():
    url = os.getenv('HNX_DERIVATIVE_GET_TRADING_DATE_URL')
    admin_token = os.getenv('HNX_DERIVATIVE_DNSE_JWT_ADMIN_TOKEN')

    payload = {}
    headers = {
        'authorization': f'Bearer {admin_token}'
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
    except Exception as e:
        print(e)
        print('ERROR WHEN GET TRADING DATE')
        return False

    data = response.json()
    print('current trading date: ', data['currentTradingDate'])
    print('next trading date: ', data['nextTradingDate'])
    return datetime.strptime(data['nextTradingDate'], '%Y-%m-%d')
