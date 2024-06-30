import logging
from os import path

from jinja2 import Environment, FileSystemLoader
import pathlib
from airflow.models import Variable
from common.renders.text_render import *
from datetime import datetime


class TemplateLoader:
    @classmethod
    def time_and_date(cls, value):
        """
        datetime.(2023, 12, 23, 8, 45, 0) -> 08:45 ngày 23-12-2023
        """
        return datetime.strftime(value, '%H:%M ngày %d-%m-%Y')

    @classmethod
    def date(cls, value):
        """
        datetime.(2023, 12, 23) -> ngày 23-12-2023
        """
        return datetime.strftime(value, 'ngày %d-%m-%Y')

    @classmethod
    def abs(cls, value):
        """
        -1231 -> 1231
        """
        return str(abs(value))

    @classmethod
    def abs_num(cls, value):
        """
        -1231 -> 1231
        """
        return abs(value)

    @classmethod
    def f_vn_currency(cls, value):
        """
        -100000000 -> âm 100 triệu đồng
        1000000000000 -> 1.000 tỉ đồng
        """
        if value <= -1000000000:
            return "âm " + format_decimal(abs(value) / 1000000000, format='#,##0.##;-#', locale='en') + " tỷ đồng"
        elif -1000000000 < value <= -1000000:
            return "âm " + format_decimal(abs(value) / 1000000, format='#,##0.##;-#', locale='en') + " triệu đồng"
        elif -1000000 < value <= -1000:
            return "âm " + format_decimal(abs(value) / 1000, format='#,##0.##;-#', locale='en') + " nghìn đồng"
        elif -1000 < value < 0:
            return "âm " + format_decimal(abs(value), format='#,##0.##;-#', locale='en') + " đồng"
        elif 0 <= value < 1000:
            return format_decimal(abs(value), format='#,##0.##;-#', locale='en') + " đồng"
        elif 1000 <= value < 1000000:
            return format_decimal(abs(value) / 1000, format='#,##0.##;-#', locale='en') + " nghìn đồng"
        elif 1000000 <= value < 1000000000:
            return format_decimal(abs(value) / 1000000, format='#,##0.##;-#', locale='en') + " triệu đồng"
        return format_decimal(value / 1000000000, format='#,##0.##;-#', locale='en') + " tỷ đồng"

    @classmethod
    def f_vn_currency_with_sign(cls, value):
        """
        -100000000 -> âm 100 triệu đồng
        1000000000000 -> 1.000 tỉ đồng
        """
        if value <= -1000000000:
            return "-" + format_decimal(abs(value) / 1000000000, format='#,##0.##;-#', locale='en') + " tỷ đồng"
        elif -1000000000 < value <= -1000000:
            return "-" + format_decimal(abs(value) / 1000000, format='#,##0.##;-#', locale='en') + " triệu đồng"
        elif -1000000 < value <= -1000:
            return "-" + format_decimal(abs(value) / 1000, format='#,##0.##;-#', locale='en') + " nghìn đồng"
        elif -1000 < value < 0:
            return "-" + format_decimal(abs(value), format='#,##0.##;-#', locale='en') + " đồng"
        elif 0 <= value < 1000:
            return "+" + format_decimal(abs(value), format='#,##0.##;-#', locale='en') + " đồng"
        elif 1000 <= value < 1000000:
            return "+" + format_decimal(abs(value) / 1000, format='#,##0.##;-#', locale='en') + " nghìn đồng"
        elif 1000000 <= value < 1000000000:
            return "+" + format_decimal(abs(value) / 1000000, format='#,##0.##;-#', locale='en') + " triệu đồng"
        return "+" + format_decimal(value / 1000000000, format='#,##0.##;-#', locale='en') + " tỷ đồng"

    @classmethod
    def vn_currency(cls, value):
        """
        -100000000 -> 0,1 tỉ đồng
        1000000000000 -> 1.000 tỉ đồng
        """
        value = abs(value)
        if 0 <= value < 1000:
            return format_decimal(value, format='#,##0.##;-#', locale='en') + " đồng"
        elif 1000 <= value < 1000000:
            return format_decimal(value / 1000, format='#,##0.##;-#', locale='en') + " nghìn đồng"
        elif 1000000 <= value < 1000000000:
            return format_decimal(value / 1000000, format='#,##0.##;-#', locale='en') + " triệu đồng"
        return format_decimal(value / 1000000000, format='#,##0.##;-#', locale='en') + " tỷ đồng"

    @classmethod
    def f_percent(cls, value):
        """
        -0.693 -> 69,3%
        12.5 -> 1.250%
        """
        return format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"

    @classmethod
    def percent_with_sign(cls, value):
        if value >= 0:
            return "+" + format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"
        else:
            return "-" + format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"

    @classmethod
    def f_percent_with_change(cls, value):
        """
        -0.693 -> 69,3%
        12.5 -> 1.250%
        """
        if value < 0:
            return 'giảm ' + format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"
        if value == 0:
            return format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"
        else:
            return 'tăng ' + format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"

    @classmethod
    def f_percent_with_change_uppercase(cls, value):
        """
        -0.693 -> 69,3%
        12.5 -> 1.250%
        """
        if value < 0:
            return 'Giảm ' + format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"
        if value == 0:
            return format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"
        else:
            return 'Tăng ' + format_decimal(abs(value) * 100, format='#,##0.##;-#', locale='en') + "%"


    @classmethod
    def time_value(cls, value):
        """
        43 -> 43 giây
        150 -> 2.5 phút
        4200 -> 1.17 tiếng
        186400 -> 2.17 ngày
        """
        value = abs(value)
        if value < 60:
            return format_decimal(value, format='#,##0.##;-#', locale='en') + " giây"
        elif 60 < value < 3600:
            return format_decimal(value / 60, format='#,##0.##;-#', locale='en') + " phút"
        elif 3600 < value < 86400:
            return format_decimal(value / 3600, format='#,##0.##;-#', locale='en') + " giờ"
        else:
            return format_decimal(value / 86400, format='#,##0.##;-#', locale='en') + " ngày"


    def __init__(self, template_path="template"):
        root_path = Variable.get("ROOT_PATH")
        tmp_path = path.join(root_path, template_path)
        # logging.info(tmp_path)
        self.loader = FileSystemLoader(tmp_path)
        self.env = Environment(loader=self.loader)
        self.env.filters['f_vn_currency'] = self.f_vn_currency
        self.env.filters['f_vn_currency_with_sign'] = self.f_vn_currency_with_sign
        self.env.filters['vn_currency'] = self.vn_currency
        self.env.filters['f_percent'] = self.f_percent
        self.env.filters['percent_with_sign'] = self.percent_with_sign
        self.env.filters['time_and_date'] = self.time_and_date
        self.env.filters['date'] = self.date
        self.env.filters['abs'] = self.abs
        self.env.filters['abs_num'] = self.abs_num
        self.env.filters['time_value'] = self.time_value
        self.env.filters['f_percent_with_change'] = self.f_percent_with_change
        self.env.filters['f_percent_with_change_uppercase'] = self.f_percent_with_change_uppercase


    def get_template(self, template_name):
        return self.env.get_template(template_name)

    def render(self, template_name, **kwargs):
        template = self.get_template(template_name)
        return template.render(**kwargs)

