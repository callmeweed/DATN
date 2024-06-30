import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, get_current_context
import logging
from dateutil.relativedelta import relativedelta
# from common.airflow_to_mattermost import send_message_to_mattermost
from miners.calculate_filter_bien_dong_gia_va_khoi_luong_daily import CalculateFilterBienDongGiaVaKhoiLuongDaily

dag_name = "miners_calculate_bien_dong_gia_va_khoi_luong_daily"

default_args = {
    "owner": "DSAI Team",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1, 0),
    "email": ["hieunguyen2@dnse.com.vn"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": 60,
    # 'on_failure_callback': send_message_to_mattermost(),
}

with (DAG(
        dag_id=dag_name,
        schedule_interval="30 22 * * *",
        default_args=default_args,
        catchup=False,
        max_active_runs=4,
        max_active_tasks=1,
        tags=[""]
) as dag):
    # Logic

    def get_running_date(params):
        context = get_current_context()
        logging.info("context: ")
        logging.info(context)

        # exec_date = context["logical_date"]
        exec_date = context["execution_date"]
        runtime_date_format = datetime.strptime(datetime.strftime(exec_date, "%Y-%m-%d"),
                                                "%Y-%m-%d") + relativedelta(days=1)
        running_date = params.get("running_date")
        start_date = params.get("start_date")
        end_date = params.get("end_date")

        if running_date is not None:
            runtime_date_format = datetime.strptime(running_date, "%Y-%m-%d")

        return runtime_date_format, start_date, end_date

    def logic_calculate_bien_dong_gia_va_khoi_luong_daily(params):
        runtime_date_format, start_date, end_date = get_running_date(params)

        if (start_date is not None) and (end_date is not None):
            logging.info(f"Process data from start_date: {start_date} - end_date: {end_date}")
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

            # Tạo dãy thời gian từ start_date đến end_date
            date_range = list(pd.date_range(start=start_date, end=end_date, freq='D'))

            date_range = sorted(date_range, reverse=True)

            for day in date_range:
                logic_calculate = CalculateFilterBienDongGiaVaKhoiLuongDaily(target_symbols=None)
                logic_calculate.process_and_save(day)
                logging.info(f"Processed day: {day}")
            return

        logic_calculate = CalculateFilterBienDongGiaVaKhoiLuongDaily(target_symbols=None)
        logic_calculate.process_and_save(runtime_date_format)
        return

    # Task
    calculate_bien_dong_gia_va_khoi_luong_daily = PythonOperator(
        task_id='calculate_bien_dong_gia_va_khoi_luong_daily',
        python_callable=logic_calculate_bien_dong_gia_va_khoi_luong_daily,
    )
