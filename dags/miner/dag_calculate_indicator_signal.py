import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from miners.calculate_filter_indicator_signal_daily import DnseIndicatorSignal
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

dag_name = "miners_calculate_indicator_signal_calculation_daily"

default_args = {
    "owner": "DSAI Team",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1, 0, tzinfo=pendulum.timezone('Asia/Ho_Chi_Minh')),
    "email": ["kienvu@dnse.com.vn"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": 60,
}
with DAG(
        dag_id=dag_name,
        schedule_interval='0 17 * * *',
        default_args=default_args,
        catchup=False,
        max_active_runs=5,
        max_active_tasks=5,
        tags=[""]

) as dag:
    def indicator_signal_calculation(params):
        context = get_current_context()
        logging.info("context: ")
        logging.info(context)

        exec_date = context["data_interval_end"]
        runtime_date_format = datetime.strptime(datetime.strftime(exec_date, "%Y-%m-%d"), "%Y-%m-%d")

        running_date = params.get("running_date", None)
        if running_date is not None:
            runtime_date_format = datetime.strptime(running_date, "%Y-%m-%d")

        logging.info(f"LOG runtime_date_format: {runtime_date_format}")

        obj = DnseIndicatorSignal(target_symbols=None)
        obj.process_and_save(runtime_date_format)

        return

    indicator_signal_calculation = PythonOperator(
        task_id="indicator_signal_calculation",
        python_callable=indicator_signal_calculation,
    )

