import logging
import os
from os import path

import pendulum
from airflow import DAG
from datetime import datetime

from airflow.models import Variable
from airflow.operators.python import PythonOperator

from generators.generator import Generator
import yaml

from storage.connection_utils import PSQLHook

dag_name = "auto_create_table"

default_args = {
    "owner": "DSAI Team",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1, 0, tzinfo=pendulum.timezone('Asia/Ho_Chi_Minh')),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": 60,
}

with DAG(
        dag_id=dag_name,
        schedule_interval=None,
        default_args=default_args,
        catchup=True,
        max_active_runs=1,
        concurrency=10,
        tags=[""],
) as dag:
    def logic_create_table():
        g = Generator(storage='')
        all_config = g.get_all_miner_cfg(g.module)
        g.create_streams(all_config)
        return

    def logic_create_statics_table():
        psql = PSQLHook()
        root_path = Variable.get("ROOT_PATH")
        schema_list_path = path.join(root_path, 'config')
        schema_list_path = [os.path.abspath(os.path.join(schema_list_path, p)) for p in os.listdir(schema_list_path) if os.path.isfile(os.path.join(schema_list_path, p))]
        logging.info("Total: %s files", len(schema_list_path))
        for schema_path in schema_list_path:
            logging.info(schema_path)
            with open(schema_path) as file_open:
                data = yaml.safe_load(file_open)
                data = data["schema"]

                table_name = data["table_name"]
                col_list = []
                col_type_list = []

                for col in data["columns"]:
                    col_list.append(col["name"])
                    col_type_list.append(col["type"])

                primary_list = data.get("primary", [])
                require_primary = data.get("require_primary", True)
                columns_list = list(zip(col_list, col_type_list))
                psql.create_table(
                    table_name=table_name,
                    columns_list=columns_list,
                    extend_column=True,
                    primary_list=primary_list,
                    require_primary=require_primary
                )

    miners_task = PythonOperator(
        task_id='auto_create_miners_table',
        python_callable=logic_create_table,
    )

    statics_task = PythonOperator(
        task_id='auto_create_statics_table',
        python_callable=logic_create_statics_table,
    )
