from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator


dag_name = "dbt_docs"

default_args = {
    "owner": "DSAI Team",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 22),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": 60,
}

with DAG(
        dag_id=dag_name,
        schedule_interval="0 20 * * *",
        default_args=default_args,
        catchup=False,
        max_active_runs=4,
        max_active_tasks=4,
        tags=[""]
) as dag:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command="""
                cd /opt/airflow/dbt_timescale_db                                           
                project_path=/opt/airflow/dbt_timescale_db
                echo $project_path
                dbt deps
            """,
        execution_timeout=timedelta(minutes=5),
        dag=dag,
        trigger_rule='none_failed'
    )

    run_task = BashOperator(
        task_id='dbt_docs',
        bash_command="""
            cd /opt/airflow/dbt_timescale_db                                           
            project_path=/opt/airflow/dbt_timescale_db
            echo $project_path
            
            dbt docs generate --profiles-dir $project_path/.config/ --project-dir $project_path
            dbt docs serve --port 8001 --profiles-dir $project_path/.config/ --project-dir $project_path
        """,
        execution_timeout=timedelta(minutes=60),
        dag=dag,
    )


    dbt_deps >> run_task
