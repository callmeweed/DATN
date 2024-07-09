import logging
from datetime import datetime, timedelta
import asyncio

import pendulum
from airflow import DAG
from airflow.models import Variable, TaskInstance, XCom
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from producer.wifeed_chi_so_tai_chinh import WiFeedChiSoTaiChinh
from producer.wifeed_bao_cao_tai_chinh import WiFeedBaoCaoTaiChinh
from producer.wifeed_api import WifeedApi

from common.utils import CATEGORY_WIFEED
from airflow.utils.session import provide_session

from producer.wifeed_thong_tin_co_ban_doanh_nghiep import WiFeedThongTinDoanhNghiep

dag_name = "dag_update_data_daily"

default_args = {
    "owner": "DSAI Team",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1, 0, tzinfo=pendulum.timezone('Asia/Ho_Chi_Minh')),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": 60,
}

with DAG(
        dag_id=dag_name,
        schedule="30 21 * * *",
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=4,
        tags=[""],
) as dag:
    def get_config(**kwargs):
        try:
            conf = kwargs['dag_run'].conf
            is_full_refresh = conf.get('is_full_refresh', False)
            backfill = conf.get('backfill', 5)
            now = datetime.now()
            quarter = (now.month - 1) // 3 + 1
            year = now.year
            quarters = []
            for i in range(backfill):
                if quarter == 1:
                    year = year - 1
                    quarter = 4
                else:
                    quarter = quarter - 1
                if quarter == 4:
                    quarters.append([year, 0])
                quarters.append([year, quarter])
            ti: TaskInstance = kwargs['task_instance']
            ti.xcom_push(key='quarters', value=quarters)
            ti.xcom_push(key='is_full_refresh', value=is_full_refresh)
            logging.info(f"Quarters: {quarters}")
        except:
            logging.info("Error when get a flag")

    def logic_wifeed_chi_so_tai_chinh(params, **kwargs):
        company_type = params['company_type']
        ti: TaskInstance = kwargs['task_instance']
        is_full_refresh = ti.xcom_pull(key='is_full_refresh', task_ids='get_config')
        if is_full_refresh == 'True':
            quarters = [[None, None]]
        else:
            quarters = ti.xcom_pull(key='quarters', task_ids='get_config')
        list_symbol = WifeedApi().get_all_code_by_company_type(company_type=company_type)['symbol_'].values.tolist()
        for year, quarter in quarters:
            logging.info(f"Call API chi_so_tai_chinh_{company_type} for year {year}, quarter {quarter}")
            vs = WiFeedChiSoTaiChinh(
                list_symbol=list_symbol,
                year=year,
                quarter=quarter,
                company_type=company_type
            )
            asyncio.run(vs.async_get_chi_so_tai_chinh())

    def logic_wifeed_chi_so_tai_chinh_v2(params, **kwargs):
        company_type = params['company_type']
        ti: TaskInstance = kwargs['task_instance']
        is_full_refresh = ti.xcom_pull(key='is_full_refresh', task_ids='get_config')
        if is_full_refresh == 'True':
            quarters = [[None, None]]
        else:
            quarters = ti.xcom_pull(key='quarters', task_ids='get_config')
        list_symbol = WifeedApi().get_all_code_by_company_type(company_type=company_type)['symbol_'].values.tolist()
        for year, quarter in quarters:
            logging.info(f"Call API chi_so_tai_chinh_v2_{company_type} for year {year}, quarter {quarter}")
            vs = WiFeedChiSoTaiChinh(
                list_symbol=list_symbol,
                year=year,
                quarter=quarter,
                company_type=company_type
            )
            asyncio.run(vs.async_get_chi_so_tai_chinh_v2())

    def logic_wifeed_bao_cao_tai_chinh(params, **kwargs):
        company_type = params['company_type']
        ti: TaskInstance = kwargs['task_instance']
        is_full_refresh = ti.xcom_pull(key='is_full_refresh', task_ids='get_config')
        if is_full_refresh == 'True':
            quarters = [[None, None]]
        else:
            quarters = ti.xcom_pull(key='quarters', task_ids='get_config')
        list_symbol = WifeedApi().get_all_code_by_company_type(company_type=company_type)['symbol_'].values.tolist()
        sub_urls = ['ket-qua-kinh-doanh']
        for year, quarter in quarters:
            logging.info(f"Call API bao_cao_tai_chinh_{company_type} for year {year}, quarter {quarter}")
            for sub_url in sub_urls:
                vs = WiFeedBaoCaoTaiChinh(
                    sub_url=sub_url,
                    list_symbol=list_symbol,
                    year=year,
                    quarter=quarter,
                    company_type=company_type
                )
                asyncio.run(vs.async_get_bao_cao_tai_chinh())

    @provide_session
    def cleanup_xcom(session=None, **context):
        dag_id = context["dag"].dag_id
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()

    def logic_wifeed_thong_tin_doanh_nghiep(params, **kwargs):

        vs = WiFeedThongTinDoanhNghiep(
        )
        vs.get_thong_tin_doanh_nghiep_v2()

    def logic_wifeed_danh_sach_ma_chung_khoan(params, **kwargs):
        vs = WiFeedThongTinDoanhNghiep(
        )
        vs.get_danh_sach_ma_chung_khoan_raw()

    # Task
    wifeed_thong_tin_doanh_nghiep = PythonOperator(
        task_id=f'get_thong_tin_doanh_nghiep_v2',
        python_callable=logic_wifeed_thong_tin_doanh_nghiep
    )

    wifeed_danh_sach_ma_chung_khoan = PythonOperator(
        task_id=f'get_danh_sach_ma_chung_khoan',
        python_callable=logic_wifeed_danh_sach_ma_chung_khoan
    )

    task_get_config = PythonOperator(
        task_id='get_config',
        python_callable=get_config,
        provide_context=True
    )

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

    task_test_cstc = BashOperator(
        task_id='test_cstc',
        bash_command="""
            cd /opt/airflow/dbt_timescale_db                                           
            project_path=/opt/airflow/dbt_timescale_db
            echo $project_path
            dbt run -m {{ params.cstc_stg }} --profiles-dir $project_path/.config/ --project-dir $project_path
            dbt test -m {{ params.cstc_stg }} --profiles-dir $project_path/.config/ --project-dir $project_path
        """,
        params={
                "cstc_stg": "stg_public__wifeed_bctc_cstc_ttm_raw stg_public__wifeed_bctc_cstc_quarter_raw stg_public__wifeed_bctc_cstc_year_raw"
        },
        execution_timeout=timedelta(minutes=5),
        dag=dag,
        trigger_rule='none_failed'
    )

    task_test_ttcb = BashOperator(
        task_id='test_thong_tin_co_ban',
        bash_command="""
                cd /opt/airflow/dbt_timescale_db                                           
                project_path=/opt/airflow/dbt_timescale_db
                echo $project_path
                dbt run -m {{ params.ttcb_stg }} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt test -m {{ params.ttcb_stg }} --profiles-dir $project_path/.config/ --project-dir $project_path
            """,
        params={
            "ttcb_stg": "stg_public__wifeed_bctc_ttcb_doanh_nghiep_v2_raw stg_public__wifeed_danh_sach_ma_chung_khoan_raw"
        },
        execution_timeout=timedelta(minutes=5),
        dag=dag,
        trigger_rule='none_failed'
    )

    task_snapshot_cstc = BashOperator(
        task_id='snapshot_cstc',
        bash_command="""
            cd /opt/airflow/dbt_timescale_db                                           
            project_path=/opt/airflow/dbt_timescale_db
            echo $project_path
            
            dbt snapshot -m {{ params.cstc_snapshot }} --profiles-dir $project_path/.config/ --project-dir $project_path
            dbt run -m {{ params.cstc_public }} --profiles-dir $project_path/.config/ --project-dir $project_path
        """,
        params={
                "cstc_snapshot": "wifeed_bctc_cstc_ttm_raw_snapshot wifeed_bctc_cstc_quarter_raw_snapshot wifeed_bctc_cstc_year_raw_snapshot",
                "cstc_public": "wifeed_bctc_chi_so_tai_chinh_ttm wifeed_bctc_chi_so_tai_chinh_quarter wifeed_bctc_chi_so_tai_chinh_year"
        },
        execution_timeout=timedelta(minutes=5),
        dag=dag,
    )

    task_snapshot_ttcb = BashOperator(
        task_id='snapshot_ttcb',
        bash_command="""
                cd /opt/airflow/dbt_timescale_db                                           
                project_path=/opt/airflow/dbt_timescale_db
                echo $project_path

                dbt snapshot -m {{ params.ttcb_snapshot }} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt run -m {{ params.ttcb_public }} --profiles-dir $project_path/.config/ --project-dir $project_path
            """,
        params={
            "ttcb_snapshot": "wifeed_bctc_ttcb_doanh_nghiep_v2_raw_snapshot wifeed_danh_sach_ma_chung_khoan_raw_snapshot",
            "ttcb_public": "wifeed_bctc_thong_tin_co_ban_doanh_nghiep wifeed_danh_sach_ma_chung_khoan"
        },
        execution_timeout=timedelta(minutes=5),
        dag=dag,
    )

    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable=cleanup_xcom,
        provide_context=True,
        trigger_rule='none_failed'
    )

    for key in CATEGORY_WIFEED.keys():
        task_get_chi_so_tai_chinh = PythonOperator(
            task_id=f'get_chi_so_tai_chinh_{CATEGORY_WIFEED[key]}',
            python_callable=logic_wifeed_chi_so_tai_chinh,
            params={
                'company_type': CATEGORY_WIFEED[key],
            },
        )

        task_get_chi_so_tai_chinh_v2 = PythonOperator(
            task_id=f'get_chi_so_tai_chinh_v2_{CATEGORY_WIFEED[key]}',
            python_callable=logic_wifeed_chi_so_tai_chinh_v2,
            params={
                'company_type': CATEGORY_WIFEED[key],
            },
        )

        task_get_bao_cao_tai_chinh = PythonOperator(
            task_id=f'get_bao_cao_tai_chinh_{CATEGORY_WIFEED[key]}',
            python_callable=logic_wifeed_bao_cao_tai_chinh,
            params={
                'company_type': CATEGORY_WIFEED[key],
            },
        )

        short_key = key if key != 'doanh_nghiep_san_xuat' else 'dnsx'
        task_test_bctc = BashOperator(
            task_id=f'test_bctc_{CATEGORY_WIFEED[key]}',
            bash_command="""
                cd /opt/airflow/dbt_timescale_db                                           
                project_path=/opt/airflow/dbt_timescale_db
                echo $project_path
                
                dbt run -m {{params.bctc_stg}} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt run -m {{params.cstc_v2_stg}} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt test -m {{params.bctc_stg}} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt test -m {{params.cstc_v2_stg}} --profiles-dir $project_path/.config/ --project-dir $project_path
            """,
            params={
                    "bctc_stg": f"stg_public__wifeed_bctc_kqkd_{short_key}_raw",
                    "cstc_v2_stg": f"stg_public__wifeed_bctc_cstc_{short_key}_ttm_v2_raw stg_public__wifeed_bctc_cstc_{short_key}_quarter_v2_raw stg_public__wifeed_bctc_cstc_{short_key}_year_v2_raw",
                    "key": key,
            },
            execution_timeout=timedelta(minutes=5),
            dag=dag,
        )

        task_snapshot_bctc = BashOperator(
            task_id=f'snapshot_bctc_{CATEGORY_WIFEED[key]}',
            bash_command="""
                cd /opt/airflow/dbt_timescale_db                                           
                project_path=/opt/airflow/dbt_timescale_db
                echo $project_path
                
                dbt snapshot -m {{params.bctc_snapshot}} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt snapshot -m {{params.cstc_v2_snapshot}} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt run -m {{params.bctc_public}} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt run -m {{params.cstc_v2_ttm_public}} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt run -m {{params.cstc_v2_quarter_public}} --profiles-dir $project_path/.config/ --project-dir $project_path
                dbt run -m {{params.cstc_v2_year_public}} --profiles-dir $project_path/.config/ --project-dir $project_path
            """,
            params={
                    "bctc_snapshot": f"wifeed_bctc_kqkd_{short_key}_raw_snapshot",
                    "cstc_v2_snapshot": f"wifeed_bctc_cstc_{short_key}_ttm_v2_raw_snapshot wifeed_bctc_cstc_{short_key}_quarter_v2_raw_snapshot wifeed_bctc_cstc_{short_key}_year_v2_raw_snapshot",
                    "bctc_public": f"wifeed_bctc_kqkd_{key} ",
                    "cstc_v2_ttm_public": f"wifeed_bctc_cstc_{short_key}_ttm_v2",
                    "cstc_v2_quarter_public": f"wifeed_bctc_cstc_{short_key}_quarter_v2",
                    "cstc_v2_year_public": f"wifeed_bctc_cstc_{short_key}_year_v2",
                    "key": key,
            },
            execution_timeout=timedelta(minutes=5),
            dag=dag,
        )

        task_get_config >> task_get_chi_so_tai_chinh >> task_get_chi_so_tai_chinh_v2 >> task_get_bao_cao_tai_chinh >> dbt_deps >> task_test_bctc >> task_test_cstc >> task_snapshot_bctc >> task_snapshot_cstc >> clean_xcom

    task_get_config >> (wifeed_thong_tin_doanh_nghiep, wifeed_danh_sach_ma_chung_khoan) >> dbt_deps >> task_test_ttcb >> task_snapshot_ttcb >> clean_xcom
