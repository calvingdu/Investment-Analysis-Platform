from __future__ import annotations

import datetime

import pendulum
import yaml
from operators.DataQualityOperator import DataQualityPandasOperator
from operators.DataQualityOperator import DataQualitySQLCheckOperator
from operators.JobLogOperator import JobLogOperator
from operators.PostgresOperator import PandasToPostgresOperator
from operators.PostgresOperator import PostgresToPandasOperator
from scripts.transform_stock_company_profile import (
    transform_stock_company_profile_bronze,
)
from scripts.transform_stock_company_profile import (
    transform_stock_company_profile_silver,
)

from airflow import DAG
from airflow.decorators import task

# Load configuration
with open("/opt/airflow/plugins/dag_configs/stock_dag_config.yaml") as config_file:
    config = yaml.safe_load(config_file)

# Generate DAGs dynamically
for dag_config in config["dags"]:
    symbol = dag_config["symbol"]
    schedule = dag_config.get("schedule", "@monthly")

    dag_id = f"{symbol}_company_profile_api_dag"

    if from_date is None:
        from_date = datetime.date.today() - datetime.timedelta(days=30)

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": pendulum.datetime(2021, 1, 1, tz="UTC"),
        "catchup": False,
        "retries": 0,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description="A dynamic DAG for extracting and processing stock data",
        schedule_interval=schedule,
        catchup=False,
        tags=["news_api"],
    )

    @task(task_id="extract_api_data", dag=dag)
    def extract_api_data(
        symbol=symbol,
    ):
        print(
            f"Extracting Company Profile for {symbol}",
        )
        operator = StockCompanyProfileAPIToDataframeOperator(
            task_id="extract_api_data",
            symbol=symbol,
        )
        df = operator.execute()
        return df

    @task(task_id="start_job_log", dag=dag)
    def start_job_log():
        job_log_operator = JobLogOperator(task_id="start_job_log")
        job_log_id = job_log_operator.start_job_log(dag_id)
        return job_log_id

    @task(task_id="transform_bronze", dag=dag)
    def transform_bronze(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        df = kwargs["ti"].xcom_pull(task_ids="extract_api_data")
        transformed_df = transform_stock_company_profile_bronze(
            df,
            job_log_id=job_log_id,
        )
        return transformed_df

    @task(task_id="load_to_sql_bronze", dag=dag)
    def bronze_sql(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="transform_bronze")
        operator = PandasToPostgresOperator(
            task_id="load_to_sql_bronze",
            table="news_bronze",
            df=df,
        )
        rows_updated = operator.execute()
        return rows_updated

    @task(task_id="dq_check_bronze", dag=dag)
    def dq_check_bronze(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        import os

        print(os.getenv("EMAIL_USERNAME"))
        print(os.getenv("EMAIL_PASSWORD"))
        operator = DataQualitySQLCheckOperator(
            task_id="sql_dq_check_task",
            sql_conn_id="POSTGRES_CONN_ID",
            data_asset_name=f"{symbol}_news_bronze",
            expectation_suite_name="news_bronze_suite",
            sql_table="news_bronze",
            job_log_id=job_log_id,
        )
        result = operator.execute()
        print(result)

    @task.branch(task_id="check_empty", dag=dag)
    def check_empty(**kwargs):
        rows_updated = kwargs["ti"].xcom_pull(task_ids="load_to_sql_bronze")
        if rows_updated == 0:
            return "update_job_log"
        else:
            return "extract_bronze"

    @task(task_id="extract_bronze", dag=dag)
    def extract_bronze(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        df = PostgresToPandasOperator(
            task_id="Extract_DF",
            sql=f"SELECT * FROM news_bronze where job_log_id = {job_log_id}",
        ).execute()
        print(df)
        return df

    @task(task_id="transform_silver", dag=dag)
    def transform_silver(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="extract_bronze")
        silver_df = transform_stock_company_profile_silver(df=df)
        return silver_df

    @task(task_id="dq_check_silver", dag=dag)
    def dq_check_silver(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="transform_silver")
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        operator = DataQualityPandasOperator(
            task_id="dq_check_task_silver",
            dataframe=df,
            data_asset_name="news_silver",
            expectation_suite_name="news_silver_suite",
            job_log_id=job_log_id,
        )
        result = operator.execute()
        print(result)

    @task(task_id="load_to_sql_silver", dag=dag)
    def silver_sql(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="transform_silver")
        operator = PandasToPostgresOperator(
            task_id="upload_to_sql",
            table="news_silver",
            df=df,
        )
        operator.execute()

    @task(task_id="update_job_log", trigger_rule="none_failed", dag=dag)
    def update_job_log(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        job_log_operator = JobLogOperator(task_id="update_job_log")
        job_log_operator.update_job_log(job_log_id)

    # Define task dependencies
    api_data_df = extract_api_data()
    job_log_id = start_job_log()
    bronze_df = transform_bronze()
    rows_updated = bronze_sql()
    dq_check_bronze_task = dq_check_bronze()
    checked_rows = check_empty()
    extracted_bronze_df = extract_bronze()
    silver_df = transform_silver()
    dq_check_silver_task = dq_check_silver()
    silver_sql_task = silver_sql()
    update_job_log_task = update_job_log()

    (
        api_data_df
        >> job_log_id
        >> bronze_df
        >> rows_updated
        >> [dq_check_bronze_task, checked_rows]
        >> extracted_bronze_df
        >> silver_df
        >> dq_check_silver_task
        >> silver_sql_task
        >> update_job_log_task
    )

    globals()[dag_id] = dag
