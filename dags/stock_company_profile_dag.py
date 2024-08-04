from __future__ import annotations

import datetime

import pendulum
import yaml
from operators.DataQualityOperator import DataQualityPandasOperator
from operators.DataQualityOperator import DataQualitySQLCheckOperator
from operators.JobLogOperator import JobLogOperator
from operators.PostgresOperator import PandasToPostgresOperator
from operators.PostgresOperator import PostgresToPandasOperator
from operators.StockAPIOperator import StockCompanyProfilesAPIToDataframeOperator
from scripts.stock_company_profile.transform_stock_company_profile import (
    transform_stock_company_profile_bronze,
)
from scripts.stock_company_profile.transform_stock_company_profile import (
    transform_stock_company_profile_silver,
)

from airflow.decorators import dag
from airflow.decorators import task
from airflow.decorators import task_group

# Load configuration
with open("/opt/airflow/plugins/dag_configs/stock_dag_config.yaml") as config_file:
    config = yaml.safe_load(config_file)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
    "catchup": False,
    "retries": 0,
}


@dag(
    schedule="@monthly",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["stock_company_profile_api"],
)
def stock_company_profiles_dag():
    @task(task_id="get_company_symbols")
    def get_stock_symbols():
        stocks = config["dags"]
        symbols = []
        for s in stocks:
            symbols.append(s["symbol"])

        return symbols

    @task(task_id="extract_api_data")
    def extract_api_data(
        symbols: list,
    ):
        print(
            f"Extracting Company Profiles for {symbols}",
        )
        operator = StockCompanyProfilesAPIToDataframeOperator(
            task_id="extract_api_data",
            symbols=symbols,
        )
        df = operator.execute()
        print(df)
        return df

    @task(task_id="start_job_log")
    def start_job_log():
        job_log_operator = JobLogOperator(task_id="start_job_log")
        job_log_id = job_log_operator.start_job_log(f"stock_company_profile_api")
        return job_log_id

    @task(task_id="transform_bronze")
    def transform_bronze(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        df = kwargs["ti"].xcom_pull(task_ids="extract_api_data")
        transformed_df = transform_stock_company_profile_bronze(
            df,
            job_log_id=job_log_id,
        )
        return transformed_df

    @task(task_id="load_to_sql_bronze")
    def bronze_sql(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="transform_bronze")
        operator = PandasToPostgresOperator(
            task_id="load_to_sql_bronze",
            table="stock_company_profile_bronze",
            df=df,
        )
        rows_updated = operator.execute()
        return rows_updated

    @task(task_id="dq_check_bronze")
    def dq_check_bronze(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        import os

        print(os.getenv("EMAIL_USERNAME"))
        print(os.getenv("EMAIL_PASSWORD"))
        operator = DataQualitySQLCheckOperator(
            task_id="sql_dq_check_task",
            sql_conn_id="POSTGRES_CONN_ID",
            data_asset_name=f"stock_company_profile_bronze",
            expectation_suite_name="stock_company_profile_bronze",
            sql_table="news_bronze",
            job_log_id=job_log_id,
        )
        result = operator.execute()
        print(result)

    @task.branch(task_id="check_empty")
    def check_empty(**kwargs):
        rows_updated = kwargs["ti"].xcom_pull(task_ids="load_to_sql_bronze")
        if rows_updated == 0:
            return "update_job_log"
        else:
            return "extract_bronze"

    @task(task_id="extract_bronze")
    def extract_bronze(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        df = PostgresToPandasOperator(
            task_id="Extract_DF",
            sql=f"SELECT * FROM news_bronze where job_log_id = {job_log_id}",
        ).execute()
        print(df)
        return df

    @task(task_id="transform_silver")
    def transform_silver(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="extract_bronze")
        silver_df = transform_stock_company_profile_silver(df=df)
        return silver_df

    @task(task_id="dq_check_silver")
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

    @task(task_id="load_to_sql_silver")
    def silver_sql(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="transform_silver")
        operator = PandasToPostgresOperator(
            task_id="upload_to_sql",
            table="news_silver",
            df=df,
        )
        operator.execute()

    @task(task_id="update_job_log", trigger_rule="none_failed")
    def update_job_log(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        job_log_operator = JobLogOperator(task_id="update_job_log")
        job_log_operator.update_job_log(job_log_id)

    # Define task dependencies
    symbols = get_stock_symbols()
    api_data_df = extract_api_data(symbols)
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
        symbols
        >> api_data_df
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


stock_company_profiles_dag()
