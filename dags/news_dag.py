from __future__ import annotations

import pendulum
from operators.DataQualityOperator import DataQualityPandasOperator
from operators.DataQualityOperator import DataQualitySQLCheckOperator
from operators.JobLogOperator import JobLogOperator
from operators.NewsAPIOperator import NewsAPIToDataframeOperator
from operators.PostgresOperator import PandasToPostgresOperator
from operators.PostgresOperator import PostgresToPandasOperator
from scripts.transform_news import transform_news_bronze
from scripts.transform_news import transform_news_silver

from airflow.decorators import dag
from airflow.decorators import task

NEWS_TOPIC = "bitcoin"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["news_api"],
)
def test_news_api():
    @task(task_id="extract_api_data")
    def extract_api_data():
        operator = NewsAPIToDataframeOperator(
            task_id="extract_api_data",
            news_topic=NEWS_TOPIC,
            endpoint="top-headlines",
            from_date="2024-04-18",
            to_date=None,
            sort_by="popularity",
            page_size=100,
            page_number=1,
        )

        df = operator.execute()
        return df

    @task(task_id="start_job_log")
    def start_job_log():
        job_log_operator = JobLogOperator(task_id="start_job_log")
        job_log_id = job_log_operator.start_job_log("bitcoin_news_api")
        return job_log_id

    @task(task_id="transform_bronze")
    def transform_bronze(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        df = kwargs["ti"].xcom_pull(task_ids="extract_api_data")
        transformed_df = transform_news_bronze(
            df,
            job_log_id=job_log_id,
            topic=NEWS_TOPIC,
        )
        return transformed_df

    @task(task_id="dq_bronze")
    def dq_bronze(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        operator = DataQualitySQLCheckOperator(
            task_id="sql_dq_task",
            sql_conn_id="POSTGRES_CONN_ID",
            data_asset_name=f"{NEWS_TOPIC}_news_bronze",
            sql_table="news_bronze",
            expectation_suite_name="news_bronze_suite",
            job_log_id=job_log_id,
        )
        result = operator.execute()
        print(result)

    @task(task_id="load_to_sql_bronze")
    def bronze_sql(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="transform_bronze")
        operator = PandasToPostgresOperator(
            task_id="load_to_sql_bronze",
            table="news_bronze",
            df=df,
        )
        rows_updated = operator.execute()
        return rows_updated

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
        silver_df = transform_news_silver(df=df)
        return silver_df

    @task(task_id="dq_silver")
    def dq_silver(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="transform_silver")
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        operator = DataQualityPandasOperator(
            task_id="dq_task_silver",
            dataframe=df,
            data_asset_name="news_silver",
            expectation_suite_name="news_silver_suite",
            job_log_id=job_log_id,
        )
        result = operator.execute()
        print(result)

    @task(task_id="silver_sql")
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
    api_data_df = extract_api_data()
    job_log_id = start_job_log()
    bronze_df = transform_bronze()
    rows_updated = bronze_sql()
    dq_bronze_task = dq_bronze()
    checked_rows = check_empty()
    extracted_bronze_df = extract_bronze()
    silver_df = transform_silver()
    dq_silver_task = dq_silver()
    silver_sql_task = silver_sql()
    update_job_log_task = update_job_log()

    (
        api_data_df
        >> job_log_id
        >> bronze_df
        >> rows_updated
        >> [dq_bronze_task, checked_rows]
        >> extracted_bronze_df
        >> silver_df
        >> dq_silver_task
        >> silver_sql_task
        >> update_job_log_task
    )


test_news_api()
