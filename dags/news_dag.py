from __future__ import annotations

import logging

import pandas as pd
import pendulum
from operators.JobLogOperator import JobLogOperator
from operators.NewsAPIOperator import NewsAPIToDataframeOperator
from operators.PostgresOperator import PandasToPostgresOperator
from operators.PostgresOperator import PostgresToPandasOperator
from scripts.transform_news import transform_news_bronze

from airflow.decorators import dag
from airflow.decorators import task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["news_api"],
)
def bitcoin_news_api():
    logger = logging.getLogger(__name__)

    @task()
    def extract_api_data():
        operator = NewsAPIToDataframeOperator(
            task_id="extract_api_data",
            news_topic="bitcoin",
            endpoint="top-headlines",
            from_date="2024-04-18",
            to_date=None,
            sort_by="popularity",
            page_size=100,
            page_number=1,
        )

        df = operator.execute()
        return df

    @task()
    def start_job_log():
        job_log_operator = JobLogOperator(task_id="start_job_log")
        job_log_id = job_log_operator.start_job_log("bitcoin_news_api")
        return job_log_id

    @task()
    def bronze_processing(df: pd.DataFrame, job_log_id: int):
        transformed_df = transform_news_bronze(df, job_log_id=job_log_id)
        return transformed_df

    @task()
    def upload_to_sql(df):
        operator = PandasToPostgresOperator(
            task_id="upload_to_sql",
            table="news_bronze",
            df=df,
        )

        operator.execute()

    @task()
    def dq_precheck_task():
        logger.info("DQ Precheck Task")
        return True

    @task()
    def extract_sql(job_log_id: int):
        df = PostgresToPandasOperator(
            task_id="Extract_DF",
            sql=f"SELECT * FROM news_bronze where job_log_id = {job_log_id}",
        ).execute()
        print(df)
        return df

    @task
    def update_job_log(job_log_id: int):
        job_log_operator = JobLogOperator(task_id="update_job_log")
        job_log_operator.update_job_log(job_log_id)

    api_data = extract_api_data()
    job_log_id = start_job_log()
    bronze_news = bronze_processing(api_data, job_log_id)
    extract = extract_sql(job_log_id)

    (
        api_data
        >> job_log_id
        >> bronze_news
        >> upload_to_sql(bronze_news)
        >> dq_precheck_task()
        >> extract
        >> update_job_log(job_log_id)
    )


bitcoin_news_api()
