from __future__ import annotations

import datetime

import pandas as pd
import pendulum
import yaml
from operators.DataQualityOperator import DataQualityPandasOperator
from operators.JobLogOperator import JobLogOperator
from operators.NewsAPIOperator import NewsAPIToDataframeOperator
from operators.PostgresOperator import PandasToPostgresOperator
from operators.PostgresOperator import PostgresToPandasOperator
from scripts.transform_news import transform_news_bronze

from airflow import DAG
from airflow.decorators import task

# Load configuration
with open("/opt/airflow/plugins/dag_configs/news_dag_config.yaml") as config_file:
    config = yaml.safe_load(config_file)

# Generate DAGs dynamically
for dag_config in config["dags"]:
    dag_id = dag_config["dag_id"]
    schedule = dag_config["schedule"]
    news_topic = dag_config["news_topic"]
    endpoint = dag_config["endpoint"]
    from_date = dag_config["from_date"]
    to_date = dag_config.get("to_date")

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
        description="A dynamic DAG for extracting and processing news data",
        schedule_interval=schedule,
        catchup=False,
        tags=["news_api"],
    )

    @task(task_id="extract_api_data", dag=dag)
    def extract_api_data():
        print(
            f"Extracting data for {news_topic} from {from_date} to {to_date} from {endpoint} endpoint",
        )
        operator = NewsAPIToDataframeOperator(
            task_id="extract_api_data",
            news_topic=news_topic,
            endpoint=endpoint,
            from_date=from_date,
            to_date=to_date,
            sort_by="popularity",
            page_size=100,
            page_number=1,
        )
        df = operator.execute()
        return df

    @task(task_id="start_job_log", dag=dag)
    def start_job_log():
        job_log_operator = JobLogOperator(task_id="start_job_log")
        job_log_id = job_log_operator.start_job_log(dag_id)
        return job_log_id

    @task(task_id="transform_news_bronze", dag=dag)
    def bronze_processing(job_log_id: int, **kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="extract_api_data")
        transformed_df = transform_news_bronze(
            df,
            job_log_id=job_log_id,
            topic=news_topic,
        )
        return transformed_df

    @task(task_id="dq_bronze", dag=dag)
    def dq_bronze(df: pd.DataFrame):
        operator = DataQualityPandasOperator(
            task_id="dq_task",
            dataframe=df,
            data_asset_name="news_bronze",
            expectation_suite_name="news_bronze_suite",
        )
        result = operator.execute()
        print(result)

    @task(task_id="upload_to_sql_bronze", dag=dag)
    def bronze_sql(**kwargs):
        df = kwargs["ti"].xcom_pull(task_ids="transform_news_bronze")
        operator = PandasToPostgresOperator(
            task_id="upload_to_sql_bronze",
            table="news_bronze",
            df=df,
        )
        rows_updated = operator.execute()
        return rows_updated

    @task.branch(task_id="check_empty", dag=dag)
    def check_empty(rows_updated):
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

    @task(task_id="transform", dag=dag)
    def transform():
        return True

    @task(task_id="silver_sql", dag=dag)
    def silver_sql(df):
        return True
        # operator = PandasToPostgresOperator(
        #     task_id="upload_to_sql",
        #     table="news_silver",
        #     df=df,
        # )
        # operator.execute()

    @task(task_id="update_job_log", trigger_rule="all_done", dag=dag)
    def update_job_log(**kwargs):
        job_log_id = kwargs["ti"].xcom_pull(task_ids="start_job_log")
        job_log_operator = JobLogOperator(task_id="update_job_log")
        job_log_operator.update_job_log(job_log_id)

    # Define task dependencies
    api_data_df = extract_api_data()
    job_log_id = start_job_log()
    bronze_df = bronze_processing(job_log_id)
    dq_task = dq_bronze(bronze_df)
    rows_updated = bronze_sql()
    checked_rows = check_empty(rows_updated)
    extracted_bronze_df = extract_bronze()
    transformed_df = transform()
    silver_task = silver_sql(transformed_df)
    update_job_log_task = update_job_log()

    (
        api_data_df
        >> job_log_id
        >> bronze_df
        >> dq_task
        >> rows_updated
        >> checked_rows
        >> extracted_bronze_df
        >> transformed_df
        >> silver_task
        >> update_job_log_task
    )

    globals()[dag_id] = dag
