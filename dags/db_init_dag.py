from __future__ import annotations

import pendulum

import pandas as pd
from airflow.decorators import dag
from airflow.decorators import task

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging
import yaml
import os 

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["db"],
)
def db_init_dag():
    logger = logging.getLogger(__name__)

    @task()
    def load_yaml_file():
        yaml_file_path = os.path.join(os.path.dirname(__file__), '/opt/airflow/plugins/data_models/model.yaml')
        with open(yaml_file_path, 'r') as file:
            table_definitions = yaml.safe_load(file)
        return table_definitions['tables']
    
    @task()
    def create_table(table_def):
        print(table_def)
        table_name = table_def['table']
        columns = table_def['columns']

        column_definitions = ", ".join([f"{key} {value}" for col in columns for key, value in col.items()])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions});"
        
        print(f"Creating table with SQL: {create_table_sql}")
        SQLExecuteQueryOperator(
            task_id = table_name, 
            conn_id = "POSTGRES_CONN_ID",
            sql = create_table_sql,
            split_statements = True,
            return_last = True,
        ).execute({})

    table_definitions = load_yaml_file()
    create_table.expand(table_def = table_definitions)

db_init_dag()
