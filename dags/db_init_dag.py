from __future__ import annotations

import os

import pendulum
import yaml

from airflow.decorators import dag
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["db"],
)
def db_init_dag():
    @task()
    def load_yaml_dir() -> list[str]:
        dirname = os.path.dirname
        tables_dir_path = os.path.join(
            dirname(dirname(__file__)),
            "plugins/data_models",
        )

        table_files = os.listdir(tables_dir_path)
        print(f"Loading files: {table_files}")
        return [os.path.join(tables_dir_path, file) for file in table_files]

    @task()
    def load_yaml_file(file_path: str) -> list[dict]:
        with open(file_path) as file:
            print(f"Loading file: {file_path}")
            return yaml.safe_load(file)["tables"]

    @task()
    def flatten_table_defs(table_defs_list: list[list[dict]]) -> list[dict]:
        """Flatten the list of lists of table definitions into a single list"""
        return [table_def for sublist in table_defs_list for table_def in sublist]

    @task()
    def create_table(table_def):
        print(table_def)
        table_name = table_def["table"]
        columns = table_def["columns"]
        indexes = table_def.get("indexes", None)
        constraints = table_def.get("constraints", None)

        column_definitions = ", ".join(
            [f"{key} {value}" for col in columns for key, value in col.items()],
        )
        create_table_sql = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions}); "
        )

        print(f"Creating table with SQL: {create_table_sql}")
        SQLExecuteQueryOperator(
            task_id=table_name,
            conn_id="POSTGRES_CONN_ID",
            sql=create_table_sql,
            split_statements=True,
            return_last=True,
        ).execute({})

        if indexes:
            for index in indexes:
                index_sql = f"CREATE INDEX IF NOT EXISTS {index['index_name']} ON {table_name} ({', '.join(index['columns'])});  "
                print(f"Creating index with SQL: {index_sql}")
                index_result = SQLExecuteQueryOperator(
                    task_id=index["index_name"],
                    conn_id="POSTGRES_CONN_ID",
                    sql=index_sql,
                    split_statements=True,
                    return_last=True,
                ).execute({})

                print(f" Index result: {index_result}")

        if constraints:
            for constraint in constraints:
                if constraint["type"] == "unique":
                    constraint_sql = f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = '{constraint['constraint_name']}'
                        AND conrelid = '{table_name}'::regclass
                    ) THEN
                        ALTER TABLE {table_name}
                        ADD CONSTRAINT {constraint['constraint_name']}
                        UNIQUE ({', '.join(constraint['columns'])});
                    END IF;
                END
                $$; """
                else:
                    raise ValueError("Unsupported constraint type")

                print(f"Creating constraint with SQL: {constraint_sql}")
                constraint_result = SQLExecuteQueryOperator(
                    task_id=constraint["constraint_name"],
                    conn_id="POSTGRES_CONN_ID",
                    sql=constraint_sql,
                    split_statements=True,
                    return_last=True,
                ).execute({})

                print(f" Constraint result: {constraint_result}")

    table_files = load_yaml_dir()
    table_defs = load_yaml_file.expand(file_path=table_files)
    flattened_table_defs = flatten_table_defs(table_defs_list=table_defs)
    table = create_table.expand(table_def=flattened_table_defs)

    table_files >> table_defs >> flattened_table_defs >> table


db_init_dag()
