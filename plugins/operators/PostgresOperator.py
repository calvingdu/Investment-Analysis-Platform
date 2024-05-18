from __future__ import annotations

import pandas as pd
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class DictToPostgresOperator(BaseOperator):
    def __init__(self, postgres_conn_id="POSTGRES_CONN_ID", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(
        self,
        table: str,
        data_dict: dict,
    ):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()

        metadata = MetaData(bind=engine)
        table = Table(table, metadata, autoload_with=engine)
        stmt = insert(table).values(data_dict)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=[table.primary_key.columns.keys()[0]],
        )

        with engine.connect() as conn:
            conn.execute(stmt)

        self.log.info(
            "Successfully inserted JSON data into the PostgreSQL table: %s",
            table,
        )


class PandasToPostgresOperator(BaseOperator):
    def __init__(
        self,
        table: str,
        df: pd.DataFrame,
        postgres_conn_id="POSTGRES_CONN_ID",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.df = df

    def execute(self, no_duplicates=True, **context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        engine = postgres_hook.get_sqlalchemy_engine()

        if no_duplicates:
            cursor.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table}'",
            )
            columns = [row[0] for row in cursor.fetchall()]

            cursor.execute(
                f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
                ON kcu.constraint_name = tco.constraint_name
            WHERE tco.table_name = '{self.table}' AND tco.constraint_type = 'PRIMARY KEY'
            """,
            )

            primary_key = [row[0] for row in cursor.fetchall()]

            temp_table = self.table + "_temp"
            with engine.connect() as conn:
                conn.execute(
                    f"CREATE TEMP TABLE {temp_table} AS TABLE {self.table} WITH NO DATA",
                )

                self.df.to_sql(temp_table, engine, if_exists="append", index=False)

            insert_sql = f"""
                INSERT INTO {self.table} ({', '.join(columns)})
                (SELECT {', '.join(columns)}
                FROM {temp_table})
                ON CONFLICT ({', '.join(primary_key)}) DO NOTHING
                RETURNING * """

            with engine.connect() as conn:
                result = conn.execute(insert_sql)
                conn.execute(f"DROP TABLE {temp_table} ")

            rows_updated = result.rowcount
        else:
            self.df.to_sql(self.table, engine, if_exists="append", index=False)
            rows_updated = len(self.df)

        print(f"Inserted {rows_updated} rows into {self.table}")
        cursor.close()
        conn.close()
        return rows_updated


class PostgresToPandasOperator(BaseOperator):
    def __init__(self, sql, postgres_conn_id="POSTGRES_CONN_ID", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def execute(self, **context) -> pd.DataFrame:
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()
        df = pd.read_sql(self.sql, engine)
        return df
