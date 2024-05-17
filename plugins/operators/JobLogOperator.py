from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine, MetaData, Table, update

"""
job_log Schema:
    job_log_id SERIAL PRIMARY KEY
    job_name TEXT NOT NULL
    run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    status TEXT CHECK (status IN ('start', 'end')) NOT NULL

"""


class JobLogOperator(BaseOperator):
    def __init__(
        self, 
        postgres_conn_id = "POSTGRES_CONN_ID",
        *args,
        **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_job_log_id(self):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(job_log_id) FROM job_log")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] + 1 if result[0] is not None else 0

    def start_job_log(self, job_name: str):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()

        job_log = Table('job_log', MetaData(), autoload_with=engine)
        stmt = insert(job_log).values(job_name=job_name, status='start').returning(job_log.c.job_log_id)

        with engine.connect() as conn:
            result = conn.execute(stmt)
            job_log_id = result.fetchone()[0]

        return job_log_id
    
    def update_job_log(self, job_log_id: int):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()

        job_log = Table('job_log', MetaData(), autoload_with=engine)
        stmt = update(job_log).where(job_log.c.job_log_id == job_log_id).values(status='end')

        with engine.connect() as conn:
            conn.execute(stmt)
