from __future__ import annotations

import pandas as pd
from data_quality.dq_checks.gx_dq_check import gx_dq_check
from data_quality.dq_checks.gx_dq_check import gx_sql_dq_check
from data_quality.dq_checks.gx_results_processor import gx_error_email
from data_quality.dq_checks.gx_results_processor import gx_validate_results

from airflow.models import BaseOperator


class DataQualityPandasOperator(BaseOperator):
    def __init__(
        self,
        dataframe: pd.DataFrame,
        data_asset_name: str,
        job_log_id: int,
        expectation_suite_name: str,
        evalution_parameters={},
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.job_log_id = job_log_id
        self.dataframe = dataframe
        self.data_asset_name = data_asset_name
        self.expectation_suite_name = expectation_suite_name
        self.evalution_parameters = evalution_parameters

    def execute(self, **context):
        gx_results = gx_dq_check(
            df_to_validate=self.dataframe,
            data_asset_name=self.data_asset_name,
            expectation_suite_name=self.expectation_suite_name,
            evaluation_parameters=self.evalution_parameters,
        )

        gx_validate_results(gx_results=gx_results, valid_exceptions=[])
        gx_error_email(
            gx_results=gx_results,
            data_asset_name=self.data_asset_name,
            job_log_id=self.job_log_id,
        )

        return gx_results


class DataQualitySQLCheckOperator(BaseOperator):
    def __init__(
        self,
        data_asset_name: str,
        job_log_id: int,
        expectation_suite_name: str = None,
        query: str = None,
        sql_table: str = None,
        sql_conn_id: str = "POSTGRES_CONN_ID",
        evaluation_parameters={},
        valid_expectations=[],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql_conn_id = sql_conn_id
        self.data_asset_name = data_asset_name
        self.expectation_suite_name = expectation_suite_name
        self.query = query
        self.sql_table = sql_table
        self.job_log_id = job_log_id
        self.evaluation_parameters = evaluation_parameters
        self.valid_expectations = valid_expectations

    def execute(self):
        if self.query is None and self.sql_table is None:
            raise ValueError("Either a query or a table name must be provided.")
        gx_results = gx_sql_dq_check(
            sql_conn_id=self.sql_conn_id,
            data_asset_name=self.data_asset_name,
            expectation_suite_name=self.expectation_suite_name,
            query=self.query,
            sql_table=self.sql_table,
            job_log_id=self.job_log_id,
            evaluation_parameters=self.evaluation_parameters,
        )

        gx_validate_results(gx_results=gx_results, valid_exceptions=[])
        gx_error_email(
            gx_results=gx_results,
            data_asset_name=self.data_asset_name,
            job_log_id=self.job_log_id,
        )

        return gx_results
