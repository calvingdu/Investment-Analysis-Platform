from __future__ import annotations

import os
from pathlib import Path

from data_quality.dq_checks.greatexpectations import gx_execution
from data_quality.dq_checks.greatexpectations import gx_sql_execution
from data_quality.dq_checks.gx_results_processor import get_data_docs
from data_quality.dq_checks.gx_results_processor import process_gx_results
from data_quality.dq_checks.gx_utils import purge_validations


# Pandas Execution
def gx_dq_check(
    df_to_validate,
    data_asset_name: str,
    expectation_suite_name: str,
    evaluation_parameters=None,
    **kwargs,
):
    ge_root_dir = os.path.join(Path(__file__).parents[1], "great_expectations")

    purge_validations(ge_root_dir)

    result = gx_execution(
        df_to_validate=df_to_validate,
        data_asset_name=data_asset_name,
        expectation_suite_name=expectation_suite_name,
        evaluation_parameters=evaluation_parameters,
        ge_root_dir=ge_root_dir,
    )
    success, error_message = process_gx_results(result)
    data_docs_site = get_data_docs(result)

    return {
        "success": success,
        "error_message": error_message,
        "data_docs_site": data_docs_site,
    }


# SQL Execution
def gx_sql_dq_check(
    sql_conn_id: str,
    data_asset_name: str,
    expectation_suite_name: str,
    query: str,
    sql_table: str,
    job_log_id: int,
    evaluation_parameters: dict,
    exclude_columns: list = [],
    *args,
    **kwargs,
):
    ge_root_dir = os.path.join(Path(__file__).parents[1], "great_expectations")

    purge_validations(ge_root_dir)

    result = gx_sql_execution(
        data_asset_name=data_asset_name,
        query=query,
        sql_table=sql_table,
        job_log_id=job_log_id,
        expectation_suite_name=expectation_suite_name,
        evaluation_parameters=evaluation_parameters,
        ge_root_dir=ge_root_dir,
        exclude_columns=exclude_columns,
    )

    success, error_message = process_gx_results(result)
    data_docs_site = get_data_docs(result)

    return {
        "success": success,
        "error_message": error_message,
        "data_docs_site": data_docs_site,
    }
