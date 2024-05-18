from __future__ import annotations

import great_expectations as gx
import yaml
from data_quality.dq_checks.gx_config import get_checkpoint_config
from data_quality.dq_checks.gx_config import get_data_context_config
from data_quality.dq_checks.gx_config import get_sql_checkpoint_config
from data_quality.dq_checks.gx_config import get_sql_datasource_config
from data_quality.great_expectations.custom_expectations.expect_column_values_to_not_contain_special_characters import (
    ExpectColumnValuesToNotContainSpecialCharacters,
)

ExpectColumnValuesToNotContainSpecialCharacters


def gx_execution(
    df_to_validate,
    data_asset_name,
    expectation_suite_name,
    ge_root_dir,
    evaluation_parameters=None,
    **kwargs,
):
    data_context_config = get_data_context_config(ge_root_dir=ge_root_dir)
    context = gx.get_context(project_config=data_context_config)

    dataframe_datasource = context.sources.add_pandas(
        name="pandas_datasource",
    )
    dataframe_asset = dataframe_datasource.add_dataframe_asset(
        name=data_asset_name,
        dataframe=df_to_validate,
    )

    batch_request = dataframe_asset.build_batch_request()

    checkpoint_config = get_checkpoint_config(
        context=context,
        run_name=data_asset_name,
        batch_request=batch_request,
        evaluation_parameters=evaluation_parameters,
        expectation_suite_name=expectation_suite_name,
    )

    context.add_or_update_checkpoint(checkpoint=checkpoint_config)

    checkpoint_result = context.run_checkpoint(checkpoint_name="gx_checkpoint")

    return checkpoint_result


def gx_sql_execution(
    data_asset_name: str,
    query: str,
    sql_table: str,
    job_log_id: int,
    expectation_suite_name: str,
    ge_root_dir: str,
    evaluation_parameters: dict,
    exclude_columns: list = [],
    **kwargs,
):
    if query is None:
        query = _prepare_sql_query(
            # sql_hook=sql_hook,
            # exclude_columns=exclude_columns,
            # evaluation_parameters=evaluation_parameters,
            sql_table=sql_table,
            job_log_id=job_log_id,
        )
    print("Performing Postcheck on query: \n" + query)

    data_context_config = get_data_context_config(ge_root_dir=ge_root_dir)

    context = gx.get_context(project_config=data_context_config)

    sql_datasource_config = get_sql_datasource_config()

    context.test_yaml_config(yaml.dump(sql_datasource_config))

    context.add_datasource(**sql_datasource_config)

    # Configuring checkpoint with data context
    checkpoint = get_sql_checkpoint_config(
        query=query,
        data_asset_name=data_asset_name,
        expectation_suite_name=expectation_suite_name,
        evaluation_parameters=evaluation_parameters,
        context=context,
    )

    context.add_or_update_checkpoint(checkpoint=checkpoint)

    checkpoint_result = context.run_checkpoint(
        checkpoint_name=f"gx_checkpoint_{expectation_suite_name}",
    )

    return checkpoint_result


def _prepare_sql_query(
    sql_table: str,
    job_log_id: int,
):
    query = f"SELECT * FROM {sql_table} WHERE job_log_id = {job_log_id}"
    return query
