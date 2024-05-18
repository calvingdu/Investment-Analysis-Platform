from __future__ import annotations

import great_expectations as gx
from data_quality.dq_checks.gx_config import get_checkpoint_config
from data_quality.dq_checks.gx_config import get_data_context_config
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
