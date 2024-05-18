from __future__ import annotations

import os
import shutil
from pathlib import Path

from data_quality.dq_checks.greatexpectations import gx_execution
from data_quality.dq_checks.gx_results_processor import get_data_docs
from data_quality.dq_checks.gx_results_processor import process_gx_results


def gx_dq_check(
    df_to_validate,
    data_asset_name: str,
    expectation_suite_name: str,
    evaluation_parameters=None,
    **kwargs,
):
    ge_root_dir = os.path.join(Path(__file__).parents[1], "great_expectations")

    # Checks if validation threshold is met
    validations_count = 0
    validations_max_count = 30

    validations_dir = os.path.join(ge_root_dir, "uncommitted", "validations")
    uncommitted_dir = os.path.join(ge_root_dir, "uncommitted")
    for root_dir, cur_dir, files in os.walk(validations_dir):
        validations_count += len(files)

    if validations_count >= validations_max_count:
        shutil.rmtree(uncommitted_dir)

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


print(os.path.join(Path(__file__).parents[2], "great_expectations"))
