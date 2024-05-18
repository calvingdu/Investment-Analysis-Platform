from __future__ import annotations

import pandas as pd
from data_quality.dq_checks.gx_dq_check import gx_dq_check

from airflow.models import BaseOperator


class DataQualityPandasOperator(BaseOperator):
    def __init__(
        self,
        dataframe: pd.DataFrame,
        data_asset_name: str,
        expectation_suite_name: str,
        evalution_parameters={},
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
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

        return gx_results
