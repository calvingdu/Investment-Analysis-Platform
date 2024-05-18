from __future__ import annotations

import os

from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context.types.base import DataContextConfig

from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_data_context_config(ge_root_dir, expectation_subdir=""):
    data_context_config = DataContextConfig(
        **{
            "config_version": 3.0,
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": os.path.join(ge_root_dir, "expectations"),
                    },
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": os.path.join(
                            ge_root_dir,
                            "uncommitted",
                            "validations",
                        ),
                    },
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore",
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "suppress_store_backend_id": True,
                        "base_directory": os.path.join(ge_root_dir, "checkpoints"),
                    },
                },
            },
            "expectations_store_name": "expectations_store",
            "validations_store_name": "validations_store",
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "checkpoint_store_name": "checkpoint_store",
            "data_docs_sites": {
                "local_site": {
                    "class_name": "SiteBuilder",
                    "show_how_to_buttons": True,
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": os.path.join(
                            ge_root_dir,
                            "uncommitted",
                            "data_docs",
                            "local_site",
                        ),
                    },
                    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                },
            },
            "anonymous_usage_statistics": {
                "data_context_id": "abcdabcd-1111-2222-3333-abcdabcdabcd",
                "enabled": False,
            },
            "notebooks": None,
            "concurrency": {"enabled": False},
        },
    )

    return data_context_config


def get_checkpoint_config(
    context,
    run_name,
    batch_request,
    evaluation_parameters,
    expectation_suite_name,
):
    checkpoint_config = Checkpoint(
        **{
            "name": "gx_checkpoint",
            "config_version": 1.0,
            "template_name": None,
            "run_name_template": run_name,
            "expectation_suite_name": expectation_suite_name,
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
                },
            ],
            "batch_request": batch_request,
            "evaluation_parameters": evaluation_parameters,
            "runtime_configuration": {},
            "validations": [],
            "data_context": context,
        },
    )
    return checkpoint_config


# SQL
def get_sql_datasource_config():
    sql_hook = PostgresHook("POSTGRES_CONN_ID")
    connection_string = sql_hook.get_uri()

    datasource_config = {
        "name": "postgresql_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
        },
        "data_connectors": {
            "runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "inferred_data_connector": {
                "class_name": "InferredAssetSqlDataConnector",
                "include_schema_name": True,
            },
        },
    }

    return datasource_config


def get_sql_checkpoint_config(
    query: str,
    data_asset_name: str,
    expectation_suite_name: str,
    evaluation_parameters: dict,
    context,
):
    checkpoint_config = Checkpoint(
        **{
            "name": f"gx_checkpoint_{expectation_suite_name}",
            "config_version": 1.0,
            "template_name": None,
            "run_name_template": data_asset_name,
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
                },
            ],
            "evaluation_parameters": evaluation_parameters,
            "runtime_configuration": {},
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "postgresql_datasource",
                        "data_connector_name": "runtime_data_connector",
                        "data_asset_name": f"sql_{data_asset_name}",
                        "runtime_parameters": {
                            "query": query,
                        },
                        "batch_identifiers": {
                            "default_identifier_name": "default_identifier",
                        },
                        "batch_spec_passthrough": {"create_temp_table": False},
                    },
                    "expectation_suite_name": expectation_suite_name,
                },
            ],
            "data_context": context,
        },
    )
    return checkpoint_config
