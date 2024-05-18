from __future__ import annotations


def process_gx_results(result):
    error_message = ""
    success = result.get("success")
    if success is not None:
        if success is False:
            for i in result["run_results"].keys():
                for j in result["run_results"][list(result["run_results"].keys())[0]][
                    "validation_result"
                ]["results"]:
                    if j.get("success") is False:
                        expectation_type = j["expectation_config"]["expectation_type"]
                        error_message += f" {expectation_type}; "

    return success, error_message


def get_data_docs(result):
    data_docs_site = result["run_results"][list(result["run_results"].keys())[0]][
        "actions_results"
    ]["update_data_docs"]["local_site"]
    return data_docs_site


def print_gx_result(directory, file, gx_result):
    print()
    print(f"----- DQ Check Results for {directory + file} -----")
    print(f"GX Result: {gx_result['success']}")
    print(f"GX Errors: {gx_result['error_message']}")
    print(f"Data Docs Site: {gx_result['data_docs_site']}")
    print()
