from __future__ import annotations

import pandas as pd
from email_sender.email_sender import EmailSender


def gx_validate_results(
    valid_exceptions: list[str],
    gx_results: dict,
    **kwargs,
):
    if not gx_results["success"]:
        error_message = gx_results["error_message"]
        exceptions = [
            exception.strip()
            for exception in error_message.split(";")
            if exception.strip()
        ]
        if all(exception in valid_exceptions for exception in exceptions):
            print("Check Failed but all exceptions are valid")
            print("Error Exceptions: ", exceptions)
            return True
        else:
            print("Check Failed and there are invalid exceptions")
            print("Error Exceptions: ", exceptions)
            return False
    else:
        print("Check Passed")
        return True


def gx_error_email(gx_results, data_asset_name, job_log_id, **kwargs):
    email_sender = EmailSender()
    dq_check_parameters = {
        "asset_name": data_asset_name,
        "file": f"Job Log Id: {job_log_id}",
        "error_exceptions": gx_results["error_message"],
        "data_docs_site": gx_results["data_docs_site"],
        "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    email_sender.send_dq_notification_email(parameters=dq_check_parameters)


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
