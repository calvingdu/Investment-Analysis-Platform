from __future__ import annotations

import os
import shutil


def purge_validations(ge_root_dir: str):
    # Checks if validation threshold is met
    validations_count = 0
    validations_max_count = 30

    validations_dir = os.path.join(ge_root_dir, "uncommitted", "validations")
    uncommitted_dir = os.path.join(ge_root_dir, "uncommitted")
    for root_dir, cur_dir, files in os.walk(validations_dir):
        validations_count += len(files)

    if validations_count >= validations_max_count:
        shutil.rmtree(uncommitted_dir)
