#!/usr/bin/env python3

import pathlib
import pytest
import sys
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import shutil
import os
import traceback

def clean_data_dir(data_path):
    if os.path.exists(data_path):
        try:
            shutil.rmtree(data_path)
        except Exception as e:
            print(f"Error deleting directory {data_path}: {e}")


def print_log_dir(path):
    # Show log contents
    log_path = os.path.join(path, "_delta_log")
    if os.path.exists(log_path):
        print("Delta log contents:")
        for file in sorted(os.listdir(log_path)):
            print(f"  {file}")

@pytest.mark.pandas
def test_failed_cleanup(tmp_path: pathlib.Path):
    data_path = tmp_path
    clean_data_dir(data_path)

    # write 10 versions of the data
    for i in range(10):
        ids = range(i*10, i*10+10)
        strings = [f"str_{i}" for i in range(10)]
        df = pd.DataFrame({"id": ids, "value": strings})
        write_deltalake(data_path, df, mode="overwrite", configuration={"delta.logRetentionDuration": "interval 0 day"})

    # checkpoint final version
    table = DeltaTable(data_path)
    table.create_checkpoint()

    # show log contents
    print("log contents before cleanup:")
    print_log_dir(data_path)

    # Call cleanup metadata
    table = DeltaTable(data_path, version=5)
    # table.create_checkpoint()  # Workaround, manually create checkpoint needed to load version >= 5
    table.cleanup_metadata()

    # show log contents
    print("\n################################################")
    print("log contents after cleanup:")
    print_log_dir(data_path)

    # Load old version
    table = DeltaTable(data_path, version=5)
    df2 = table.to_pandas()
    print("\n################################################")
    print("Version 5 of the data:")
    print(df2)
