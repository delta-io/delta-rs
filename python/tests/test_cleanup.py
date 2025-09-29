#!/usr/bin/env python3

import os
import pathlib
import shutil

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import DeltaTable, write_deltalake


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


def valid_gc_data(version) -> Table:
    id_col = ArrowField("id", DataType.int32(), nullable=True)
    gc = ArrowField("gc", DataType.int32(), nullable=True).with_metadata(
        {"delta.generationExpression": "10"}
    )
    data = Table.from_pydict(
        {"id": Array([version, version], type=id_col), "gc": Array([10, 10], type=gc)},
    )
    return data


@pytest.mark.pandas
def test_failed_cleanup(tmp_path: pathlib.Path):
    data_path = tmp_path
    clean_data_dir(data_path)

    # write 10 versions of the data
    for i in range(10):
        data = valid_gc_data(i)
        write_deltalake(
            data_path,
            mode="overwrite",
            data=data,
            configuration={
                "delta.minWriterVersion": "7",
                "delta.logRetentionDuration": "interval 0 day",
            },
        )

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
