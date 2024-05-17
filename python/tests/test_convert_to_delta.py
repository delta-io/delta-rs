import pathlib

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from deltalake import convert_to_deltalake
from deltalake.exceptions import DeltaError
from deltalake.table import DeltaTable


def test_local_convert_to_delta(tmp_path: pathlib.Path, sample_data: pa.Table):
    ds.write_dataset(
        sample_data,
        tmp_path,
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )

    name = "converted_table"
    description = "parquet table converted to delta table with delta-rs"
    convert_to_deltalake(
        tmp_path,
        name=name,
        description=description,
        configuration={"delta.appendOnly": "true"},
        custom_metadata={"userName": "John Doe"},
    )

    dt = DeltaTable(tmp_path)

    assert dt.version() == 0
    assert dt.files() == ["part-0.parquet"]
    assert dt.metadata().name == name
    assert dt.metadata().description == description
    assert dt.metadata().configuration == {"delta.appendOnly": "true"}
    assert dt.history()[0]["userName"] == "John Doe"


def test_convert_delta_write_modes(tmp_path: pathlib.Path, sample_data: pa.Table):
    ds.write_dataset(
        sample_data,
        tmp_path,
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )

    convert_to_deltalake(
        tmp_path,
    )

    with pytest.raises(DeltaError):
        convert_to_deltalake(
            tmp_path,
        )

    convert_to_deltalake(tmp_path, mode="ignore")


def test_convert_delta_with_partitioning(tmp_path: pathlib.Path, sample_data: pa.Table):
    ds.write_dataset(
        sample_data,
        tmp_path,
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
        partitioning=["utf8"],
        partitioning_flavor="hive",
    )

    with pytest.raises(
        DeltaError,
        match="Generic error: The schema of partition columns must be provided to convert a Parquet table to a Delta table",
    ):
        convert_to_deltalake(
            tmp_path,
        )
    with pytest.raises(
        ValueError, match="Partition strategy has to be provided with partition_by"
    ):
        convert_to_deltalake(
            tmp_path,
            partition_by=pa.schema([pa.field("utf8", pa.string())]),
        )

    with pytest.raises(
        ValueError,
        match="Currently only `hive` partition strategy is supported to be converted.",
    ):
        convert_to_deltalake(
            tmp_path,
            partition_by=pa.schema([pa.field("utf8", pa.string())]),
            partition_strategy="directory",
        )

    convert_to_deltalake(
        tmp_path,
        partition_by=pa.schema([pa.field("utf8", pa.string())]),
        partition_strategy="hive",
    )
