"""Tests for Metadata.format.options, e.g. Parquet CDC (content-defined chunking) settings.

Format options are a Parquet-format concern per the Delta spec, so they live on
``Metadata.format.options`` rather than in the table-level ``configuration`` map.
"""

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.query import QueryBuilder


@pytest.fixture(scope="module")
def sample_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array(list(range(1000)), type=pa.int32()),
            "payload": pa.array(
                [f"row-{i:06d}" for i in range(1000)], type=pa.large_utf8()
            ),
        }
    )


def test_cdc_format_options_are_persisted(tmp_path, sample_table):
    """Format options with CDC settings are stored on Metadata.format.options."""
    path = str(tmp_path)
    write_deltalake(
        path,
        sample_table,
        format_options={
            "contentDefinedChunking.enabled": "true",
            "contentDefinedChunking.minChunkSize": "65536",
            "contentDefinedChunking.maxChunkSize": "524288",
            "contentDefinedChunking.normLevel": "2",
        },
    )

    dt = DeltaTable(path)
    opts = dt.metadata().format_options
    assert opts.get("contentDefinedChunking.enabled") == "true"
    assert opts.get("contentDefinedChunking.minChunkSize") == "65536"
    assert opts.get("contentDefinedChunking.maxChunkSize") == "524288"
    assert opts.get("contentDefinedChunking.normLevel") == "2"


def test_cdc_write_and_read(tmp_path, sample_table):
    """A CDC-enabled table can be written and read back correctly."""
    path = str(tmp_path)
    write_deltalake(
        path,
        sample_table,
        format_options={"contentDefinedChunking.enabled": "true"},
    )

    dt = DeltaTable(path)
    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert result.num_rows == len(sample_table)


def test_cdc_append_preserves_format_options(tmp_path, sample_table):
    """Appending to a CDC-enabled table preserves CDC format options across versions."""
    path = str(tmp_path)
    write_deltalake(
        path,
        sample_table,
        format_options={"contentDefinedChunking.enabled": "true"},
    )
    write_deltalake(path, sample_table, mode="append")

    dt = DeltaTable(path)
    assert dt.version() == 1
    opts = dt.metadata().format_options
    assert opts.get("contentDefinedChunking.enabled") == "true"

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert result.num_rows == 2 * len(sample_table)
