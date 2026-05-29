"""Tests for HuggingFace Hub URI support and CDC (content-defined chunking) options.

CDC options are standard Delta table properties and work automatically when set —
no special API is required beyond passing them in the ``configuration`` dict.

HF credentials are passed as ``storage_options={"hf.token": ...}`` to
``write_deltalake`` / ``DeltaTable``.  Tests requiring live HF credentials are
skipped when HF_TOKEN or HF_BUCKET is not set.
"""

import os

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.query import QueryBuilder

# ---------------------------------------------------------------------------
# Marks
# ---------------------------------------------------------------------------


def _hf_token() -> str | None:
    """huggingface_hub.get_token() reads HF_TOKEN env, then ~/.cache/huggingface/token."""
    try:
        from huggingface_hub import get_token
    except ImportError:
        return None
    return get_token()


requires_hf = pytest.mark.skipif(
    not _hf_token() or not os.environ.get("HF_BUCKET"),
    reason="No HF token (HF_TOKEN env or `hf auth login`) or HF_BUCKET env not set",
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# CDC tests — run without any external credentials
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# HF + CDC integration tests — skipped when credentials are absent
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def hf_table_uri() -> str:
    """Unique HF path for this test run; avoids cross-run collisions."""
    bucket = os.environ["HF_BUCKET"]
    return f"hf://buckets/{bucket}/delta-ci-{os.getpid()}"


@pytest.fixture(scope="module")
def hf_storage_options() -> dict:
    return {"hf.token": _hf_token()}


@requires_hf
def test_hf_write_and_read(hf_table_uri, hf_storage_options, sample_table):
    """Write a Delta table to HF Hub and read it back."""
    write_deltalake(
        hf_table_uri,
        sample_table,
        storage_options=hf_storage_options,
    )

    dt = DeltaTable(hf_table_uri, storage_options=hf_storage_options)
    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert result.num_rows == len(sample_table)


@requires_hf
def test_hf_cdc_write_and_read(hf_table_uri, hf_storage_options, sample_table):
    """Write a CDC-enabled Delta table to HF Hub, read it back, and verify format options."""
    uri = hf_table_uri + "-cdc"
    write_deltalake(
        uri,
        sample_table,
        storage_options=hf_storage_options,
        format_options={"contentDefinedChunking.enabled": "true"},
    )

    dt = DeltaTable(uri, storage_options=hf_storage_options)
    opts = dt.metadata().format_options
    assert opts.get("contentDefinedChunking.enabled") == "true"

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert result.num_rows == len(sample_table)


@requires_hf
def test_hf_append(hf_table_uri, hf_storage_options, sample_table):
    """Append to an existing HF Delta table and verify row counts."""
    uri = hf_table_uri + "-append"
    write_deltalake(uri, sample_table, storage_options=hf_storage_options)
    write_deltalake(
        uri, sample_table, mode="append", storage_options=hf_storage_options
    )

    dt = DeltaTable(uri, storage_options=hf_storage_options)
    assert dt.version() == 1
    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert result.num_rows == 2 * len(sample_table)
