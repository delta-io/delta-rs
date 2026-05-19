"""Tests for HuggingFace Hub URI support and CDC (content-defined chunking) options.

CDC options are standard Delta table properties and work automatically when set —
no special API is required beyond passing them in the ``configuration`` dict.

HF credentials are passed as ``storage_options={"hf.token": ...}`` to
``write_deltalake`` / ``DeltaTable``.  Tests requiring live HF credentials are
skipped when HF_TOKEN or HF_DATASET is not set.
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
    not _hf_token() or not os.environ.get("HF_DATASET"),
    reason="No HF token (HF_TOKEN env or `hf auth login`) or HF_DATASET env not set",
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


def test_cdc_table_properties_are_persisted(tmp_path, sample_table):
    """Table properties with CDC options are stored and returned as-is."""
    path = str(tmp_path)
    write_deltalake(
        path,
        sample_table,
        configuration={
            "delta.parquet.contentDefinedChunking.enabled": "true",
            "delta.parquet.contentDefinedChunking.minChunkSize": "65536",
            "delta.parquet.contentDefinedChunking.maxChunkSize": "524288",
            "delta.parquet.contentDefinedChunking.normLevel": "2",
        },
    )

    dt = DeltaTable(path)
    cfg = dt.metadata().configuration
    assert cfg.get("delta.parquet.contentDefinedChunking.enabled") == "true"
    assert cfg.get("delta.parquet.contentDefinedChunking.minChunkSize") == "65536"
    assert cfg.get("delta.parquet.contentDefinedChunking.maxChunkSize") == "524288"
    assert cfg.get("delta.parquet.contentDefinedChunking.normLevel") == "2"


def test_cdc_write_and_read(tmp_path, sample_table):
    """A CDC-enabled table can be written and read back correctly."""
    path = str(tmp_path)
    write_deltalake(
        path,
        sample_table,
        configuration={"delta.parquet.contentDefinedChunking.enabled": "true"},
    )

    dt = DeltaTable(path)
    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert result.num_rows == len(sample_table)


def test_cdc_append_preserves_properties(tmp_path, sample_table):
    """Appending to a CDC-enabled table preserves CDC properties across versions."""
    path = str(tmp_path)
    write_deltalake(
        path,
        sample_table,
        configuration={"delta.parquet.contentDefinedChunking.enabled": "true"},
    )
    write_deltalake(path, sample_table, mode="append")

    dt = DeltaTable(path)
    assert dt.version() == 1
    cfg = dt.metadata().configuration
    assert cfg.get("delta.parquet.contentDefinedChunking.enabled") == "true"

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert result.num_rows == 2 * len(sample_table)


# ---------------------------------------------------------------------------
# HF + CDC integration tests — skipped when credentials are absent
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def hf_table_uri() -> str:
    """Unique HF path for this test run; avoids cross-run collisions."""
    dataset = os.environ["HF_DATASET"]
    return f"hf://datasets/{dataset}/delta-ci-{os.getpid()}"


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
    """Write a CDC-enabled Delta table to HF Hub, read it back, and verify properties."""
    uri = hf_table_uri + "-cdc"
    write_deltalake(
        uri,
        sample_table,
        storage_options=hf_storage_options,
        configuration={"delta.parquet.contentDefinedChunking.enabled": "true"},
    )

    dt = DeltaTable(uri, storage_options=hf_storage_options)
    cfg = dt.metadata().configuration
    assert cfg.get("delta.parquet.contentDefinedChunking.enabled") == "true"

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
