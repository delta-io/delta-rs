import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from deltalake import (
    BloomFilterProperties,
    ColumnProperties,
    DeltaTable,
    WriterProperties,
    write_deltalake,
)


def test_writer_properties_all_filled():
    wp = WriterProperties(
        data_page_size_limit=100,
        dictionary_page_size_limit=200,
        data_page_row_count_limit=300,
        write_batch_size=400,
        max_row_group_size=500,
        compression="SNAPPY",
        statistics_truncate_length=600,
        default_column_properties=ColumnProperties(
            dictionary_enabled=False,
        ),
        column_properties={
            "a": ColumnProperties(
                dictionary_enabled=True,
                max_statistics_size=40,
                bloom_filter_properties=BloomFilterProperties(
                    set_bloom_filter_enabled=True, fpp=0.2, ndv=30
                ),
            ),
            "b": ColumnProperties(
                dictionary_enabled=True,
                max_statistics_size=400,
                bloom_filter_properties=BloomFilterProperties(
                    set_bloom_filter_enabled=False, fpp=0.2, ndv=30
                ),
            ),
        },
    )

    assert wp.default_column_properties.bloom_filter_properties is None
    assert wp.column_properties["a"].bloom_filter_properties.fpp == 0.2


def test_writer_properties_lower_case_compression():
    wp = WriterProperties(compression="snappy")  # type: ignore

    assert wp.compression == "SNAPPY"


@pytest.mark.parametrize(
    "compression,expected",
    [("GZIP", "GZIP(6)"), ("BROTLI", "BROTLI(1)"), ("ZSTD", "ZSTD(1)")],
)
def test_writer_properties_missing_compression_level(compression, expected):
    wp = WriterProperties(compression=compression)

    assert wp.compression == expected


@pytest.mark.parametrize(
    "compression,wrong_level",
    [
        ("GZIP", -1),
        ("GZIP", 11),
        ("BROTLI", -1),
        ("BROTLI", 12),
        ("ZSTD", 0),
        ("ZSTD", 23),
    ],
)
def test_writer_properties_incorrect_level_range(compression, wrong_level):
    with pytest.raises(ValueError):
        WriterProperties(compression=compression, compression_level=wrong_level)


def test_writer_properties_no_compression():
    with pytest.raises(ValueError):
        WriterProperties(compression_level=10)


def test_invalid_fpp_value():
    with pytest.raises(ValueError):
        BloomFilterProperties(set_bloom_filter_enabled=True, fpp=1.1, ndv=30)


def test_write_with_writerproperties(
    tmp_path: pathlib.Path, sample_table: pa.Table, writer_properties: WriterProperties
):
    write_deltalake(
        tmp_path, sample_table, engine="rust", writer_properties=writer_properties
    )

    parquet_path = DeltaTable(tmp_path).file_uris()[0]
    metadata = pq.read_metadata(parquet_path)

    assert metadata.to_dict()["row_groups"][0]["columns"][0]["compression"] == "GZIP"
