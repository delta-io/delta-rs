import os
from datetime import date, datetime
from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, DataType, Field, Table

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError

if TYPE_CHECKING:
    import pyarrow as pa


def test_read_cdf_partitioned_with_predicate():
    dt = DeltaTable("../crates/test/tests/data/cdf-table/")
    data = dt.load_cdf(0, 3, predicate="birthday = '2023-12-25'").read_all()

    values = list(set(data["birthday"].to_pylist()))
    assert len(values) == 1
    assert values[0] == date(2023, 12, 25)


def test_read_cdf_partitioned():
    dt = DeltaTable("../crates/test/tests/data/cdf-table/")
    b = dt.load_cdf(0, 3).read_all()
    assert sorted(b["id"].to_pylist()) == [
        1,
        2,
        2,
        2,
        3,
        3,
        3,
        4,
        4,
        4,
        5,
        5,
        5,
        6,
        6,
        6,
        7,
        7,
        7,
        7,
        8,
        9,
        10,
    ]
    assert sorted(b["name"].to_pylist()) == [
        "Ada",
        "Bob",
        "Bob",
        "Bob",
        "Borb",
        "Carl",
        "Carl",
        "Carl",
        "Claire",
        "Dave",
        "Dave",
        "Dave",
        "Dennis",
        "Dennis",
        "Dennis",
        "Dennis",
        "Emily",
        "Emily",
        "Emily",
        "Kate",
        "Kate",
        "Kate",
        "Steve",
    ]
    assert sorted(b["_change_type"].to_pylist()) == [
        "delete",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "update_postimage",
        "update_postimage",
        "update_postimage",
        "update_postimage",
        "update_postimage",
        "update_postimage",
        "update_preimage",
        "update_preimage",
        "update_preimage",
        "update_preimage",
        "update_preimage",
        "update_preimage",
    ]
    assert sorted(b["_commit_version"].to_pylist()) == [
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        2,
        2,
        2,
        2,
        2,
        3,
    ]
    assert sorted(b["_commit_timestamp"].to_pylist()) == [
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 18, 828000),
        datetime(2023, 12, 22, 17, 10, 21, 675000),
        datetime(2023, 12, 22, 17, 10, 21, 675000),
        datetime(2023, 12, 22, 17, 10, 21, 675000),
        datetime(2023, 12, 22, 17, 10, 21, 675000),
        datetime(2023, 12, 22, 17, 10, 21, 675000),
        datetime(2023, 12, 22, 17, 10, 21, 675000),
        datetime(2023, 12, 29, 21, 41, 33, 785000),
        datetime(2023, 12, 29, 21, 41, 33, 785000),
        datetime(2023, 12, 29, 21, 41, 33, 785000),
        datetime(2023, 12, 29, 21, 41, 33, 785000),
        datetime(2023, 12, 29, 21, 41, 33, 785000),
        datetime(2023, 12, 29, 21, 41, 33, 785000),
        datetime(2024, 1, 6, 16, 44, 59, 570000),
    ]
    assert sorted(b["birthday"].to_pylist()) == [
        date(2023, 12, 22),
        date(2023, 12, 22),
        date(2023, 12, 22),
        date(2023, 12, 22),
        date(2023, 12, 23),
        date(2023, 12, 23),
        date(2023, 12, 23),
        date(2023, 12, 23),
        date(2023, 12, 23),
        date(2023, 12, 23),
        date(2023, 12, 24),
        date(2023, 12, 24),
        date(2023, 12, 24),
        date(2023, 12, 24),
        date(2023, 12, 24),
        date(2023, 12, 24),
        date(2023, 12, 25),
        date(2023, 12, 25),
        date(2023, 12, 25),
        date(2023, 12, 29),
        date(2023, 12, 29),
        date(2023, 12, 29),
        date(2023, 12, 29),
    ]


def test_read_cdf_non_partitioned():
    dt = DeltaTable("../crates/test/tests/data/cdf-table-non-partitioned/")
    b = dt.load_cdf(0, 3).read_all()

    assert sorted(b["id"].to_pylist()) == [
        1,
        2,
        2,
        2,
        3,
        3,
        3,
        4,
        4,
        4,
        5,
        5,
        5,
        6,
        6,
        6,
        7,
        7,
        7,
        7,
        8,
        9,
        10,
    ]
    assert sorted(b["name"].to_pylist()) == [
        "Ada",
        "Bob",
        "Bob",
        "Bob",
        "Borb",
        "Carl",
        "Carl",
        "Carl",
        "Claire",
        "Dave",
        "Dave",
        "Dave",
        "Dennis",
        "Dennis",
        "Dennis",
        "Dennis",
        "Emily",
        "Emily",
        "Emily",
        "Kate",
        "Kate",
        "Kate",
        "Steve",
    ]
    assert sorted(b["birthday"].to_pylist()) == [
        date(2024, 4, 14),
        date(2024, 4, 14),
        date(2024, 4, 14),
        date(2024, 4, 14),
        date(2024, 4, 14),
        date(2024, 4, 14),
        date(2024, 4, 14),
        date(2024, 4, 14),
        date(2024, 4, 15),
        date(2024, 4, 15),
        date(2024, 4, 15),
        date(2024, 4, 15),
        date(2024, 4, 15),
        date(2024, 4, 15),
        date(2024, 4, 16),
        date(2024, 4, 16),
        date(2024, 4, 16),
        date(2024, 4, 16),
        date(2024, 4, 16),
        date(2024, 4, 16),
        date(2024, 4, 17),
        date(2024, 4, 17),
        date(2024, 4, 17),
    ]
    assert sorted(b["long_field"].to_pylist()) == [
        1,
        1,
        1,
        1,
        2,
        2,
        2,
        3,
        3,
        3,
        4,
        4,
        4,
        5,
        5,
        5,
        6,
        6,
        6,
        6,
        7,
        8,
        99999999999999999,
    ]
    assert sorted(b["boolean_field"].to_pylist()) == [
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
    ]
    assert sorted(b["double_field"].to_pylist()) == [
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
        3.14,
    ]
    assert sorted(b["smallint_field"].to_pylist()) == [
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
    ]
    assert sorted(b["_change_type"].to_pylist()) == [
        "delete",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "insert",
        "update_postimage",
        "update_postimage",
        "update_postimage",
        "update_postimage",
        "update_postimage",
        "update_postimage",
        "update_preimage",
        "update_preimage",
        "update_preimage",
        "update_preimage",
        "update_preimage",
        "update_preimage",
    ]
    assert sorted(b["_commit_version"].to_pylist()) == [
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        2,
        2,
        2,
        2,
        2,
        3,
    ]
    assert sorted(b["_commit_timestamp"].to_pylist()) == [
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 26, 249000),
        datetime(2024, 4, 14, 15, 58, 29, 393000),
        datetime(2024, 4, 14, 15, 58, 29, 393000),
        datetime(2024, 4, 14, 15, 58, 29, 393000),
        datetime(2024, 4, 14, 15, 58, 29, 393000),
        datetime(2024, 4, 14, 15, 58, 29, 393000),
        datetime(2024, 4, 14, 15, 58, 29, 393000),
        datetime(2024, 4, 14, 15, 58, 31, 257000),
        datetime(2024, 4, 14, 15, 58, 31, 257000),
        datetime(2024, 4, 14, 15, 58, 31, 257000),
        datetime(2024, 4, 14, 15, 58, 31, 257000),
        datetime(2024, 4, 14, 15, 58, 31, 257000),
        datetime(2024, 4, 14, 15, 58, 31, 257000),
        datetime(2024, 4, 14, 15, 58, 32, 495000),
    ]


def test_read_cdf_partitioned_projection():
    dt = DeltaTable("../crates/test/tests/data/cdf-table/")
    columns = ["id", "_change_type", "_commit_version"]
    assert columns == dt.load_cdf(0, 3, columns=columns).schema.names


@pytest.mark.pyarrow
def test_delete_unpartitioned_cdf(tmp_path, sample_data_pyarrow: "pa.Table"):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data_pyarrow,
        mode="append",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    dt = DeltaTable(tmp_path)
    dt.delete("int64 > 2")

    expected_data = (
        ds.dataset(sample_data_pyarrow)
        .to_table(filter=(pc.field("int64") > 2))
        .append_column(
            field_=pa.field("_change_type", pa.string(), nullable=False),
            column=[["delete"] * 2],
        )
    )
    cdc_data = pq.read_table(cdc_path)

    assert os.path.exists(cdc_path), "_change_data doesn't exist"
    assert cdc_data == expected_data


@pytest.mark.pyarrow
def test_delete_partitioned_cdf(tmp_path, sample_data_pyarrow: "pa.Table"):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data_pyarrow,
        mode="overwrite",
        partition_by=["utf8"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    dt = DeltaTable(tmp_path)
    dt.delete("int64 > 2")

    expected_data = (
        ds.dataset(sample_data_pyarrow)
        .to_table(filter=(pc.field("int64") > 2))
        .append_column(
            field_=pa.field("_change_type", pa.string(), nullable=False),
            column=[["delete"] * 2],
        )
    )

    table_schema = pa.schema(dt.schema())
    table_schema = table_schema.insert(
        len(table_schema), pa.field("_change_type", pa.string(), nullable=False)
    )
    cdc_data = pq.read_table(cdc_path, schema=table_schema)

    assert os.path.exists(cdc_path), "_change_data doesn't exist"
    assert len(os.listdir(cdc_path)) == 2
    assert cdc_data == expected_data


@pytest.mark.pyarrow
def test_write_predicate_unpartitioned_cdf(tmp_path, sample_data_pyarrow: "pa.Table"):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data_pyarrow,
        mode="append",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    dt = DeltaTable(tmp_path)
    write_deltalake(
        dt,
        data=ds.dataset(sample_data_pyarrow).to_table(filter=(pc.field("int64") > 2)),
        mode="overwrite",
        predicate="int64 > 2",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    expected_data = pa.concat_tables(
        [
            ds.dataset(sample_data_pyarrow)
            .to_table(filter=(pc.field("int64") > 2))
            .append_column(
                field_=pa.field("_change_type", pa.string(), nullable=False),
                column=[["delete"] * 2],
            ),
            ds.dataset(sample_data_pyarrow)
            .to_table(filter=(pc.field("int64") > 2))
            .append_column(
                field_=pa.field("_change_type", pa.string(), nullable=False),
                column=[["insert"] * 2],
            ),
        ]
    )
    cdc_data = pq.read_table(cdc_path)

    assert os.path.exists(cdc_path), "_change_data doesn't exist"
    assert cdc_data.sort_by([("_change_type", "ascending")]) == expected_data.sort_by(
        [("_change_type", "ascending")]
    )
    assert dt.to_pyarrow_table().sort_by([("utf8", "ascending")]) == sample_data_pyarrow


@pytest.mark.pyarrow
def test_write_predicate_partitioned_cdf(tmp_path, sample_data_pyarrow: "pa.Table"):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data_pyarrow,
        mode="overwrite",
        partition_by=["utf8"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    dt = DeltaTable(tmp_path)
    write_deltalake(
        dt,
        data=ds.dataset(sample_data_pyarrow).to_table(filter=(pc.field("int64") > 3)),
        mode="overwrite",
        predicate="int64 > 3",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    expected_data = pa.concat_tables(
        [
            ds.dataset(sample_data_pyarrow)
            .to_table(filter=(pc.field("int64") > 3))
            .append_column(
                field_=pa.field("_change_type", pa.string(), nullable=False),
                column=[["delete"] * 1],
            ),
            ds.dataset(sample_data_pyarrow)
            .to_table(filter=(pc.field("int64") > 3))
            .append_column(
                field_=pa.field("_change_type", pa.string(), nullable=False),
                column=[["insert"] * 1],
            ),
        ]
    )

    table_schema = pa.schema(dt.schema())
    table_schema = table_schema.insert(
        len(table_schema), pa.field("_change_type", pa.string(), nullable=False)
    )
    cdc_data = pq.read_table(cdc_path, schema=table_schema)

    assert os.path.exists(cdc_path), "_change_data doesn't exist"
    assert len(os.listdir(cdc_path)) == 1
    expected_data = expected_data.combine_chunks().sort_by(
        [("_change_type", "ascending")]
    )
    cdc_data = cdc_data.combine_chunks().sort_by([("_change_type", "ascending")])

    assert expected_data == cdc_data
    assert dt.to_pyarrow_table().sort_by([("utf8", "ascending")]) == sample_data_pyarrow


@pytest.mark.pyarrow
def test_write_overwrite_unpartitioned_cdf(tmp_path, sample_data_pyarrow: "pa.Table"):
    import pyarrow as pa
    import pyarrow.dataset as ds

    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data_pyarrow,
        mode="append",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    dt = DeltaTable(tmp_path)
    write_deltalake(
        dt,
        data=ds.dataset(sample_data_pyarrow).to_table(),
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    sort_values = [("_change_type", "ascending"), ("utf8", "ascending")]
    expected_data = (
        ds.dataset(pa.concat_tables([sample_data_pyarrow] * 3))
        .to_table()
        .append_column(
            field_=pa.field("_change_type", pa.string(), nullable=True),
            column=[["delete"] * 5 + ["insert"] * 10],
        )
    ).sort_by(sort_values)

    assert not os.path.exists(cdc_path), (
        "_change_data shouldn't exist since table was overwritten"
    )

    tbl = dt.load_cdf().read_all()

    select_cols = [
        col
        for col in tbl.column_names
        if col not in ["_commit_version", "_commit_timestamp"]
    ]
    assert pa.table(tbl.select(select_cols)).sort_by(sort_values) == expected_data
    assert dt.to_pyarrow_table().sort_by([("utf8", "ascending")]) == sample_data_pyarrow


@pytest.mark.pyarrow
def test_write_overwrite_partitioned_cdf(tmp_path, sample_data_pyarrow: "pa.Table"):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data_pyarrow,
        mode="append",
        partition_by=["int64"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    batch2 = ds.dataset(sample_data_pyarrow).to_table(filter=(pc.field("int64") > 3))

    dt = DeltaTable(tmp_path)
    write_deltalake(
        dt,
        data=batch2,
        mode="overwrite",
        predicate="int64 > 3",
        partition_by=["int64"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    table_schema = pa.schema(dt.schema())
    table_schema = table_schema.insert(
        len(table_schema), pa.field("_change_type", pa.string(), nullable=False)
    )

    sort_values = [("_change_type", "ascending"), ("utf8", "ascending")]

    first_batch = sample_data_pyarrow.append_column(
        field_=pa.field("_change_type", pa.string(), nullable=True),
        column=[["insert"] * 5],
    )

    expected_data = pa.concat_tables([batch2] * 2).append_column(
        field_=pa.field("_change_type", pa.string(), nullable=True),
        column=[["delete", "insert"]],
    )

    assert not os.path.exists(cdc_path), (
        "_change_data shouldn't exist since a specific partition was overwritten"
    )

    assert pa.table(dt.load_cdf().read_all()).drop_columns(
        ["_commit_version", "_commit_timestamp"]
    ).sort_by(sort_values).select(expected_data.column_names) == pa.concat_tables(
        [first_batch, expected_data]
    ).sort_by(sort_values)


def test_read_cdf_version_out_of_range():
    dt = DeltaTable("../crates/test/tests/data/cdf-table/")

    with pytest.raises(DeltaError) as e:
        dt.load_cdf(4).read_all()

    assert "invalid table version: 4" in str(e).lower()


def test_read_cdf_version_out_of_range_with_flag():
    dt = DeltaTable("../crates/test/tests/data/cdf-table/")
    b = dt.load_cdf(4, allow_out_of_range=True).read_all()

    assert len(b) == 0


def test_read_timestamp_cdf_out_of_range():
    dt = DeltaTable("../crates/test/tests/data/cdf-table/")
    start = "2033-12-22T17:10:21.675Z"

    with pytest.raises(DeltaError) as e:
        dt.load_cdf(starting_timestamp=start).read_all()

    assert "is greater than latest commit timestamp" in str(e).lower()


def test_read_timestamp_cdf_out_of_range_with_flag():
    dt = DeltaTable("../crates/test/tests/data/cdf-table/")

    start = "2033-12-22T17:10:21.675Z"
    b = dt.load_cdf(starting_timestamp=start, allow_out_of_range=True).read_all()

    assert len(b) == 0


def test_read_cdf_last_version(tmp_path):
    data = Table.from_pydict(
        {"foo": Array([1, 2, 3], type=Field("foo", DataType.int32(), nullable=True))}
    )

    expected = Table.from_pydict(
        {
            "foo": Array([1, 2, 3], type=Field("foo", DataType.int32(), nullable=True)),
            "_change_type": Array(
                ["insert", "insert", "insert"],
                type=Field("foo", DataType.string(), nullable=True),
            ),
            "_commit_version": Array(
                [0, 0, 0], type=Field("foo", DataType.int64(), nullable=True)
            ),
        }
    )

    write_deltalake(
        tmp_path,
        data=data,
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    data = (
        DeltaTable(tmp_path)
        .load_cdf(
            starting_version=0,
            ending_version=0,
            allow_out_of_range=False,
            columns=["foo", "_change_type", "_commit_version"],
        )
        .read_all()
    )

    assert expected == data
