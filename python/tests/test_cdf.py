import os
from datetime import date, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from deltalake import DeltaTable, write_deltalake


def test_read_cdf_partitioned():
    dt = DeltaTable("../crates/test/tests/data/cdf-table/")
    b = dt.load_cdf(0, 3).read_all().to_pydict()
    assert sorted(b["id"]) == [
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
    assert sorted(b["name"]) == [
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
    assert sorted(b["_change_type"]) == [
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
    assert sorted(b["_commit_version"]) == [
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
    assert sorted(b["_commit_timestamp"]) == [
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
    assert sorted(b["birthday"]) == [
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
    b = dt.load_cdf(0, 3).read_all().to_pydict()

    assert sorted(b["id"]) == [
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
    assert sorted(b["name"]) == [
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
    assert sorted(b["birthday"]) == [
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
    assert sorted(b["long_field"]) == [
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
    assert sorted(b["boolean_field"]) == [
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
    assert sorted(b["double_field"]) == [
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
    assert sorted(b["smallint_field"]) == [
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
    assert sorted(b["_change_type"]) == [
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
    assert sorted(b["_commit_version"]) == [
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
    assert sorted(b["_commit_timestamp"]) == [
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


def test_delete_unpartitioned_cdf(tmp_path, sample_data: pa.Table):
    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data,
        mode="append",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    dt = DeltaTable(tmp_path)
    dt.delete("int64 > 2")

    expected_data = (
        ds.dataset(sample_data)
        .to_table(filter=(pc.field("int64") > 2))
        .append_column(
            field_=pa.field("_change_type", pa.string(), nullable=False),
            column=[["delete"] * 2],
        )
    )
    cdc_data = pq.read_table(cdc_path)

    assert os.path.exists(cdc_path), "_change_data doesn't exist"
    assert cdc_data == expected_data


def test_delete_partitioned_cdf(tmp_path, sample_data: pa.Table):
    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data,
        mode="overwrite",
        partition_by=["utf8"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    dt = DeltaTable(tmp_path)
    dt.delete("int64 > 2")

    expected_data = (
        ds.dataset(sample_data)
        .to_table(filter=(pc.field("int64") > 2))
        .append_column(
            field_=pa.field("_change_type", pa.string(), nullable=False),
            column=[["delete"] * 2],
        )
    )
    table_schema = dt.schema().to_pyarrow()
    table_schema = table_schema.insert(
        len(table_schema), pa.field("_change_type", pa.string(), nullable=False)
    )
    cdc_data = pq.read_table(cdc_path, schema=table_schema)

    assert os.path.exists(cdc_path), "_change_data doesn't exist"
    assert len(os.listdir(cdc_path)) == 2
    assert cdc_data == expected_data


def test_write_predicate_unpartitioned_cdf(tmp_path, sample_data: pa.Table):
    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data,
        mode="append",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    dt = DeltaTable(tmp_path)
    write_deltalake(
        dt,
        data=ds.dataset(sample_data).to_table(filter=(pc.field("int64") > 2)),
        mode="overwrite",
        predicate="int64 > 2",
        engine="rust",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    expected_data = pa.concat_tables(
        [
            ds.dataset(sample_data)
            .to_table(filter=(pc.field("int64") > 2))
            .append_column(
                field_=pa.field("_change_type", pa.string(), nullable=False),
                column=[["delete"] * 2],
            ),
            ds.dataset(sample_data)
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
    assert dt.to_pyarrow_table().sort_by([("utf8", "ascending")]) == sample_data


def test_write_predicate_partitioned_cdf(tmp_path, sample_data: pa.Table):
    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data,
        mode="overwrite",
        partition_by=["utf8"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    dt = DeltaTable(tmp_path)
    write_deltalake(
        dt,
        data=ds.dataset(sample_data).to_table(filter=(pc.field("int64") > 3)),
        mode="overwrite",
        predicate="int64 > 3",
        engine="rust",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    expected_data = pa.concat_tables(
        [
            ds.dataset(sample_data)
            .to_table(filter=(pc.field("int64") > 3))
            .append_column(
                field_=pa.field("_change_type", pa.string(), nullable=False),
                column=[["delete"] * 1],
            ),
            ds.dataset(sample_data)
            .to_table(filter=(pc.field("int64") > 3))
            .append_column(
                field_=pa.field("_change_type", pa.string(), nullable=False),
                column=[["insert"] * 1],
            ),
        ]
    )

    table_schema = dt.schema().to_pyarrow()
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
    assert dt.to_pyarrow_table().sort_by([("utf8", "ascending")]) == sample_data


def test_write_overwrite_unpartitioned_cdf(tmp_path, sample_data: pa.Table):
    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data,
        mode="append",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    dt = DeltaTable(tmp_path)
    write_deltalake(
        dt,
        data=ds.dataset(sample_data).to_table(),
        mode="overwrite",
        engine="rust",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    sort_values = [("_change_type", "ascending"), ("utf8", "ascending")]
    expected_data = (
        ds.dataset(pa.concat_tables([sample_data] * 3))
        .to_table()
        .append_column(
            field_=pa.field("_change_type", pa.string(), nullable=True),
            column=[["delete"] * 5 + ["insert"] * 10],
        )
    ).sort_by(sort_values)

    assert not os.path.exists(
        cdc_path
    ), "_change_data shouldn't exist since table was overwritten"

    ## TODO(ion): check if you see insert and deletes in commit version 1

    assert (
        dt.load_cdf()
        .read_all()
        .drop_columns(["_commit_version", "_commit_timestamp"])
        .sort_by(sort_values)
        == expected_data
    )
    assert dt.to_pyarrow_table().sort_by([("utf8", "ascending")]) == sample_data


def test_write_overwrite_partitioned_cdf(tmp_path, sample_data: pa.Table):
    cdc_path = f"{tmp_path}/_change_data"

    write_deltalake(
        tmp_path,
        sample_data,
        mode="append",
        partition_by=["int64"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    batch2 = ds.dataset(sample_data).to_table(filter=(pc.field("int64") > 3))

    dt = DeltaTable(tmp_path)
    write_deltalake(
        dt,
        data=batch2,
        engine="rust",
        mode="overwrite",
        predicate="int64 > 3",
        partition_by=["int64"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    table_schema = dt.schema().to_pyarrow()
    table_schema = table_schema.insert(
        len(table_schema), pa.field("_change_type", pa.string(), nullable=False)
    )

    sort_values = [("_change_type", "ascending"), ("utf8", "ascending")]

    first_batch = sample_data.append_column(
        field_=pa.field("_change_type", pa.string(), nullable=True),
        column=[["insert"] * 5],
    )

    expected_data = pa.concat_tables([batch2] * 2).append_column(
        field_=pa.field("_change_type", pa.string(), nullable=True),
        column=[["delete", "insert"]],
    )

    assert not os.path.exists(
        cdc_path
    ), "_change_data shouldn't exist since a specific partition was overwritten"

    assert dt.load_cdf().read_all().drop_columns(
        ["_commit_version", "_commit_timestamp"]
    ).sort_by(sort_values).select(expected_data.column_names) == pa.concat_tables(
        [first_batch, expected_data]
    ).sort_by(sort_values)
