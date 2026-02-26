import datetime
import os
import pathlib
from decimal import Decimal

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import (
    CommitProperties,
    DeltaTable,
    Field,
    Schema,
    write_deltalake,
)
from deltalake.query import QueryBuilder
from deltalake.schema import PrimitiveType


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_matched_delete_wo_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(["5"], ArrowField("id", DataType.string_view(), nullable=True)),
            "weight": Array([105], ArrowField("id", DataType.int32(), nullable=True)),
        }
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        commit_properties=commit_properties,
        streamed_exec=streaming,
    ).when_matched_delete().execute()

    nrows = 4

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                list(range(nrows)),
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                list(range(nrows)),
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * nrows,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        },
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert last_action["userName"] == "John Doe"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_matched_delete_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["5", "4"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "weight": Array(
                [1, 2],
                ArrowField("weight", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [1, 2],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [True, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "customer": Array(
                ["Adam", "Patrick"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        },
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        streamed_exec=streaming,
    ).when_matched_delete("s.deleted = True").execute()

    nrows = 4
    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                list(range(nrows)),
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                list(range(nrows)),
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * nrows,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        },
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_matched_update_wo_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
        },
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        streamed_exec=streaming,
    ).when_matched_update({"price": "s.price", "sold": "s.sold"}).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 5,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        },
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_matched_update_wo_predicate_with_schema_evolution(
    tmp_path: pathlib.Path, sample_table: Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "customer": Array(
                ["john", "doe"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        },
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        merge_schema=True,
    ).when_matched_update(
        {"price": "s.price", "sold": "s.sold+int'10'", "customer": "s.customer"}
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 20, 30],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 5,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "customer": Array(
                [None, None, None, "john", "doe"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        },
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result.schema == expected.schema
    assert result == expected


def test_merge_when_matched_update_wo_predicate_and_insert_with_schema_evolution(
    tmp_path: pathlib.Path, sample_table: Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "customer": Array(
                ["john", "doe"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        },
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        merge_schema=True,
    ).when_matched_update(
        {"price": "s.price", "sold": "s.sold+int'10'", "customer": "s.customer"}
    ).when_not_matched_insert_all().execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 20, 30],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 5,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "customer": Array(
                [None, None, None, "john", "doe"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        },
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result.schema == expected.schema
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_matched_update_all_wo_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [True, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "weight": Array(
                [10, 15],
                ArrowField("weight", type=DataType.int64(), nullable=True),
            ),
        },
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        streamed_exec=streaming,
    ).when_matched_update_all().execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, True, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        },
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_matched_update_all_with_exclude(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [15, 25],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [True, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "weight": Array(
                [10, 15],
                ArrowField("weight", type=DataType.int64(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        streamed_exec=streaming,
    ).when_matched_update_all(except_cols=["sold"]).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, True, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )
    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_matched_update_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_matched_update(
        updates={"price": "source.price", "sold": "source.sold"},
        predicate="source.deleted = False",
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 10, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 10, 4],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 5,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )
    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_insert_wo_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "6"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        }
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5", "6"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 6,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_insert_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "10"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        },
        predicate="source.price < 50",
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5", "6"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4, 10],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4, 10],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 6,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_not_matched_insert_with_predicate_schema_evolution(
    tmp_path: pathlib.Path, sample_table: Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "10"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "customer": Array(
                ["john", "doe"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
            "deleted": Array(
                [False, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        merge_schema=True,
        predicate="target.id = source.id",
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "customer": "source.customer",
            "deleted": "False",
        },
        predicate="source.price < 50",
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5", "6"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4, 10],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4, 10],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 6,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "customer": Array(
                [None, None, None, None, None, "john"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result.schema == expected.schema
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_insert_all_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "10"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [None, None],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_insert_all(
        predicate="source.price < 50",
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5", "6"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4, 10],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4, 10],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, False, False, None],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_insert_all_with_exclude(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "9"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [None, None],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_insert_all(except_cols=["sold"]).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5", "6", "9"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4, None, None],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, False, False, None, None],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_not_matched_insert_all_with_exclude_and_with_schema_evo(
    tmp_path: pathlib.Path, sample_table: Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "9"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [None, None],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "customer": Array(
                ["john", "doe"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        merge_schema=True,
        predicate="target.id = source.id",
    ).when_not_matched_insert_all(except_cols=["sold"]).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5", "6", "9"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4, None, None],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, False, False, None, None],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "customer": Array(
                [None, None, None, None, None, "john", "doe"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result.schema == expected.schema
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_insert_all_with_predicate_special_column_names(
    tmp_path: pathlib.Path, sample_table_with_spaces_numbers: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table_with_spaces_numbers, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "1id": Array(
                ["6", "10"],
                ArrowField("1id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold items": Array(
                [10, 20],
                ArrowField("sold items", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [None, None],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.`1id` = source.`1id`",
        streamed_exec=streaming,
    ).when_not_matched_insert_all(
        predicate="source.price < 50",
    ).execute()

    expected = Table(
        {
            "1id": Array(
                ["1", "2", "3", "4", "5", "6"],
                ArrowField("1id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4, 10],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold items": Array(
                [0, 1, 2, 3, 4, 10],
                ArrowField("sold items", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, False, False, None],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by `1id` asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_by_source_update_wo_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "7"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_by_source_update(
        updates={
            "sold": "int'10'",
        }
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 10, 10, 10, 10],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 5,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_by_source_update_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "7"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_by_source_update(
        updates={
            "sold": "int'10'",
        },
        predicate="target.price > 3",
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 10],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 5,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_by_source_delete_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "7"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_by_source_delete(predicate="target.price > 3").execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 4,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_when_not_matched_by_source_delete_wo_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "weight": Array(
                [1.5, 1.6],
                ArrowField("weight", type=DataType.float64(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_by_source_delete().execute()

    expected = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [3, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [3, 4],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 2,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_multiple_when_matched_update_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_matched_update(
        updates={"price": "source.price", "sold": "source.sold"},
        predicate="source.deleted = False",
    ).when_matched_update(
        updates={"price": "source.price", "sold": "source.sold"},
        predicate="source.deleted = True",
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 5,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_multiple_when_matched_update_all_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_matched_update_all(
        predicate="source.deleted = False",
    ).when_matched_update_all(
        predicate="source.deleted = True",
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, False, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_multiple_when_not_matched_insert_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "9"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        },
        predicate="source.price < 50",
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        },
        predicate="source.price > 50",
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5", "6", "9"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4, 10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4, 10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 7,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_multiple_when_matched_delete_with_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["5", "4"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "weight": Array(
                [1, 2],
                ArrowField("weight", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [1, 2],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [True, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
            "customer": Array(
                ["Adam", "Patrick"],
                ArrowField("customer", type=DataType.string_view(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        streamed_exec=streaming,
    ).when_matched_delete("s.deleted = True").when_matched_delete(
        "s.deleted = false"
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                list(range(3)),
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                list(range(3)),
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 3,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_multiple_when_not_matched_by_source_update_wo_predicate(
    tmp_path: pathlib.Path, sample_table: Table, streaming: bool
):
    """The first match clause that meets the predicate will be executed, so if you do an update
    without an other predicate the first clause will be matched and therefore the other ones are skipped.
    """
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["6", "7"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [10, 100],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False, False],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
        streamed_exec=streaming,
    ).when_not_matched_by_source_update(
        updates={
            "sold": "int'10'",
        }
    ).when_not_matched_by_source_update(
        updates={
            "sold": "int'100'",
        }
    ).execute()

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 10, 10, 10, 10],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * 5,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


@pytest.mark.pyarrow
@pytest.mark.parametrize("streaming", (True, False))
def test_merge_date_partitioned_2344(tmp_path: pathlib.Path, streaming: bool):
    from datetime import date

    import pyarrow as pa

    schema = Schema(
        fields=[
            Field("date", PrimitiveType("date")),
            Field("foo", PrimitiveType("string")),
            Field("bar", PrimitiveType("string")),
        ]
    )

    dt = DeltaTable.create(
        tmp_path, schema=schema, partition_by=["date"], mode="overwrite"
    )

    data = pa.table(
        {
            "date": pa.array([date(2022, 2, 1)]),
            "foo": pa.array(["hello"]),
            "bar": pa.array(["world"]),
        }
    )

    expected = data
    dt.merge(
        data,
        predicate="s.date = t.date",
        source_alias="s",
        target_alias="t",
        streamed_exec=streaming,
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected
    if not streaming:
        assert (
            last_action["operationParameters"].get("predicate")
            == "date = '2022-02-01'::date"
        )
    else:
        # In streaming mode we don't use aggregated stats of the source in the predicate
        assert last_action["operationParameters"].get("predicate") is None


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "timezone,predicate",
    [
        (
            None,
            "datetime = arrow_cast('2022-02-01T00:00:00.000000', 'Timestamp(Microsecond, None)')",
        ),
        (
            "UTC",
            "datetime = arrow_cast('2022-02-01T00:00:00.000000', 'Timestamp(Microsecond, Some(\"UTC\"))')",
        ),
    ],
)
def test_merge_timestamps_partitioned_2344(tmp_path: pathlib.Path, timezone, predicate):
    from datetime import datetime

    import pyarrow as pa

    schema = Schema(
        fields=[
            Field(
                "datetime",
                PrimitiveType("timestamp")
                if timezone
                else PrimitiveType("timestamp_ntz"),
            ),
            Field("foo", PrimitiveType("string")),
            Field("bar", PrimitiveType("string")),
        ]
    )

    dt = DeltaTable.create(
        tmp_path, schema=schema, partition_by=["datetime"], mode="overwrite"
    )

    data = pa.table(
        {
            "datetime": pa.array(
                [datetime(2022, 2, 1)], pa.timestamp("us", tz=timezone)
            ),
            "foo": pa.array(["hello"]),
            "bar": pa.array(["world"]),
        }
    )

    dt.merge(
        data,
        predicate="s.datetime = t.datetime",
        source_alias="s",
        target_alias="t",
        streamed_exec=False,  # only with streamed execution off can we use stats to create a pruning predicate
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == data
    assert last_action["operationParameters"].get("predicate") == predicate


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_stats_columns_stats_provided(tmp_path: pathlib.Path, streaming: bool):
    data = Table(
        {
            "foo": Array(
                ["a", "b", None, None],
                ArrowField("foo", type=DataType.string_view(), nullable=True),
            ),
            "bar": Array(
                [1, 2, 3, None],
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "baz": Array(
                [1, 1, None, None],
                ArrowField("baz", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(
        tmp_path,
        data,
        mode="append",
        configuration={"delta.dataSkippingStatsColumns": "foo,baz"},
    )
    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)

    def get_value(name: str):
        return add_actions_table.column(name)[0].as_py()

    # x1 has no max, since inf was the highest value
    assert get_value("null_count.foo") == 2
    assert get_value("min.foo") == "a"
    assert get_value("max.foo") == "b"
    assert get_value("null_count.baz") == 2
    assert get_value("min.baz") == 1
    assert get_value("max.baz") == 1

    with pytest.raises(Exception):
        get_value("null_count.bar")

    data = Table(
        {
            "foo": Array(
                ["a"],
                ArrowField("foo", type=DataType.string_view(), nullable=True),
            ),
            "bar": Array(
                [10],
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "baz": Array(
                [10],
                ArrowField("baz", type=DataType.int64(), nullable=True),
            ),
        }
    )

    dt.merge(
        data,
        predicate="source.foo = target.foo",
        source_alias="source",
        target_alias="target",
        streamed_exec=streaming,
    ).when_matched_update_all().execute()

    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)

    assert dt.version() == 1

    def get_value(name: str):
        return add_actions_table.column(name)[0].as_py()

    # x1 has no max, since inf was the highest value
    assert get_value("null_count.foo") == 2
    assert get_value("min.foo") == "a"
    assert get_value("max.foo") == "b"
    assert get_value("null_count.baz") == 2
    assert get_value("min.baz") == 1
    assert get_value("max.baz") == 10

    with pytest.raises(Exception):
        get_value("null_count.bar")


def test_merge_field_special_characters_delete_2438(tmp_path: pathlib.Path):
    ## See issue: https://github.com/delta-io/delta-rs/issues/2438
    data = Table(
        {
            "x": Array(
                [1, 2, 3],
                ArrowField("x", type=DataType.int64(), nullable=True),
            ),
            "y--1": Array(
                [4, 5, 6],
                ArrowField("y--1", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data, mode="append")

    dt = DeltaTable(tmp_path)
    new_data = Table(
        {
            "x": Array(
                [2, 3],
                ArrowField("x", type=DataType.int64(), nullable=True),
            ),
        }
    )

    (
        dt.merge(
            source=new_data,
            predicate="target.x = source.x",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_delete()
        .execute()
    )

    expected = Table(
        {
            "x": Array(
                [1],
                ArrowField("x", type=DataType.int64(), nullable=True),
            ),
            "y--1": Array(
                [4],
                ArrowField("y--1", type=DataType.int64(), nullable=True),
            ),
        }
    )

    assert (
        QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    ) == expected


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_preserves_casing_in_quoted_identifiers(
    tmp_path: pathlib.Path, streaming: bool
):
    data = Table(
        {
            "fooBar": Array(
                ["A001", "A002"],
                ArrowField("fooBar", type=DataType.string_view(), nullable=True),
            )
        }
    )

    write_deltalake(tmp_path, data, mode="append")

    dt = DeltaTable(tmp_path)
    source_table = Table(
        {
            "fooBar": Array(
                ["A003"],
                ArrowField("fooBar", type=DataType.string_view(), nullable=True),
            )
        }
    )

    (
        dt.merge(
            source=source_table,
            predicate='target."fooBar" = source."fooBar"',
            source_alias="source",
            target_alias="target",
            streamed_exec=streaming,
        )
        .when_not_matched_insert_all()
        .execute()
    )

    expected = Table(
        {
            "fooBar": Array(
                ["A001", "A002", "A003"],
                ArrowField("fooBar", type=DataType.string_view(), nullable=True),
            )
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute('select * from tbl order by "fooBar" asc')
        .read_all()
    )

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_camelcase_non_nullable_column_4082(tmp_path: pathlib.Path):
    # Regression test for https://github.com/delta-io/delta-rs/issues/4082
    schema = Schema(
        [
            Field("submittedAt", PrimitiveType("long"), nullable=False),
            Field("id", PrimitiveType("string"), nullable=True),
        ]
    )
    dt = DeltaTable.create(tmp_path, schema=schema, mode="ignore")

    source_table = Table(
        {
            "submittedAt": Array(
                [123],
                ArrowField("submittedAt", type=DataType.int64(), nullable=True),
            ),
            "id": Array(
                ["test"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
        }
    )

    (
        dt.merge(
            source=source_table,
            predicate="source.id = target.id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    assert dt.history(1)[0]["operation"] == "MERGE"

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert result["submittedAt"].to_pylist() == [123]
    assert result["id"].to_pylist() == ["test"]


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_struct_casting(tmp_path: pathlib.Path):
    import pandas as pd
    import pyarrow as pa

    cols = ["id", "name", "address"]
    data = [
        (
            2,
            "Marry Doe",
            {"street": "123 Main St", "city": "Anytown", "state": "CA"},
        )
    ]
    df = pd.DataFrame(data, columns=cols)
    df_merge = pd.DataFrame(
        [
            (
                2,
                "Merged",
                {"street": "1 Front", "city": "San Francisco", "state": "CA"},
            )
        ],
        columns=cols,
    )
    assert not df.empty

    schema = pa.Table.from_pandas(df=df).schema

    dt = DeltaTable.create(tmp_path, Schema.from_arrow(schema), name="test")
    metadata = dt.metadata()
    assert metadata.name == "test"

    result = (
        dt.merge(
            source=df_merge,
            predicate="t.id = s.id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .execute()
    )
    assert result is not None


@pytest.mark.parametrize("streaming", (True, False))
def test_merge_isin_partition_pruning(tmp_path: pathlib.Path, streaming: bool):
    nrows = 5
    data = Table(
        {
            "id": Array(
                [str(x) for x in range(nrows)],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "partition": Array(
                list(range(nrows)),
                ArrowField("partition", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                list(range(nrows)),
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data, mode="append", partition_by="partition")

    dt = DeltaTable(tmp_path)

    source_table = Table(
        {
            "id": Array(
                ["3", "4"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "partition": Array(
                [3, 4],
                ArrowField("partition", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
        }
    )

    metrics = (
        dt.merge(
            source=source_table,
            predicate="t.id = s.id and t.partition in (3,4)",
            source_alias="s",
            target_alias="t",
            streamed_exec=streaming,
        )
        .when_matched_update_all()
        .execute()
    )

    expected = Table(
        {
            "id": Array(
                ["0", "1", "2", "3", "4"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "partition": Array(
                [0, 1, 2, 3, 4],
                ArrowField("partition", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 10, 20],
                ArrowField("sold", type=DataType.int32(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select id, partition, sold from tbl order by id asc")
        .read_all()
    )
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected
    assert metrics["num_target_files_scanned"] == 2
    assert metrics["num_target_files_skipped_during_scan"] == 3


@pytest.mark.pyarrow
@pytest.mark.parametrize("streaming", (True, False))
def test_cdc_merge_planning_union_2908(tmp_path, streaming: bool):
    """https://github.com/delta-io/delta-rs/issues/2908"""
    cdc_path = f"{tmp_path}/_change_data"
    import pyarrow as pa

    data = {
        "id": pa.array([1, 2], pa.int64()),
        "date": pa.array(
            [datetime.date(1970, 1, 1), datetime.date(1970, 1, 2)], pa.date32()
        ),
    }

    table = pa.Table.from_pydict(data)

    dt = DeltaTable.create(
        table_uri=tmp_path,
        schema=Schema.from_arrow(table.schema),
        mode="overwrite",
        partition_by=["id"],
        configuration={
            "delta.enableChangeDataFeed": "true",
        },
    )

    dt.merge(
        source=table,
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
        streamed_exec=streaming,
    ).when_not_matched_insert_all().execute()

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert dt.version() == 1
    assert os.path.exists(cdc_path), "_change_data doesn't exist"


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_merge_non_nullable(tmp_path):
    import re

    import pandas as pd

    from deltalake.schema import Field, PrimitiveType, Schema

    schema = Schema(
        [
            Field("id", PrimitiveType("integer"), nullable=False),
            Field("bool", PrimitiveType("boolean"), nullable=False),
        ]
    )

    dt = DeltaTable.create(tmp_path, schema=schema)
    df = pd.DataFrame(
        columns=["id", "bool"],
        data=[
            [1, True],
            [2, None],
            [3, False],
        ],
    )

    with pytest.raises(
        Exception,
        match=re.escape("Invalid data found:"),
    ):
        dt.merge(
            source=df,
            source_alias="s",
            target_alias="t",
            predicate="s.id = t.id",
        ).when_matched_update_all().when_not_matched_insert_all().execute()


@pytest.mark.pyarrow
def test_merge_when_wrong_but_castable_type_passed_while_merge(
    tmp_path: pathlib.Path, sample_table: Table
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["7", "8"]),
            "price": pa.array(["1", "2"], pa.string()),
            "sold": pa.array([1, 2], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )
    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
    ).when_not_matched_insert_all().execute()

    table_schema = pq.read_table(
        tmp_path / dt.get_add_actions().column(0)[0].as_py()
    ).schema
    assert table_schema.field("price").type == sample_table["price"].type


@pytest.mark.pyarrow
def test_merge_on_decimal_3033(tmp_path):
    import pyarrow as pa

    data = {
        "timestamp": [datetime.datetime(2024, 3, 20, 12, 30, 0)],
        "altitude": [Decimal("150.5")],
    }

    table = pa.Table.from_pydict(data)

    schema = Schema.from_arrow(
        pa.schema(
            [
                ("timestamp", pa.timestamp("us")),
                ("altitude", pa.decimal128(6, 1)),
            ]
        )
    )

    dt = DeltaTable.create(tmp_path, schema=schema)

    write_deltalake(dt, table, mode="append")

    dt.merge(
        source=table,
        predicate="target.timestamp = source.timestamp",
        source_alias="source",
        target_alias="target",
        streamed_exec=False,
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    dt.merge(
        source=table,
        predicate="target.timestamp = source.timestamp AND target.altitude = source.altitude",
        source_alias="source",
        target_alias="target",
        streamed_exec=False,  # only with streamed execution off can we use stats to create a pruning predicate
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    string_predicate = dt.history(1)[0]["operationParameters"]["predicate"]

    assert (
        string_predicate
        == "timestamp = arrow_cast('2024-03-20T12:30:00.000000', 'Timestamp(Microsecond, None)') AND altitude = '1505'::decimal(4, 1)"
    )


@pytest.mark.polars
@pytest.mark.xfail(reason="polars will require an update")
def test_merge(tmp_path: pathlib.Path):
    import polars as pl
    from polars.testing import assert_frame_equal

    pl.DataFrame({"id": ["a", "b", "c"], "val": [4.0, 5.0, 6.0]}).write_delta(
        tmp_path, mode="overwrite"
    )

    df = pl.DataFrame({"id": ["a", "b", "c", "d"], "val": [4.1, 5, 6.1, 7]})

    df.write_delta(
        tmp_path,
        mode="merge",
        delta_merge_options={
            "predicate": "tgt.id = src.id",
            "source_alias": "src",
            "target_alias": "tgt",
        },
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    new_df = pl.read_delta(str(tmp_path))
    assert_frame_equal(df, new_df, check_row_order=False)


@pytest.mark.pyarrow
def test_merge_with_spill_config(tmp_path: pathlib.Path):
    """Verify merge accepts and uses spill configuration without error."""
    import pyarrow as pa

    from deltalake import DeltaTable, write_deltalake

    data = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
    write_deltalake(str(tmp_path), data)

    dt = DeltaTable(str(tmp_path))
    new_data = pa.table({"id": [1, 2], "value": [100, 200]})

    result = (
        dt.merge(
            source=new_data,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target",
            max_spill_size=100_000_000,  # 100MB
            max_temp_directory_size=1_000_000_000,  # 1GB
        )
        .when_matched_update_all()
        .execute()
    )

    assert result["num_target_rows_updated"] == 2
    assert result["num_target_rows_copied"] == 1
