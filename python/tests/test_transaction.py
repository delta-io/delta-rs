"""Tests for the low level transaction entrypoints in ``deltalake.transaction``:

- ``create_table_with_add_actions``: create a table by directly supplying ``AddAction``s.
- ``create_table_with_actions``: generalization that also accepts ``RemoveAction``s.
"""

from __future__ import annotations

import pathlib

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField
from arro3.io import write_parquet

from deltalake import CommitProperties, DeltaTable, Field, Schema
from deltalake.schema import PrimitiveType
from deltalake.transaction import (
    AddAction,
    RemoveAction,
    create_table_with_actions,
    create_table_with_add_actions,
)


def _action_schema() -> Schema:
    return Schema(
        fields=[
            Field("id", type=PrimitiveType("string"), nullable=True),
            Field("price", type=PrimitiveType("long"), nullable=True),
        ]
    )


def _write_parquet_file(
    table_path: pathlib.Path, file_name: str, id_value: str, price_value: int
) -> AddAction:
    data = Table(
        {
            "id": Array(
                [id_value], ArrowField("id", type=DataType.string_view(), nullable=True)
            ),
            "price": Array(
                [price_value], ArrowField("price", type=DataType.int64(), nullable=True)
            ),
        }
    )
    file_path = table_path / file_name
    write_parquet(data, file_path)
    return AddAction(
        file_name,
        file_path.stat().st_size,
        {},
        0,
        True,
        "{}",
    )


def _file_names(dt: DeltaTable) -> list[str]:
    return sorted(pathlib.Path(uri).name for uri in dt.file_uris())


def test_create_table_with_add_actions(tmp_path: pathlib.Path) -> None:
    action_schema = _action_schema()
    add_action = _write_parquet_file(tmp_path, "part-0.parquet", "1", 10)

    create_table_with_add_actions(
        str(tmp_path),
        action_schema,
        [add_action],
        mode="error",
    )

    dt = DeltaTable(tmp_path)
    assert dt.version() == 0
    assert dt.schema() == action_schema
    assert _file_names(dt) == ["part-0.parquet"]


def test_create_table_with_add_actions_does_not_accept_remove_actions(
    tmp_path: pathlib.Path,
) -> None:
    action_schema = _action_schema()
    add_action = _write_parquet_file(tmp_path, "part-0.parquet", "1", 10)

    with pytest.raises(TypeError, match="remove_actions"):
        create_table_with_add_actions(
            str(tmp_path),
            action_schema,
            [add_action],
            mode="error",
            remove_actions=[],  # type: ignore[call-arg]
        )


def test_create_table_with_actions_add_only(tmp_path: pathlib.Path) -> None:
    action_schema = _action_schema()
    add_action = _write_parquet_file(tmp_path, "part-0.parquet", "1", 10)

    create_table_with_actions(
        str(tmp_path),
        action_schema,
        [add_action],
        [],
        mode="error",
    )

    dt = DeltaTable(tmp_path)
    assert dt.version() == 0
    assert dt.schema() == action_schema
    assert _file_names(dt) == ["part-0.parquet"]


def test_create_table_with_actions_supports_remove_actions(
    tmp_path: pathlib.Path,
) -> None:
    action_schema = _action_schema()
    drop_action = _write_parquet_file(tmp_path, "drop-me.parquet", "1", 10)
    keep_action = _write_parquet_file(tmp_path, "keep.parquet", "2", 20)
    remove_action = RemoveAction(
        "drop-me.parquet",
        True,
        0,
        True,
        {},
        drop_action.size,
        {},
    )

    create_table_with_actions(
        str(tmp_path),
        action_schema,
        [drop_action, keep_action],
        [remove_action],
        mode="error",
    )

    dt = DeltaTable(tmp_path)
    assert dt.version() == 0
    assert dt.schema() == action_schema
    # drop-me.parquet was added and removed in the same commit, so only
    # keep.parquet should be a live file in the resulting table.
    assert _file_names(dt) == ["keep.parquet"]


def test_create_table_with_actions_forwards_commit_properties(
    tmp_path: pathlib.Path,
) -> None:
    action_schema = _action_schema()
    add_action = _write_parquet_file(tmp_path, "part-0.parquet", "1", 10)

    create_table_with_actions(
        str(tmp_path),
        action_schema,
        [add_action],
        [],
        mode="error",
        commit_properties=CommitProperties(custom_metadata={"userName": "Jane Doe"}),
    )

    dt = DeltaTable(tmp_path)
    assert dt.history()[0]["userName"] == "Jane Doe"


def test_create_table_with_actions_rejects_positional_commit_properties(
    tmp_path: pathlib.Path,
) -> None:
    action_schema = _action_schema()
    add_action = _write_parquet_file(tmp_path, "part-0.parquet", "1", 10)
    commit = CommitProperties(custom_metadata={"userName": "Jane Doe"})

    with pytest.raises(TypeError):
        create_table_with_actions(
            str(tmp_path),
            action_schema,
            [add_action],
            [],
            "error",
            None,
            None,
            None,
            None,
            None,
            commit,  # type: ignore[misc]
        )
