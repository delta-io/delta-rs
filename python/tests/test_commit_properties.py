from __future__ import annotations

import json
import math
import pathlib
from collections.abc import Iterable
from typing import Any

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import (
    CommitProperties,
    DeltaTable,
    Field,
    Schema,
    Transaction,
    write_deltalake,
)
from deltalake.schema import PrimitiveType
from deltalake.transaction import AddAction


def _reject_duplicate_object_pairs(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        assert key not in result, f"duplicate JSON key found: {key}"
        result[key] = value
    return result


def _commit_info_from_log(table_path: pathlib.Path, version: int = 0) -> dict[str, Any]:
    log_path = table_path / "_delta_log" / f"{version:020}.json"
    for line in log_path.read_text().splitlines():
        action = json.loads(line, object_pairs_hook=_reject_duplicate_object_pairs)
        if "commitInfo" in action:
            return action["commitInfo"]
    raise AssertionError(f"No commitInfo action found in {log_path}")


def _schema() -> Schema:
    return Schema(
        fields=[
            Field("id", type=PrimitiveType("string"), nullable=True),
            Field("price", type=PrimitiveType("long"), nullable=True),
        ]
    )


def _table() -> Table:
    return Table(
        {
            "id": Array(
                ["1"], ArrowField("id", type=DataType.string_view(), nullable=True)
            ),
            "price": Array(
                [10], ArrowField("price", type=DataType.int64(), nullable=True)
            ),
        }
    )


def _manual_write_transaction_parts(
    table_path: pathlib.Path,
) -> tuple[DeltaTable, Schema, AddAction]:
    from arro3.io import write_parquet

    schema = _schema()
    table = _table()
    DeltaTable.create(table_path, schema)

    data_file = table_path / "manual.parquet"
    write_parquet(table, data_file)

    action = AddAction(
        "manual.parquet",
        data_file.stat().st_size,
        {},
        0,
        True,
        "{}",
    )
    return DeltaTable(table_path), schema, action


def test_custom_metadata_json_values_round_trip(tmp_path: pathlib.Path) -> None:
    custom_metadata = {
        "intValue": 7,
        "nested": {"flag": True, "items": [1, None, {"name": "value"}]},
        "boolValue": False,
        "nullValue": None,
    }

    write_deltalake(
        tmp_path,
        _table(),
        commit_properties=CommitProperties(custom_metadata=custom_metadata),
    )

    history = DeltaTable(tmp_path).history(1)[0]
    assert history["intValue"] == 7
    assert history["nested"] == {"flag": True, "items": [1, None, {"name": "value"}]}
    assert history["boolValue"] is False
    assert history["nullValue"] is None

    raw_commit_info = _commit_info_from_log(tmp_path)
    assert raw_commit_info["intValue"] == 7


def test_create_write_transaction_accepts_json_custom_metadata(
    tmp_path: pathlib.Path,
) -> None:
    dt, schema, action = _manual_write_transaction_parts(tmp_path)
    dt.create_write_transaction(
        actions=[action],
        mode="append",
        schema=schema,
        commit_properties=CommitProperties(
            custom_metadata={"attempt": 1, "details": {"manual": True}}
        ),
    )

    history = DeltaTable(tmp_path).history(1)[0]
    assert history["attempt"] == 1
    assert history["details"] == {"manual": True}


def test_operation_parameters_merge_without_duplicate_json_keys(
    tmp_path: pathlib.Path,
) -> None:
    write_deltalake(
        tmp_path,
        _table(),
        mode="overwrite",
        partition_by=["id"],
        commit_properties=CommitProperties(
            custom_metadata={
                "operationParameters": {
                    "mode": "custom-mode",
                    "partitionBy": "custom-partitioning",
                    "customParameter": {"from": "metadata"},
                    "customBoolean": True,
                    "customNumber": 7,
                }
            }
        ),
    )

    history = DeltaTable(tmp_path).history(1)[0]
    assert history["operationParameters"]["mode"] == "Overwrite"
    assert history["operationParameters"]["partitionBy"] == '["id"]'
    assert history["operationParameters"]["customParameter"] == '{"from":"metadata"}'
    assert history["operationParameters"]["customBoolean"] == "true"
    assert history["operationParameters"]["customNumber"] == "7"

    raw_commit_info = _commit_info_from_log(tmp_path)
    assert raw_commit_info["operationParameters"]["mode"] == "Overwrite"
    assert raw_commit_info["operationParameters"]["partitionBy"] == '["id"]'
    assert (
        raw_commit_info["operationParameters"]["customParameter"]
        == '{"from":"metadata"}'
    )


@pytest.mark.parametrize(
    ("custom_metadata", "match"),
    [
        ({"operationParameters": "not-an-object"}, "operationParameters"),
        ({"readVersion": -1}, "readVersion"),
        ({"readVersion": 1.0}, "readVersion"),
        ({"readVersion": 1.5}, "readVersion"),
        ({"readVersion": "1"}, "readVersion"),
        ({"isolationLevel": "NotAnIsolationLevel"}, "isolationLevel"),
        ({"userName": 123}, "userName"),
        ({"timestamp": 123}, "timestamp"),
        ({"operation": "WRITE"}, "operation"),
        ({"engineInfo": "custom-engine"}, "engineInfo"),
    ],
)
def test_invalid_reserved_custom_metadata_values_raise(
    tmp_path: pathlib.Path, custom_metadata: dict[str, Any], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        write_deltalake(
            tmp_path,
            _table(),
            commit_properties=CommitProperties(custom_metadata=custom_metadata),
        )


@pytest.mark.parametrize(
    ("custom_metadata", "match"),
    [
        ([("key", "value")], "mapping"),
        ({1: "value"}, "keys must be strings"),
        ({"score": math.nan}, "JSON"),
        ({"payload": object()}, "JSON"),
    ],
)
def test_invalid_custom_metadata_json_values_raise(
    tmp_path: pathlib.Path, custom_metadata: Any, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        write_deltalake(
            tmp_path,
            _table(),
            commit_properties=CommitProperties(custom_metadata=custom_metadata),
        )


def test_valid_reserved_user_fields_are_visible_in_history(
    tmp_path: pathlib.Path,
) -> None:
    write_deltalake(
        tmp_path,
        _table(),
        commit_properties=CommitProperties(
            custom_metadata={"userName": "Jane Doe", "userId": "jane"}
        ),
    )

    history = DeltaTable(tmp_path).history(1)[0]
    assert history["userName"] == "Jane Doe"
    assert history["userId"] == "jane"

    raw_commit_info = _commit_info_from_log(tmp_path)
    assert raw_commit_info["userName"] == "Jane Doe"
    assert raw_commit_info["userId"] == "jane"


def test_reserved_read_version_is_visible_in_history(
    tmp_path: pathlib.Path,
) -> None:
    write_deltalake(
        tmp_path,
        _table(),
        commit_properties=CommitProperties(custom_metadata={"readVersion": 15}),
    )

    history = DeltaTable(tmp_path).history(1)[0]
    assert history["readVersion"] == 15

    raw_commit_info = _commit_info_from_log(tmp_path)
    assert raw_commit_info["readVersion"] == 15


def test_custom_client_version_is_preserved(tmp_path: pathlib.Path) -> None:
    write_deltalake(
        tmp_path,
        _table(),
        commit_properties=CommitProperties(
            custom_metadata={"clientVersion": "test-client.1.2.3"}
        ),
    )

    history = DeltaTable(tmp_path).history(1)[0]
    assert history["clientVersion"] == "test-client.1.2.3"

    raw_commit_info = _commit_info_from_log(tmp_path)
    assert raw_commit_info["clientVersion"] == "test-client.1.2.3"


def test_create_write_transaction_forwards_app_transactions(
    tmp_path: pathlib.Path,
) -> None:
    dt, schema, action = _manual_write_transaction_parts(tmp_path)
    dt.create_write_transaction(
        actions=[action],
        mode="append",
        schema=schema,
        commit_properties=CommitProperties(
            app_transactions=[Transaction(app_id="manual-writer", version=42)]
        ),
    )

    assert DeltaTable(tmp_path).transaction_version("manual-writer") == 42
