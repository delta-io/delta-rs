import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError


def test_add_constraint(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price": "price >= 0"})

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "ADD CONSTRAINT"
    assert dt.version() == 1
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": "price >= 0"
    }

    with pytest.raises(DeltaError):
        # Invalid constraint
        dt.alter.add_constraint({"check_price": "price < 0"})

    with pytest.raises(DeltaProtocolError):
        data = pa.table(
            {
                "id": pa.array(["1"]),
                "price": pa.array([-1], pa.int64()),
                "sold": pa.array(list(range(1)), pa.int32()),
                "deleted": pa.array([False] * 1),
            }
        )
        write_deltalake(tmp_path, data, engine="rust", mode="append")


def test_add_multiple_constraints(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    with pytest.raises(ValueError):
        dt.alter.add_constraint(
            {"check_price": "price >= 0", "check_price2": "price >= 0"}
        )


def test_add_constraint_roundtrip_metadata(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint(
        {"check_price2": "price >= 0"}, custom_metadata={"userName": "John Doe"}
    )

    assert dt.history(1)[0]["userName"] == "John Doe"


def test_unset_table_properties(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(
        tmp_path,
        sample_table,
        mode="append",
        configuration={"delta.checkpointInterval": "10", "delta.appendOnly": "true"},
    )

    dt = DeltaTable(tmp_path)

    dt.alter.unset_table_properties("delta.checkpointInterval")

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "UNSET TBLPROPERTIES"
    assert dt.version() == 1
    assert dt.metadata().configuration == {"delta.appendOnly": "true"}

    with pytest.raises(DeltaError):
        # Invalid properties
        dt.alter.unset_table_properties("invalid_property")

    with pytest.raises(DeltaError):
        # Invalid properties
        dt.alter.unset_table_properties(
            ["delta.checkpointInterval", "invalid_property"]
        )
