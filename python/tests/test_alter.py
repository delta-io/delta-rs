import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError


def test_add_constraint(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraints({"check_price": "price >= 0"})

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "ADD CONSTRAINT"
    assert dt.version() == 1
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": "price >= 0"
    }

    with pytest.raises(DeltaError):
        # Invalid constraint
        dt.alter.add_constraints({"check_price": "price < 0"})


def test_add_multiple_constraints(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    with pytest.raises(ValueError):
        dt.alter.add_constraints(
            {"check_price": "price >= 0", "check_price2": "price >= 0"}
        )
