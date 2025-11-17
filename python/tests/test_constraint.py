import pytest
from arro3.core import Array, DataType, Field, Schema, Table

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError


@pytest.fixture()
def sample_table() -> Table:
    nrows = 5
    return Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                Field("id", type=DataType.string(), nullable=True),
            ),
            "high price": Array(
                list(range(nrows)),
                Field("high price", type=DataType.int64(), nullable=True),
            ),
        },
    )


def test_not_corrupting_expression(tmp_path):
    data = Table.from_pydict(
        {
            "b": Array([1], DataType.int64()),
            "color_column": Array(["red"], DataType.string()),
        },
    )

    data2 = Table.from_pydict(
        {
            "b": Array([1], DataType.int64()),
            "color_column": Array(["blue"], DataType.string()),
        },
    )

    write_deltalake(
        tmp_path,
        data,
        mode="overwrite",
        partition_by=["color_column"],
        predicate="color_column = 'red'",
    )
    write_deltalake(
        tmp_path,
        data2,
        mode="overwrite",
        partition_by=["color_column"],
        predicate="color_column = 'blue'",
    )


def test_not_corrupting_expression_columns_spaced(tmp_path):
    data = Table.from_pydict(
        {
            "b": Array([1], DataType.int64()),
            "color column": Array(["red"], DataType.string()),
        },
    )

    data2 = Table.from_pydict(
        {
            "b": Array([1], DataType.int64()),
            "color column": Array(["blue"], DataType.string()),
        },
    )

    write_deltalake(
        tmp_path,
        data,
        mode="overwrite",
        # partition_by=["color column"],
        predicate="`color column` = 'red'",
    )
    write_deltalake(
        tmp_path,
        data2,
        mode="overwrite",
        # partition_by=["color column"],
        predicate="`color column` = 'blue'",
    )


# fmt: off

@pytest.mark.parametrize("sql_string", [
    "`high price` >= 0",
    '"high price" >= 0',
    "\"high price\" >= 0"
])
def test_add_constraint(tmp_path, sample_table: Table, sql_string: str):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price": sql_string})

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "ADD CONSTRAINT"
    assert dt.version() == 1
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": '"high price" >= 0'
    }
    assert dt.protocol().min_writer_version == 3

    with pytest.raises(DeltaError):
        # Invalid constraint
        dt.alter.add_constraint({"check_price": '"high price" < 0'})

    with pytest.raises(DeltaProtocolError):
        data = Table(
            {
                "id": Array(["1"], DataType.string()),
                "high price": Array([-1], DataType.int64()),
            },
            schema=Schema(
                fields=[
                    Field("id", type=DataType.string(), nullable=True),
                    Field("high price", type=DataType.int64(), nullable=True),
                ]
            ),
        )

        write_deltalake(tmp_path, data, mode="append")

def test_add_multiple_constraint(tmp_path, sample_table: Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price": '"high price" >= 0', "min_price": '"high price" < 5'})

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "ADD CONSTRAINT"
    assert dt.version() == 1
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": '"high price" >= 0',
        "delta.constraints.min_price": '"high price" < 5'

    }
    assert dt.protocol().min_writer_version == 3

    with pytest.raises(DeltaProtocolError):
        data = Table(
            {
                "id": Array(["1"], DataType.string()),
                "high price": Array([5], DataType.int64()),
            },
            schema=Schema(
                fields=[
                    Field("id", type=DataType.string(), nullable=True),
                    Field("high price", type=DataType.int64(), nullable=True),
                ]
            ),
        )

        write_deltalake(tmp_path, data, mode="append")
