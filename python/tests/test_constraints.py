from arro3.core import Array, DataType, Table

from deltalake import write_deltalake


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
