import pathlib

import pytest
from arro3.core import DataType, Field, Schema

from deltalake import DeltaTable, write_deltalake
from deltalake.writer._conversion import _convert_arro3_schema_to_delta


@pytest.mark.parametrize(
    "input_schema,expected_schema",
    [
        # Basic types - identity
        (
            Schema(fields=[Field("foo", DataType.int64())]),
            Schema(fields=[Field("foo", DataType.int64())]),
        ),
        (
            Schema(fields=[Field("foo", DataType.int32())]),
            Schema(fields=[Field("foo", DataType.int32())]),
        ),
        (
            Schema(fields=[Field("foo", DataType.int16())]),
            Schema(fields=[Field("foo", DataType.int16())]),
        ),
        # Unsigned integers to signed
        (
            Schema(fields=[Field("foo", DataType.uint8())]),
            Schema(fields=[Field("foo", DataType.int8())]),
        ),
        (
            Schema(fields=[Field("foo", DataType.uint16())]),
            Schema(fields=[Field("foo", DataType.int16())]),
        ),
        (
            Schema(fields=[Field("foo", DataType.uint32())]),
            Schema(fields=[Field("foo", DataType.int32())]),
        ),
        (
            Schema(fields=[Field("foo", DataType.uint64())]),
            Schema(fields=[Field("foo", DataType.int64())]),
        ),
        # Timestamps
        (
            Schema(fields=[Field("foo", DataType.timestamp("s"))]),
            Schema(fields=[Field("foo", DataType.timestamp("us"))]),
        ),
        (
            Schema(
                fields=[Field("foo", DataType.timestamp("us", tz="Europe/Amsterdam"))]
            ),
            Schema(fields=[Field("foo", DataType.timestamp("us", tz="UTC"))]),
        ),
        (
            Schema(
                fields=[Field("foo", DataType.timestamp("ns", tz="Europe/Amsterdam"))]
            ),
            Schema(fields=[Field("foo", DataType.timestamp("ns", tz="UTC"))]),
        ),
        # Nullability variations
        (
            Schema(fields=[Field("foo", DataType.uint16(), nullable=False)]),
            Schema(fields=[Field("foo", DataType.int16(), nullable=False)]),
        ),
        (
            Schema(fields=[Field("foo", DataType.timestamp("ns"), nullable=True)]),
            Schema(fields=[Field("foo", DataType.timestamp("us"), nullable=True)]),
        ),
        # List of unsigned ints
        (
            Schema(fields=[Field("foo", DataType.list(DataType.uint32()))]),
            Schema(fields=[Field("foo", DataType.list(DataType.int32()))]),
        ),
        (
            Schema(fields=[Field("foo", DataType.large_list(DataType.uint8()))]),
            Schema(fields=[Field("foo", DataType.large_list(DataType.int8()))]),
        ),
        # List with nullable inner fields
        (
            Schema(
                fields=[
                    Field(
                        "my_list",
                        DataType.list(Field("foo", DataType.uint8(), nullable=True)),
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "my_list",
                        DataType.list(Field("foo", DataType.int8(), nullable=True)),
                    )
                ]
            ),
        ),
        # List with non-nullable inner fields
        (
            Schema(
                fields=[
                    Field(
                        "my_list",
                        DataType.list(Field("foo", DataType.uint8(), nullable=False)),
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "my_list",
                        DataType.list(Field("foo", DataType.int8(), nullable=False)),
                    )
                ]
            ),
        ),
        # Deeply nested list
        (
            Schema(
                fields=[
                    Field(
                        "deep_list",
                        DataType.list(
                            Field(
                                "level_1",
                                DataType.list(
                                    Field(
                                        "level_2",
                                        DataType.list(
                                            Field(
                                                "value",
                                                DataType.uint16(),
                                                nullable=True,
                                            )
                                        ),
                                        nullable=True,
                                    )
                                ),
                                nullable=True,
                            )
                        ),
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "deep_list",
                        DataType.list(
                            Field(
                                "level_1",
                                DataType.list(
                                    Field(
                                        "level_2",
                                        DataType.list(
                                            Field(
                                                "value", DataType.int16(), nullable=True
                                            )
                                        ),
                                        nullable=True,
                                    )
                                ),
                                nullable=True,
                            )
                        ),
                    )
                ]
            ),
        ),
        # Fixed-size list
        (
            Schema(fields=[Field("foo", DataType.list(DataType.uint16(), 5))]),
            Schema(fields=[Field("foo", DataType.list(DataType.int16(), 5))]),
        ),
        # Struct with mixed fields
        (
            Schema(
                fields=[
                    Field(
                        "foo",
                        DataType.struct(
                            [
                                Field("a", DataType.uint64()),
                                Field("b", DataType.timestamp("ns")),
                                Field("c", DataType.uint32()),
                            ]
                        ),
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "foo",
                        DataType.struct(
                            [
                                Field("a", DataType.int64()),
                                Field("b", DataType.timestamp("us")),
                                Field("c", DataType.int32()),
                            ]
                        ),
                    )
                ]
            ),
        ),
        # Nested struct in list
        (
            Schema(
                fields=[
                    Field(
                        "foo",
                        DataType.list(
                            DataType.struct(
                                [
                                    Field("a", DataType.uint8(), nullable=False),
                                    Field("b", DataType.timestamp("s")),
                                ]
                            )
                        ),
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "foo",
                        DataType.list(
                            DataType.struct(
                                [
                                    Field("a", DataType.int8(), nullable=False),
                                    Field("b", DataType.timestamp("us")),
                                ]
                            )
                        ),
                    )
                ]
            ),
        ),
        # Mixed schema with multiple fields
        (
            Schema(
                fields=[
                    Field("a", DataType.uint16()),
                    Field("b", DataType.timestamp("ms", tz="Europe/Berlin")),
                    Field(
                        "d",
                        DataType.struct(
                            [
                                Field("x", DataType.uint32()),
                                Field("y", DataType.int64()),
                            ]
                        ),
                    ),
                ]
            ),
            Schema(
                fields=[
                    Field("a", DataType.int16()),
                    Field("b", DataType.timestamp("us", tz="UTC")),
                    Field(
                        "d",
                        DataType.struct(
                            [Field("x", DataType.int32()), Field("y", DataType.int64())]
                        ),
                    ),
                ]
            ),
        ),
        # Field metadata preservations
        (
            Schema(
                fields=[
                    Field(
                        "foo",
                        DataType.uint16(),
                        metadata={"description": "an unsigned int"},
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "foo",
                        DataType.int16(),
                        metadata={"description": "an unsigned int"},
                    )
                ]
            ),
        ),
        (
            Schema(
                fields=[
                    Field(
                        "bar",
                        DataType.timestamp("ns"),
                        nullable=True,
                        metadata={"origin": "sensor_7"},
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "bar",
                        DataType.timestamp("us"),
                        nullable=True,
                        metadata={"origin": "sensor_7"},
                    )
                ]
            ),
        ),
        (
            Schema(
                fields=[
                    Field(
                        "record",
                        DataType.struct(
                            [
                                Field(
                                    "id",
                                    DataType.uint32(),
                                    metadata={"index": "primary"},
                                ),
                                Field(
                                    "value",
                                    DataType.timestamp("s"),
                                    nullable=True,
                                    metadata={"unit": "seconds"},
                                ),
                            ]
                        ),
                        metadata={"type": "event"},
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "record",
                        DataType.struct(
                            [
                                Field(
                                    "id",
                                    DataType.int32(),
                                    metadata={"index": "primary"},
                                ),
                                Field(
                                    "value",
                                    DataType.timestamp("us"),
                                    nullable=True,
                                    metadata={"unit": "seconds"},
                                ),
                            ]
                        ),
                        metadata={"type": "event"},
                    )
                ]
            ),
        ),
        (
            Schema(
                fields=[
                    Field(
                        "sensor_readings",
                        DataType.list(
                            Field(
                                "reading",
                                DataType.uint8(),
                                nullable=False,
                                metadata={"unit": "celsius"},
                            )
                        ),
                        metadata={"shape": "1D"},
                    )
                ]
            ),
            Schema(
                fields=[
                    Field(
                        "sensor_readings",
                        DataType.list(
                            Field(
                                "reading",
                                DataType.int8(),
                                nullable=False,
                                metadata={"unit": "celsius"},
                            )
                        ),
                        metadata={"shape": "1D"},
                    )
                ]
            ),
        ),
    ],
)
def test_schema_conversion(input_schema: Schema, expected_schema: Schema):
    assert expected_schema == _convert_arro3_schema_to_delta(input_schema)


@pytest.mark.pandas
def test_merge_casting_table_provider(tmp_path):
    import pandas as pd

    df = pd.DataFrame(
        {
            "a": 1,
            "ts": pd.date_range(
                "2021-01-01", "2021-01-02", freq="h", tz="America/Chicago"
            ),
        }
    )
    write_deltalake(tmp_path, df, mode="overwrite")

    df2 = pd.DataFrame(
        {
            "a": 2,
            "ts": pd.date_range(
                "2021-01-01", "2021-01-03", freq="h", tz="America/Chicago"
            ),
        }
    )

    dt = DeltaTable(tmp_path)
    dt.merge(
        df2,
        predicate="source.ts = target.ts",
        source_alias="source",
        target_alias="target",
    ).when_matched_update_all().when_not_matched_insert_all().execute()


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_pandas_null_columns_to_existing_table_should_work(tmp_path: pathlib.Path):
    import pandas as pd
    import pyarrow as pa

    initial_data = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "score": [85.5, 92.0, 88.5],
        }
    )

    write_deltalake(tmp_path, initial_data)

    df_with_nulls = pd.DataFrame(
        {
            "id": [4, 5],
            "name": [None, None],  # object dtype -> null type in Arrow
            "age": [None, None],  # object dtype -> null type in Arrow
            "score": [None, None],  # object dtype -> null type in Arrow
        }
    )

    arrow_table = pa.Table.from_pandas(df_with_nulls)

    assert arrow_table.schema.field("name").type == pa.null()
    assert arrow_table.schema.field("age").type == pa.null()
    assert arrow_table.schema.field("score").type == pa.null()

    write_deltalake(tmp_path, df_with_nulls, mode="append")

    updated_dt = DeltaTable(tmp_path)
    result_df = updated_dt.to_pandas()

    # Should have 5 rows total (3 initial + 2 appended)
    assert len(result_df) == 5

    # Check that all expected IDs are present (order may vary)
    all_ids = sorted(result_df["id"].tolist())
    assert all_ids == [1, 2, 3, 4, 5]

    # Check that rows with IDs 4 and 5 have null values for other columns
    new_rows = result_df[result_df["id"].isin([4, 5])]
    assert len(new_rows) == 2
    assert pd.isna(new_rows["name"]).all()
    assert pd.isna(new_rows["age"]).all()
    assert pd.isna(new_rows["score"]).all()


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_null_conversion_prevents_infinite_recursion():
    from deltalake.writer._conversion import _convert_arro3_schema_to_delta

    source_schema = Schema(
        [
            Field("id", DataType.int64()),
            Field("problematic_field", DataType.null()),
        ]
    )

    existing_schema = Schema(
        [
            Field("id", DataType.int64()),
            Field("problematic_field", DataType.null()),
        ]
    )

    converted = _convert_arro3_schema_to_delta(source_schema, existing_schema)

    assert converted.field("id").type == DataType.int64()
    assert DataType.is_null(converted.field("problematic_field").type)


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_null_conversion_with_mixed_types():
    from deltalake.writer._conversion import _convert_arro3_schema_to_delta

    source_schema = Schema(
        [
            Field("concrete_field", DataType.int64()),
            Field("null_field", DataType.null()),
            Field("missing_field", DataType.string()),
        ]
    )

    existing_schema = Schema(
        [
            Field("concrete_field", DataType.int64()),
            Field("null_field", DataType.string()),
        ]
    )

    converted = _convert_arro3_schema_to_delta(source_schema, existing_schema)

    assert converted.field("concrete_field").type == DataType.int64()
    assert converted.field("null_field").type == DataType.string()
    assert converted.field("missing_field").type == DataType.string()


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_null_conversion_without_existing_schema():
    from deltalake.writer._conversion import _convert_arro3_schema_to_delta

    source_schema = Schema(
        [
            Field("id", DataType.int64()),
            Field("null_field", DataType.null()),
            Field("timestamp_field", DataType.timestamp("ns")),
        ]
    )

    converted = _convert_arro3_schema_to_delta(source_schema)

    assert converted.field("id").type == DataType.int64()
    assert DataType.is_null(converted.field("null_field").type)
    assert converted.field("timestamp_field").type == DataType.timestamp("us")


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_null_conversion_field_not_found():
    from deltalake.writer._conversion import _convert_arro3_schema_to_delta

    source_schema = Schema(
        [
            Field("missing_field", DataType.null()),
        ]
    )

    existing_schema = Schema(
        [
            Field("other_field", DataType.string()),
        ]
    )

    converted = _convert_arro3_schema_to_delta(source_schema, existing_schema)

    assert DataType.is_null(converted.field("missing_field").type)


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_null_conversion_no_field_name():
    from deltalake.writer._conversion import _convert_arro3_schema_to_delta

    source_schema = Schema(
        [
            Field("list_field", DataType.list(Field("element", DataType.null()))),
        ]
    )

    existing_schema = Schema(
        [
            Field("list_field", DataType.list(Field("element", DataType.string()))),
        ]
    )

    converted = _convert_arro3_schema_to_delta(source_schema, existing_schema)

    list_field = converted.field("list_field")
    assert DataType.is_list(list_field.type)


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_null_conversion_with_struct_types():
    from deltalake.writer._conversion import _convert_arro3_schema_to_delta

    source_schema = Schema(
        [
            Field(
                "struct_field",
                DataType.struct(
                    [
                        Field("inner_null", DataType.null()),
                        Field("inner_int", DataType.int32()),
                    ]
                ),
            ),
        ]
    )

    existing_schema = Schema(
        [
            Field(
                "struct_field",
                DataType.struct(
                    [
                        Field("inner_null", DataType.string()),
                        Field("inner_int", DataType.int32()),
                    ]
                ),
            ),
        ]
    )

    converted = _convert_arro3_schema_to_delta(source_schema, existing_schema)

    struct_field = converted.field("struct_field")
    assert DataType.is_struct(struct_field.type)
    inner_fields = struct_field.type.fields
    assert DataType.is_null(inner_fields[0].type)
    assert inner_fields[1].type == DataType.int32()
