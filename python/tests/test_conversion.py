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
                fields=[Field("foo", DataType.timestamp("ns", tz="Europe/Amsterdam"))]
            ),
            Schema(fields=[Field("foo", DataType.timestamp("us", tz="UTC"))]),
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
