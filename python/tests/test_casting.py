import math

import pytest
from arro3.core import Array, DataType, Field, Schema, Table

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.query import QueryBuilder


def test_unsafe_cast(tmp_path):
    tbl = Table.from_pydict({"foo": Array([1, 2, 3, 200], DataType.uint8())})

    with pytest.raises(
        DeltaError,
        match="Cast error: Failed to convert into Arrow schema: Cast error: Failed to cast foo from Int8 to UInt8: Can't cast value 200 to type Int8",
    ):
        write_deltalake(tmp_path, tbl)


def test_safe_cast(tmp_path):
    tbl = Table.from_pydict({"foo": Array([1, 2, 3, 4], DataType.uint8())})

    write_deltalake(tmp_path, tbl)


@pytest.mark.parametrize("schema_mode", [None, "merge"])
def test_append_unsigned_to_wider_signed_column(tmp_path, schema_mode):
    write_deltalake(
        tmp_path, Table.from_pydict({"foo": Array([1, 2], DataType.int16())})
    )
    kwargs = {} if schema_mode is None else {"schema_mode": schema_mode}
    # 200 doesn't fit in int8 but does fit in the existing int16 column
    write_deltalake(
        tmp_path,
        Table.from_pydict({"foo": Array([3, 200], DataType.uint8())}),
        mode="append",
        **kwargs,
    )

    dt = DeltaTable(tmp_path)
    result = (
        QueryBuilder().register("tbl", dt).execute("select foo from tbl").read_all()
    )
    assert result.schema.field("foo").type == DataType.int16()
    assert sorted(result["foo"].to_pylist()) == [1, 2, 3, 200]


def test_append_unsigned_int_overflows_signed_column(tmp_path):
    write_deltalake(
        tmp_path, Table.from_pydict({"foo": Array([1, 2], DataType.int8())})
    )
    with pytest.raises(DeltaError, match="Can't cast value 200 to type Int8"):
        write_deltalake(
            tmp_path,
            Table.from_pydict({"foo": Array([3, 200], DataType.uint8())}),
            mode="append",
        )


def test_append_unsigned_int_to_float_column(tmp_path):
    write_deltalake(
        tmp_path, Table.from_pydict({"foo": Array([1.0, 2.0], DataType.float64())})
    )
    write_deltalake(
        tmp_path,
        Table.from_pydict({"foo": Array([3, 200], DataType.uint8())}),
        mode="append",
    )

    dt = DeltaTable(tmp_path)
    result = (
        QueryBuilder().register("tbl", dt).execute("select foo from tbl").read_all()
    )
    assert result.schema.field("foo").type == DataType.float64()
    assert sorted(result["foo"].to_pylist()) == [1.0, 2.0, 3.0, 200.0]


def test_append_unsigned_with_generated_column(tmp_path):
    from deltalake import Field as DeltaField
    from deltalake import Schema as DeltaSchema

    schema = DeltaSchema(
        [
            DeltaField("a", "long"),
            DeltaField("b", "long"),
            DeltaField("c", "long", metadata={"delta.generationExpression": "a - b"}),
        ]
    )
    DeltaTable.create(tmp_path, schema)
    write_deltalake(
        tmp_path,
        Table.from_pydict(
            {
                "a": Array([0, 200], DataType.uint8()),
                "b": Array([1, 250], DataType.uint8()),
            }
        ),
        mode="append",
    )

    dt = DeltaTable(tmp_path)
    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select a, b, c from tbl order by a")
        .read_all()
    )
    assert result["a"].to_pylist() == [0, 200]
    assert result["b"].to_pylist() == [1, 250]
    assert result["c"].to_pylist() == [-1, -50]


def test_overwrite_schema_with_unsigned_column(tmp_path):
    write_deltalake(
        tmp_path, Table.from_pydict({"foo": Array([1, 2], DataType.int64())})
    )
    # Overwriting the schema tries to write as the signed type with the
    # same width, leading to overflow.
    with pytest.raises(DeltaError, match="Can't cast value 200 to type Int8"):
        write_deltalake(
            tmp_path,
            Table.from_pydict({"foo": Array([3, 200], DataType.uint8())}),
            mode="overwrite",
            schema_mode="overwrite",
        )

    # Overwrite without overflow succeeds, resulting in the same-width signed type
    write_deltalake(
        tmp_path,
        Table.from_pydict({"foo": Array([3, 4], DataType.uint8())}),
        mode="overwrite",
        schema_mode="overwrite",
    )
    dt = DeltaTable(tmp_path)
    result = (
        QueryBuilder().register("tbl", dt).execute("select foo from tbl").read_all()
    )
    assert result.schema.field("foo").type == DataType.int8()
    assert sorted(result["foo"].to_pylist()) == [3, 4]


def test_merge_unsigned_into_signed_column(tmp_path):
    write_deltalake(
        tmp_path,
        Table.from_pydict(
            {
                "id": Array([1], DataType.int64()),
                "wide": Array([10], DataType.int64()),
                "narrow": Array([10], DataType.int8()),
            },
            schema=Schema(
                [
                    Field("id", DataType.int64(), nullable=False),
                    Field("wide", DataType.int64(), nullable=True),
                    Field("narrow", DataType.int8(), nullable=True),
                ]
            ),
        ),
    )
    source = Table.from_pydict(
        {
            "id": Array([1, 2], DataType.int64()),
            "wide": Array([200, 250], DataType.uint8()),
            "narrow": Array([200, 5], DataType.uint8()),
        }
    )

    dt = DeltaTable(tmp_path)
    with pytest.raises(DeltaError, match="Can't cast value 200 to type Int8"):
        (
            dt.merge(
                source,
                predicate="s.id = t.id",
                source_alias="s",
                target_alias="t",
                error_on_type_mismatch=True,
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    # With type mismatches not treated as errors, the cast to the wide column succeeds,
    # and the failed cast into the narrow column results in a null
    (
        dt.merge(
            source,
            predicate="s.id = t.id",
            source_alias="s",
            target_alias="t",
            error_on_type_mismatch=False,
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select id, wide, narrow from tbl order by id")
        .read_all()
    )
    assert result["id"].to_pylist() == [1, 2]
    assert result["wide"].to_pylist() == [200, 250]
    assert result["narrow"].to_pylist() == [None, 5]


@pytest.mark.pyarrow
def test_merge_preserves_null_source_type_for_renamed_assignment(tmp_path):
    import pyarrow as pa

    write_deltalake(
        tmp_path,
        pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "a": pa.array([{"x": 10}], type=pa.struct([("x", pa.int64())])),
                "b": pa.array([42], type=pa.int64()),
            }
        ),
    )
    source = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "a": pa.array([None], type=pa.null()),
        }
    )

    dt = DeltaTable(tmp_path)
    (
        dt.merge(
            source,
            predicate="s.id = t.id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update({"b": "s.a"})
        .execute()
    )

    result = pa.table(
        QueryBuilder()
        .register("tbl", dt)
        .execute("select id, a, b from tbl")
        .read_all()
    )
    assert result["id"].to_pylist() == [1]
    assert result["a"].to_pylist() == [{"x": 10}]
    assert result["b"].to_pylist() == [None]


def test_merge_renamed_unsigned_source_column(tmp_path):
    write_deltalake(
        tmp_path,
        Table.from_pydict(
            {
                "id": Array([1], DataType.int64()),
                "b": Array([42], DataType.int64()),
            }
        ),
    )
    source = Table.from_pydict(
        {
            "id": Array([1], DataType.int64()),
            "u": Array([200], DataType.uint8()),
        }
    )

    dt = DeltaTable(tmp_path)
    (
        dt.merge(
            source,
            predicate="s.id = t.id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update({"b": "s.u"})
        .execute()
    )

    result = (
        QueryBuilder().register("tbl", dt).execute("select id, b from tbl").read_all()
    )
    assert result["id"].to_pylist() == [1]
    assert result["b"].to_pylist() == [200]


@pytest.mark.parametrize("schema_mode", [None, "merge"])
def test_append_fractional_float64_to_int64_rejected(tmp_path, schema_mode):
    """Appending fractional float64 values into an int64 column must raise
    instead of silently truncating the values.

    See <https://github.com/delta-io/delta-rs/issues/4811>
    """
    write_deltalake(tmp_path, Table.from_pydict({"x": Array([1], DataType.int64())}))

    kwargs = {} if schema_mode is None else {"schema_mode": schema_mode}
    with pytest.raises(DeltaError):
        write_deltalake(
            tmp_path,
            Table.from_pydict({"x": Array([2.9, -2.9], DataType.float64())}),
            mode="append",
            **kwargs,
        )


@pytest.mark.parametrize("schema_mode", [None, "merge"])
def test_append_float64_to_float32(tmp_path, schema_mode):
    """Appending float64 values to a float32 column is allowed.
    In-range values are rounded to float32 precision and
    out-of-range values saturate to +/- inf.
    """
    write_deltalake(
        tmp_path, Table.from_pydict({"x": Array([0.0], DataType.float32())})
    )

    kwargs = {} if schema_mode is None else {"schema_mode": schema_mode}
    write_deltalake(
        tmp_path,
        Table.from_pydict(
            {"x": Array([1e300, -1e300, 1.5, float("nan")], DataType.float64())}
        ),
        mode="append",
        **kwargs,
    )

    result = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select x from tbl")
        .read_all()
    )
    assert result.schema.field("x").type == DataType.float32()
    values = result["x"].to_pylist()
    assert len(values) == 5
    assert sorted(v for v in values if not math.isnan(v)) == [
        float("-inf"),
        0.0,
        1.5,
        float("inf"),
    ]
