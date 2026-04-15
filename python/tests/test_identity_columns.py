from concurrent.futures import ThreadPoolExecutor

from deltalake import DeltaTable, Field, write_deltalake
from deltalake import Schema as DeltaSchema
from deltalake.exceptions import CommitFailedError, DeltaError
from deltalake.schema import PrimitiveType
from arro3.core import Array, DataType, Table
import pytest
from tests.test_generated_columns import ArrowField
from deltalake.query import QueryBuilder


@pytest.fixture
def identity_schema() -> DeltaSchema:
    return DeltaSchema(
        [
            Field(name="id", type=PrimitiveType("integer"), nullable=False),
            Field(
                name="identity_col",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "1",
                    "delta.identity.step": "1",
                    "delta.identity.highWaterMark": "0",
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
        ]
    )


@pytest.fixture
def valid_identity_data() -> Table:
    id_col = ArrowField("id", DataType.int32(), nullable=True)
    identity = ArrowField(
        "identity_col", DataType.int64(), nullable=False
    ).with_metadata(
        {
            "delta.identity.start": "1",
            "delta.identity.step": "1",
            "delta.identity.highWaterMark": "2",
            "delta.identity.allowExplicitInsert": "true",  # explicit insert allowed
        }
    )
    data = Table.from_pydict(
        {
            "id": Array([1, 2], type=id_col),
            "identity_col": Array([1, 2], type=identity),
        }
    )
    return data


def test_create_table_with_identity_columns(tmp_path, identity_schema: DeltaSchema):
    dt = DeltaTable.create(
        tmp_path,
        schema=identity_schema,
    )
    protocol = dt.protocol()
    assert protocol.min_writer_version >= 6

    dt = DeltaTable.create(
        tmp_path,
        schema=identity_schema,
        mode="overwrite",
        configuration={"delta.minWriterVersion": "7"},
    )
    protocol = dt.protocol()

    assert dt.version() == 1
    assert protocol.writer_features is not None
    assert "identityColumns" in protocol.writer_features

    # Verify schema metadata is preserved
    fields_by_name = {field.name: field for field in dt.schema().fields}
    identity_metadata = fields_by_name["identity_col"].metadata
    assert identity_metadata["delta.identity.start"] == "1"
    assert identity_metadata["delta.identity.step"] == "1"
    assert identity_metadata["delta.identity.allowExplicitInsert"] == "false"


@pytest.fixture
def experiment_table_schema() -> DeltaSchema:
    return DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "1",
                    "delta.identity.step": "1",
                    "delta.identity.highWaterMark": "0",
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="experiment", type=PrimitiveType("string"), nullable=False),
            Field(name="score", type=PrimitiveType("double"), nullable=False),
        ]
    )


@pytest.fixture
def experiment_table(tmp_path, experiment_table_schema) -> DeltaTable:
    return DeltaTable.create(tmp_path, schema=experiment_table_schema)


def test_write_to_table_generating_identity_values(experiment_table: DeltaTable):
    experiment_col = ArrowField("experiment", DataType.string_view(), nullable=False)
    score_col = ArrowField("score", DataType.float64(), nullable=False)
    data = Table.from_pydict(
        {
            "experiment": Array(["exp_a", "exp_b"], type=experiment_col),
            "score": Array([0.85, 0.92], type=score_col),
        }
    )
    write_deltalake(experiment_table, mode="append", data=data)

    # id has start=1, step=1, highWaterMark=0
    # generated values: 0 + 1*1 = 1, 0 + 2*1 = 2
    id_field = ArrowField("id", DataType.int64(), nullable=False).with_metadata(
        {
            "delta.identity.start": "1",
            "delta.identity.step": "1",
            "delta.identity.highWaterMark": "2",
            "delta.identity.allowExplicitInsert": "false",
        }
    )
    expected = Table.from_pydict(
        {
            "id": Array([1, 2], type=id_field),
            "experiment": Array(["exp_a", "exp_b"], type=experiment_col),
            "score": Array([0.85, 0.92], type=score_col),
        },
    )

    assert experiment_table.version() == 1
    result = (
        QueryBuilder()
        .register("tbl", experiment_table)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    assert result.schema == expected.schema
    assert result.column("id").to_pylist() == [1, 2]
    assert result.column("experiment").to_pylist() == ["exp_a", "exp_b"]
    assert result.column("score").to_pylist() == [0.85, 0.92]


def test_sequential_writes_update_hwm(experiment_table: DeltaTable):
    experiment_col = ArrowField("experiment", DataType.string_view(), nullable=False)
    score_col = ArrowField("score", DataType.float64(), nullable=False)

    # First write: 2 rows → should generate id=1, 2
    batch1 = Table.from_pydict(
        {
            "experiment": Array(["exp_a", "exp_b"], type=experiment_col),
            "score": Array([0.85, 0.92], type=score_col),
        }
    )
    write_deltalake(experiment_table, mode="append", data=batch1)

    result1 = (
        QueryBuilder()
        .register("tbl", experiment_table)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    assert result1.column("id").to_pylist() == [1, 2]
    hwm_after_first = result1.schema.field("id").metadata.get(
        b"delta.identity.highWaterMark"
    )
    assert hwm_after_first == b"2"

    # Second write: 3 rows → should generate id=3, 4, 5 (hwm should be 2 after first write)
    batch2 = Table.from_pydict(
        {
            "experiment": Array(["exp_c", "exp_d", "exp_e"], type=experiment_col),
            "score": Array([0.78, 0.91, 0.88], type=score_col),
        }
    )
    write_deltalake(experiment_table, mode="append", data=batch2)

    result2 = (
        QueryBuilder()
        .register("tbl", experiment_table)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    assert result2.column("id").to_pylist() == [1, 2, 3, 4, 5]
    assert result2.column("experiment").to_pylist() == [
        "exp_a",
        "exp_b",
        "exp_c",
        "exp_d",
        "exp_e",
    ]
    hwm_after_second = result2.schema.field("id").metadata.get(
        b"delta.identity.highWaterMark"
    )
    assert hwm_after_second == b"5"


def test_concurrent_writes_identity_conflict(tmp_path, experiment_table_schema):
    table_path = str(tmp_path)
    DeltaTable.create(table_path, schema=experiment_table_schema)

    experiment_col = ArrowField("experiment", DataType.string_view(), nullable=False)
    score_col = ArrowField("score", DataType.float64(), nullable=False)

    def make_batch(i):
        return Table.from_pydict(
            {
                "experiment": Array([f"exp_{i}_a", f"exp_{i}_b"], type=experiment_col),
                "score": Array([float(i), float(i) + 0.1], type=score_col),
            }
        )

    def do_write(i):
        write_deltalake(table_path, make_batch(i), mode="append")

    conflicts = 0
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = [exe.submit(do_write, i) for i in range(4)]
        for f in futures:
            try:
                f.result()
            except CommitFailedError:
                conflicts += 1

    # Verify: all committed IDs must be unique (no stale HWM duplicates)
    dt = DeltaTable(table_path)
    result = QueryBuilder().register("tbl", dt).execute("select id from tbl").read_all()
    ids = result.column("id").to_pylist()
    assert len(ids) == len(set(ids)), f"duplicate IDs found: {ids}"


@pytest.mark.xfail(
    reason="allowExplicitInsert=false should reject user-provided values, but currently silently overwrites them"
)
def test_explicit_insert_rejected_when_not_allowed(tmp_path):
    """When allowExplicitInsert=false, writes that include explicit values for
    the identity column must be rejected."""
    schema = DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "1",
                    "delta.identity.step": "1",
                    "delta.identity.highWaterMark": "0",
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="val", type=PrimitiveType("string"), nullable=False),
        ]
    )
    dt = DeltaTable.create(tmp_path, schema=schema)

    id_field = ArrowField("id", DataType.int64(), nullable=False)
    val_field = ArrowField("val", DataType.string_view(), nullable=False)
    data = Table.from_pydict(
        {
            "id": Array([100, 200], type=id_field),
            "val": Array(["a", "b"], type=val_field),
        }
    )

    with pytest.raises((DeltaError, CommitFailedError)):
        write_deltalake(dt, mode="append", data=data)


@pytest.mark.xfail(
    reason="step=0 should be rejected at table creation or write time, but no validation exists"
)
def test_step_zero_rejected(tmp_path):
    """Identity column step cannot be 0 per the Delta protocol spec."""
    schema = DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "1",
                    "delta.identity.step": "0",
                    "delta.identity.highWaterMark": "0",
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="val", type=PrimitiveType("string"), nullable=False),
        ]
    )

    with pytest.raises((DeltaError, CommitFailedError)):
        dt = DeltaTable.create(tmp_path, schema=schema)
        # If creation doesn't fail, writing should
        val_field = ArrowField("val", DataType.string_view(), nullable=False)
        data = Table.from_pydict({"val": Array(["a", "b"], type=val_field)})
        write_deltalake(dt, mode="append", data=data)


@pytest.mark.xfail(
    reason="Overflow in identity value generation uses unchecked arithmetic and should be detected"
)
def test_identity_overflow_detected(tmp_path):
    """Writing rows that would overflow i64 in value generation must raise an error."""
    i64_max = 9223372036854775807
    schema = DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "1",
                    "delta.identity.step": "1",
                    "delta.identity.highWaterMark": str(i64_max - 1),
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="val", type=PrimitiveType("string"), nullable=False),
        ]
    )
    dt = DeltaTable.create(tmp_path, schema=schema)

    val_field = ArrowField("val", DataType.string_view(), nullable=False)
    data = Table.from_pydict({"val": Array(["a", "b"], type=val_field)})

    with pytest.raises((DeltaError, OverflowError, CommitFailedError)):
        write_deltalake(dt, mode="append", data=data)


@pytest.mark.xfail(
    reason="Negative-direction overflow (underflow) in value generation uses unchecked arithmetic"
)
def test_identity_underflow_negative_step(tmp_path):
    """With negative step, values that would go below i64::MIN must raise an error.
    Overflow path: identity_columns.rs:91-93 — base + row_number * step
    hwm = i64::MIN + 1, step = -1 → row 1 gives i64::MIN (ok), row 2 gives
    i64::MIN - 1 (underflow).
    """
    i64_min = -9223372036854775808
    schema = DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "-1",
                    "delta.identity.step": "-1",
                    "delta.identity.highWaterMark": str(i64_min + 1),
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="val", type=PrimitiveType("string"), nullable=False),
        ]
    )
    dt = DeltaTable.create(tmp_path, schema=schema)

    val_field = ArrowField("val", DataType.string_view(), nullable=False)
    data = Table.from_pydict({"val": Array(["a", "b"], type=val_field)})

    with pytest.raises((DeltaError, OverflowError, CommitFailedError)):
        write_deltalake(dt, mode="append", data=data)


@pytest.mark.xfail(
    reason="Large step causes row_number * step multiplication overflow before addition"
)
def test_identity_overflow_large_step(tmp_path):
    """When step is very large, row_number * step overflows even for small row counts.
    Overflow path: identity_columns.rs:91-93 — row_number * step
    step = i64::MAX, row 2 → 2 * i64::MAX overflows.
    """
    i64_max = 9223372036854775807
    schema = DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "1",
                    "delta.identity.step": str(i64_max),
                    "delta.identity.highWaterMark": "0",
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="val", type=PrimitiveType("string"), nullable=False),
        ]
    )
    dt = DeltaTable.create(tmp_path, schema=schema)

    val_field = ArrowField("val", DataType.string_view(), nullable=False)
    data = Table.from_pydict({"val": Array(["a", "b"], type=val_field)})

    with pytest.raises((DeltaError, OverflowError, CommitFailedError)):
        write_deltalake(dt, mode="append", data=data)


@pytest.mark.xfail(
    reason="HWM update uses unchecked arithmetic: base + num_rows * step can overflow"
)
def test_identity_overflow_hwm_update(tmp_path):
    """Even if value generation doesn't overflow, the HWM update can.
    Overflow path: identity_columns.rs:129 — base + (num_rows as i64) * step
    hwm = i64::MAX - 2, step = 1, write 1 row → value gen produces i64::MAX - 1
    (ok), but if extra rows are counted in HWM the update overflows.
    Here we use hwm = i64::MAX - 1, step = 2, write 1 row:
    - value gen: (i64::MAX - 1) + 1 * 2 = i64::MAX + 1 → overflow in both paths
    """
    i64_max = 9223372036854775807
    schema = DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "1",
                    "delta.identity.step": "2",
                    "delta.identity.highWaterMark": str(i64_max - 1),
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="val", type=PrimitiveType("string"), nullable=False),
        ]
    )
    dt = DeltaTable.create(tmp_path, schema=schema)

    val_field = ArrowField("val", DataType.string_view(), nullable=False)
    data = Table.from_pydict({"val": Array(["a"], type=val_field)})

    with pytest.raises((DeltaError, OverflowError, CommitFailedError)):
        write_deltalake(dt, mode="append", data=data)


def test_identity_overflow_base_calc_start_minus_step(tmp_path):
    """When no HWM is set, `start - step` overflows i64.
    Caught by: identity_columns.rs:84-87 — checked_sub in with_identity_columns.
    Note: the same calc in update_identity_column_hwm (line 127) is unchecked,
    but value generation catches it first so the HWM path is never reached.
    """
    i64_min = -9223372036854775808
    schema = DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": str(i64_min),
                    "delta.identity.step": "1",
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="val", type=PrimitiveType("string"), nullable=False),
        ]
    )

    with pytest.raises((DeltaError, OverflowError, CommitFailedError)):
        dt = DeltaTable.create(tmp_path, schema=schema)
        val_field = ArrowField("val", DataType.string_view(), nullable=False)
        data = Table.from_pydict({"val": Array(["a"], type=val_field)})
        write_deltalake(dt, mode="append", data=data)


def test_negative_step_generates_decreasing_values(tmp_path):
    """Negative step should produce decreasing identity values."""
    schema = DeltaSchema(
        [
            Field(
                name="id",
                type=PrimitiveType("long"),
                nullable=False,
                metadata={
                    "delta.identity.start": "-1",
                    "delta.identity.step": "-1",
                    "delta.identity.highWaterMark": "0",
                    "delta.identity.allowExplicitInsert": "false",
                },
            ),
            Field(name="val", type=PrimitiveType("string"), nullable=False),
        ]
    )
    dt = DeltaTable.create(tmp_path, schema=schema)

    val_field = ArrowField("val", DataType.string_view(), nullable=False)
    data = Table.from_pydict({"val": Array(["a", "b", "c"], type=val_field)})
    write_deltalake(dt, mode="append", data=data)

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id desc")
        .read_all()
    )
    assert result.column("id").to_pylist() == [-1, -2, -3]
