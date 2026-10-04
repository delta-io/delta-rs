import os
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import CommitProperties, DeltaTable, TableFeatures
from deltalake._internal import Field, PrimitiveType, Schema
from deltalake.exceptions import DeltaError
from deltalake.query import QueryBuilder
from deltalake.writer import write_deltalake
from tests.test_alter import _sort_fields

if TYPE_CHECKING:
    import lakefs


@pytest.fixture
def delta_schema() -> Schema:
    return Schema(
        fields=[
            Field("date", PrimitiveType("date")),
            Field("foo", PrimitiveType("string")),
            Field("bar", PrimitiveType("string")),
        ]
    )


@pytest.fixture
def lakefs_client():
    import lakefs

    return lakefs.Client(
        username="LAKEFSID", password="LAKEFSKEY", host="http://127.0.0.1:8000"
    )


@pytest.fixture
def lakefs_path() -> str:
    return os.path.join("lakefs://bronze/main", str(uuid.uuid4()))


@pytest.fixture
def lakefs_storage_options():
    return {
        "endpoint": "http://127.0.0.1:8000",
        "allow_http": "true",
        "access_key_id": "LAKEFSID",
        "secret_access_key": "LAKEFSKEY",
    }


@pytest.mark.lakefs
@pytest.mark.integration
def test_create(lakefs_path: str, delta_schema: Schema, lakefs_storage_options):
    dt = DeltaTable.create(
        lakefs_path,
        delta_schema,
        mode="error",
        storage_options=lakefs_storage_options,
    )
    last_action = dt.history(1)[0]

    with pytest.raises(DeltaError):
        dt = DeltaTable.create(
            lakefs_path,
            delta_schema,
            mode="error",
            storage_options=lakefs_storage_options,
        )

    assert last_action["operation"] == "CREATE TABLE"
    with pytest.raises(DeltaError):
        dt = DeltaTable.create(
            lakefs_path,
            delta_schema,
            mode="append",
            storage_options=lakefs_storage_options,
        )

    dt = DeltaTable.create(
        lakefs_path,
        delta_schema,
        mode="ignore",
        storage_options=lakefs_storage_options,
    )
    assert dt.version() == 0

    dt = DeltaTable.create(
        lakefs_path,
        delta_schema,
        mode="overwrite",
        storage_options=lakefs_storage_options,
    )
    assert dt.version() == 1

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "CREATE OR REPLACE TABLE"


@pytest.mark.lakefs
@pytest.mark.integration
def test_delete(lakefs_path: str, sample_table: Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path,
        sample_table,
        partition_by=["id"],
        storage_options=lakefs_storage_options,
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    old_version = dt.version()
    dt.delete(commit_properties=commit_properties)

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "DELETE"
    assert dt.version() == old_version + 1
    assert last_action["userName"] == "John Doe"

    dataset = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert dataset.num_rows == 0
    assert len(dt.file_uris()) == 0


@pytest.mark.lakefs
@pytest.mark.integration
def test_optimize_min_commit_interval(
    lakefs_path: str, sample_table: Table, lakefs_storage_options
):
    write_deltalake(
        lakefs_path,
        sample_table,
        partition_by="id",
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_table,
        partition_by="id",
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_table,
        partition_by="id",
        mode="append",
        storage_options=lakefs_storage_options,
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    old_version = dt.version()

    dt.optimize.z_order(["sold", "price"], min_commit_interval=timedelta(0))

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    # The table has 5 distinct partitions, each of which are Z-ordered
    # independently. So with min_commit_interval=0, each will get its
    # own commit.
    assert dt.version() == old_version + 5


@pytest.mark.lakefs
@pytest.mark.integration
def test_optimize(lakefs_path: str, sample_table: Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path,
        sample_table,
        partition_by="id",
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_table,
        partition_by="id",
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_table,
        partition_by="id",
        mode="append",
        storage_options=lakefs_storage_options,
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    old_version = dt.version()

    dt.optimize.z_order(["sold", "price"])

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    # The table has 5 distinct partitions, each of which are Z-ordered
    # independently. So with min_commit_interval=0, each will get its
    # own commit.
    assert dt.version() == old_version + 1


@pytest.mark.lakefs
@pytest.mark.integration
def test_repair_wo_dry_run(
    lakefs_path,
    sample_table,
    lakefs_storage_options,
    lakefs_client: "lakefs.Client",
):
    import lakefs

    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        storage_options=lakefs_storage_options,
    )
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)

    branch = lakefs.Branch(
        repository_id="bronze", branch_id="main", client=lakefs_client
    )
    branch.object(dt.file_uris()[0].replace("lakefs://bronze/main/", "")).delete()
    branch.commit("remove commit file for test")

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    metrics = dt.repair(dry_run=False, commit_properties=commit_properties)
    last_action = dt.history(1)[0]

    assert len(metrics["files_removed"]) == 1
    assert metrics["dry_run"] is False
    assert last_action["operation"] == "FSCK"
    assert last_action["userName"] == "John Doe"


@pytest.mark.lakefs
@pytest.mark.integration
def test_add_constraint(lakefs_path, sample_table: Table, lakefs_storage_options):
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)

    dt.alter.add_constraint({"check_price": "price >= 0"})

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "ADD CONSTRAINT"
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": "price >= 0"
    }

    with pytest.raises(DeltaError):
        # Invalid constraint
        dt.alter.add_constraint({"check_price": "price < 0"})

    with pytest.raises(Exception, match="Invalid data found:"):
        data = Table(
            {
                "id": Array(
                    ["1"],
                    type=ArrowField("id", type=DataType.string_view(), nullable=True),
                ),
                "price": Array(
                    [-1], type=ArrowField("price", type=DataType.int64(), nullable=True)
                ),
                "sold": Array(
                    list(range(1)),
                    type=ArrowField("sold", type=DataType.int32(), nullable=True),
                ),
                "deleted": Array(
                    [False],
                    type=ArrowField("deleted", type=DataType.bool(), nullable=True),
                ),
            }
        )
        write_deltalake(
            lakefs_path,
            data,
            mode="append",
            storage_options=lakefs_storage_options,
        )


@pytest.mark.lakefs
@pytest.mark.integration
def test_drop_constraint(lakefs_path, sample_table: Table, lakefs_storage_options):
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)

    dt.alter.add_constraint({"check_price": "price >= 0"})
    dt.alter.drop_constraint(name="check_price")
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "DROP CONSTRAINT"
    assert dt.version() == 2


@pytest.mark.lakefs
@pytest.mark.integration
def test_set_table_properties(lakefs_path, sample_table: Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        storage_options=lakefs_storage_options,
    )
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    dt.alter.set_table_properties({"delta.enableChangeDataFeed": "true"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {"delta.enableChangeDataFeed": "true"}
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 4


@pytest.mark.lakefs
@pytest.mark.integration
def test_add_features(lakefs_path, sample_table: Table, lakefs_storage_options):
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    dt.alter.add_feature(
        feature=[
            TableFeatures.ChangeDataFeed,
            TableFeatures.DeletionVectors,
            TableFeatures.ColumnMapping,
        ],
        allow_protocol_versions_increase=True,
    )
    protocol = dt.protocol()

    assert sorted(protocol.reader_features) == sorted(  # type: ignore
        ["columnMapping", "deletionVectors"]
    )
    assert sorted(protocol.writer_features) == sorted(  # type: ignore
        [
            "changeDataFeed",
            "columnMapping",
            "deletionVectors",
        ]
    )  # type: ignore


@pytest.mark.lakefs
@pytest.mark.integration
def test_merge(lakefs_path, sample_table: Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path, sample_table, mode="append", storage_options=lakefs_storage_options
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)

    source_table = Table(
        {
            "id": Array(
                ["5"], type=ArrowField("id", DataType.string_view(), nullable=True)
            ),
            "weight": Array(
                [105], type=ArrowField("weight", DataType.int32(), nullable=True)
            ),
        }
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        commit_properties=commit_properties,
    ).when_matched_delete().execute()

    nrows = 4
    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4"],
                type=ArrowField("id", DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3],
                type=ArrowField("price", DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3],
                type=ArrowField("sold", DataType.int32(), nullable=True),
            ),
            "deleted": Array(
                [False] * nrows,
                type=ArrowField("deleted", DataType.bool(), nullable=True),
            ),
        }
    )

    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id asc")
        .read_all()
    )

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert last_action["userName"] == "John Doe"
    assert result == expected


@pytest.mark.lakefs
@pytest.mark.integration
def test_restore(
    lakefs_path,
    sample_table: Table,
    lakefs_storage_options,
):
    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        storage_options=lakefs_storage_options,
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    old_version = dt.version()
    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.restore(1, commit_properties=commit_properties)
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert last_action["userName"] == "John Doe"
    assert dt.version() == old_version + 1


@pytest.mark.lakefs
@pytest.mark.integration
def test_add_column(lakefs_path, sample_table: Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        storage_options=lakefs_storage_options,
    )
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    current_fields = dt.schema().fields

    new_fields_to_add = [
        Field("foo", PrimitiveType("integer")),
        Field("bar", PrimitiveType("float")),
    ]

    dt.alter.add_columns(new_fields_to_add)
    new_fields = dt.schema().fields

    assert _sort_fields(new_fields) == _sort_fields(
        [*current_fields, *new_fields_to_add]
    )


@pytest.fixture()
def sample_table_update():
    nrows = 5
    return Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                type=ArrowField("id", DataType.string_view(), nullable=True),
            ),
            "price": Array(
                list(range(nrows)),
                type=ArrowField("price", DataType.int64(), nullable=True),
            ),
            "sold": Array(
                list(range(nrows)),
                type=ArrowField("sold", DataType.int64(), nullable=True),
            ),
            "price_float": Array(
                list(range(nrows)),
                type=ArrowField("price_float", DataType.float64(), nullable=True),
            ),
            "deleted": Array(
                [False] * nrows,
                type=ArrowField("deleted", DataType.bool(), nullable=True),
            ),
        }
    )


@pytest.mark.lakefs
@pytest.mark.integration
def test_update(lakefs_path, sample_table_update: Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path,
        sample_table_update,
        mode="append",
        storage_options=lakefs_storage_options,
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    nrows = 5

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                type=ArrowField("id", DataType.string_view(), nullable=True),
            ),
            "price": Array(
                list(range(nrows)),
                type=ArrowField("price", DataType.int64(), nullable=True),
            ),
            "sold": Array(
                list(range(nrows)),
                type=ArrowField("sold", DataType.int64(), nullable=True),
            ),
            "price_float": Array(
                list(range(nrows)),
                type=ArrowField("price_float", DataType.float64(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, False, True],
                type=ArrowField("deleted", DataType.bool(), nullable=True),
            ),
        }
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.update(
        updates={"deleted": "True"},
        predicate="price > 3",
        commit_properties=commit_properties,
    )

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert last_action["userName"] == "John Doe"
    assert result == expected


@pytest.mark.lakefs
@pytest.mark.integration
def test_checkpoint(sample_table: Table, lakefs_storage_options, lakefs_client):
    import lakefs

    table = str(uuid.uuid4())
    tmp_table_path = os.path.join("lakefs://bronze/main", table)
    checkpoint_path = os.path.join(table, "_delta_log", "_last_checkpoint")
    last_checkpoint_path = os.path.join(
        table, "_delta_log", "00000000000000000000.checkpoint.parquet"
    )

    branch = lakefs.Branch(
        repository_id="bronze", branch_id="main", client=lakefs_client
    )

    write_deltalake(
        str(tmp_table_path), sample_table, storage_options=lakefs_storage_options
    )

    assert not branch.object(checkpoint_path).exists()

    delta_table = DeltaTable(
        str(tmp_table_path), storage_options=lakefs_storage_options
    )
    delta_table.create_checkpoint()

    assert branch.object(last_checkpoint_path).exists()
    assert branch.object(checkpoint_path).exists()


@pytest.mark.lakefs
@pytest.mark.integration
def test_no_empty_commits(
    lakefs_path, sample_table: Table, lakefs_storage_options, lakefs_client
):
    import lakefs

    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        storage_options=lakefs_storage_options,
    )
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)

    # Get current branch head commit before operation
    branch = lakefs.Branch(
        repository_id="bronze", branch_id="main", client=lakefs_client
    )
    commits_before = list(branch.log())
    before_commit_id = commits_before[0].id if commits_before else None

    # Since there should be no files to vacuum in a fresh table this should be a no-op operation
    dt.vacuum(dry_run=False)

    commits_after = list(branch.log())
    after_commit_id = commits_after[0].id if commits_after else None

    assert before_commit_id == after_commit_id, "Empty commit should be skipped"


@pytest.mark.lakefs
@pytest.mark.integration
def test_storage_options(sample_table: Table):
    with pytest.raises(
        DeltaError, match="LakeFS endpoint is missing in storage options."
    ):
        write_deltalake(
            "lakefs://bronze/main/oops",
            data=sample_table,
            storage_options={
                "allow_http": "true",
                "access_key_id": "LAKEFSID",
                "secret_access_key": "LAKEFSKEY",
            },
        )

    with pytest.raises(
        DeltaError, match="LakeFS username is missing in storage options."
    ):
        write_deltalake(
            "lakefs://bronze/main/oops",
            data=sample_table,
            storage_options={
                "endpoint": "http://127.0.0.1:8000",
                "allow_http": "true",
                "bearer_token": "test",
            },
        )


def _transaction_branches() -> set[str]:
    """Names of the hidden `delta-tx-*` branches in the test repository."""
    import base64
    import json
    import urllib.request

    request = urllib.request.Request(
        "http://127.0.0.1:8000/api/v1/repositories/bronze/branches"
        "?show_hidden=true&amount=1000"
    )
    token = base64.b64encode(b"LAKEFSID:LAKEFSKEY").decode()
    request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request) as response:
        results = json.load(response)["results"]
    return {branch["id"] for branch in results if branch["id"].startswith("delta-tx-")}


def _priced_row(price: int) -> Table:
    return Table(
        {
            "id": Array(
                ["9"], ArrowField("id", type=DataType.string_view(), nullable=True)
            ),
            "price": Array(
                [price], ArrowField("price", type=DataType.int64(), nullable=True)
            ),
            "sold": Array(
                [1], ArrowField("sold", type=DataType.int32(), nullable=True)
            ),
            "deleted": Array(
                [False], ArrowField("deleted", type=DataType.bool(), nullable=True)
            ),
        }
    )


@pytest.mark.lakefs
@pytest.mark.integration
def test_failed_write_leaves_no_transaction_branch(
    lakefs_path, sample_table: Table, lakefs_storage_options
):
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    dt.alter.add_constraint({"price_cap": "price < 100"})
    assert dt.version() == 1
    before = _transaction_branches()

    # The data files are uploaded to the transaction branch before the constraint check
    # fails, so the branch must be deleted by the failure path.
    with pytest.raises(DeltaError):
        write_deltalake(
            lakefs_path,
            _priced_row(1000),
            mode="append",
            storage_options=lakefs_storage_options,
        )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    assert dt.version() == 1
    assert _transaction_branches() <= before


@pytest.mark.lakefs
@pytest.mark.integration
def test_concurrent_writers_commit_distinct_versions(
    lakefs_path, sample_table: Table, lakefs_storage_options
):
    from concurrent.futures import ThreadPoolExecutor

    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)
    before = _transaction_branches()

    def append(_: int) -> None:
        write_deltalake(
            lakefs_path,
            sample_table,
            mode="append",
            storage_options=lakefs_storage_options,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(append, range(2)))

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    assert dt.version() == 2
    assert [entry["operation"] for entry in dt.history()] == ["WRITE", "WRITE", "WRITE"]
    assert len(dt.file_uris()) == 3
    assert _transaction_branches() <= before


@pytest.mark.lakefs
@pytest.mark.integration
def test_create_write_transaction_commits_files_staged_on_the_branch(
    lakefs_path, sample_table: Table, lakefs_storage_options, lakefs_client
):
    import io

    import lakefs
    import pyarrow as pa
    import pyarrow.parquet as pq

    from deltalake.transaction import AddAction

    table_name = lakefs_path.rsplit("/", 1)[1]
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    branch = lakefs.Branch(
        repository_id="bronze", branch_id="main", client=lakefs_client
    )
    commits_before = len(list(branch.log()))

    # A low-level writer stages its own file on the source branch, without a transaction
    # branch. The commit is a conditional put of the commit file plus a LakeFS commit of
    # the source branch.
    buffer = io.BytesIO()
    pq.write_table(
        pa.table(
            {"id": ["9"], "price": [9], "sold": [9], "deleted": [False]},
            schema=pa.schema(
                [
                    ("id", pa.string()),
                    ("price", pa.int64()),
                    ("sold", pa.int32()),
                    ("deleted", pa.bool_()),
                ]
            ),
        ),
        buffer,
    )
    data = buffer.getvalue()
    branch.object(f"{table_name}/part-staged.parquet").upload(data=data, mode="wb")
    assert list(branch.uncommitted(prefix=table_name)), "the file is only staged"

    dt.create_write_transaction(
        actions=[AddAction("part-staged.parquet", len(data), {}, 0, True, "{}")],
        mode="append",
        schema=dt.schema(),
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    assert dt.version() == 1
    assert len(dt.file_uris()) == 2
    rows = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert rows.num_rows == 6
    assert list(branch.uncommitted(prefix=table_name)) == []
    assert len(list(branch.log())) == commits_before + 1


@pytest.mark.lakefs
@pytest.mark.integration
def test_write_into_dirty_branch_fails_and_leaves_the_source_unchanged(
    lakefs_path, sample_table: Table, lakefs_storage_options, lakefs_client
):
    import lakefs

    table_name = lakefs_path.rsplit("/", 1)[1]
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)
    branch = lakefs.Branch(
        repository_id="bronze", branch_id="main", client=lakefs_client
    )
    dirty_path = f"{table_name}/staged-by-another-tool.txt"
    branch.object(dirty_path).upload(data="not committed", mode="w")
    before = _transaction_branches()

    try:
        with pytest.raises(DeltaError, match="uncommitted changes"):
            write_deltalake(
                lakefs_path,
                sample_table,
                mode="append",
                storage_options=lakefs_storage_options,
            )

        dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
        assert dt.version() == 0
        assert not branch.object(
            f"{table_name}/_delta_log/00000000000000000001.json"
        ).exists()
        assert _transaction_branches() <= before
    finally:
        branch.reset_changes(path_type="object", path=dirty_path)
