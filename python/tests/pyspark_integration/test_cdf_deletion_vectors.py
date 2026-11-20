"""Tests that deltalake(delta-rs) reads the change data feed of tables written by PySpark."""

import pathlib

import pytest

from deltalake import DeltaTable

from .utils import get_spark

COLUMNS = ["id", "_change_type", "_commit_version"]


def _spark_changes(path: str, start: int, end: int) -> list[tuple]:
    rows = (
        get_spark()
        .read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start)
        .option("endingVersion", end)
        .load(path)
        .select(*COLUMNS)
        .collect()
    )
    return sorted(tuple(row) for row in rows)


def _deltalake_changes(path: str, start: int, end: int) -> list[tuple]:
    data = DeltaTable(path).load_cdf(start, end, columns=COLUMNS).read_all()
    return sorted(zip(*(data[column].to_pylist() for column in COLUMNS)))


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_read_cdf_unpaired_actions_with_deletion_vectors(tmp_path: pathlib.Path):
    """Every commit after version 2 removes or re-adds a file whose deletion vector drops
    ids 2 and 3, without writing change data files, so its change rows are the eight live rows.
    """
    spark = get_spark()
    path = tmp_path.as_posix()
    table = f"delta.`{path}`"
    spark.sql(
        f"CREATE TABLE {table} (id INT, g INT) USING delta PARTITIONED BY (g) "
        "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', "
        "'delta.enableDeletionVectors' = 'true')"
    )

    def write(ids: list[int], mode: str, **options: str) -> None:
        df = spark.createDataFrame([(i, 1) for i in ids], "id INT, g INT")
        df.coalesce(1).write.format("delta").mode(mode).options(**options).save(path)

    restore = f"RESTORE TABLE {table} TO VERSION AS OF 2"
    write(list(range(10)), "append")
    spark.sql(f"DELETE FROM {table} WHERE id IN (2, 3)")
    spark.sql(f"DELETE FROM {table} WHERE g = 1")
    spark.sql(restore)
    spark.sql(f"DELETE FROM {table}")
    spark.sql(restore)
    write([10, 11], "overwrite", replaceWhere="g = 1")
    spark.sql(restore)
    write([20, 21], "overwrite")

    metrics = [
        row.operationMetrics
        for row in spark.sql(f"DESCRIBE HISTORY {table}").orderBy("version").collect()
    ]
    assert metrics[2]["numDeletionVectorsAdded"] == "1"
    assert all(m.get("numAddedChangeFiles", "0") == "0" for m in metrics)

    assert _deltalake_changes(path, 2, 9) == _spark_changes(path, 2, 9)
