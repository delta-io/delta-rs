#!/usr/bin/env python3
#
# This filue contains all the tests of the deltalake python package in a
# multithreaded environment

import pathlib
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

if TYPE_CHECKING:
    import pyarrow as pa


@pytest.mark.pyarrow
def test_concurrency(existing_table: DeltaTable, sample_data_pyarrow: "pa.Table"):
    exception = None

    def comp():
        nonlocal exception
        dt = DeltaTable(existing_table.table_uri)
        for _ in range(5):
            # We should always be able to get a consistent table state
            data = DeltaTable(dt.table_uri).to_pyarrow_table()
            # If two overwrites delete the same file and then add their own
            # concurrently, then this will fail.
            assert data.num_rows == sample_data_pyarrow.num_rows
            try:
                write_deltalake(dt.table_uri, sample_data_pyarrow, mode="overwrite")
            except Exception as e:
                exception = e

    n_threads = 2
    threads = [threading.Thread(target=comp) for _ in range(n_threads)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert isinstance(exception, CommitFailedError)
    assert (
        "a concurrent transaction deleted the same data your transaction deletes"
        in str(exception)
    )


@pytest.mark.polars
def test_multithreaded_write_using_table(tmp_path: pathlib.Path):
    import polars as pl

    table = pl.DataFrame({"a": [1, 2, 3]})
    write_deltalake(tmp_path, table, mode="overwrite")

    dt = DeltaTable(tmp_path)

    with pytest.raises(RuntimeError, match="borrowed"):
        with ThreadPoolExecutor() as exe:
            list(exe.map(lambda _: write_deltalake(dt, table, mode="append"), range(5)))


@pytest.mark.polars
@pytest.mark.xfail(reason="Can fail because of already borrowed")
def test_multithreaded_write_using_path(tmp_path: pathlib.Path):
    import polars as pl

    table = pl.DataFrame({"a": [1, 2, 3]})
    write_deltalake(tmp_path, table, mode="overwrite")

    with ThreadPoolExecutor() as exe:
        list(
            exe.map(lambda _: write_deltalake(tmp_path, table, mode="append"), range(5))
        )
