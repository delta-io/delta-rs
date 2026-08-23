#!/usr/bin/env python3
#
# This filue contains all the tests of the deltalake python package in a
# multithreaded environment

import multiprocessing
import pathlib
import queue
import threading
import time
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
    assert "a concurrent transaction deleted data this operation read" in str(exception)


@pytest.mark.polars
def test_multithreaded_write_using_table(tmp_path: pathlib.Path):
    import polars as pl

    table = pl.DataFrame({"a": [1, 2, 3]})
    write_deltalake(tmp_path, table, mode="overwrite")

    dt = DeltaTable(tmp_path)

    with ThreadPoolExecutor() as exe:
        list(
            exe.map(
                lambda i: write_deltalake(dt, pl.DataFrame({"a": [i]}), mode="append"),
                range(5),
            )
        )


@pytest.mark.polars
def test_multithreaded_write_using_path(tmp_path: pathlib.Path):
    import polars as pl

    table = pl.DataFrame({"a": [1, 2, 3]})
    write_deltalake(tmp_path, table, mode="overwrite")

    with ThreadPoolExecutor() as exe:
        list(
            exe.map(
                lambda _: write_deltalake(
                    tmp_path, pl.DataFrame({"a": [1, 2, 3]}), mode="append"
                ),
                range(5),
            )
        )


def _gil_release_ratio(
    fn, min_iterations: int = 10, min_elapsed: float = 0.5
) -> tuple[float, float]:
    """Measure whether a Python thread can run while fn executes.

    The return value is the counter thread tick rate while fn runs divided by
    its baseline tick rate. Near 1.0 means the GIL was mostly released; near 0.0
    means the calling thread mostly held it.
    Returns (ratio, elapsed_seconds); min_elapsed is in seconds.
    """
    counter = 0
    stop = threading.Event()

    def tick():
        nonlocal counter
        while not stop.is_set():
            counter += 1

    thread = threading.Thread(target=tick, daemon=True)
    thread.start()
    try:
        # Give the counter thread time to start before measuring its baseline.
        time.sleep(0.25)

        baseline_start = counter
        time.sleep(0.1)
        baseline_rate = (counter - baseline_start) / 0.1
        assert baseline_rate > 0, (
            f"counter thread never ticked during baseline (counter={counter})"
        )

        start_count = counter
        started_at = time.perf_counter()
        iterations = 0
        while (
            iterations < min_iterations
            or time.perf_counter() - started_at < min_elapsed
        ):
            fn()
            iterations += 1
        elapsed = time.perf_counter() - started_at
        assert elapsed > 0
        during_rate = (counter - start_count) / elapsed
        return during_rate / baseline_rate, elapsed
    finally:
        stop.set()
        thread.join(timeout=1)


def _probe_s3_update_and_read_version(table_uri, result_queue):
    dt = DeltaTable(table_uri)
    barrier = threading.Barrier(2, timeout=5)
    errors = queue.Queue()
    updates_started = threading.Event()
    update_done = threading.Event()
    read_count = 0

    def run_update():
        try:
            barrier.wait()
            updates_started.set()
            started_at = time.perf_counter()
            iterations = 0
            while iterations < 5 or time.perf_counter() - started_at < 0.5:
                dt.update_incremental()
                iterations += 1
        except Exception as err:
            errors.put(f"update thread: {err!r}")
        finally:
            update_done.set()

    def read_version():
        nonlocal read_count
        try:
            barrier.wait()
            assert updates_started.wait(timeout=5)
            while not update_done.is_set():
                assert dt.version() is not None
                read_count += 1
                time.sleep(0)
            assert dt.version() is not None
            read_count += 1
        except Exception as err:
            errors.put(f"reader thread: {err!r}")

    threads = [
        threading.Thread(target=run_update, name="update_incremental", daemon=True),
        threading.Thread(target=read_version, name="read_version", daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    alive_threads = [thread.name for thread in threads if thread.is_alive()]
    if alive_threads:
        result_queue.put(("deadlock", alive_threads, read_count))
        return

    error_messages = []
    while True:
        try:
            error_messages.append(errors.get_nowait())
        except queue.Empty:
            break

    if error_messages:
        result_queue.put(("error", error_messages, read_count))
    elif read_count == 0:
        result_queue.put(("error", ["reader thread did not run"], read_count))
    else:
        result_queue.put(("ok", [], read_count))


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=30, method="thread")
@pytest.mark.parametrize(
    "method_factory",
    [
        lambda dt: dt.update_incremental,
        lambda dt: lambda: dt.transaction_version("missing_app_id"),
    ],
    ids=["update_incremental", "transaction_version"],
)
def test_s3_methods_release_gil(
    request, s3_localstack, s3_localstack_simple_table_uri, method_factory
):
    dt = DeltaTable(s3_localstack_simple_table_uri)
    # Keep this loose: CI CPU scheduling can reduce the counter thread's rate even
    # when the tested method releases the GIL around Rust I/O. Issue #4402 reported
    # before fix ratios below 5% and released call ratios near 90%.
    min_release_ratio = 0.2

    release_ratio, elapsed = _gil_release_ratio(method_factory(dt))
    method_name = request.node.callspec.id
    assert release_ratio > min_release_ratio, (
        f"{method_name} held the GIL too long: "
        f"ratio={release_ratio:.1%}, elapsed={elapsed:.3f}s"
    )


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=30, method="thread")
def test_s3_update_incremental_does_not_deadlock_concurrent_table_access(
    s3_localstack, s3_localstack_simple_table_uri
):
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()
    process = ctx.Process(
        target=_probe_s3_update_and_read_version,
        args=(s3_localstack_simple_table_uri, result_queue),
        daemon=True,
    )
    process.start()
    process.join(timeout=15)

    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
        assert False, "concurrent update and read version probe timed out"

    try:
        status, details, read_count = result_queue.get(timeout=5)
    except queue.Empty:
        assert False, f"concurrent probe exited without a result: {process.exitcode=}"

    assert process.exitcode == 0
    assert status == "ok", (
        f"concurrent probe failed with {status}: {details}; read_count={read_count}"
    )
