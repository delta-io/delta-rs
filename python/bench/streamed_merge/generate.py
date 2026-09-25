"""Generate the target tables, the MERGE sources, and the manifest of the benchmark.

The data comes from the TPC-DS fact tables, generated with duckdb's `dsdgen`. Each
fact table gives one target for each file count and partitioning, and one source
for each source size and batch count. README.md, "The data", tells how they are
made.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from functools import cache, partial
from pathlib import Path
from typing import NamedTuple

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# run.py is next to this script, and Python puts the directory of a script on the
# import path.
from run import Cell

from deltalake import Field, Schema, convert_to_deltalake


class FactTable(NamedTuple):
    date: str
    key: list[str]
    new_key: str
    """The column that gets new values, so that a copied row is an insert."""
    quantity: str
    """The column that an update changes."""

    @property
    def sort_key(self) -> list[str]:
        """Return the date column, then the rest of the primary key."""
        return [self.date, *(k for k in self.key if k != self.date)]


TABLES = {
    "store_sales": FactTable(
        "ss_sold_date_sk",
        ["ss_item_sk", "ss_ticket_number"],
        "ss_ticket_number",
        "ss_quantity",
    ),
    "catalog_sales": FactTable(
        "cs_sold_date_sk",
        ["cs_item_sk", "cs_order_number"],
        "cs_order_number",
        "cs_quantity",
    ),
    "web_sales": FactTable(
        "ws_sold_date_sk",
        ["ws_item_sk", "ws_order_number"],
        "ws_order_number",
        "ws_quantity",
    ),
    "store_returns": FactTable(
        "sr_returned_date_sk",
        ["sr_item_sk", "sr_ticket_number"],
        "sr_ticket_number",
        "sr_return_quantity",
    ),
    "catalog_returns": FactTable(
        "cr_returned_date_sk",
        ["cr_item_sk", "cr_order_number"],
        "cr_order_number",
        "cr_return_quantity",
    ),
    "web_returns": FactTable(
        "wr_returned_date_sk",
        ["wr_item_sk", "wr_order_number"],
        "wr_order_number",
        "wr_return_quantity",
    ),
    "inventory": FactTable(
        "inv_date_sk",
        ["inv_date_sk", "inv_item_sk", "inv_warehouse_sk"],
        "inv_item_sk",
        "inv_quantity_on_hand",
    ),
}


class Target(NamedTuple):
    path: Path
    partitions: int


@dataclass
class Axes:
    """The values of the grid axes to make data for."""

    tables: list[str]
    files: list[int]
    partitioning: list[str]
    source_fractions: list[float]
    batches: list[int]


PARTITION_COLUMNS = {"none": None, "year": "p_year", "month": "p_month"}
TOUCHED_FRACTION = 0.1
UPDATE_FRACTION = 0.8
MIN_ROWS_PER_BATCH = 10


@contextmanager
def complete(path: Path) -> Iterator[Path]:
    """Yield the path to write `path` to, and give the result its name when it is done.

    So a stopped run leaves no file or directory that looks complete, and the next
    run writes it again.
    """
    partial = path.with_name(path.name + ".partial")

    # Remove what a stopped run left, for example also the log of a duckdb database.
    for stale in path.parent.glob(f"{partial.name}*"):
        if stale.is_dir():
            shutil.rmtree(stale)
        else:
            stale.unlink()

    yield partial
    partial.rename(path)


def slices(count: int, parts: int) -> Iterator[tuple[int, int]]:
    """Yield the offset and length of each of `parts` nearly equal slices of `count` rows."""
    for i in range(parts):
        start = i * count // parts
        yield start, (i + 1) * count // parts - start


def tpcds(out: Path, scale_factor: int) -> duckdb.DuckDBPyConnection:
    """Open the TPC-DS database, and generate it first when it does not exist."""
    path = out / f"tpcds_sf{scale_factor}.duckdb"

    if not path.exists():
        start = time.perf_counter()
        with complete(path) as partial, duckdb.connect(str(partial)) as con:
            con.sql(f"INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf = {scale_factor})")

        seconds = time.perf_counter() - start
        print(f"TPC-DS at scale factor {scale_factor} in {seconds:.0f} s", flush=True)

    return duckdb.connect(str(path), read_only=True)


def dated(table: str) -> str:
    """Return the query of the rows of a fact table that have a date, with p_year and p_month."""
    return f"""
        select t.*,
               strftime(d.d_date, '%Y') as p_year,
               strftime(d.d_date, '%Y-%m') as p_month
        from {table} t join date_dim d on t.{TABLES[table].date} = d.d_date_sk
    """


def load(con: duckdb.DuckDBPyConnection, table: str) -> pl.DataFrame:
    """Return the rows of a fact table that have a date, sorted by date and primary key."""
    order = ", ".join(f"t.{column}" for column in TABLES[table].sort_key)
    rows = con.sql(f"{dated(table)} order by {order}").pl()
    print(f"{table}: {rows.height:,} rows with a date", flush=True)
    return rows


def partition_counts(
    con: duckdb.DuckDBPyConnection, table: str, column: str | None
) -> list[tuple[str | None, int]]:
    """Return the value and the row count of each partition, in the order of load().

    Without a partition column, return one pair for all rows. duckdb counts without a
    sort, so a run that finds all files of a table does not load the table.
    """
    if not column:
        (count,) = con.sql(f"select count(*) from ({dated(table)})").fetchone()
        return [(None, count)]

    # The values are years or months, so their order is the order of the dates.
    return con.sql(
        f"select {column}, count(*) from ({dated(table)}) group by 1 order by 1"
    ).fetchall()


def share(files: int, rows_per_partition: list[int]) -> list[int]:
    """Share the files among the partitions in proportion to their rows.

    Each partition gets at least one file.
    """
    total = sum(rows_per_partition)
    quotas = [files * rows / total for rows in rows_per_partition]
    shares = [max(1, int(quota)) for quota in quotas]

    # Give the files that are left to the partitions with the largest remainders.
    by_remainder = sorted(
        range(len(quotas)), key=lambda i: quotas[i] - int(quotas[i]), reverse=True
    )
    for i in range(files - sum(shares)):
        shares[by_remainder[i]] += 1

    # Take back the files that the minimum of one file per partition added.
    while sum(shares) > files:
        shares[shares.index(max(shares))] -= 1

    return shares


def write_target(
    path: Path,
    rows: pl.DataFrame,
    files: int,
    column: str | None,
    counts: list[tuple[str | None, int]],
) -> None:
    """Write the rows as a Delta table with the given number of files.

    `counts` holds the value and the row count of each partition, in the order of the
    rows, or one pair for all rows when the table has no partition column.
    """
    shares = share(files, [count for _, count in counts])
    data = rows.drop(column) if column else rows

    offset = index = 0
    for (value, count), part_files in zip(counts, shares, strict=True):
        directory = path / f"{column}={value}" if column else path
        part = data.slice(offset, count)

        directory.mkdir(parents=True, exist_ok=True)
        for start, length in slices(count, part_files):
            file = directory / f"part-{index:05d}.parquet"
            part.slice(start, length).write_parquet(file)
            index += 1

        offset += count

    if column:
        partition_schema = Schema([Field(column, "string", nullable=True)])
        convert_to_deltalake(
            path, partition_by=partition_schema, partition_strategy="hive"
        )
    else:
        convert_to_deltalake(path)


def source_rows(table: str, rows: pl.DataFrame, count: int) -> pl.DataFrame:
    """Return `count` source rows: updates and inserts for the most recent target rows."""
    fact = TABLES[table]
    touched = rows.slice(rows.height - int(rows.height * TOUCHED_FRACTION))
    updates = int(count * UPDATE_FRACTION)
    inserts = count - updates

    updated = touched.sample(n=updates, seed=1).with_columns(pl.col(fact.quantity) + 1)

    new_keys = pl.lit(rows[fact.new_key].max()) + 1 + pl.int_range(inserts)
    inserted = touched.sample(n=inserts, seed=2).with_columns(
        new_keys.cast(rows.schema[fact.new_key]).alias(fact.new_key)
    )

    return pl.concat([updated, inserted]).sample(fraction=1.0, shuffle=True, seed=3)


def predicate(table: str, partitioning: str) -> str:
    columns = TABLES[table].sort_key
    if PARTITION_COLUMNS[partitioning]:
        columns.append(PARTITION_COLUMNS[partitioning])

    return " AND ".join(f"t.{column} = s.{column}" for column in columns)


def write_targets(
    out: Path,
    con: duckdb.DuckDBPyConnection,
    table: str,
    rows: Callable[[], pl.DataFrame],
    axes: Axes,
) -> dict[tuple[int, str], Target]:
    """Write the targets of one fact table, for each file count and partitioning."""
    targets = {}

    for partitioning in axes.partitioning:
        column = PARTITION_COLUMNS[partitioning]
        counts = partition_counts(con, table, column)

        for files in axes.files:
            if files < len(counts):
                continue

            path = Path("targets") / f"{table}_files{files}_{partitioning}"
            if not (out / path).exists():
                start = time.perf_counter()
                with complete(out / path) as partial:
                    write_target(partial, rows(), files, column, counts)
                print(f"{path}: {time.perf_counter() - start:.1f} s", flush=True)

            targets[files, partitioning] = Target(path, len(counts))

    return targets


def write_source(path: Path, source: pa.Table, batches: int) -> None:
    """Write the source rows in exactly `batches` row groups of nearly equal size.

    One row group size cannot always give `batches` row groups: 10,500 rows in 1,000
    row groups need groups of 10 and of 11 rows.
    """
    with (
        complete(path) as partial,
        pq.ParquetWriter(partial, source.schema, compression="zstd") as writer,
    ):
        for start, length in slices(source.num_rows, batches):
            writer.write_table(source.slice(start, length), length)


def write_sources(
    out: Path,
    table: str,
    rows: Callable[[], pl.DataFrame],
    target_rows: int,
    fraction: float,
    batch_counts: list[int],
) -> tuple[int, dict[int, Path]]:
    """Write the sources of one fact table and size, one file for each batch count.

    Returns the number of source rows, and the file of each batch count.
    """
    count = max(1, round(target_rows * fraction))
    paths = {
        batches: Path("sources") / f"{table}_{fraction:g}_batches{batches}.parquet"
        for batches in batch_counts
        if count >= MIN_ROWS_PER_BATCH * batches
    }
    if all((out / path).exists() for path in paths.values()):
        return count, paths

    source = source_rows(table, rows(), count).to_arrow()
    (out / "sources").mkdir(exist_ok=True)
    for batches, path in paths.items():
        write_source(out / path, source, batches)

    print(f"sources of {table}: {count} rows, {fraction:g} of the target", flush=True)
    return count, paths


def generate_table(
    out: Path, con: duckdb.DuckDBPyConnection, table: str, axes: Axes
) -> list[Cell]:
    """Write the targets and sources of one fact table, and return its cells of the grid.

    The table loads only when one of its targets or sources is missing. Its rows are
    freed when this returns, before the next table loads.
    """
    rows = cache(partial(load, con, table))
    [(_, target_rows)] = partition_counts(con, table, None)

    targets = write_targets(out, con, table, rows, axes)

    cells = []
    for fraction in axes.source_fractions:
        count, sources = write_sources(
            out, table, rows, target_rows, fraction, axes.batches
        )

        for (files, partitioning), target in targets.items():
            for batches, source in sources.items():
                cells.append(
                    Cell(
                        table=table,
                        files=files,
                        partitioning=partitioning,
                        source_fraction=fraction,
                        batches=batches,
                        target_rows=target_rows,
                        partitions=target.partitions,
                        source_rows=count,
                        target=str(target.path),
                        source=str(source),
                        predicate=predicate(table, partitioning),
                    )
                )

    return cells


def comma_list(kind: Callable[[str], object]) -> Callable[[str], list]:
    """Return an argparse type that splits a value at the commas and converts each item."""
    return lambda value: [kind(item) for item in value.split(",")]


def parse_args() -> tuple[Path, int, Axes]:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="the directory for the data and the manifest, one per scale factor",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        default=10,
        help="the TPC-DS scale factor (default: 10)",
    )
    parser.add_argument(
        "--tables",
        type=comma_list(str),
        default=list(TABLES),
        help="the fact tables, separated by commas (default: all 7)",
    )
    parser.add_argument(
        "--files",
        type=comma_list(int),
        default=[10, 100, 1000],
        help="the file counts of the targets (default: 10,100,1000)",
    )
    parser.add_argument(
        "--partitioning",
        type=comma_list(str),
        default=list(PARTITION_COLUMNS),
        help="the partitionings of the targets (default: none,year,month)",
    )
    parser.add_argument(
        "--source-fractions",
        type=comma_list(float),
        default=[0.001, 0.01, 0.1],
        help="the source sizes, as fractions of the target rows (default: 0.001,0.01,0.1)",
    )
    parser.add_argument(
        "--batches",
        type=comma_list(int),
        default=[10, 100, 1000],
        help="the batch counts of the sources (default: 10,100,1000)",
    )
    args = parser.parse_args()

    unknown = [table for table in args.tables if table not in TABLES]
    if unknown:
        parser.error(f"unknown tables {unknown}, choose from {list(TABLES)}")

    unknown = [kind for kind in args.partitioning if kind not in PARTITION_COLUMNS]
    if unknown:
        parser.error(
            f"unknown partitionings {unknown}, choose from {list(PARTITION_COLUMNS)}"
        )

    most = TOUCHED_FRACTION / UPDATE_FRACTION
    if max(args.source_fractions) > most:
        parser.error(
            f"a source fraction can be at most {most:g}, "
            f"because the updates come from the last {TOUCHED_FRACTION:.0%} of the target"
        )

    # The paths of the targets and sources do not contain the scale factor, so a
    # directory must hold one scale factor only.
    manifest = args.out / "manifest.json"
    if manifest.exists():
        existing = json.loads(manifest.read_text())["scale_factor"]
        if existing != args.scale_factor:
            parser.error(
                f"{args.out} has data for scale factor {existing}, "
                "so use another --out directory"
            )

    axes = Axes(
        args.tables, args.files, args.partitioning, args.source_fractions, args.batches
    )
    return args.out, args.scale_factor, axes


def main() -> None:
    out, scale_factor, axes = parse_args()
    out.mkdir(parents=True, exist_ok=True)

    # Record the scale factor first, so that it is also known after a stopped run.
    manifest = out / "manifest.json"
    if not manifest.exists():
        manifest.write_text(json.dumps({"scale_factor": scale_factor, "cells": []}))

    con = tpcds(out, scale_factor)

    cells: list[Cell] = []
    for table in axes.tables:
        cells += generate_table(out, con, table, axes)

    manifest.write_text(
        json.dumps(
            {"scale_factor": scale_factor, "cells": [asdict(cell) for cell in cells]},
            indent=1,
        )
    )
    print(f"manifest with {len(cells)} cells", flush=True)


if __name__ == "__main__":
    main()
