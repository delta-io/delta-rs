# Streamed MERGE benchmark

This benchmark compares the two ways that deltalake can MERGE a Polars `LazyFrame`
with `sink_delta(mode="merge")`:

- `streamed_exec=True`: deltalake reads the source once, while the MERGE runs. It
  finds the target files to skip when the whole source is read.
- `streamed_exec=False`: deltalake first reads the whole source into memory, and
  then finds the target files to skip before the MERGE starts.

It runs both modes on the TPC-DS fact tables, with many table layouts and source
sizes, and measures the time and the peak memory of each MERGE.

## What it measures

For each run:

- **Time**: from the call to `sink_delta` until `execute()` returns. This includes
  loading the target table, which `sink_delta` does.
- **Peak memory**: the peak resident memory of the process. Each MERGE runs in a
  new Python process, so the peak belongs to that MERGE.
- **MERGE metrics**, for example the number of target files read.

Both modes must read the same target files, and give the same row counts.
`report.py` checks this.

## The grid

The benchmark runs every combination of these values:

| Axis | Values |
|---|---|
| Table | The 7 TPC-DS fact tables. At scale factor 10, from 0.7M rows (`web_returns`) to 133M rows (`inventory`). |
| Target files | 10, 100, 1000 |
| Partitions | None, by year, by month. A target has at least as many files as partitions. |
| Source size | 0.1%, 1%, 10% of the target rows |
| Source batches | 10, 100, 1000. A source has at least 10 rows per batch. |

At scale factor 10, the grid has 456 cells. Both modes, 5 times each, give 4,560
runs. With 3 runs each, the grid took 45 minutes on an Apple M-series with 16
cores, so 5 runs take about 75 minutes.

## The data

`generate.py` makes the data from TPC-DS, which it generates with duckdb's `dsdgen`.
With the same versions of duckdb and Polars, a scale factor always gives the same
data.

- **Targets**: each fact table keeps all its columns, and gets two more, `p_year`
  (`"2002"`) and `p_month` (`"2002-11"`), from `date_dim`, to partition by. Rows
  without a date are dropped. The rows are sorted by date and primary key, and cut
  into files of equal size, so each file holds a range of dates. A partitioned
  target is partitioned by `p_year` or `p_month`, and shares its files among the
  partitions, in proportion to their rows. The files are written with Polars, and
  become one Delta commit with `convert_to_deltalake`.
- **Sources**: rows from the most recent 10% of the target rows. 80% of them update
  existing rows (the quantity column + 1). 20% are copies with new order numbers
  (new item numbers for `inventory`), so they are inserts. The rows are in random
  order. `sink_delta` streams one batch per Parquet row group, so each source file
  has as many row groups as its batch count, of nearly equal size.
- **Predicate**: the date column, the primary key, and the partition column. For
  example, for `store_sales` partitioned by month:

  ```text
  t.ss_sold_date_sk = s.ss_sold_date_sk AND t.ss_item_sk = s.ss_item_sk
  AND t.ss_ticket_number = s.ss_ticket_number AND t.p_month = s.p_month
  ```

  TPC-DS order numbers do not follow the dates. Without the date column, the
  source rows could match rows in every file, and no file could be skipped.

The MERGE is an upsert: `when_matched_update_all()` and `when_not_matched_insert_all()`.

At scale factor 10, the data uses about 30 GB of disk, and `generate.py` needs
about 25 GB of memory, because it holds a whole fact table in memory. A data
directory holds one scale factor only.

## Requirements

- macOS or Linux, because the peak memory comes from `getrusage`.
- A release build of deltalake. Debug builds are much slower.
- The `bench` dependency group: the Polars of the `polars` group
  (`LazyFrame.sink_delta` needs Polars 1.37 or newer), duckdb, Great Tables for the
  tables of the report, and reactable for its sortable table of every cell. duckdb
  downloads its `tpcds` extension on the first run.

## How to run it

From the `python` directory:

```bash
# The bench group and a release build of deltalake. `make develop`, and `uv run`
# without --no-sync, remove the bench group again.
make develop-bench

# Generate TPC-DS, the targets, the sources, and the manifest (about 3 minutes).
uv run --no-sync python bench/streamed_merge/generate.py --out /tmp/streamed-merge

# Run the grid. You can stop it at any time, and the same command continues it.
uv run --no-sync python bench/streamed_merge/run.py --data /tmp/streamed-merge

# Write /tmp/streamed-merge/report.html.
uv run --no-sync python bench/streamed_merge/report.py --data /tmp/streamed-merge
```

For a quick check, for example of a code change, use a small grid:

```bash
uv run --no-sync python bench/streamed_merge/generate.py --out /tmp/streamed-merge-small \
    --scale-factor 1 --tables store_sales,web_returns --files 10,100 \
    --partitioning none,month --source-fractions 0.01 --batches 10
uv run --no-sync python bench/streamed_merge/run.py --data /tmp/streamed-merge-small --repetitions 1
uv run --no-sync python bench/streamed_merge/report.py --data /tmp/streamed-merge-small
```

Each script shows all its options with `--help`. Some useful ones:

- `run.py --python PATH` measures the deltalake build of another Python
  environment. A results file holds the runs of one build, Polars version and
  machine, so give each build its own `--results` file.
- `run.py --warmup` runs each cell once before its measured runs, without
  recording it. Then the measured runs read the target from the page cache, and
  vary less. It adds one run per cell, so about 10% more runs.

When you run `run.py` again, it skips the runs that the results file has, and runs
failed runs again.

## The files

| File | Purpose |
|---|---|
| `generate.py` | Makes TPC-DS, the targets, the sources, and `manifest.json` with every cell of the grid. |
| `run.py` | Runs each cell with both modes, and appends one JSON line per run to `results.jsonl`. Each run gets its own copy of the target, made of hard links to the target files, so each run starts from the same table. |
| `merge_once.py` | Runs one MERGE and prints its measurements. `run.py` starts it in a new process for each run. |
| `report.py` | Writes `report.html` with tables made with Great Tables and reactable, and checks the results. It stops with an error when a run failed, when the runs of a cell give different row counts or read different files, or when the results come from more than one build, Polars version or machine. |

## How to read the report

The page starts with the result over all cells. Each table shows the streamed
value, the in-memory value, and the change: streamed / in memory − 1, from the
medians of the runs. A blue change means that the streamed MERGE is faster or uses
less memory, an orange change that it is slower or uses more memory. The page has
four tabs:

- **Summary**: the geometric means over the cells with each value of each axis. It
  also counts the cells where the streamed MERGE is faster, and where it uses less
  memory.
- **By table**: time and peak memory, with one row per table and one column group
  per source size. The values are geometric means over the layouts and batch counts.
- **Every cell**: all cells. Click a column title to sort on its values, and type in
  the boxes under the titles to filter. The runs of one cell can differ by 10–20%,
  and sometimes by more. When the runs of the two modes overlap, the change is pale
  and italic, because it can be noise. This table is a reactable table, which loads
  React from `esm.sh`, so it needs a network connection.
- **Setup**: the deltalake build, the Polars version, the machine, and the method.

## Things to know

- The in-memory MERGE can build its hash join on the target instead of the source.
  DataFusion compares the size of the source in memory with the compressed size of
  the target files, and builds on the smaller one. The streamed source has no size
  estimate, so the streamed MERGE always builds on the source. This can make a large
  difference in memory.
- All runs use a local disk, so there is no object store latency.
