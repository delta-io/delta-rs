"""Write the benchmark results as one HTML page of tables, and check that both modes agree.

For each cell and mode, the time and the peak memory are the medians of the runs.
Change = streamed / in memory - 1, so a negative change means that the streamed
MERGE is faster or uses less memory. A group of cells uses the geometric mean.
"""

from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path
from string import Template

import polars as pl
import polars.selectors as cs
import reactable
from great_tables import GT
from reactable import ColFormat, ColGroup, Column, Reactable

# run.py is next to this script, and Python puts the directory of a script on the
# import path.
from run import CELL_FIELDS, ENVIRONMENT_FIELDS

AXES = list(CELL_FIELDS)
TITLES = {
    "table": "Table",
    "files": "Target files",
    "partitioning": "Partitions",
    "source_fraction": "Source size",
    "batches": "Source batches",
}

# The counts that all runs of a cell must agree on. The number of files that a MERGE
# writes is not one of them: it depends on how the writer splits the new rows into
# files, which can differ between the modes and between runs.
COUNTS = [
    "num_source_rows",
    "num_target_rows_inserted",
    "num_target_rows_updated",
    "num_target_rows_deleted",
    "num_target_rows_copied",
    "num_output_rows",
    "num_target_files_scanned",
    "num_target_files_removed",
]

# The order of the values of each axis in the tables, when it is not the order of the
# values themselves: tables from small to large, partitionings from few partitions
# to many.
ORDER = {"table": pl.col("target_rows"), "partitioning": pl.col("partitions")}

# Blue when the streamed MERGE is better, orange when it is worse. Colour-blind
# readers can tell these apart, unlike green and red.
BLUE_ORANGE = ["#2166ac", "#f7f7f7", "#e66101"]
# The change that gets the full blue or orange.
CHANGE_LIMIT = 0.5

# The JavaScript and CSS of the table of every cell, which the reactable package ships.
REACTABLE_ASSETS = Path(reactable.__file__).parent / "static"

# A short header, then one tab for each view. The tabs are radio buttons, so they need
# no JavaScript. The table of every cell is a reactable table: its script loads the
# reactable component, and React from esm.sh.
PAGE = Template("""<!doctype html>
<html><head><meta charset="utf-8"><title>Streamed MERGE benchmark</title>
<style>$reactable_css</style>
<script type="importmap">
{"imports": {"react": "https://esm.sh/react@18", "react-dom": "https://esm.sh/react-dom@18",
"react-dom/client": "https://esm.sh/react-dom@18/client"}}
</script>
<style>
body { font-family: system-ui, sans-serif; margin: 2rem; }
.tabs > input { display: none; }
.tabs > label { display: inline-block; padding: .5rem 1rem; cursor: pointer; border-bottom: 3px solid transparent; }
.tabs > input:checked + label { border-bottom-color: #2166ac; font-weight: 600; }
.tabs > section { display: none; border-top: 1px solid #d0d7de; padding-top: 1rem; }
#summary:checked ~ .summary, #tables:checked ~ .tables,
#cells:checked ~ .cells, #setup:checked ~ .setup { display: block; }
</style></head><body>
<h1>Streamed MERGE: streamed_exec=True against False</h1>
<p>$description</p>
<p><b>$result</b></p>
<p>Each table shows the streamed value, the in-memory value, and the change: streamed / in
memory &minus; 1. A blue change means that the streamed MERGE is faster or uses less memory,
an orange change that it is slower or uses more memory.</p>
$problems
<div class="tabs">
<input type="radio" name="tab" id="summary" checked><label for="summary">Summary</label>
<input type="radio" name="tab" id="tables"><label for="tables">By table</label>
<input type="radio" name="tab" id="cells"><label for="cells">Every cell</label>
<input type="radio" name="tab" id="setup"><label for="setup">Setup</label>
<section class="summary">$summary</section>
<section class="tables">$tables</section>
<section class="cells">
<h3>Every cell</h3>
<p>Median of $repetitions runs. Click a column title to sort, and type in the boxes under the
titles to filter. A pale, italic change can be noise: the runs of the two modes overlap.</p>
<div id="cells-table"></div>
</section>
<section class="setup">
<p>$environment</p>
<ul>
<li>Target: the rows of a TPC-DS fact table that have a date, sorted by date and primary
key, and cut into files of equal size. A partitioned target is partitioned by the year or
the month of the date, and shares its files among the partitions.</li>
<li>Source: rows from the most recent 10% of the target. 80% update existing rows, 20% are
inserts. In random order. Polars streams one batch per Parquet row group of the source.</li>
<li>Predicate: the date column, the primary key, and the partition column.</li>
<li>Each run uses a new Python process and a new copy of the target. The order of the two
modes alternates. All runs use a local disk, so there is no object store latency.</li>
</ul>
<p>See python/bench/streamed_merge/README.md for the details.</p>
</section>
</div>
<script type="module">
import React from "react";
import { createRoot } from "react-dom/client";

const component = new Blob([$reactable_js], { type: "text/javascript" });
const { default: Reactable } = await import(URL.createObjectURL(component));
const props = $cells_props;
createRoot(document.getElementById("cells-table")).render(
  React.createElement(Reactable, props),
);
</script>
</body></html>
""")


def gmean(column: str) -> pl.Expr:
    return pl.col(column).log().mean().exp()


def order(axis: str) -> pl.Expr:
    return ORDER.get(axis, pl.col(axis))


# The statistics of a group of cells.
STATS = [
    pl.len().alias("cells"),
    gmean("s_streamed"),
    gmean("s_in_memory"),
    gmean("mb_streamed"),
    gmean("mb_in_memory"),
    (pl.col("s_streamed") < pl.col("s_in_memory")).sum().alias("faster"),
    (pl.col("mb_streamed") < pl.col("mb_in_memory")).sum().alias("smaller"),
]


def change(streamed: str, in_memory: str) -> pl.Expr:
    """Return streamed / in memory - 1, and 0 when that rounds to 0%."""
    value = pl.col(streamed) / pl.col(in_memory) - 1
    return pl.when(value.abs() < 0.005).then(0.0).otherwise(value)


# The changes of the time and of the memory, from the columns of the two modes.
CHANGES = {
    "time_change": change("s_streamed", "s_in_memory"),
    "memory_change": change("mb_streamed", "mb_in_memory"),
}


def check(runs: pl.DataFrame, succeeded: pl.DataFrame) -> list[str]:
    """Return the problems of the results.

    A problem is a failed run that no later run of the same cell, mode and repetition
    replaced, no successful run at all, runs of more than one environment, or a cell
    whose runs give different row and file counts.
    """
    problems = []
    run = [*AXES, "mode", "repetition"]

    failed = runs.filter(pl.col("error").is_not_null()).join(
        succeeded.select(run), on=run, how="anti"
    )
    for record in failed.iter_rows(named=True):
        cell = {axis: record[axis] for axis in AXES}
        problems.append(f"failed run {cell} {record['mode']}: {record['error'][:300]}")

    if succeeded.is_empty():
        problems.append("no run succeeded")
        return problems

    for field in ENVIRONMENT_FIELDS:
        values = succeeded[field].unique().to_list()
        if len(values) > 1:
            problems.append(f"the runs have more than one {field}: {values}")

    # All runs of a cell, in both modes, must give the same row and file counts.
    counts = pl.struct([pl.col("metrics").struct.field(name) for name in COUNTS])
    different = (
        succeeded.group_by(AXES)
        .agg(counts.n_unique().alias("n"))
        .filter(pl.col("n") > 1)
    )
    for cell in different.iter_rows(named=True):
        problems.append(f"the runs of this cell give different counts: {cell}")

    return problems


def overlap(key: str) -> pl.Expr:
    """Return whether the runs of the two modes overlap, so that the change can be noise."""
    return (pl.col(f"{key}_min_streamed") <= pl.col(f"{key}_max_in_memory")) & (
        pl.col(f"{key}_min_in_memory") <= pl.col(f"{key}_max_streamed")
    )


def cells(succeeded: pl.DataFrame) -> pl.DataFrame:
    """Return one row per cell, with the median time and memory of both modes."""
    info = ["target_rows", "partitions", "source_rows"]
    files_read = pl.col("metrics").struct.field("num_target_files_scanned")
    per_mode = succeeded.group_by([*AXES, *info, "mode"]).agg(
        pl.col("elapsed_s").median().alias("s"),
        pl.col("elapsed_s").min().alias("s_min"),
        pl.col("elapsed_s").max().alias("s_max"),
        pl.col("max_rss_mb").median().alias("mb"),
        pl.col("max_rss_mb").min().alias("mb_min"),
        pl.col("max_rss_mb").max().alias("mb_max"),
        files_read.first().alias("files_read"),
    )

    # One column per mode, for example s_streamed and s_in_memory.
    wide = per_mode.pivot(on="mode", index=[*AXES, *info])

    return (
        wide.drop_nulls()
        .with_columns(**CHANGES, time_noise=overlap("s"), memory_noise=overlap("mb"))
        .with_columns(
            table_label=pl.format(
                "{} ({}M rows)", "table", (pl.col("target_rows") / 1e6).round(1)
            ),
            files_label=pl.col("files").cast(pl.String),
            partitioning_label=pl.when(pl.col("partitioning") == "none")
            .then(pl.col("partitioning"))
            .otherwise("by " + pl.col("partitioning")),
            source_fraction_label=pl.format(
                "{}%", (pl.col("source_fraction") * 100).round(3)
            ),
            batches_label=pl.col("batches").cast(pl.String),
        )
        .sort(*(order(axis) for axis in AXES))
    )


def format_values(table: GT) -> GT:
    """Format the times, the memory, and the changes of a table, and color the changes.

    A change is blue when the streamed MERGE is better, and orange when it is worse.
    """
    changes = cs.contains("change")
    return (
        table.fmt_number(cs.starts_with("s_"), decimals=3, pattern="{x} s")
        .fmt_integer(cs.starts_with("mb_"), pattern="{x} MB")
        .fmt_percent(changes, decimals=0, force_sign=True)
        .data_color(
            columns=changes,
            palette=BLUE_ORANGE,
            domain=[-CHANGE_LIMIT, CHANGE_LIMIT],
            na_color="#ffffff",
            truncate=True,
        )
        # A table can lack a source size.
        .sub_missing(missing_text="")
    )


def rgb(color: str) -> tuple[int, ...]:
    return tuple(bytes.fromhex(color[1:]))


def mix(a: tuple[float, ...], b: tuple[float, ...], share: float) -> tuple[float, ...]:
    """Return the color that is `share` of the way from `a` to `b`."""
    return tuple(x + (y - x) * share for x, y in zip(a, b, strict=True))


def change_style(value: float, noise: bool) -> dict[str, str]:
    """Return the style of a change cell, with the colors of format_values().

    A change that can be noise is pale and italic.
    """
    low, middle, high = (rgb(color) for color in BLUE_ORANGE)
    share = max(-1.0, min(1.0, value / CHANGE_LIMIT))
    color = mix(middle, low, -share) if share < 0 else mix(middle, high, share)
    if noise:
        color = mix(color, (255, 255, 255), 0.65)

    luminance = (0.299 * color[0] + 0.587 * color[1] + 0.114 * color[2]) / 255
    style = {
        "background": "#{:02x}{:02x}{:02x}".format(*(round(c) for c in color)),
        "color": "white" if luminance < 0.5 else "black",
    }
    if noise:
        style["fontStyle"] = "italic"
    return style


def summary(grid: pl.DataFrame) -> str:
    """Return one sentence with the result over all cells."""
    all_cells = grid.select(STATS).with_columns(**CHANGES).row(0, named=True)

    return (
        f"The streamed MERGE is faster in {all_cells['faster']} of {grid.height} cells "
        f"(geometric mean change {all_cells['time_change']:+.0%}), and uses less "
        f"memory in {all_cells['smaller']} of {grid.height} cells "
        f"(geometric mean change {all_cells['memory_change']:+.0%})."
    )


def description(grid: pl.DataFrame, scale_factor: int, repetitions: int) -> str:
    """Return one sentence about what the benchmark compares."""
    return (
        'Polars scan_parquet(source).sink_delta(target, mode="merge"), as an upsert, '
        f"on the TPC-DS fact tables at scale factor {scale_factor}: {grid.height} cells "
        "of table layouts and sources, each run "
        f"{repetitions} times with streamed_exec=True and {repetitions} times with "
        "streamed_exec=False. The tables show the medians of the runs."
    )


def environment(succeeded: pl.DataFrame) -> str:
    """Return one sentence about the software and the machine of the runs."""
    first = succeeded.row(0, named=True)
    host = first["machine"]

    return (
        f"deltalake {first['deltalake']} (build {first['build']}), "
        f"Polars {first['polars']}, {host['processor']}, {host['cpus']} CPUs, "
        f"{host['memory_gib']} GiB, {host['platform']}. Memory is the peak RSS of the "
        f"Python process, which is {first['baseline_rss_mb']} MB before the MERGE starts."
    )


def axis_table(grid: pl.DataFrame) -> GT:
    """Return the table with the geometric means of the cells of each axis value."""
    groups = [
        grid.group_by(pl.col(f"{axis}_label").alias("value"))
        .agg(order(axis).first().alias("order"), *STATS)
        .sort("order")
        .select(pl.lit(TITLES[axis]).alias("axis"), pl.exclude("order"))
        for axis in AXES
    ]

    rows = (
        pl.concat(groups)
        .with_columns(
            **CHANGES,
            faster=pl.format("{} of {}", "faster", "cells"),
            smaller=pl.format("{} of {}", "smaller", "cells"),
        )
        .select(
            "axis",
            "value",
            "cells",
            "s_streamed",
            "s_in_memory",
            "time_change",
            "faster",
            "mb_streamed",
            "mb_in_memory",
            "memory_change",
            "smaller",
        )
    )

    table = (
        GT(rows, rowname_col="value", groupname_col="axis")
        .tab_header(
            title="By axis",
            subtitle="Geometric mean over the cells with each value of each axis",
        )
        .tab_spanner("Time", ["s_streamed", "s_in_memory", "time_change", "faster"])
        .tab_spanner(
            "Peak memory", ["mb_streamed", "mb_in_memory", "memory_change", "smaller"]
        )
        .cols_label(
            cells="Cells",
            s_streamed="Streamed",
            s_in_memory="In memory",
            time_change="Change",
            faster="Streamed faster",
            mb_streamed="Streamed",
            mb_in_memory="In memory",
            memory_change="Change",
            smaller="Streamed smaller",
        )
    )
    return format_values(table)


def matrix_table(grid: pl.DataFrame, key: str, title: str) -> GT:
    """Return one row per table and three columns per source size, for one metric.

    `key` is the prefix of the metric's columns: "s" for time, "mb" for memory.
    """
    streamed, in_memory = f"{key}_streamed", f"{key}_in_memory"

    per_size = (
        grid.group_by("table_label", "target_rows", "source_fraction_label")
        .agg(gmean(streamed), gmean(in_memory), pl.col("source_fraction").first())
        .with_columns(change=change(streamed, in_memory))
        .sort("source_fraction", "target_rows")
    )
    sizes = per_size["source_fraction_label"].unique(maintain_order=True).to_list()
    values = [streamed, in_memory, "change"]

    # One column per value and source size, for example s_streamed_1%. The columns
    # are in the order of the sizes, because the smallest table can lack a size.
    rows = (
        per_size.pivot(
            on="source_fraction_label",
            index=["table_label", "target_rows"],
            values=values,
        )
        .sort("target_rows")
        .select("table_label", *(f"{v}_{size}" for size in sizes for v in values))
    )

    table = GT(rows, rowname_col="table_label").tab_header(
        title=f"{title} by table and source size",
        subtitle="Geometric mean over the layouts and batch counts",
    )
    for size in sizes:
        columns = [f"{v}_{size}" for v in values]
        labels = dict(zip(columns, ["Streamed", "In memory", "Change"], strict=True))
        table = table.tab_spanner(f"Source {size} of the target", columns).cols_label(
            cases=labels
        )

    return format_values(table)


def cells_table(grid: pl.DataFrame) -> Reactable:
    """Return every cell as a reactable table, which sorts and filters on the values."""

    def styles(kind: str) -> list[dict[str, str]]:
        changes, noise = grid[f"{kind}_change"], grid[f"{kind}_noise"]
        return [change_style(c, n) for c, n in zip(changes, noise, strict=True)]

    seconds = ColFormat(suffix=" s", digits=3)
    megabytes = ColFormat(suffix=" MB", digits=0, separators=True)
    percent = ColFormat(percent=True, digits=0)

    columns = [
        Column(id="table", name="Table", min_width=150),
        Column(id="target_rows", name="Rows", format=ColFormat(separators=True)),
        Column(id="files", name="Files"),
        Column(id="partitioning", name="Partitions", min_width=130),
        Column(id="files_read", name="Files read"),
        Column(id="source_rows", name="Rows", format=ColFormat(separators=True)),
        Column(id="batches", name="Batches"),
        Column(id="s_streamed", name="Streamed", format=seconds),
        Column(id="s_in_memory", name="In memory", format=seconds),
        Column(id="time_change", name="Change", format=percent, style=styles("time")),
        Column(id="mb_streamed", name="Streamed", format=megabytes),
        Column(id="mb_in_memory", name="In memory", format=megabytes),
        Column(
            id="memory_change", name="Change", format=percent, style=styles("memory")
        ),
    ]
    rows = grid.with_columns(
        partitioning=pl.when(pl.col("partitioning") == "none")
        .then(pl.col("partitioning_label"))
        .otherwise(pl.format("{} ({})", "partitioning_label", "partitions")),
        files_read="files_read_streamed",
    ).select(column.id for column in columns)

    return Reactable(
        rows,
        columns=columns,
        column_groups=[
            ColGroup(
                name="Target",
                columns=["table", "target_rows", "files", "partitioning", "files_read"],
            ),
            ColGroup(name="Source", columns=["source_rows", "batches"]),
            ColGroup(name="Time", columns=["s_streamed", "s_in_memory", "time_change"]),
            ColGroup(
                name="Peak memory",
                columns=["mb_streamed", "mb_in_memory", "memory_change"],
            ),
        ],
        filterable=True,
        pagination=False,
        # With a fixed height, the table scrolls in its box and keeps its header visible.
        height="85vh",
        wrap=False,
        compact=True,
        highlight=True,
    )


def script_json(value: object) -> str:
    """Return a value as JSON that can stand inside a script element."""
    return json.dumps(value).replace("</", "<\\/")


def render(
    succeeded: pl.DataFrame, grid: pl.DataFrame, scale_factor: int, problems: list[str]
) -> str:
    repetitions = succeeded.group_by([*AXES, "mode"]).len()["len"].max()
    matrices = [
        matrix_table(grid, "s", "Time"),
        matrix_table(grid, "mb", "Peak memory"),
    ]
    problem_list = "".join(f"<li>{html.escape(p)}</li>" for p in problems)

    return PAGE.substitute(
        description=html.escape(description(grid, scale_factor, repetitions)),
        result=html.escape(summary(grid)),
        problems=f"<p><b>Problems</b></p><ul>{problem_list}</ul>" if problems else "",
        summary=axis_table(grid).as_raw_html(),
        tables="\n".join(table.as_raw_html() for table in matrices),
        repetitions=repetitions,
        environment=html.escape(environment(succeeded)),
        reactable_css=(REACTABLE_ASSETS / "reactable-py.esm.css").read_text(),
        reactable_js=script_json(
            (REACTABLE_ASSETS / "reactable-py.esm.js").read_text()
        ),
        cells_props=script_json(cells_table(grid).to_props()),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data", type=Path, required=True, help="the directory of generate.py"
    )
    parser.add_argument(
        "--results", type=Path, help="the results file (default: DATA/results.jsonl)"
    )
    parser.add_argument(
        "--out", type=Path, help="the HTML page (default: DATA/report.html)"
    )
    args = parser.parse_args()

    data: Path = args.data
    manifest = json.loads((data / "manifest.json").read_text())
    results = args.results or data / "results.jsonl"
    runs = pl.read_ndjson(results, infer_schema_length=None)

    # Leave out the runs of cells that are not in the manifest now.
    in_manifest = pl.DataFrame(manifest["cells"]).select(AXES)
    runs = runs.join(in_manifest, on=AXES, how="semi")

    # A results file without failed runs has no error column.
    if "error" not in runs.columns:
        runs = runs.with_columns(error=pl.lit(None, pl.String))
    succeeded = runs.filter(pl.col("error").is_null())

    problems = check(runs, succeeded)
    for problem in problems:
        print(problem, file=sys.stderr)

    # Without a successful run, the report has no values to show.
    if succeeded.is_empty():
        sys.exit(1)

    grid = cells(succeeded)
    out: Path = args.out or data / "report.html"
    out.write_text(render(succeeded, grid, manifest["scale_factor"], problems))
    print(f"{out}: {grid.height} cells, {len(problems)} problems")

    if problems:
        sys.exit(1)


if __name__ == "__main__":
    main()
