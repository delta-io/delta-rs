"""Run every cell of the benchmark manifest with the streamed and the in-memory MERGE.

Each run merges into a fresh copy of its target table, in a new Python process
(`merge_once.py`). The order of the two modes alternates between cells and
repetitions. Each run appends one JSON line to the results file.

When the results file exists, its successful runs are skipped, so a stopped run
can continue, and failed runs run again. A results file holds the runs of one
deltalake build, Polars version and machine only.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

MODES = ("streamed", "in_memory")
CELL_FIELDS = ("table", "files", "partitioning", "source_fraction", "batches")
ENVIRONMENT_FIELDS = ("deltalake", "polars", "build", "machine")
MERGE_ONCE = Path(__file__).with_name("merge_once.py")

# Runs in the measured Python. `build` identifies the native library of deltalake,
# because two local builds can have the same version.
BUILD_SCRIPT = """
import hashlib, json, pathlib
import deltalake, polars
library = next(pathlib.Path(deltalake.__file__).parent.glob("_internal*"))
print(json.dumps({
    "deltalake": deltalake.__version__,
    "polars": polars.__version__,
    "build": hashlib.sha256(library.read_bytes()).hexdigest()[:16],
}))
"""


@dataclass(frozen=True)
class Cell:
    """One cell of the grid in manifest.json: a target table and a MERGE source."""

    table: str
    files: int
    partitioning: str
    source_fraction: float
    batches: int
    target_rows: int
    partitions: int
    source_rows: int
    # The paths of the target table and the source file, in the data directory.
    target: str
    source: str
    predicate: str

    def record(self) -> dict[str, Any]:
        """Return the fields that each run record holds: all but the paths and predicate."""
        fields = asdict(self)
        for name in ("target", "source", "predicate"):
            del fields[name]
        return fields


def environment(python: str) -> dict[str, Any]:
    """Return the deltalake build, the Polars version, and the machine of the runs."""
    # -P keeps the current directory out of the import path: in the python directory,
    # `import deltalake` would find the source package, not the installed build.
    process = subprocess.run(
        [python, "-P", "-c", BUILD_SCRIPT], capture_output=True, text=True
    )
    if process.returncode != 0:
        sys.exit(f"cannot find the deltalake build of {python}:\n{process.stderr}")

    memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")

    return {
        **json.loads(process.stdout),
        "machine": {
            "platform": platform.platform(),
            "processor": platform.processor() or platform.machine(),
            "cpus": os.cpu_count(),
            "memory_gib": round(memory / 1024**3),
        },
    }


def schedule(cells: list[Cell], repetitions: int) -> Iterator[tuple[int, Cell, str]]:
    """Yield the runs in order. The order of the two modes alternates."""
    for repetition in range(repetitions):
        for index, cell in enumerate(cells):
            modes = MODES if (repetition + index) % 2 == 0 else MODES[::-1]
            for mode in modes:
                yield repetition, cell, mode


def run_key(record: dict[str, Any]) -> tuple[Any, ...]:
    """Return the key of a run in the results file: the cell key, mode and repetition."""
    cell = tuple(record[field] for field in CELL_FIELDS)
    return cell, record["mode"], record["repetition"]


def measure(
    python: str, data: Path, cell: Cell, mode: str, copy: Path
) -> dict[str, Any]:
    """Run one MERGE on a new copy of the target, and return its measurements."""
    command = [python, MERGE_ONCE, copy, data / cell.source, cell.predicate, mode]
    try:
        # Copy the target as hard links to its files. deltalake never changes a file in
        # place: it writes new files, and replaces a file by renaming a new one over
        # it. So a run cannot change the original table.
        shutil.copytree(data / cell.target, copy, copy_function=os.link)

        process = subprocess.run(command, capture_output=True, text=True, timeout=1800)
        output = process.stdout.strip().splitlines()
        if process.returncode != 0 or not output:
            return {"error": process.stderr[-2000:]}

        return json.loads(output[-1])
    except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError) as error:
        # OSError includes a failed copy of the target (shutil.Error).
        return {"error": str(error)}
    finally:
        shutil.rmtree(copy, ignore_errors=True)


def outcome(record: dict[str, Any]) -> str:
    if "error" in record:
        return "failed: " + record["error"][:200]

    return (
        f"{record['elapsed_s']:7.3f} s {record['max_rss_mb']:6} MB, "
        f"read {record['metrics']['num_target_files_scanned']} of {record['files']} files"
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
        "--repetitions", type=int, default=5, help="runs per cell and mode (default: 5)"
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="the Python with the deltalake build to measure (default: this Python)",
    )
    parser.add_argument(
        "--warmup",
        action="store_true",
        help="run each cell once before its first measured run, without recording it, "
        "so that the measured runs read the target from the page cache",
    )
    args = parser.parse_args()

    data: Path = args.data
    results_path: Path = args.results or data / "results.jsonl"
    manifest = json.loads((data / "manifest.json").read_text())
    cells = [Cell(**cell) for cell in manifest["cells"]]
    env = environment(args.python)

    # Continue the results file, if it has runs of the same build, Polars and machine.
    done = set()
    if results_path.exists():
        previous = [json.loads(line) for line in results_path.read_text().splitlines()]
        for record in previous:
            if any(record.get(field) != env[field] for field in ENVIRONMENT_FIELDS):
                parser.error(
                    f"{results_path} has runs of another deltalake build, Polars "
                    "version or machine, so use another --results file"
                )
        done = {run_key(record) for record in previous if "error" not in record}

    # The results file can have runs that are not in the schedule now, for example
    # of a larger --repetitions, so count the runs of the schedule only.
    runs = [
        (cell, {**cell.record(), "mode": mode, "repetition": repetition, **env})
        for repetition, cell, mode in schedule(cells, args.repetitions)
    ]
    todo = [(cell, record) for cell, record in runs if run_key(record) not in done]
    already = len(runs) - len(todo)
    print(
        f"{len(cells)} cells, {len(runs)} runs, {already} already done. "
        f"deltalake {env['deltalake']} (build {env['build']}), Polars {env['polars']}",
        flush=True,
    )

    started = time.perf_counter()
    warm: set[Cell] = set()

    with (
        tempfile.TemporaryDirectory(dir=data, prefix="run-") as scratch,
        results_path.open("a") as results,
    ):
        copy = Path(scratch) / "target"

        for finished, (cell, record) in enumerate(todo, start=1):
            mode = record["mode"]
            if args.warmup and cell not in warm:
                measure(args.python, data, cell, mode, copy)
                warm.add(cell)

            record.update(measure(args.python, data, cell, mode, copy))
            results.write(json.dumps(record) + "\n")
            results.flush()

            minutes_left = (
                (len(todo) - finished)
                * (time.perf_counter() - started)
                / (finished * 60)
            )
            print(
                f"[{already + finished}/{len(runs)}, {minutes_left:.0f} min left] "
                f"{cell.table} files={cell.files} partitioning={cell.partitioning} "
                f"source={cell.source_fraction:g} batches={cell.batches} "
                f"{mode} #{record['repetition']}: {outcome(record)}",
                flush=True,
            )


if __name__ == "__main__":
    main()
