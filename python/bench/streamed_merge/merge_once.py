"""Run one MERGE with Polars `sink_delta` and print its measurements as one JSON line.

`run.py` starts a new process for each MERGE, so that the peak memory of the process
belongs to that MERGE only.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time

import polars as pl

# run.py is next to this script, and imports only the standard library.
from run import MODES

# Loaded before the measured part, so that loading it is not part of the MERGE.
import deltalake  # noqa: F401


def max_rss_mb() -> float:
    """Return the peak resident memory of this process until now, in MiB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    # macOS gives bytes, Linux gives KiB.
    return peak / (1024 * 1024 if sys.platform == "darwin" else 1024)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", help="the Delta table to merge into")
    parser.add_argument(
        "source", help="the Parquet file to merge, read with pl.scan_parquet"
    )
    parser.add_argument(
        "predicate", help="the MERGE predicate, with the aliases t and s"
    )
    parser.add_argument("mode", choices=MODES, help="streamed_exec=True or False")
    args = parser.parse_args()

    baseline_mb = max_rss_mb()

    # The measured part: from sink_delta until execute() returns.
    start = time.perf_counter()
    metrics = (
        pl.scan_parquet(args.source)
        .sink_delta(
            args.target,
            mode="merge",
            delta_merge_options={
                "predicate": args.predicate,
                "source_alias": "s",
                "target_alias": "t",
                "streamed_exec": args.mode == "streamed",
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    elapsed_s = time.perf_counter() - start

    print(
        json.dumps(
            {
                "elapsed_s": round(elapsed_s, 4),
                "max_rss_mb": round(max_rss_mb()),
                "baseline_rss_mb": round(baseline_mb),
                "metrics": metrics,
            }
        ),
        flush=True,
    )

    # Skip the interpreter shutdown, because it is not part of the MERGE.
    os._exit(0)


if __name__ == "__main__":
    main()
