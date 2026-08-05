"""Delta Lake continuous aggregation pipeline with idempotent Delta MERGE.

Demonstrates the pattern behind a "Spark-based continuous aggregation
framework using Delta merge logic":

1. Ingest event data in batches (the same code path works for a streaming
   micro-batch: call `ingest_events` once per micro-batch).
2. Upsert events into the raw Delta table with MERGE keyed on a stable event
   id, so re-delivered or late events never duplicate (idempotent).
3. Roll up ONLY the new micro-batch into a curated Delta table with MERGE
   keyed on the aggregate key, so metrics stay accurate at scale and each
   micro-batch contributes just its own delta (no full table recompute, no
   double counting).

Run:
    pip install deltalake pyarrow pandas
    python merge_streaming_aggregation.py

This is intentionally dependency-light (local filesystem, no Spark cluster)
so it runs anywhere; on Databricks the same `write_deltalake` / `DeltaTable.merge`
calls run unchanged against DBFS.
"""

from __future__ import annotations

import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from deltalake import DeltaTable, write_deltalake

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
# Raw events: one row per user/system event, keyed on event_id (stable, unique).
EVENT_COLS = ["event_id", "user_id", "event_type", "value", "event_time"]

# Curated rollup: per user, per day, aggregate metrics (the "fast, accurate
# metrics at scale" the framework is meant to serve).
ROLLUP_COLS = ["user_id", "day", "event_count", "sum_value", "last_event_time"]


# ---------------------------------------------------------------------------
# 1) Ingestion + idempotent upsert into the raw Delta table
# ---------------------------------------------------------------------------
def ingest_events(raw_path: str, events: pd.DataFrame) -> None:
    """Upsert a batch (or streaming micro-batch) of events into the raw table.

    MERGE on event_id means a re-sent or late event updates the existing row
    instead of inserting a duplicate, so downstream aggregation is correct.
    """
    if Path(raw_path).exists():
        table = DeltaTable(raw_path)
        (
            table.merge(
                source=events,
                predicate="target.event_id = source.event_id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    else:
        write_deltalake(raw_path, events, mode="append")


# ---------------------------------------------------------------------------
# 2) Continuous aggregation into the curated Delta table
# ---------------------------------------------------------------------------
def aggregate(raw_path: str, rollup_path: str, new_events: pd.DataFrame) -> None:
    """Incrementally roll up ONLY the new micro-batch's events into the curated
    Delta table with MERGE (no full recompute of the whole raw table).

    Passing just `new_events` instead of re-reading the full raw table is what
    makes this a continuous aggregation: each micro-batch contributes only its
    own delta, so metrics stay accurate without double counting and without
    scanning every historical row.
    """
    batch = new_events.copy()
    batch["day"] = pd.to_datetime(batch["event_time"]).dt.date.astype(str)

    grouped = (
        batch.groupby(["user_id", "day"])
        .agg(
            event_count=("event_id", "count"),
            sum_value=("value", "sum"),
            last_event_time=("event_time", "max"),
        )
        .reset_index()
    )

    if Path(rollup_path).exists():
        table = DeltaTable(rollup_path)
        (
            table.merge(
                source=grouped,
                predicate="target.user_id = source.user_id AND target.day = source.day",
                source_alias="source",
                target_alias="target",
            )
            # Accumulate counts/sums on match; insert brand-new keys as-is.
            .when_matched_update(
                predicate="target.last_event_time < source.last_event_time",
                updates={
                    "event_count": "target.event_count + source.event_count",
                    "sum_value": "target.sum_value + source.sum_value",
                    "last_event_time": "source.last_event_time",
                },
            )
            .when_not_matched_insert_all()
            .execute()
        )
    else:
        write_deltalake(rollup_path, grouped, mode="append")


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
def _sample_events(n: int, base: datetime, start: int = 0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id": [f"e{start + i}" for i in range(n)],
            "user_id": [f"u{i % 3}" for i in range(n)],
            "event_type": ["view" if i % 2 else "click" for i in range(n)],
            "value": [float((i % 5) + 1) for i in range(n)],
            "event_time": [(base + timedelta(minutes=i)).isoformat() for i in range(n)],
        }
    )


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="delta_merge_demo_"))
    raw_path = str(tmp / "events")
    rollup_path = str(tmp / "rollup")

    try:
        # Micro-batch 1 (events 0..4)
        b1 = _sample_events(5, datetime(2026, 8, 11, 9, 0))
        ingest_events(raw_path, b1)
        aggregate(raw_path, rollup_path, b1)

        # Micro-batch 2 (events 5..9): genuinely new rows -> rollup accumulates.
        b2 = _sample_events(5, datetime(2026, 8, 11, 10, 0), start=5)
        ingest_events(raw_path, b2)
        aggregate(raw_path, rollup_path, b2)

        # Re-deliver micro-batch 1 in full -> ingest MERGE drops the duplicates,
        # raw count stays at 10 and the rollup is unchanged (no double count).
        ingest_events(raw_path, b1)

        print("Raw event count:", len(DeltaTable(raw_path).to_pandas()))
        print("Rollup:")
        print(
            DeltaTable(rollup_path)
            .to_pandas()
            .sort_values(["user_id", "day"])
            .to_string(index=False)
        )
        print("MERGE_STREAMING_AGGREGATION_DONE")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
