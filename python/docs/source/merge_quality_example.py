"""
Merge/Upsert with Pre-Merge Quality Validation
================================================

This example demonstrates how to perform a Delta Lake merge (upsert) operation
with data quality validation *before* the merge executes. This ensures that
only clean, validated data enters your Delta tables.

Pattern: validate → merge → assert

This is a common pattern in medallion architecture pipelines where data
quality gates sit between Bronze and Silver layers.

Requirements:
    pip install deltalake pandas
"""

import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from deltalake import DeltaTable, write_deltalake


# ---------------------------------------------------------------------------
# 1. Quality Gate Definitions
# ---------------------------------------------------------------------------


@dataclass
class QualityRule:
    """A single quality check applied before merge."""

    name: str
    column: str
    check: str  # "not_null", "positive", "in_set"
    allowed_values: list[str] | None = None


QUALITY_RULES = [
    QualityRule(name="customer_id_not_null", column="customer_id", check="not_null"),
    QualityRule(name="region_not_null", column="region", check="not_null"),
    QualityRule(name="amount_positive", column="amount", check="positive"),
    QualityRule(
        name="region_valid",
        column="region",
        check="in_set",
        allowed_values=["APAC", "EMEA", "NA", "LATAM"],
    ),
]


def validate_dataframe(df: pd.DataFrame, rules: list[QualityRule]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split a DataFrame into passing and failing rows based on quality rules.

    Returns:
        (clean_df, rejected_df) where rejected_df has an extra 'rejection_reason' column.
    """
    rejection_mask = pd.Series(False, index=df.index)
    rejection_reasons = pd.Series("", index=df.index)

    for rule in rules:
        if rule.column not in df.columns:
            continue

        if rule.check == "not_null":
            failed = df[rule.column].isna()
        elif rule.check == "positive":
            failed = df[rule.column].fillna(0) <= 0
        elif rule.check == "in_set" and rule.allowed_values:
            failed = ~df[rule.column].isin(rule.allowed_values) & df[rule.column].notna()
        else:
            continue

        # Accumulate rejection reasons (a row can fail multiple rules)
        new_reasons = failed.map(lambda x: rule.name if x else "")
        rejection_reasons = rejection_reasons.where(
            ~failed, rejection_reasons + "," + new_reasons
        )
        rejection_mask = rejection_mask | failed

    clean_df = df[~rejection_mask].copy()
    rejected_df = df[rejection_mask].copy()
    rejected_df["rejection_reason"] = rejection_reasons[rejection_mask].str.strip(",")

    return clean_df, rejected_df


# ---------------------------------------------------------------------------
# 2. Setup: Create Initial Delta Table
# ---------------------------------------------------------------------------


def main():
    table_path = Path(tempfile.mkdtemp()) / "sales_silver"

    # Initial data (Silver layer)
    initial_data = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": ["C001", "C002", "C003"],
            "region": ["APAC", "EMEA", "NA"],
            "amount": [1200.0, 850.0, 2300.0],
            "updated_at": ["2026-01-15", "2026-01-20", "2026-02-01"],
        }
    )

    write_deltalake(str(table_path), initial_data, mode="overwrite")
    print(f"Created Delta table at {table_path}")
    print(f"Initial rows: {len(initial_data)}")

    # -----------------------------------------------------------------------
    # 3. Incoming Data with Quality Issues
    # -----------------------------------------------------------------------

    incoming_data = pd.DataFrame(
        {
            "order_id": [2, 4, 5, 6],
            "customer_id": ["C002", "C004", None, "C006"],  # None = quality issue
            "region": ["EMEA", "APAC", "NA", "INVALID"],  # INVALID = quality issue
            "amount": [900.0, 1750.0, 500.0, -100.0],  # Negative = quality issue
            "updated_at": ["2026-02-10", "2026-02-15", "2026-02-20", "2026-02-25"],
        }
    )

    print(f"\nIncoming rows: {len(incoming_data)}")

    # -----------------------------------------------------------------------
    # 4. Pre-Merge Quality Validation
    # -----------------------------------------------------------------------

    clean_data, rejected_data = validate_dataframe(incoming_data, QUALITY_RULES)

    print(f"Rows passing validation: {len(clean_data)}")
    print(f"Rows rejected: {len(rejected_data)}")

    if len(rejected_data) > 0:
        print("\nRejected rows:")
        print(rejected_data[["order_id", "customer_id", "region", "amount", "rejection_reason"]].to_string())

    # -----------------------------------------------------------------------
    # 5. Merge Clean Data into Delta Table
    # -----------------------------------------------------------------------

    if len(clean_data) > 0:
        dt = DeltaTable(str(table_path))

        (
            dt.merge(
                source=clean_data,
                predicate="target.order_id = source.order_id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

        print(f"\nMerge complete.")

    # -----------------------------------------------------------------------
    # 6. Post-Merge Assertions
    # -----------------------------------------------------------------------

    dt = DeltaTable(str(table_path))
    result_df = dt.to_pandas()

    print(f"\nFinal table rows: {len(result_df)}")
    print(f"Table version: {dt.version()}")

    # Assert no nulls in customer_id after merge
    assert result_df["customer_id"].notna().all(), "Post-merge assertion failed: null customer_id found"
    # Assert all amounts are positive
    assert (result_df["amount"] > 0).all(), "Post-merge assertion failed: non-positive amount found"

    print("Post-merge assertions passed.")
    print(f"\nFinal data:\n{result_df.to_string()}")


if __name__ == "__main__":
    main()
