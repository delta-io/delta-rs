import pathlib
from datetime import datetime, timezone

import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.mark.pandas
@pytest.mark.polars
@pytest.mark.pyarrow
def test_stats_usage_3201(tmp_path):
    # https://github.com/delta-io/delta-rs/issues/3201
    # https://github.com/delta-io/delta-rs/issues/3173
    import pandas as pd
    import polars as pl
    from polars.testing import assert_frame_equal

    excepted = pl.DataFrame(
        [
            pl.Series(
                "date",
                [
                    datetime(2020, 1, 2, 0, 0),
                    datetime(2020, 1, 3, 0, 0),
                    datetime(2020, 1, 1, 0, 0),
                    datetime(2020, 1, 2, 0, 0),
                ],
                dtype=pl.Datetime(time_unit="us", time_zone=None),
            ),
            pl.Series(
                "ref_date",
                [
                    datetime(2020, 1, 2, 0, 0),
                    datetime(2020, 1, 3, 0, 0),
                    datetime(2020, 1, 1, 0, 0),
                    datetime(2020, 1, 2, 0, 0),
                ],
                dtype=pl.Datetime(time_unit="us", time_zone=None),
            ),
            pl.Series("values", [1, 2, 3, 4], dtype=pl.Int64),
        ]
    )

    write_deltalake(
        data=excepted.filter(pl.col("values").is_in([1, 2])).to_arrow(),
        table_or_uri=tmp_path,
        configuration={"delta.dataSkippingStatsColumns": "ref_date"},
    )

    dt = DeltaTable(tmp_path)
    dt.create_checkpoint()

    write_deltalake(
        data=excepted.filter(pl.col("values").is_in([3, 4])).to_arrow(),
        table_or_uri=dt,
        mode="append",
        configuration={"delta.dataSkippingStatsColumns": "ref_date"},
    )

    dt = DeltaTable(tmp_path)
    data = dt.to_pyarrow_table(filters=[("date", ">=", pd.Timestamp("2020-01-01"))])

    assert_frame_equal(excepted, pl.from_arrow(data), check_row_order=False)

    data = dt.to_pyarrow_table(filters=[("ref_date", ">=", pd.Timestamp("2020-01-01"))])

    assert_frame_equal(excepted, pl.from_arrow(data), check_row_order=False)

    data = dt.to_pyarrow_table(filters=[("values", ">=", 0)])

    assert_frame_equal(excepted, pl.from_arrow(data), check_row_order=False)


@pytest.mark.pyarrow
@pytest.mark.parametrize("use_stats_struct", (True, False))
def test_microsecond_truncation_parquet_stats(tmp_path, use_stats_struct):
    import pyarrow as pa

    """In checkpoints the min,max value gets truncated to milliseconds precision.
    For min values this is not an issue, but for max values we need to round upwards.

    This checks whether we can still read tables with truncated timestamp stats.
    """

    batch1 = pa.Table.from_pydict(
        {
            "p": [1],
            "dt": [datetime(2023, 3, 29, 23, 59, 59, 807126, tzinfo=timezone.utc)],
        }
    )

    write_deltalake(
        tmp_path,
        batch1,
        mode="error",
        partition_by=["p"],
        configuration={
            "delta.checkpoint.writeStatsAsStruct": str(use_stats_struct).lower()
        },
    )

    batch2 = pa.Table.from_pydict(
        {
            "p": [1],
            "dt": [datetime(2023, 3, 30, 0, 0, 0, 902, tzinfo=timezone.utc)],
        }
    )

    write_deltalake(
        tmp_path,
        batch2,
        mode="append",
        partition_by=["p"],
    )

    dt = DeltaTable(tmp_path)

    result = dt.to_pyarrow_table(
        filters=[
            (
                "dt",
                "<=",
                datetime(2023, 3, 30, 0, 0, 0, 0, tzinfo=timezone.utc),
            ),
        ]
    )
    assert batch1 == result

    dt.optimize.compact()
    dt.create_checkpoint()

    result = dt.to_pyarrow_table(
        filters=[
            (
                "dt",
                "<=",
                datetime(2023, 3, 30, 0, 0, 0, 0, tzinfo=timezone.utc),
            ),
        ]
    )
    assert batch1 == result


@pytest.mark.polars
def test_struct_stats_3172(tmp_path: pathlib.Path) -> None:
    """
    Regression test for computing stats of struct leaf nodes
    <https://github.com/delta-io/delta-rs/issues/3172>
    """
    import polars as pl
    from polars import Int64, List, Schema, String, Struct

    from deltalake import DeltaTable, write_deltalake

    schema = Schema(
        {
            "1": String,
            "nested": List(
                Struct(
                    {
                        "2": Int64,
                        "3": Int64,
                        "4": Int64,
                        "5": Int64,
                        "6": String,
                        "7": String,
                        "8": String,
                        "9": String,
                        "10": String,
                        "11": String,
                        "12": String,
                        "13": String,
                        "14": String,
                        "15": String,
                        "16": String,
                        "17": String,
                        "18": String,
                        "19": String,
                        "20": String,
                        "21": String,
                        "22": String,
                        "23": String,
                        "24": String,
                        "25": String,
                        "26": String,
                        "27": String,
                        "28": String,
                        "29": String,
                        "30": String,
                        "31": String,
                        "32": String,
                    }
                )
            ),
            "year": Int64,
            "month": Int64,
            "day": Int64,
        }
    )

    df = pl.DataFrame(
        {
            "1": ["foo"],
            "nested": [[]],
            "year": [2024],
            "month": [12],
            "day": [1],
        },
        schema=schema,
    )

    write_deltalake(tmp_path, df.to_arrow())
    dt = DeltaTable(tmp_path)
    actions = dt.get_add_actions()
    min_stats = actions["min"][0].as_py()
    assert "year" in min_stats
