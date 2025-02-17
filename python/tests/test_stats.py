from datetime import datetime

import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.mark.pandas
@pytest.mark.polars
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
