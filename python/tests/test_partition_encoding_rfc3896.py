import pytest

from deltalake import DeltaTable


@pytest.mark.polars
def test_partition_encoding_rfc3896(tmp_path):
    import polars as pl
    from polars.testing import assert_frame_equal

    (start, stop) = (32, 255)
    df = pl.DataFrame(
        {
            "a": list(i for i in range(start, stop)),
            "strings": list("a" + chr(i) for i in range(start, stop)),
        }
    )
    print(f"df:\n{df}")

    # write:
    df.write_delta(tmp_path, delta_write_options={"partition_by": "strings"})

    # read:
    partitioned_tbl = DeltaTable(tmp_path)
    pl_df_partitioned = pl.read_delta(partitioned_tbl).sort("a")
    print(f"pl_df_partitioned:\n{pl_df_partitioned}")

    assert_frame_equal(
        df,
        pl_df_partitioned,
    )
