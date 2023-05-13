import pathlib

import pyarrow as pa

from deltalake import DeltaTable, write_deltalake


def test_checkpoint(tmp_path: pathlib.Path, sample_data: pa.Table):
    tmp_table_path = tmp_path / "path" / "to" / "table"
    checkpoint_path = tmp_table_path / "_delta_log" / "_last_checkpoint"
    last_checkpoint_path = (
        tmp_table_path / "_delta_log" / "00000000000000000000.checkpoint.parquet"
    )

    # TODO: Include binary after fixing issue "Json error: binary type is not supported"
    sample_data = sample_data.drop(["binary"])
    write_deltalake(str(tmp_table_path), sample_data)

    assert not checkpoint_path.exists()

    delta_table = DeltaTable(str(tmp_table_path))
    delta_table.create_checkpoint()

    assert last_checkpoint_path.exists()
    assert checkpoint_path.exists()
