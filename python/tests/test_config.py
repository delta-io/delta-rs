from arro3.core import Table

from deltalake import write_deltalake
from deltalake.table import DeltaTable, DeltaTableConfig


def test_config_roundtrip(tmp_path, sample_table: Table):
    write_deltalake(tmp_path, sample_table)

    config = DeltaTableConfig(without_files=True, log_buffer_size=100)

    dt = DeltaTable(
        tmp_path,
        without_files=config.without_files,
        log_buffer_size=config.log_buffer_size,
    )

    assert config == dt.table_config

    config = DeltaTableConfig(without_files=False, log_buffer_size=1)

    dt = DeltaTable(
        tmp_path,
        without_files=config.without_files,
        log_buffer_size=config.log_buffer_size,
    )

    assert config == dt.table_config
