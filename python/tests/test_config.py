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

    config = DeltaTableConfig(without_files=False, log_buffer_size=1, skip_stats=True)

    dt = DeltaTable(
        tmp_path,
        without_files=config.without_files,
        log_buffer_size=config.log_buffer_size,
        skip_stats=config.skip_stats,
    )

    assert config == dt.table_config


def test_open_with_skip_stats(tmp_path, sample_table: Table):
    write_deltalake(tmp_path, sample_table)

    default_actions = DeltaTable(tmp_path).get_add_actions(flatten=True)
    skip_actions = DeltaTable(tmp_path, skip_stats=True).get_add_actions(flatten=True)

    default_num_records = default_actions.column("num_records").to_pylist()
    default_min_price = default_actions.column("min.price").to_pylist()
    default_max_price = default_actions.column("max.price").to_pylist()

    assert all(v is not None for v in default_num_records), default_num_records
    assert all(v is not None for v in default_min_price), default_min_price
    assert all(v is not None for v in default_max_price), default_max_price

    skip_num_records = skip_actions.column("num_records").to_pylist()
    skip_min_price = skip_actions.column("min.price").to_pylist()
    skip_max_price = skip_actions.column("max.price").to_pylist()

    assert skip_num_records == [None] * len(skip_num_records)
    assert skip_min_price == [None] * len(skip_min_price)
    assert skip_max_price == [None] * len(skip_max_price)
