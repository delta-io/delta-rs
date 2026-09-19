from arro3.core import Table

from deltalake import write_deltalake
from deltalake.table import DeltaTable, DeltaTableConfig


def test_config_roundtrip(tmp_path, sample_table: Table):
    import warnings

    write_deltalake(tmp_path, sample_table)

    # Test DeltaTableConfig constructor deprecation
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        config = DeltaTableConfig(without_files=True, log_buffer_size=100)

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "DeltaTableConfig class is deprecated" in str(w[0].message)

    dt = DeltaTable(
        tmp_path,
        without_files=config.without_files,
        log_buffer_size=config.log_buffer_size,
    )

    # Test table_config property deprecation
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        table_config = dt.table_config

        # Debug: print the warnings to see what we actually get
        for i, warning in enumerate(w):
            print(f"Warning {i}: {warning.message} (category: {warning.category})")

        # We expect 2 warnings: one from the property access and one from the DeltaTableConfig constructor
        assert len(w) == 2
        dep_warnings = [
            warning for warning in w if issubclass(warning.category, DeprecationWarning)
        ]
        assert len(dep_warnings) == 2
        assert any(
            "The 'table_config' property is deprecated" in str(warning.message)
            for warning in dep_warnings
        )
        assert any(
            "DeltaTableConfig class is deprecated" in str(warning.message)
            for warning in dep_warnings
        )

    assert config == table_config

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
