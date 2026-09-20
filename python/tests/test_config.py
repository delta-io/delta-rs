import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.mark.parametrize(
    "options",
    [{"without_files": True}, {"skip_stats": True}, {"log_buffer_size": 1}],
)
def test_deprecated_load_options_are_ignored(tmp_path, sample_table, options):
    write_deltalake(tmp_path, sample_table)
    expected = DeltaTable(tmp_path)

    with pytest.warns(DeprecationWarning, match="ignored"):
        table = DeltaTable(tmp_path, **options)

    with pytest.warns(DeprecationWarning):
        assert table.table_config == expected.table_config

    expected_actions = expected.get_add_actions(flatten=True)
    actual_actions = table.get_add_actions(flatten=True)
    assert table.file_uris() == expected.file_uris()
    assert actual_actions.column_names == expected_actions.column_names
    for name in expected_actions.column_names:
        assert (
            actual_actions.column(name).to_pylist()
            == expected_actions.column(name).to_pylist()
        )
    assert all(
        value is not None
        for value in expected_actions.column("num_records").to_pylist()
    )
