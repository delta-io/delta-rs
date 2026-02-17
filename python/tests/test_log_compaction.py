import pathlib
from typing import TYPE_CHECKING

from arro3.core import Table

from deltalake import (
    DeltaTable,
    write_deltalake,
)

if TYPE_CHECKING:
    pass


def test_log_compaction(tmp_path: pathlib.Path, sample_table: Table):
    tmp_table_path = tmp_path / "path" / "to" / "table"
    compaction_log_path = (
        tmp_table_path
        / "_delta_log"
        / "00000000000000000000.00000000000000000003.compacted.json"
    )
    for i in range(4):
        write_deltalake(str(tmp_table_path), sample_table, mode="append")

    assert not compaction_log_path.exists()

    delta_table = DeltaTable(str(tmp_table_path))
    delta_table.compact_logs(starting_version=0, ending_version=3)

    assert compaction_log_path.exists()
