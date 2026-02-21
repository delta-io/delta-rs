import json
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

    with open(compaction_log_path) as fp:
        compaction_data = fp.readlines()
        compaction_data = [json.loads(row) for row in compaction_data]

        action_types = {}
        for action in compaction_data:
            key = next(iter(action.keys()))
            if key not in action_types:
                action_types[key] = 1
            else:
                action_types[next(iter(action.keys()))] += 1

        assert len(compaction_data) == 6
        assert action_types == {"add": 4, "protocol": 1, "metaData": 1}
