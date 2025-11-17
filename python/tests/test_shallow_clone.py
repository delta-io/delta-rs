import os
import pathlib
from urllib.parse import urlparse

from arro3.core import Table

from deltalake import (
    DeltaTable,
    write_deltalake,
)


def to_names(uris):
    result = []
    for u in uris:
        p = urlparse(u)
        path = p.path if p.scheme else u
        result.append(os.path.basename(path))
    return result


def test_shallow_clone(tmp_path: pathlib.Path, sample_table: Table):
    # Create table and shallow clone
    tmp_table_path = tmp_path / "path" / "to" / "table"
    tmp_table_clone = tmp_path / "path" / "to" / "clone"
    write_deltalake(str(tmp_table_path), sample_table)
    delta_table = DeltaTable(str(tmp_table_path))
    delta_clone = delta_table.shallow_clone(str(tmp_table_clone))

    # Compare metadata
    m_tbl = delta_table.metadata()
    m_clone = delta_clone.metadata()
    assert m_tbl.name == m_clone.name
    assert m_tbl.description == m_clone.description
    assert m_tbl.configuration == m_clone.configuration

    # Compare only file names because the cloned table has a different base path
    src_uris = delta_table.file_uris()
    clone_uris = delta_clone.file_uris()
    assert to_names(src_uris) == to_names(clone_uris)

    # Compare data
    table_data = delta_table.to_pyarrow_table()
    clone_data = delta_clone.to_pyarrow_table()
    assert table_data == clone_data
