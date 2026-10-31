import pathlib

import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.mark.pyarrow
def test_generate_removes_manifest_of_emptied_partition(tmp_path: pathlib.Path):
    """https://github.com/delta-io/delta-rs/issues/4989

    generate() must delete the symlink format manifest of a partition that no
    longer has files, as delta-spark's GenerateSymlinkManifest does, so a
    manifest reader (Presto/Athena) does not see the removed rows.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    write_deltalake(
        tmp_path, pa.table({"id": [1, 2], "p": ["a", "b"]}), partition_by=["p"]
    )
    DeltaTable(tmp_path).generate()
    DeltaTable(tmp_path).delete("p = 'b'")
    DeltaTable(tmp_path).generate()

    manifest_root = tmp_path / "_symlink_format_manifest"
    # what Presto or Athena reads: every file listed in every manifest
    referenced_files = [
        f.strip() for m in manifest_root.glob("*/manifest") for f in open(m)
    ]
    referenced_ids = sorted(
        i
        for f in referenced_files
        for i in pq.ParquetFile(f.removeprefix("file://")).read()["id"].to_pylist()
    )
    assert referenced_ids == [1]
