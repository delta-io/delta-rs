from typing import Any, Callable, List

import pyarrow as pa

from deltalake.writer import AddAction

RawDeltaTable: Any
rust_core_version: Callable[[], str]
DeltaStorageFsBackend: Any

write_new_deltalake: Callable[[str, pa.Schema, List[AddAction], str, List[str]], None]

class PyDeltaTableError(BaseException): ...
