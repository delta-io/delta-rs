from datetime import date, datetime
from typing import Any


def encode_partition_value(val: Any) -> str:
    # Rules based on: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    if isinstance(val, bool):
        return str(val).lower()
    if isinstance(val, str):
        return val
    elif isinstance(val, (int, float)):
        return str(val)
    elif isinstance(val, datetime):
        return val.isoformat(sep=" ")
    elif isinstance(val, date):
        return val.isoformat()
    elif isinstance(val, bytes):
        return val.decode("unicode_escape", "backslashreplace")
    else:
        raise ValueError(f"Could not encode partition value for type: {val}")
