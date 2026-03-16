import warnings
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


def deprecate_positional_commit_args(
    method_name: str,
    args: tuple,
    commit_properties: Any = None,
    post_commithook_properties: Any = None,
    legacy_order: tuple[str, ...] = ("commit_properties", "post_commithook_properties"),
) -> tuple[Any, Any]:
    if not args:
        return commit_properties, post_commithook_properties
    if len(args) > len(legacy_order):
        raise TypeError(
            f"{method_name}() takes at most {len(legacy_order)} positional commit argument(s), got {len(args)}"
        )
    warnings.warn(
        f"Passing commit arguments positionally to {method_name}() is deprecated "
        "and will be removed in a future release. Use keyword arguments instead.",
        DeprecationWarning,
        stacklevel=3,
    )
    mapped = dict(zip(legacy_order, args))
    if "commit_properties" in mapped and commit_properties is not None:
        raise TypeError(f"{method_name}() got multiple values for 'commit_properties'")
    if (
        "post_commithook_properties" in mapped
        and post_commithook_properties is not None
    ):
        raise TypeError(
            f"{method_name}() got multiple values for 'post_commithook_properties'"
        )
    return (
        mapped.get("commit_properties", commit_properties),
        mapped.get("post_commithook_properties", post_commithook_properties),
    )
