from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from deltalake.table import FilterType


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


def validate_filters(filters: Optional["FilterType"]) -> Optional["FilterType"]:
    """Ensures that the filters are a list of list of tuples in DNF format.

    :param filters: Filters to be validated

    Examples:
        >>> validate_filters([("a", "=", 1), ("b", "=", 2)])
        >>> validate_filters([[("a", "=", 1), ("b", "=", 2)], [("c", "=", 3)]])
        >>> validate_filters([("a", "=", 1)])
        >>> validate_filters([[("a", "=", 1)], [("b", "=", 2)], [("c", "=", 3)]])

    """
    from deltalake.table import FilterType

    if filters is None:
        return None

    if not isinstance(filters, list) or len(filters) == 0:
        raise ValueError("Filters must be a non-empty list.")

    if all(isinstance(item, tuple) and len(item) == 3 for item in filters):
        return cast(FilterType, [filters])

    elif all(
        isinstance(conjunction, list)
        and len(conjunction) > 0
        and all(
            isinstance(literal, tuple) and len(literal) == 3 for literal in conjunction
        )
        for conjunction in filters
    ):
        if len(filters) == 0 or any(len(c) == 0 for c in filters):
            raise ValueError("Malformed DNF")
        return filters

    else:
        raise ValueError(
            "Filters must be a list of tuples, or a list of lists of tuples"
        )


def stringify_partition_values(
    partition_filters: Optional["FilterType"],
) -> Optional["FilterType"]:
    if partition_filters is None:
        return None

    if all(isinstance(item, tuple) for item in partition_filters):
        return [  # type: ignore
            (
                field,
                op,
                [encode_partition_value(val) for val in value]
                if isinstance(value, (list, tuple))
                else encode_partition_value(value),
            )
            for field, op, value in partition_filters
        ]
    return [
        [
            (
                field,
                op,
                [encode_partition_value(val) for val in value]
                if isinstance(value, (list, tuple))
                else encode_partition_value(value),
            )
            for field, op, value in sublist
        ]
        for sublist in partition_filters
    ]
