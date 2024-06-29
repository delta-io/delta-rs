from typing import Callable

from pyarrow.dataset import Expression

from deltalake.table import FilterType

filters_to_expression: Callable[[FilterType], Expression]
_filters_to_expression: Callable[[FilterType], Expression]
