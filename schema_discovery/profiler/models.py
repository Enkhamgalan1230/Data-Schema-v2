# schema_discovery/profiler/models.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, List


@dataclass(frozen=True)
class ColumnProfile:
    table_name: str
    column_name: str
    dtype: str

    n_rows: int
    n_null: int
    n_non_null: int
    null_ratio: float

    n_unique: int
    unique_ratio: float
    unique_ratio_non_null: float

    # Note: only trustworthy if n_unique was computed exactly
    is_unary_ucc: bool

    sample_values: List[Any]

    avg_len: Optional[float]
    min_len: Optional[int]
    max_len: Optional[int]

    top1_ratio: float