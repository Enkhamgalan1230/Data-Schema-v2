from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class NormalizedColumnProfile:
    table_name: str
    column_name: str
    dtype: str
    dtype_family: str

    n_rows: int
    n_null: int
    n_non_null: int
    null_ratio: float

    approx_n_unique: int
    approx_unique_ratio: float
    approx_unique_ratio_non_null: float
    top1_ratio: float

    sample_values: List[object]
    avg_len: Optional[float]
    min_len: Optional[int]
    max_len: Optional[int]

    numeric_string_like: bool
    integer_like_float: bool
    zero_padded_string: bool
    free_text_like: bool

    canonical_types: List[str]