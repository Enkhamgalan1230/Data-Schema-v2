from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class PrunedColumnCandidate:
    table_name: str
    column_name: str
    dtype: str
    dtype_family: str

    canonical_types: List[str]

    pk_like: bool
    fk_like: bool
    other_key_like: bool
    reject: bool

    reasons: List[str]