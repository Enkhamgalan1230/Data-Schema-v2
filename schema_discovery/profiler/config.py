# schema_discovery/profiler/config.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence


@dataclass(frozen=True)
class ProfilerConfig:
    # DuckDB safety
    duckdb_path: Path
    temp_dir: Path
    memory_limit: str = "2GB"
    threads: int = 4

    # Sampling and costs
    sample_values_n: int = 10

    compute_top1_ratio: bool = True

    # Only compute top1_ratio when approx_distinct <= this threshold
    # Because top1_ratio is most useful for low-card columns anyway.
    top1_max_approx_distinct: int = 50_000

    # Uniqueness strategy
    # If row count <= exact_unique_max_rows -> compute exact COUNT(DISTINCT)
    # else use approx_count_distinct
    exact_unique_max_rows: int = 200_000

    # If you want to treat some string tokens as null
    # Applied in SQL as NULLIF chains for VARCHAR-like cols
    string_null_tokens: Sequence[str] = ("", "NULL", "null", "N/A", "na")

    # If True, treat uniqueness only on non-null values when deciding is_unary_ucc
    # In relational terms, nullable unique keys are allowed. Choose your policy.
    ucc_ignore_nulls: bool = True

    compute_sample_values: bool = True

    sample_max_approx_distinct: int = 100_000