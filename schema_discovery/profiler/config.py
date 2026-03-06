# schema_discovery/profiler/config.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


@dataclass(frozen=True)
class ProfilerConfig:
    # DuckDB safety
    duckdb_path: Path
    temp_dir: Path
    memory_limit: str = "2GB"
    threads: int = 4

    # Optional metrics
    compute_sample_values: bool = True
    compute_top1_ratio: bool = True
    compute_length_stats: bool = False

    # Sampling and costs
    sample_values_n: int = 10
    sample_max_approx_distinct: int = 100_000
    top1_max_approx_distinct: int = 50_000

    # Null token handling for text-like columns
    string_null_tokens: Sequence[str] = ("", "NULL", "null", "N/A", "na")

    # Presentation safety
    clamp_ratios: bool = True

    """
    cfg = ProfilerConfig(
    duckdb_path=out_dir / "profile.duckdb",
    temp_dir=out_dir / "tmp_duckdb",
    memory_limit="2GB",
    threads=4,
    compute_sample_values=False,
    compute_top1_ratio=True,
    compute_length_stats=False,
    sample_values_n=10,
    sample_max_approx_distinct=100_000,
    top1_max_approx_distinct=10_000,
    string_null_tokens=("", "NULL", "null", "N/A", "na"),
    clamp_ratios=True,
)
    """