from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from schema_discovery.profiler.models import ColumnProfile
from schema_discovery.profiler.duckdb_engine import DuckDbEngine
from schema_discovery.profiler.profiler import _qident, _sql_string_nullify, register_csv_view
from schema_discovery.normalization.models import NormalizedColumnProfile
from schema_discovery.normalization.detectors import dtype_family
from schema_discovery.normalization.canonical_types import (
    RAW,
    TRIMMED_STRING,
    INTEGER_CANONICAL,
    NUMERIC_STRING_CANONICAL,
)


@dataclass(frozen=True)
class NormalizationConfig:
    duckdb_path: Path
    temp_dir: Path
    memory_limit: str = "2GB"
    threads: int = 4

    string_null_tokens: Iterable[str] = ("", "NULL", "null", "N/A", "na")

    numeric_string_threshold: float = 0.98
    zero_padded_threshold: float = 0.20
    integer_like_float_threshold: float = 0.999
    float_integer_epsilon: float = 1e-9

    free_text_min_avg_len: float = 30.0
    free_text_min_unique_ratio: float = 0.80


def _build_text_expr(column_name: str, cfg: NormalizationConfig) -> str:
    col_ident = _qident(column_name)
    return _sql_string_nullify(f"TRIM(CAST({col_ident} AS VARCHAR))", cfg.string_null_tokens)


def _detect_text_flags(con, table_name: str, column_name: str, cfg: NormalizationConfig) -> tuple[bool, bool]:
    expr = _build_text_expr(column_name, cfg)

    query = f"""
    SELECT
        COUNT(*) FILTER (WHERE {expr} IS NOT NULL) AS n_non_null,
        SUM(
            CASE
                WHEN {expr} IS NOT NULL
                 AND regexp_full_match({expr}, '^[+-]?[0-9]+([.]0+)?$')
                THEN 1 ELSE 0
            END
        ) AS numeric_like_count,
        SUM(
            CASE
                WHEN {expr} IS NOT NULL
                 AND regexp_full_match({expr}, '^[+-]?0+[0-9]+$')
                THEN 1 ELSE 0
            END
        ) AS zero_padded_count
    FROM {_qident(table_name)}
    """

    n_non_null, numeric_like_count, zero_padded_count = con.execute(query).fetchone()

    n_non_null = int(n_non_null or 0)
    numeric_like_count = int(numeric_like_count or 0)
    zero_padded_count = int(zero_padded_count or 0)

    if n_non_null == 0:
        return False, False

    numeric_ratio = numeric_like_count / n_non_null
    zero_padded_ratio = zero_padded_count / n_non_null

    numeric_string_like = numeric_ratio >= cfg.numeric_string_threshold
    zero_padded_string = zero_padded_ratio >= cfg.zero_padded_threshold

    return numeric_string_like, zero_padded_string


def _detect_integer_like_float(con, table_name: str, column_name: str, cfg: NormalizationConfig) -> bool:
    col_ident = _qident(column_name)

    query = f"""
    SELECT
        COUNT(*) FILTER (WHERE {col_ident} IS NOT NULL) AS n_non_null,
        SUM(
            CASE
                WHEN {col_ident} IS NOT NULL
                 AND ABS({col_ident} - ROUND({col_ident})) <= {cfg.float_integer_epsilon}
                THEN 1 ELSE 0
            END
        ) AS integer_like_count
    FROM {_qident(table_name)}
    """

    n_non_null, integer_like_count = con.execute(query).fetchone()

    n_non_null = int(n_non_null or 0)
    integer_like_count = int(integer_like_count or 0)

    if n_non_null == 0:
        return False

    ratio = integer_like_count / n_non_null
    return ratio >= cfg.integer_like_float_threshold


def _is_free_text_like(profile: ColumnProfile, family: str, cfg: NormalizationConfig) -> bool:
    if family != "text":
        return False

    avg_len = profile.avg_len if profile.avg_len is not None else 0.0

    return (
        avg_len >= cfg.free_text_min_avg_len
        and profile.approx_unique_ratio >= cfg.free_text_min_unique_ratio
    )


def _canonical_types_for(
    family: str,
    numeric_string_like: bool,
    integer_like_float: bool,
) -> List[str]:
    out: List[str] = [RAW]

    if family == "text":
        out.append(TRIMMED_STRING)
        if numeric_string_like:
            out.append(NUMERIC_STRING_CANONICAL)

    if family == "integer":
        out.append(INTEGER_CANONICAL)

    if family == "float" and integer_like_float:
        out.append(INTEGER_CANONICAL)

    return list(dict.fromkeys(out))


def normalize_table_profiles(
    cfg: NormalizationConfig,
    table_name: str,
    csv_path: Path,
    profiles: List[ColumnProfile],
) -> List[NormalizedColumnProfile]:
    engine = DuckDbEngine(
        db_path=cfg.duckdb_path,
        temp_dir=cfg.temp_dir,
        memory_limit=cfg.memory_limit,
        threads=cfg.threads,
    )

    con = engine.connect()
    try:
        register_csv_view(con, table_name, csv_path)

        out: List[NormalizedColumnProfile] = []

        for profile in profiles:
            family = dtype_family(profile.dtype)

            numeric_string_like = False
            zero_padded_string = False
            integer_like_float = False

            if family == "text":
                numeric_string_like, zero_padded_string = _detect_text_flags(
                    con, table_name, profile.column_name, cfg
                )

            if family == "float":
                integer_like_float = _detect_integer_like_float(
                    con, table_name, profile.column_name, cfg
                )

            free_text_like = _is_free_text_like(profile, family, cfg)

            canonical_types = _canonical_types_for(
                family=family,
                numeric_string_like=numeric_string_like,
                integer_like_float=integer_like_float,
            )

            out.append(
                NormalizedColumnProfile(
                    table_name=profile.table_name,
                    column_name=profile.column_name,
                    dtype=profile.dtype,
                    dtype_family=family,
                    n_rows=profile.n_rows,
                    n_null=profile.n_null,
                    n_non_null=profile.n_non_null,
                    null_ratio=profile.null_ratio,
                    approx_n_unique=profile.approx_n_unique,
                    approx_unique_ratio=profile.approx_unique_ratio,
                    approx_unique_ratio_non_null=profile.approx_unique_ratio_non_null,
                    top1_ratio=profile.top1_ratio,
                    sample_values=profile.sample_values,
                    avg_len=profile.avg_len,
                    min_len=profile.min_len,
                    max_len=profile.max_len,
                    numeric_string_like=numeric_string_like,
                    integer_like_float=integer_like_float,
                    zero_padded_string=zero_padded_string,
                    free_text_like=free_text_like,
                    canonical_types=canonical_types,
                )
            )

        return out
    finally:
        con.close()


def normalize_all_tables(
    cfg: NormalizationConfig,
    tables: Dict[str, Path],
    profiles: List[ColumnProfile],
) -> List[NormalizedColumnProfile]:
    grouped: Dict[str, List[ColumnProfile]] = {}

    for p in profiles:
        grouped.setdefault(p.table_name, []).append(p)

    out: List[NormalizedColumnProfile] = []

    for table_name, table_profiles in grouped.items():
        csv_path = tables[table_name]
        out.extend(normalize_table_profiles(cfg, table_name, csv_path, table_profiles))

    return out