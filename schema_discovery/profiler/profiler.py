# schema_discovery/profiler/profiler.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional
import hashlib

from schema_discovery.profiler.config import ProfilerConfig
from schema_discovery.profiler.duckdb_engine import DuckDbEngine
from schema_discovery.profiler.models import ColumnProfile


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_string_nullify(expr: str, tokens: Iterable[str]) -> str:
    out = expr
    for t in tokens:
        t_escaped = t.replace("'", "''")
        out = f"NULLIF({out}, '{t_escaped}')"
    return out


def _safe_alias(prefix: str, col: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in col)
    h = hashlib.md5(col.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}__{clean}__{h}"


def _is_text_dtype(dtype: str) -> bool:
    dtype_upper = (dtype or "").upper()
    return any(t in dtype_upper for t in ["VARCHAR", "CHAR", "TEXT"])


def _clamp_ratio(value: float, cfg: ProfilerConfig) -> float:
    if not cfg.clamp_ratios:
        return value
    return max(0.0, min(1.0, value))


def register_csv_view(con, table_name: str, csv_path: Path) -> None:
    csv_file = csv_path.as_posix()

    con.execute(
        f"""
        CREATE OR REPLACE VIEW {_qident(table_name)} AS
        SELECT *
        FROM read_csv(
            '{csv_file}',
            auto_detect=true,
            all_varchar=true
        )
        """
    )


def describe_columns(con, table_name: str) -> List[Tuple[str, str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {_qident(table_name)}").fetchall()
    return [(r[0], r[1]) for r in rows]


def profile_table_core_one_scan(
    con,
    cfg: ProfilerConfig,
    table_name: str,
    columns: List[Tuple[str, str]],
) -> dict:
    """
    One scan per table for:
      - n_rows
      - n_null per column
      - approx distinct per column
      - optional length stats for text columns
    """
    select_parts: List[str] = ["COUNT(*) AS n_rows"]

    for col_name, dtype in columns:
        col_ident = _qident(col_name)
        expr = col_ident

        if _is_text_dtype(dtype):
            expr = _sql_string_nullify(f"CAST({col_ident} AS VARCHAR)", cfg.string_null_tokens)

        null_alias = _safe_alias("n_null", col_name)
        approx_alias = _safe_alias("approx_n_unique", col_name)

        select_parts.append(
            f"SUM(CASE WHEN {expr} IS NULL THEN 1 ELSE 0 END) AS {_qident(null_alias)}"
        )
        select_parts.append(
            f"approx_count_distinct(CASE WHEN {expr} IS NULL THEN NULL ELSE {expr} END) AS {_qident(approx_alias)}"
        )

        avg_alias = _safe_alias("avg_len", col_name)
        min_alias = _safe_alias("min_len", col_name)
        max_alias = _safe_alias("max_len", col_name)

        if cfg.compute_length_stats and _is_text_dtype(dtype):
            select_parts.append(f"AVG(LENGTH(CAST({expr} AS VARCHAR))) AS {_qident(avg_alias)}")
            select_parts.append(f"MIN(LENGTH(CAST({expr} AS VARCHAR))) AS {_qident(min_alias)}")
            select_parts.append(f"MAX(LENGTH(CAST({expr} AS VARCHAR))) AS {_qident(max_alias)}")
        else:
            select_parts.append(f"NULL AS {_qident(avg_alias)}")
            select_parts.append(f"NULL AS {_qident(min_alias)}")
            select_parts.append(f"NULL AS {_qident(max_alias)}")

    sql = f"""
    SELECT
      {", ".join(select_parts)}
    FROM {_qident(table_name)}
    """

    cur = con.execute(sql)
    row = cur.fetchone()
    keys = [d[0] for d in cur.description]
    return dict(zip(keys, row))


def top1_ratio_for_column(con, table_name: str, expr: str, n_non_null: int) -> float:
    if n_non_null <= 0:
        return 0.0

    top1_count = con.execute(
        f"""
        SELECT MAX(cnt) FROM (
          SELECT COUNT(*) AS cnt
          FROM {_qident(table_name)}
          WHERE {expr} IS NOT NULL
          GROUP BY {expr}
        )
        """
    ).fetchone()[0]

    return (float(top1_count) / n_non_null) if top1_count else 0.0


def sample_values_for_column(
    con,
    cfg: ProfilerConfig,
    table_name: str,
    expr: str,
) -> List[object]:
    """
    Cheaper than SELECT DISTINCT ... LIMIT.
    Pull a few non-null values, then de-duplicate in Python.
    """
    rows = con.execute(
        f"""
        SELECT {expr} AS v
        FROM {_qident(table_name)}
        WHERE {expr} IS NOT NULL
        LIMIT {int(cfg.sample_values_n * 3)}
        """
    ).fetchall()

    seen = set()
    out: List[object] = []

    for (v,) in rows:
        key = repr(v)
        if key not in seen:
            seen.add(key)
            out.append(v)
        if len(out) >= cfg.sample_values_n:
            break

    return out


def profile_table_csv(
    cfg: ProfilerConfig,
    table_name: str,
    csv_path: Path,
) -> List[ColumnProfile]:
    engine = DuckDbEngine(
        db_path=cfg.duckdb_path,
        temp_dir=cfg.temp_dir,
        memory_limit=cfg.memory_limit,
        threads=cfg.threads,
    )

    con = engine.connect()
    try:
        register_csv_view(con, table_name, csv_path)
        cols = describe_columns(con, table_name)

        print(f"\n[Profiler] Table -> {table_name}  Columns -> {len(cols)}")

        core = profile_table_core_one_scan(con, cfg, table_name, cols)
        n_rows = int(core["n_rows"] or 0)

        out: List[ColumnProfile] = []

        for i, (col_name, dtype) in enumerate(cols, start=1):
            if i == 1 or i % 10 == 0 or i == len(cols):
                print(f"[Profiler] {table_name} progress -> {i}/{len(cols)} columns")

            col_ident = _qident(col_name)
            expr = col_ident

            if _is_text_dtype(dtype):
                expr = _sql_string_nullify(f"CAST({col_ident} AS VARCHAR)", cfg.string_null_tokens)

            n_null = int(core.get(_safe_alias("n_null", col_name)) or 0)
            n_non_null = int(n_rows - n_null)
            null_ratio = (n_null / n_rows) if n_rows else 0.0
            null_ratio = _clamp_ratio(null_ratio, cfg)

            approx_n_unique = int(core.get(_safe_alias("approx_n_unique", col_name)) or 0)

            approx_unique_ratio = (approx_n_unique / n_rows) if n_rows else 0.0
            approx_unique_ratio_non_null = (approx_n_unique / n_non_null) if n_non_null else 0.0

            approx_unique_ratio = _clamp_ratio(approx_unique_ratio, cfg)
            approx_unique_ratio_non_null = _clamp_ratio(approx_unique_ratio_non_null, cfg)

            if cfg.compute_sample_values and approx_n_unique <= cfg.sample_max_approx_distinct:
                sample_values = sample_values_for_column(con, cfg, table_name, expr)
            else:
                sample_values = []

            avg_len = core.get(_safe_alias("avg_len", col_name))
            min_len = core.get(_safe_alias("min_len", col_name))
            max_len = core.get(_safe_alias("max_len", col_name))

            avg_len = float(avg_len) if avg_len is not None else None
            min_len = int(min_len) if min_len is not None else None
            max_len = int(max_len) if max_len is not None else None

            if cfg.compute_top1_ratio and approx_n_unique <= cfg.top1_max_approx_distinct:
                top1_ratio = top1_ratio_for_column(con, table_name, expr, n_non_null)
                top1_ratio = _clamp_ratio(top1_ratio, cfg)
            else:
                top1_ratio = 0.0

            out.append(
                ColumnProfile(
                    table_name=table_name,
                    column_name=col_name,
                    dtype=dtype,
                    n_rows=n_rows,
                    n_null=n_null,
                    n_non_null=n_non_null,
                    null_ratio=float(null_ratio),
                    approx_n_unique=approx_n_unique,
                    approx_unique_ratio=float(approx_unique_ratio),
                    approx_unique_ratio_non_null=float(approx_unique_ratio_non_null),
                    sample_values=sample_values,
                    avg_len=avg_len,
                    min_len=min_len,
                    max_len=max_len,
                    top1_ratio=float(top1_ratio),
                )
            )

        return out
    finally:
        con.close()


def profile_all_tables_csv(cfg: ProfilerConfig, tables: Dict[str, Path]) -> List[ColumnProfile]:
    all_profiles: List[ColumnProfile] = []
    for table_name, csv_path in tables.items():
        all_profiles.extend(profile_table_csv(cfg, table_name, csv_path))
    return all_profiles