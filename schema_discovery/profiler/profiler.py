# schema_discovery/profiler/profiler.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Any, Optional

from schema_discovery.profiler.config import ProfilerConfig
from schema_discovery.profiler.duckdb_engine import DuckDbEngine
from schema_discovery.profiler.models import ColumnProfile


def _qident(name: str) -> str:
    # Safe SQL identifier quoting for DuckDB
    return '"' + name.replace('"', '""') + '"'


def _sql_string_nullify(expr: str, tokens: Iterable[str]) -> str:
    # Applies NULLIF nesting: NULLIF(NULLIF(expr, 'NULL'), '')
    out = expr
    for t in tokens:
        t_escaped = t.replace("'", "''")
        out = f"NULLIF({out}, '{t_escaped}')"
    return out


def register_csv_view(con, table_name: str, csv_path: Path) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {_qident(table_name)} AS
        SELECT *
        FROM read_csv('{csv_path.as_posix()}', auto_detect=true)
        """
    )


def describe_columns(con, table_name: str) -> List[Tuple[str, str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {_qident(table_name)}").fetchall()
    return [(r[0], r[1]) for r in rows]


def row_count(con, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {_qident(table_name)}").fetchone()[0])


def profile_one_column(
    con,
    cfg: ProfilerConfig,
    table_name: str,
    column_name: str,
    dtype: str,
    n_rows: int,
) -> ColumnProfile:
    col_ident = _qident(column_name)

    # Optional null token handling for string columns
    # Only apply to text-like types, otherwise leave as is
    expr = col_ident
    dtype_upper = (dtype or "").upper()
    if any(t in dtype_upper for t in ["VARCHAR", "CHAR", "TEXT"]):
        expr = _sql_string_nullify(f"CAST({col_ident} AS VARCHAR)", cfg.string_null_tokens)

    # Null and non-null counts
    n_null = int(
        con.execute(
            f"""
            SELECT SUM(CASE WHEN {expr} IS NULL THEN 1 ELSE 0 END)
            FROM {_qident(table_name)}
            """
        ).fetchone()[0]
        or 0
    )
    n_non_null = int(n_rows - n_null)
    null_ratio = (n_null / n_rows) if n_rows else 0.0

    # Unique counts
    if n_rows and n_rows <= cfg.exact_unique_max_rows:
        n_unique = int(
            con.execute(
                f"""
                SELECT COUNT(DISTINCT {expr})
                FROM {_qident(table_name)}
                WHERE {expr} IS NOT NULL
                """
            ).fetchone()[0]
            or 0
        )
        unique_is_exact = True
    else:
        n_unique = int(
            con.execute(
                f"""
                SELECT approx_count_distinct({expr})
                FROM {_qident(table_name)}
                WHERE {expr} IS NOT NULL
                """
            ).fetchone()[0]
            or 0
        )
        unique_is_exact = False

    unique_ratio = (n_unique / n_rows) if n_rows else 0.0
    unique_ratio_non_null = (n_unique / n_non_null) if n_non_null else 0.0

    # UCC flag: only trustworthy in exact mode
    if unique_is_exact:
        if cfg.ucc_ignore_nulls:
            is_unary_ucc = (n_unique == n_non_null) and (n_non_null > 0)
        else:
            # treat null as a value, stricter
            is_unary_ucc = (n_unique == n_rows) and (n_rows > 0)
    else:
        is_unary_ucc = False

    # Sample values: small and stable
    sample_values = [
        r[0]
        for r in con.execute(
            f"""
            SELECT DISTINCT {expr} AS v
            FROM {_qident(table_name)}
            WHERE {expr} IS NOT NULL
            LIMIT {int(cfg.sample_values_n)}
            """
        ).fetchall()
    ]

    # Length stats for text columns only
    avg_len: Optional[float] = None
    min_len: Optional[int] = None
    max_len: Optional[int] = None
    if any(t in dtype_upper for t in ["VARCHAR", "CHAR", "TEXT"]):
        row = con.execute(
            f"""
            SELECT
              AVG(LENGTH(CAST({expr} AS VARCHAR))),
              MIN(LENGTH(CAST({expr} AS VARCHAR))),
              MAX(LENGTH(CAST({expr} AS VARCHAR)))
            FROM {_qident(table_name)}
            WHERE {expr} IS NOT NULL
            """
        ).fetchone()
        avg_len = float(row[0]) if row[0] is not None else None
        min_len = int(row[1]) if row[1] is not None else None
        max_len = int(row[2]) if row[2] is not None else None

    # Top1 ratio: most frequent non-null value proportion
    # If all null -> 0.0
    if n_non_null == 0:
        top1_ratio = 0.0
    else:
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
        top1_ratio = (float(top1_count) / n_non_null) if top1_count else 0.0

    return ColumnProfile(
        table_name=table_name,
        column_name=column_name,
        dtype=dtype,

        n_rows=int(n_rows),
        n_null=int(n_null),
        n_non_null=int(n_non_null),
        null_ratio=float(null_ratio),

        n_unique=int(n_unique),
        unique_ratio=float(unique_ratio),
        unique_ratio_non_null=float(unique_ratio_non_null),

        is_unary_ucc=bool(is_unary_ucc),

        sample_values=sample_values,

        avg_len=avg_len,
        min_len=min_len,
        max_len=max_len,

        top1_ratio=float(top1_ratio),
    )


import hashlib

def _safe_alias(prefix: str, col: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in col)
    h = hashlib.md5(col.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}__{clean}__{h}"


def profile_table_core_one_scan(
    con,
    cfg: ProfilerConfig,
    table_name: str,
    columns: List[Tuple[str, str]],
) -> dict:
    """
    Returns a dict with:
      n_rows
      per-column: n_null, approx_distinct, avg_len/min_len/max_len (text only)
    Uses one table scan.
    """
    select_parts: List[str] = []
    select_parts.append("COUNT(*) AS n_rows")

    for col_name, dtype in columns:
        col_ident = _qident(col_name)
        dtype_upper = (dtype or "").upper()

        # null tokens for text columns
        expr = col_ident
        if any(t in dtype_upper for t in ["VARCHAR", "CHAR", "TEXT"]):
            expr = _sql_string_nullify(f"CAST({col_ident} AS VARCHAR)", cfg.string_null_tokens)

        null_alias = _safe_alias("n_null", col_name)
        ad_alias = _safe_alias("approx_distinct", col_name)

        select_parts.append(f"SUM(CASE WHEN {expr} IS NULL THEN 1 ELSE 0 END) AS {_qident(null_alias)}")
        select_parts.append(
            f"approx_count_distinct(CASE WHEN {expr} IS NULL THEN NULL ELSE {expr} END) AS {_qident(ad_alias)}"
        )

        # length stats only for text types
        if any(t in dtype_upper for t in ["VARCHAR", "CHAR", "TEXT"]):
            avg_alias = _safe_alias("avg_len", col_name)
            min_alias = _safe_alias("min_len", col_name)
            max_alias = _safe_alias("max_len", col_name)
            select_parts.append(f"AVG(LENGTH(CAST({expr} AS VARCHAR))) AS {_qident(avg_alias)}")
            select_parts.append(f"MIN(LENGTH(CAST({expr} AS VARCHAR))) AS {_qident(min_alias)}")
            select_parts.append(f"MAX(LENGTH(CAST({expr} AS VARCHAR))) AS {_qident(max_alias)}")
        else:
            # keep keys present with NULL, so downstream parsing is consistent
            select_parts.append(f"NULL AS {_qident(_safe_alias('avg_len', col_name))}")
            select_parts.append(f"NULL AS {_qident(_safe_alias('min_len', col_name))}")
            select_parts.append(f"NULL AS {_qident(_safe_alias('max_len', col_name))}")

    sql = f"""
    SELECT
      {", ".join(select_parts)}
    FROM {_qident(table_name)}
    """

    # One scan
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

        # Progress: table start
        print(f"\n[Profiler] Table -> {table_name}  Columns -> {len(cols)}")

        # One-scan core stats
        core = profile_table_core_one_scan(con, cfg, table_name, cols)
        n_rows = int(core["n_rows"] or 0)

        out: List[ColumnProfile] = []

        for i, (col_name, dtype) in enumerate(cols, start=1):
            if i == 1 or i % 10 == 0 or i == len(cols):
                print(f"[Profiler] {table_name} progress -> {i}/{len(cols)} columns")

            dtype_upper = (dtype or "").upper()
            col_ident = _qident(col_name)

            expr = col_ident
            if any(t in dtype_upper for t in ["VARCHAR", "CHAR", "TEXT"]):
                expr = _sql_string_nullify(f"CAST({col_ident} AS VARCHAR)", cfg.string_null_tokens)

            n_null = int(core.get(_safe_alias("n_null", col_name)) or 0)
            n_non_null = int(n_rows - n_null)
            null_ratio = (n_null / n_rows) if n_rows else 0.0

            approx_distinct = int(core.get(_safe_alias("approx_distinct", col_name)) or 0)

            # In core mode, treat n_unique as approx_distinct.
            n_unique = approx_distinct

            unique_ratio = (n_unique / n_rows) if n_rows else 0.0
            unique_ratio_non_null = (n_unique / n_non_null) if n_non_null else 0.0

            # Only true if you later run exact distinct checks.
            is_unary_ucc = False

            # D: Gate sample_values to avoid per-column scans on huge/high-cardinality columns
            if cfg.compute_sample_values and approx_distinct <= cfg.sample_max_approx_distinct:
                sample_values = [
                    r[0]
                    for r in con.execute(
                        f"""
                        SELECT DISTINCT {expr} AS v
                        FROM {_qident(table_name)}
                        WHERE {expr} IS NOT NULL
                        LIMIT {int(cfg.sample_values_n)}
                        """
                    ).fetchall()
                ]
            else:
                sample_values = []

            avg_len = core.get(_safe_alias("avg_len", col_name))
            min_len = core.get(_safe_alias("min_len", col_name))
            max_len = core.get(_safe_alias("max_len", col_name))

            avg_len = float(avg_len) if avg_len is not None else None
            min_len = int(min_len) if min_len is not None else None
            max_len = int(max_len) if max_len is not None else None

            # top1_ratio gated
            if cfg.compute_top1_ratio and approx_distinct <= cfg.top1_max_approx_distinct:
                top1_ratio = top1_ratio_for_column(con, table_name, expr, n_non_null)
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
                    n_unique=n_unique,
                    unique_ratio=float(unique_ratio),
                    unique_ratio_non_null=float(unique_ratio_non_null),
                    is_unary_ucc=is_unary_ucc,
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