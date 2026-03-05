# tests/test_profiler.py

from __future__ import annotations

from pathlib import Path
import csv
import sys

# If running directly and imports fail, uncomment:
sys.path.append(str(Path(__file__).resolve().parents[1]))

from schema_discovery.profiler.config import ProfilerConfig
from schema_discovery.profiler.profiler import profile_all_tables_csv


def find_csv_tables(data_dir: Path) -> dict[str, Path]:
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {data_dir}")

    # table_name -> path
    tables: dict[str, Path] = {p.stem: p for p in csv_files}
    return tables


def write_profiles_csv(out_csv: Path, profiles) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # ColumnProfile is a dataclass, so vars(p) is safe
    rows = [vars(p) for p in profiles]

    # Ensure stable header order (matches your requested fields)
    header = [
        "table_name",
        "column_name",
        "dtype",
        "n_rows",
        "n_null",
        "n_non_null",
        "null_ratio",
        "n_unique",
        "unique_ratio",
        "unique_ratio_non_null",
        "is_unary_ucc",
        "sample_values",
        "avg_len",
        "min_len",
        "max_len",
        "top1_ratio",
    ]

    # If you later add fields, include them automatically at the end
    extra_keys = sorted({k for r in rows for k in r.keys()} - set(header))
    full_header = header + extra_keys

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=full_header)
        w.writeheader()
        for r in rows:
            # store sample_values as a compact string
            if isinstance(r.get("sample_values"), list):
                r["sample_values"] = "|".join(str(x) for x in r["sample_values"])
            w.writerow(r)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    data_dir = repo_root / "data" / "Anon BML Data"
    out_dir = repo_root / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_csv = out_dir / "profiler_result.csv"

    cfg = ProfilerConfig(
        duckdb_path=out_dir / "profile.duckdb",
        temp_dir=out_dir / "tmp_duckdb",
        memory_limit="2GB",     # safe starting point for 8GB RAM
        threads=4,
        sample_values_n=10,
        exact_unique_max_rows=200_000,
        string_null_tokens=("", "NULL", "null", "N/A", "na"),
        ucc_ignore_nulls=True,
    )

    tables = find_csv_tables(data_dir)

    # Optional: run biggest first
    tables = dict(sorted(tables.items(), key=lambda kv: kv[1].stat().st_size, reverse=True))

    print(f"Found {len(tables)} tables in: {data_dir}")
    print(f"Output -> {out_csv}")

    profiles = profile_all_tables_csv(cfg, tables)
    write_profiles_csv(out_csv, profiles)

    print(f"Done. Profile rows: {len(profiles)}")


if __name__ == "__main__":
    main()