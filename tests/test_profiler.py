# tests/test_profiler.py

from __future__ import annotations

from pathlib import Path
import csv
import sys
import time

sys.path.append(str(Path(__file__).resolve().parents[1]))

from schema_discovery.profiler.config import ProfilerConfig
from schema_discovery.profiler.profiler import profile_table_csv

from schema_discovery.normalization.normalizer import (
    NormalizationConfig,
    normalize_all_tables,
)
from schema_discovery.pruning.rules import PruningConfig
from schema_discovery.pruning.pruner import prune_normalized_profiles


def find_csv_tables(data_dir: Path) -> dict[str, Path]:
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {data_dir}")

    tables: dict[str, Path] = {p.stem: p for p in csv_files}
    return tables


def write_rows_csv(out_csv: Path, rows_obj, header: list[str]) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    rows = [vars(r).copy() for r in rows_obj]

    extra_keys = sorted({k for r in rows for k in r.keys()} - set(header))
    full_header = header + extra_keys

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=full_header)
        w.writeheader()

        for r in rows:
            for k, v in list(r.items()):
                if isinstance(v, list):
                    r[k] = "|".join(str(x) for x in v)
            w.writerow(r)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    #data_dir = repo_root / "data" / "Anon BML Data"
    data_dir = repo_root / "data" / "Raw Data"
    out_dir = repo_root / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    profiler_out_csv = out_dir / "profiler_result_cazloyd.csv"
    normalized_out_csv = out_dir / "normalized_result_cazloyd.csv"
    pruned_out_csv = out_dir / "pruned_result_cazloyd.csv"

    profiler_cfg = ProfilerConfig(
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

    normalization_cfg = NormalizationConfig(
        duckdb_path=out_dir / "normalize.duckdb",
        temp_dir=out_dir / "tmp_normalize_duckdb",
        memory_limit="2GB",
        threads=4,
        string_null_tokens=("", "NULL", "null", "N/A", "na"),
        numeric_string_threshold=0.98,
        zero_padded_threshold=0.20,
        integer_like_float_threshold=0.999,
        float_integer_epsilon=1e-9,
        free_text_min_avg_len=30.0,
        free_text_min_unique_ratio=0.80,
    )

    pruning_cfg = PruningConfig(
        reject_null_ratio=0.98,
        reject_top1_ratio=0.999,
        pk_like_min_unique_ratio_non_null=0.90,
        pk_like_max_top1_ratio=0.50,
        other_key_like_min_unique_ratio_non_null=0.70,
        fk_like_min_non_null=2,
        fk_like_max_null_ratio=0.98,
        reject_free_text=True,
        reject_boolean=True,
    )

    tables = find_csv_tables(data_dir)
    tables = dict(sorted(tables.items(), key=lambda kv: kv[1].stat().st_size, reverse=True))

    print(f"Found {len(tables)} tables in: {data_dir}")
    print(f"Profiler output   -> {profiler_out_csv}")
    print(f"Normalized output -> {normalized_out_csv}")
    print(f"Pruned output     -> {pruned_out_csv}")

    overall_start = time.perf_counter()

    # ============================================================
    # 1. PROFILING
    # ============================================================
    print("\n" + "=" * 80)
    print("STAGE 1 -> PROFILING")
    print("=" * 80)

    all_profiles = []
    profiling_start = time.perf_counter()

    for table_name, csv_path in tables.items():
        print("\n" + "-" * 70)
        print(f"Starting table: {table_name}")
        print(f"File: {csv_path.name}")
        print(f"Size: {csv_path.stat().st_size / (1024**2):.2f} MB")

        table_start = time.perf_counter()
        table_profiles = profile_table_csv(profiler_cfg, table_name, csv_path)
        table_elapsed = time.perf_counter() - table_start

        print(f"[Runtime] {table_name} finished in {table_elapsed:.2f} seconds ({table_elapsed/60:.2f} min)")
        all_profiles.extend(table_profiles)

    profiling_elapsed = time.perf_counter() - profiling_start

    write_rows_csv(
        profiler_out_csv,
        all_profiles,
        header=[
            "table_name",
            "column_name",
            "dtype",
            "n_rows",
            "n_null",
            "n_non_null",
            "null_ratio",
            "approx_n_unique",
            "approx_unique_ratio",
            "approx_unique_ratio_non_null",
            "sample_values",
            "avg_len",
            "min_len",
            "max_len",
            "top1_ratio",
        ],
    )

    print("\n" + "-" * 70)
    print(f"[Runtime] Profiling stage finished in {profiling_elapsed:.2f} seconds ({profiling_elapsed/60:.2f} min)")
    print(f"Profile rows: {len(all_profiles)}")

    # ============================================================
    # 2. NORMALIZATION
    # ============================================================
    print("\n" + "=" * 80)
    print("STAGE 2 -> NORMALIZATION")
    print("=" * 80)

    normalization_start = time.perf_counter()

    normalized_profiles = normalize_all_tables(
        normalization_cfg,
        tables,
        all_profiles,
    )

    normalization_elapsed = time.perf_counter() - normalization_start

    write_rows_csv(
        normalized_out_csv,
        normalized_profiles,
        header=[
            "table_name",
            "column_name",
            "dtype",
            "dtype_family",
            "n_rows",
            "n_null",
            "n_non_null",
            "null_ratio",
            "approx_n_unique",
            "approx_unique_ratio",
            "approx_unique_ratio_non_null",
            "top1_ratio",
            "sample_values",
            "avg_len",
            "min_len",
            "max_len",
            "numeric_string_like",
            "integer_like_float",
            "zero_padded_string",
            "free_text_like",
            "canonical_types",
        ],
    )

    print(f"[Runtime] Normalization stage finished in {normalization_elapsed:.2f} seconds ({normalization_elapsed/60:.2f} min)")
    print(f"Normalized rows: {len(normalized_profiles)}")

    # ============================================================
    # 3. PRUNING
    # ============================================================
    print("\n" + "=" * 80)
    print("STAGE 3 -> PRUNING")
    print("=" * 80)

    pruning_start = time.perf_counter()

    pruned_candidates = prune_normalized_profiles(
        pruning_cfg,
        normalized_profiles,
    )

    pruning_elapsed = time.perf_counter() - pruning_start

    write_rows_csv(
        pruned_out_csv,
        pruned_candidates,
        header=[
            "table_name",
            "column_name",
            "dtype",
            "dtype_family",
            "canonical_types",
            "pk_like",
            "fk_like",
            "other_key_like",
            "reject",
            "reasons",
        ],
    )

    print(f"[Runtime] Pruning stage finished in {pruning_elapsed:.2f} seconds ({pruning_elapsed/60:.2f} min)")
    print(f"Pruned rows: {len(pruned_candidates)}")

    # ============================================================
    # FINAL SUMMARY
    # ============================================================
    overall_elapsed = time.perf_counter() - overall_start

    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"Profiling runtime     : {profiling_elapsed:.2f} sec ({profiling_elapsed/60:.2f} min)")
    print(f"Normalization runtime : {normalization_elapsed:.2f} sec ({normalization_elapsed/60:.2f} min)")
    print(f"Pruning runtime       : {pruning_elapsed:.2f} sec ({pruning_elapsed/60:.2f} min)")
    print(f"Overall runtime       : {overall_elapsed:.2f} sec ({overall_elapsed/60:.2f} min)")
    print(f"Profiler output rows  : {len(all_profiles)}")
    print(f"Normalized rows       : {len(normalized_profiles)}")
    print(f"Pruned rows           : {len(pruned_candidates)}")
    print("=" * 80)


if __name__ == "__main__":
    main()