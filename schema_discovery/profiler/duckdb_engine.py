# schema_discovery/profiler/duckdb_engine.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class DuckDbEngine:
    db_path: Path
    temp_dir: Path
    memory_limit: str = "2GB"
    threads: int = 4

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self.db_path))

        con.execute(f"PRAGMA temp_directory='{self.temp_dir.as_posix()}'")
        con.execute(f"PRAGMA memory_limit='{self.memory_limit}'")
        con.execute(f"PRAGMA threads={int(self.threads)}")

        return con