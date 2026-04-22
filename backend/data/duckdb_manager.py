"""Unified DuckDB connection helpers.

Keep DuckDB connect behavior consistent across modules to avoid
"same database file with a different configuration" conflicts.
"""

from __future__ import annotations

import time
from typing import Callable, Optional

import duckdb

from config import DB_PATH


def is_duckdb_lock_error(msg: str) -> bool:
    m = (msg or "").lower()
    return ("cannot open file" in m) and (
        "used by another process" in m
        or "进程无法访问" in m
        or "另一个程序正在使用此文件" in m
    )


def open_duckdb_conn(
    db_path: str = DB_PATH,
    *,
    retries: int = 3,
    base_sleep_sec: float = 0.08,
    on_retry: Optional[Callable[[int, Exception], None]] = None,
) -> duckdb.DuckDBPyConnection:
    """Open DuckDB connection with consistent options and lock backoff.

    Notes:
    - We intentionally do not pass read_only/read_write flags here.
    - Centralizing connect options prevents per-module drift.
    """

    tries = max(1, int(retries))
    last_exc: Optional[Exception] = None
    for i in range(tries):
        try:
            return duckdb.connect(db_path)
        except Exception as exc:
            last_exc = exc
            if i < tries - 1 and is_duckdb_lock_error(str(exc)):
                if on_retry is not None:
                    try:
                        on_retry(i + 1, exc)
                    except Exception:
                        pass
                time.sleep(base_sleep_sec * (i + 1))
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("duckdb connect failed")

