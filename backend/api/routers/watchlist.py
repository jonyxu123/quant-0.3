"""自选股路由。"""

from __future__ import annotations

import os
import sys
from typing import Optional

import duckdb
from fastapi import APIRouter, HTTPException, Query
from loguru import logger
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import DB_PATH
from backend.data.duckdb_manager import open_duckdb_conn

router = APIRouter()


def _ensure_table(conn: duckdb.DuckDBPyConnection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watchlist (
            ts_code VARCHAR PRIMARY KEY,
            name VARCHAR,
            industry VARCHAR,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_strategy VARCHAR,
            note VARCHAR,
            sort_order INTEGER DEFAULT 0
        )
        """
    )


def _get_conn():
    conn = open_duckdb_conn(DB_PATH, retries=3, base_sleep_sec=0.08)
    _ensure_table(conn)
    return conn


class WatchlistItem(BaseModel):
    ts_code: str
    name: str
    industry: Optional[str] = None
    added_at: Optional[str] = None
    source_strategy: Optional[str] = None
    note: Optional[str] = None
    sort_order: int = 0


@router.get("/list")
def get_watchlist():
    conn = _get_conn()
    try:
        df = conn.execute(
            """
            SELECT ts_code, name, industry, added_at, source_strategy, note, sort_order
            FROM watchlist
            ORDER BY sort_order, added_at
            """
        ).fetchdf()
        records = df.to_dict(orient="records") if len(df) > 0 else []
        for r in records:
            for k, v in r.items():
                if v is None or (isinstance(v, float) and (v != v)):
                    r[k] = None
                elif hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {"count": len(records), "data": records}
    except Exception as e:
        logger.error(f"获取自选股失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/add")
def add_to_watchlist(item: WatchlistItem):
    conn = _get_conn()
    try:
        existing = conn.execute(
            "SELECT ts_code FROM watchlist WHERE ts_code = ?",
            [item.ts_code],
        ).fetchall()
        if existing:
            return {"ok": False, "msg": f"{item.name} 已在自选列表中"}
        conn.execute(
            """
            INSERT INTO watchlist (ts_code, name, industry, added_at, source_strategy, note, sort_order)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)
            """,
            [item.ts_code, item.name, item.industry, item.source_strategy, item.note, item.sort_order],
        )
        return {"ok": True, "msg": "添加成功"}
    except Exception as e:
        logger.error(f"添加自选股失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.delete("/remove/{ts_code}")
def remove_from_watchlist(ts_code: str):
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM watchlist WHERE ts_code = ?", [ts_code])
        return {"ok": True, "msg": "移除成功"}
    except Exception as e:
        logger.error(f"移除自选股失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/sync")
def sync_watchlist(items: list[WatchlistItem]):
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM watchlist")
        for idx, item in enumerate(items):
            conn.execute(
                """
                INSERT INTO watchlist (ts_code, name, industry, added_at, source_strategy, note, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    item.ts_code,
                    item.name,
                    item.industry,
                    item.added_at or "2025-01-01T00:00:00",
                    item.source_strategy,
                    item.note,
                    idx,
                ],
            )
        return {"ok": True, "msg": f"同步完成，共 {len(items)} 只"}
    except Exception as e:
        logger.error(f"同步自选股失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.put("/note/{ts_code}")
def update_note(ts_code: str, note: str = Query(...)):
    conn = _get_conn()
    try:
        conn.execute("UPDATE watchlist SET note = ? WHERE ts_code = ?", [note, ts_code])
        return {"ok": True}
    except Exception as e:
        logger.error(f"更新备注失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/check/{ts_code}")
def check_in_watchlist(ts_code: str):
    conn = _get_conn()
    try:
        result = conn.execute(
            "SELECT COUNT(*) FROM watchlist WHERE ts_code = ?",
            [ts_code],
        ).fetchone()
        return {"in_watchlist": bool(result and result[0] > 0)}
    except Exception as e:
        logger.error(f"检查自选股失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

