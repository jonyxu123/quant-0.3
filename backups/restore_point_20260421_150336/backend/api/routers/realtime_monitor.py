"""Realtime monitor router (recovered minimal stable version)."""
from __future__ import annotations

import asyncio
import datetime
import json
import queue as _queue_mod
import threading
import time
from typing import Optional

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from loguru import logger
from pydantic import BaseModel

from config import DB_PATH, REALTIME_UI_CONFIG
from backend.realtime import signals as sig
from backend.realtime.mootdx_client import client as mootdx_client
from backend.realtime.tick_provider import get_tick_provider

router = APIRouter()

# Keep txn sample size aligned with Layer2 analysis window.
_TXN_ANALYZE_COUNT = 50


POOLS = {
    1: {
        "pool_id": 1,
        "name": "Pool 1 Timing",
        "strategy": "Position Building",
        "desc": "left side + right side breakout",
        "signal_types": ["left_side_buy", "right_side_breakout"],
    },
    2: {
        "pool_id": 2,
        "name": "Pool 2 T+0",
        "strategy": "Intraday Trading",
        "desc": "positive_t + reverse_t",
        "signal_types": ["positive_t", "reverse_t"],
    },
}

POOL_SIGNAL_TYPES = {
    1: {"left_side_buy", "right_side_breakout"},
    2: {"positive_t", "reverse_t"},
}

INDICES = [
    ("000001.SH", "上证指数"),
    ("000016.SH", "上证50"),
    ("000300.SH", "沪深300"),
    ("000905.SH", "中证500"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
]

_TABLE_READY = False
_TABLE_LOCK = threading.Lock()

_tick_persist_started = False
_txn_refresher_started = False
_quality_monitor_started = False
_drift_monitor_started = False
_quality_track_queue: _queue_mod.Queue = _queue_mod.Queue(maxsize=10000)


def _is_duckdb_lock_message(msg: str) -> bool:
    m = (msg or "").lower()
    return ("cannot open file" in m) and ("used by another process" in m or "杩涚▼鏃犳硶璁块棶" in m)


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    with _TABLE_LOCK:
        if _TABLE_READY:
            return
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monitor_pools (
                pool_id INTEGER,
                ts_code VARCHAR,
                name VARCHAR,
                industry VARCHAR,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                note VARCHAR,
                sort_order INTEGER DEFAULT 0,
                PRIMARY KEY (pool_id, ts_code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_history (
                id INTEGER PRIMARY KEY,
                pool_id INTEGER,
                ts_code VARCHAR,
                name VARCHAR,
                signal_type VARCHAR,
                strength DOUBLE,
                message VARCHAR,
                price DOUBLE,
                pct_chg DOUBLE,
                triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS t0_signal_quality (
                id BIGINT,
                ts_code VARCHAR,
                signal_type VARCHAR,
                direction VARCHAR,
                trigger_time TIMESTAMP,
                trigger_price DOUBLE,
                eval_horizon_sec INTEGER,
                eval_price DOUBLE,
                ret_bps DOUBLE,
                mfe_bps DOUBLE,
                mae_bps DOUBLE,
                direction_correct BOOLEAN,
                market_phase VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS t0_feature_drift (
                id BIGINT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                feature_name VARCHAR,
                psi DOUBLE,
                ks_stat DOUBLE,
                ks_p DOUBLE,
                severity VARCHAR,
                drifted BOOLEAN,
                precision_drop DOUBLE,
                alerted BOOLEAN DEFAULT FALSE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tick_history (
                ts_code VARCHAR,
                name VARCHAR,
                price DOUBLE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                pre_close DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                pct_chg DOUBLE,
                ts BIGINT,
                saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS signal_history_id_seq")
        except Exception:
            pass
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS t0_signal_quality_id_seq")
        except Exception:
            pass
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS t0_feature_drift_id_seq")
        except Exception:
            pass
        for ddl in (
            "CREATE INDEX IF NOT EXISTS idx_monitor_pools_pool_order ON monitor_pools(pool_id, sort_order, added_at)",
            "CREATE INDEX IF NOT EXISTS idx_signal_history_pool_code_type_ts ON signal_history(pool_id, ts_code, signal_type, triggered_at)",
            "CREATE INDEX IF NOT EXISTS idx_signal_history_triggered_at ON signal_history(triggered_at)",
            "CREATE INDEX IF NOT EXISTS idx_tick_history_code_ts ON tick_history(ts_code, ts)",
            "CREATE INDEX IF NOT EXISTS idx_t0_quality_created_at ON t0_signal_quality(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_t0_quality_code_type_time ON t0_signal_quality(ts_code, signal_type, trigger_time, eval_horizon_sec)",
            "CREATE INDEX IF NOT EXISTS idx_t0_drift_checked_at ON t0_feature_drift(checked_at)",
            "CREATE INDEX IF NOT EXISTS idx_t0_drift_feature_checked ON t0_feature_drift(feature_name, checked_at)",
        ):
            try:
                conn.execute(ddl)
            except Exception:
                pass
        _TABLE_READY = True


def _get_conn() -> duckdb.DuckDBPyConnection:
    last_exc: Optional[Exception] = None
    for i in range(3):
        try:
            conn = duckdb.connect(DB_PATH)
            _ensure_table(conn)
            return conn
        except Exception as e:
            last_exc = e
            if i < 2 and _is_duckdb_lock_message(str(e)):
                # Transient Windows file lock: short backoff retry.
                time.sleep(0.08 * (i + 1))
                continue
            raise
    # defensive (loop always returns or raises)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("duckdb connection failed")


def _is_market_open() -> dict:
    now = datetime.datetime.now()
    weekday = now.weekday()
    t = now.hour * 100 + now.minute
    if weekday >= 5:
        return {"is_open": False, "status": "weekend", "next_open": "next weekday 09:15"}
    if 915 <= t < 925:
        return {"is_open": True, "status": "pre_auction", "desc": "pre auction"}
    if 930 <= t <= 1130:
        return {"is_open": True, "status": "morning_session", "desc": "morning session"}
    if 1130 < t < 1300:
        return {"is_open": False, "status": "lunch_break", "desc": "lunch break"}
    if 1300 <= t <= 1500:
        return {"is_open": True, "status": "afternoon_session", "desc": "afternoon session"}
    if t > 1500:
        return {"is_open": False, "status": "after_close", "desc": "after close"}
    return {"is_open": False, "status": "before_open", "desc": "before open"}


def _get_latest_factor_date() -> Optional[str]:
    conn = _get_conn()
    try:
        row = conn.execute("SELECT MAX(trade_date) FROM stk_factor_pro").fetchone()
        if not row or not row[0]:
            return None
        val = row[0]
        if hasattr(val, "strftime"):
            return val.strftime("%Y%m%d")
        return str(val).replace("-", "")
    except Exception as e:
        logger.warning(f"latest factor date query failed: {e}")
        return None
    finally:
        conn.close()


def _refresh_daily_factors_cache(pool_id: Optional[int] = None) -> int:
    provider = get_tick_provider()
    conn = _get_conn()
    try:
        pool_rows = conn.execute("SELECT ts_code, pool_id FROM monitor_pools").fetchall()
        stock_pool_map: dict[str, set] = {}
        for ts_code, pid in pool_rows:
            stock_pool_map.setdefault(ts_code, set()).add(int(pid))
        try:
            provider.bulk_update_stock_pools(stock_pool_map)
        except Exception:
            pass

        ts_codes = list(stock_pool_map.keys()) if pool_id is None else [
            code for code, pools in stock_pool_map.items() if int(pool_id) in pools
        ]
        if not ts_codes:
            return 0

        # Compatible column fallback: vol_20/vol20/vol and atr_14/atr14/atr.
        try:
            schema_rows = conn.execute("PRAGMA table_info('stk_factor_pro')").fetchall()
            columns = {str(r[1]).lower() for r in schema_rows}
        except Exception:
            columns = set()

        vol_col = None
        for c in ("vol_20", "vol20", "vol"):
            if c in columns:
                vol_col = c
                break
        atr_col = None
        for c in ("atr_14", "atr14", "atr"):
            if c in columns:
                atr_col = c
                break

        vol_expr = f"f.{vol_col}" if vol_col else "NULL"
        atr_expr = f"f.{atr_col}" if atr_col else "NULL"

        placeholders = ",".join(["?"] * len(ts_codes))
        rows = conn.execute(
            f"""
            WITH latest AS (
                SELECT ts_code, MAX(trade_date) AS max_date
                FROM stk_factor_pro
                WHERE ts_code IN ({placeholders})
                GROUP BY ts_code
            )
            SELECT f.ts_code,
                   f.boll_upper_qfq AS boll_upper,
                   f.boll_mid_qfq AS boll_mid,
                   f.boll_lower_qfq AS boll_lower,
                   f.rsi_qfq_6 AS rsi6,
                   f.ma_qfq_5 AS ma5,
                   f.ma_qfq_10 AS ma10,
                   f.volume_ratio AS volume_ratio,
                   {vol_expr} AS vol_20,
                   {atr_expr} AS atr_14
            FROM stk_factor_pro f
            JOIN latest l ON f.ts_code = l.ts_code AND f.trade_date = l.max_date
            """,
            ts_codes,
        ).fetchall()

        factors_map: dict[str, dict] = {}
        for r in rows:
            factors_map[r[0]] = {
                "boll_upper": r[1] or 0,
                "boll_mid": r[2] or 0,
                "boll_lower": r[3] or 0,
                "rsi6": r[4],
                "ma5": r[5],
                "ma10": r[6],
                "volume_ratio": r[7],
                "vol_20": r[8],
                "atr_14": r[9],
            }
        try:
            provider.bulk_update_daily_factors(factors_map)
        except Exception:
            pass
        return len(factors_map)
    except Exception as e:
        logger.warning(f"refresh_daily_factors_cache failed: {e}")
        return 0
    finally:
        conn.close()


def _safe_mean(values: list[float]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _round_or_none(v: Optional[float], ndigits: int = 4) -> Optional[float]:
    if v is None:
        return None
    try:
        return round(float(v), ndigits)
    except Exception:
        return None


def _calc_ret_bps(trigger_price: float, eval_price: float, direction: str) -> float:
    if trigger_price <= 0:
        return 0.0
    if direction == "sell":
        return (trigger_price - eval_price) / trigger_price * 10000
    return (eval_price - trigger_price) / trigger_price * 10000


def _calc_churn_stats(df_signal: pd.DataFrame, hours: int) -> dict:
    if df_signal is None or len(df_signal) == 0:
        return {
            "flip_count": 0,
            "sample_count": 0,
            "symbols": 0,
            "churn_ratio": None,
            "flips_per_hour": None,
        }

    d = df_signal.copy()
    if "triggered_at" in d.columns:
        d["triggered_at"] = pd.to_datetime(d["triggered_at"], errors="coerce")
    d = d.sort_values(["ts_code", "triggered_at"], na_position="last")

    flip_count = 0
    by_symbol = 0
    sample_count = len(d)
    for _, g in d.groupby("ts_code"):
        seq = g["signal_type"].astype(str).tolist()
        by_symbol += 1
        for i in range(1, len(seq)):
            if seq[i] != seq[i - 1]:
                flip_count += 1

    return {
        "flip_count": int(flip_count),
        "sample_count": int(sample_count),
        "symbols": int(by_symbol),
        "churn_ratio": _round_or_none((flip_count / sample_count) if sample_count > 0 else None, 4),
        "flips_per_hour": _round_or_none((flip_count / hours) if hours > 0 else None, 4),
    }


def _build_t0_quality_payload(df: pd.DataFrame, hours: int, churn: Optional[dict] = None) -> dict:
    if df is None or len(df) == 0:
        return {
            "hours": int(hours),
            "total_signals": 0,
            "total_signals_any": 0,
            "precision_1m": None,
            "precision_3m": None,
            "precision_5m": None,
            "avg_ret_bps": None,
            "avg_mfe_bps": None,
            "avg_mae_bps": None,
            "by_horizon": {},
            "by_type": {},
            "by_phase": {},
            "signal_churn": churn or {
                "flip_count": 0,
                "sample_count": 0,
                "symbols": 0,
                "churn_ratio": None,
                "flips_per_hour": None,
            },
        }

    d = df.copy()
    d["eval_horizon_sec"] = pd.to_numeric(d["eval_horizon_sec"], errors="coerce").fillna(0).astype(int)
    d["ret_bps"] = pd.to_numeric(d["ret_bps"], errors="coerce")
    d["mfe_bps"] = pd.to_numeric(d["mfe_bps"], errors="coerce")
    d["mae_bps"] = pd.to_numeric(d["mae_bps"], errors="coerce")
    d["direction_correct"] = d["direction_correct"].astype(bool)

    def agg(part: pd.DataFrame) -> dict:
        return {
            "count": int(len(part)),
            "precision": _round_or_none(float(part["direction_correct"].mean()) if len(part) > 0 else None),
            "avg_ret_bps": _round_or_none(part["ret_bps"].mean() if len(part) > 0 else None, 2),
            "avg_mfe_bps": _round_or_none(part["mfe_bps"].mean() if len(part) > 0 else None, 2),
            "avg_mae_bps": _round_or_none(part["mae_bps"].mean() if len(part) > 0 else None, 2),
        }

    by_horizon: dict[str, dict] = {}
    for h in (60, 180, 300):
        ph = d[d["eval_horizon_sec"] == h]
        if len(ph) > 0:
            by_horizon[str(h)] = agg(ph)

    d1 = d[d["eval_horizon_sec"] == 60]
    d3 = d[d["eval_horizon_sec"] == 180]
    d5 = d[d["eval_horizon_sec"] == 300]
    if len(d5) == 0:
        d5 = d

    by_type: dict[str, dict] = {}
    signal_types = sorted({str(x) for x in d["signal_type"].dropna().tolist()})
    for t in signal_types:
        p1 = d1[d1["signal_type"] == t]
        p3 = d3[d3["signal_type"] == t]
        p5 = d5[d5["signal_type"] == t]
        a5 = agg(p5)
        by_type[t] = {
            "count": int(len(p5)),
            "precision_1m": _round_or_none(float(p1["direction_correct"].mean()) if len(p1) > 0 else None),
            "precision_3m": _round_or_none(float(p3["direction_correct"].mean()) if len(p3) > 0 else None),
            "precision_5m": _round_or_none(float(p5["direction_correct"].mean()) if len(p5) > 0 else None),
            "avg_ret_bps": a5["avg_ret_bps"],
            "avg_mfe_bps": a5["avg_mfe_bps"],
            "avg_mae_bps": a5["avg_mae_bps"],
        }

    by_phase: dict[str, dict] = {}
    phases = sorted({str(x) for x in d5["market_phase"].dropna().tolist()})
    for p in phases:
        pp = d5[d5["market_phase"] == p]
        a = agg(pp)
        by_phase[p] = {
            "count": a["count"],
            "precision_5m": a["precision"],
            "avg_ret_bps": a["avg_ret_bps"],
        }

    a1 = agg(d1) if len(d1) > 0 else {"precision": None}
    a3 = agg(d3) if len(d3) > 0 else {"precision": None}
    a5 = agg(d5)
    total_any = int(d["signal_type"].nunique())
    total_5m = int(d5["signal_type"].nunique())
    return {
        "hours": int(hours),
        "total_signals": total_5m,
        "total_signals_any": total_any,
        "precision_1m": a1["precision"],
        "precision_3m": a3["precision"],
        "precision_5m": a5["precision"],
        "avg_ret_bps": a5["avg_ret_bps"],
        "avg_mfe_bps": a5["avg_mfe_bps"],
        "avg_mae_bps": a5["avg_mae_bps"],
        "by_horizon": by_horizon,
        "by_type": by_type,
        "by_phase": by_phase,
        "signal_churn": churn or {
            "flip_count": 0,
            "sample_count": 0,
            "symbols": 0,
            "churn_ratio": None,
            "flips_per_hour": None,
        },
    }


def _phase_of_ts(ts: float) -> str:
    dt = datetime.datetime.fromtimestamp(ts)
    hhmm = dt.hour * 100 + dt.minute
    if 930 <= hhmm < 1000:
        return "open"
    if 1000 <= hhmm <= 1130:
        return "morning"
    if 1300 <= hhmm < 1430:
        return "afternoon"
    if 1430 <= hhmm <= 1500:
        return "close"
    return "off"


def _load_tick_history_map(pool_id: int, limit: int = 240) -> dict:
    conn = _get_conn()
    try:
        members = conn.execute(
            "SELECT ts_code FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
            [pool_id],
        ).fetchall()
        out: dict[str, list] = {}
        for (ts_code,) in members:
            rows = conn.execute(
                """
                SELECT ts, price, volume, amount
                FROM tick_history
                WHERE ts_code = ?
                ORDER BY ts ASC
                LIMIT ?
                """,
                [ts_code, int(limit)],
            ).fetchall()
            if rows:
                out[ts_code] = [
                    {"time": int(r[0]), "price": r[1], "volume": r[2], "amount": r[3]}
                    for r in rows
                ]
        return out
    finally:
        conn.close()


def _start_tick_persistence() -> None:
    global _tick_persist_started
    if _tick_persist_started:
        return
    _tick_persist_started = True

    def _loop() -> None:
        logger.info("[Layer3] tick persistence thread started")
        while True:
            try:
                conn = _get_conn()
                try:
                    members = conn.execute("SELECT DISTINCT ts_code, name FROM monitor_pools").fetchall()
                    if not members:
                        time.sleep(5.0)
                        continue
                    provider = get_tick_provider()
                    rows = []
                    now_ts = int(time.time())
                    for ts_code, name in members:
                        try:
                            t = provider.get_tick(ts_code)
                            rows.append(
                                (
                                    ts_code,
                                    name or t.get("name", "") or "",
                                    float(t.get("price", 0) or 0),
                                    float(t.get("open", 0) or 0),
                                    float(t.get("high", 0) or 0),
                                    float(t.get("low", 0) or 0),
                                    float(t.get("pre_close", 0) or 0),
                                    int(t.get("volume", 0) or 0),
                                    float(t.get("amount", 0) or 0),
                                    float(t.get("pct_chg", 0) or 0),
                                    int(t.get("timestamp", now_ts) or now_ts),
                                )
                            )
                        except Exception:
                            pass
                    if rows:
                        conn.executemany(
                            """
                            INSERT INTO tick_history
                            (ts_code, name, price, open, high, low, pre_close, volume, amount, pct_chg, ts)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            rows,
                        )
                finally:
                    conn.close()
            except Exception as e:
                logger.warning(f"[Layer3] tick persistence error: {e}")
            time.sleep(5.0)

    threading.Thread(target=_loop, daemon=True, name="tick-persist").start()


def _start_txn_refresher(interval: float = 3.0) -> None:
    global _txn_refresher_started
    if _txn_refresher_started:
        return
    _txn_refresher_started = True

    def _loop() -> None:
        logger.info(f"[Layer2] txn refresher started interval={interval}s")
        while True:
            try:
                if not _is_market_open().get("is_open", False):
                    time.sleep(max(interval, 15.0))
                    continue
                provider = get_tick_provider()
                conn = _get_conn()
                try:
                    rows = conn.execute("SELECT DISTINCT ts_code FROM monitor_pools").fetchall()
                    ts_codes = [r[0] for r in rows]
                finally:
                    conn.close()
                if not ts_codes:
                    time.sleep(interval)
                    continue
                mapping = {}
                for code in ts_codes:
                    try:
                        txns = mootdx_client.get_transactions(code, int(_TXN_ANALYZE_COUNT))
                        if txns:
                            mapping[code] = txns
                    except Exception:
                        pass
                if mapping:
                    provider.bulk_update_recent_txns(mapping)
            except Exception as e:
                logger.warning(f"[Layer2] txn refresher error: {e}")
            time.sleep(interval)

    threading.Thread(target=_loop, daemon=True, name="txn-refresher").start()


def _start_t0_quality_monitor(interval: float = 1.0) -> None:
    global _quality_monitor_started
    if _quality_monitor_started:
        return
    _quality_monitor_started = True

    def _loop() -> None:
        logger.info(f"[T0 quality] monitor started interval={interval}s")
        pending: list[dict] = []
        horizons = [60, 180, 300]
        while True:
            try:
                while True:
                    try:
                        task = _quality_track_queue.get_nowait()
                        task["done_horizons"] = set()
                        pending.append(task)
                    except _queue_mod.Empty:
                        break

                now = time.time()
                provider = get_tick_provider()
                keep: list[dict] = []
                rows_to_insert = []

                for task in pending:
                    ts_code = task["ts_code"]
                    signal_type = task["signal_type"]
                    direction = task["direction"]
                    trigger_time = float(task["trigger_time"])
                    trigger_price = float(task["trigger_price"])
                    done = task["done_horizons"]

                    for h in horizons:
                        if h in done:
                            continue
                        if now < trigger_time + h:
                            continue
                        try:
                            t = provider.get_tick(ts_code)
                            eval_price = float(t.get("price", 0) or 0)
                            if eval_price > 0:
                                ret = _calc_ret_bps(trigger_price, eval_price, direction)
                                rows_to_insert.append(
                                    (
                                        ts_code,
                                        signal_type,
                                        direction,
                                        datetime.datetime.fromtimestamp(trigger_time),
                                        trigger_price,
                                        int(h),
                                        eval_price,
                                        round(ret, 2),
                                        round(max(ret, 0.0), 2),
                                        round(min(ret, 0.0), 2),
                                        bool(ret > 0),
                                        _phase_of_ts(trigger_time),
                                    )
                                )
                        except Exception:
                            pass
                        done.add(h)

                    if len(done) < len(horizons) and now < (trigger_time + max(horizons) + 60):
                        keep.append(task)

                pending = keep

                if rows_to_insert:
                    conn = _get_conn()
                    try:
                        conn.executemany(
                            """
                            INSERT INTO t0_signal_quality
                            (id, ts_code, signal_type, direction, trigger_time, trigger_price,
                             eval_horizon_sec, eval_price, ret_bps, mfe_bps, mae_bps,
                             direction_correct, market_phase)
                            VALUES (nextval('t0_signal_quality_id_seq'),
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            rows_to_insert,
                        )
                    finally:
                        conn.close()
            except Exception as e:
                logger.error(f"[T0 quality] monitor error: {e}")
            time.sleep(interval)

    threading.Thread(target=_loop, daemon=True, name="t0-quality-monitor").start()


def _start_drift_monitor(check_interval_min: float = 30.0) -> None:
    global _drift_monitor_started
    if _drift_monitor_started:
        return
    _drift_monitor_started = True

    def _severity(score: float) -> str:
        if score >= 3.0:
            return "high"
        if score >= 2.0:
            return "medium"
        return "low"

    def _loop() -> None:
        logger.info(f"[T0 drift] monitor started interval={check_interval_min}m")
        while True:
            try:
                time.sleep(max(1.0, check_interval_min) * 60.0)
                conn = _get_conn()
                try:
                    baseline = conn.execute(
                        """
                        SELECT ret_bps, mfe_bps, mae_bps, direction_correct
                        FROM t0_signal_quality
                        WHERE eval_horizon_sec = 300
                          AND created_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '7 days'
                                              AND CURRENT_TIMESTAMP - INTERVAL '3 days'
                        """
                    ).fetchall()
                    recent = conn.execute(
                        """
                        SELECT ret_bps, mfe_bps, mae_bps, direction_correct
                        FROM t0_signal_quality
                        WHERE eval_horizon_sec = 300
                          AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 day'
                        """
                    ).fetchall()
                    if len(baseline) < 20 or len(recent) < 10:
                        continue

                    b_prec = _safe_mean([1.0 if r[3] else 0.0 for r in baseline]) or 0.0
                    r_prec = _safe_mean([1.0 if r[3] else 0.0 for r in recent]) or 0.0
                    precision_drop = b_prec - r_prec

                    feature_specs = {
                        "ret_bps": ([r[0] for r in baseline], [r[0] for r in recent]),
                        "mfe_bps": ([r[1] for r in baseline], [r[1] for r in recent]),
                        "mae_bps": ([r[2] for r in baseline], [r[2] for r in recent]),
                    }

                    for feature_name, (b_vals, r_vals) in feature_specs.items():
                        b_mean = _safe_mean(b_vals) or 0.0
                        r_mean = _safe_mean(r_vals) or 0.0
                        b_var = _safe_mean([(x - b_mean) ** 2 for x in b_vals]) or 0.0
                        b_std = (b_var ** 0.5) if b_var > 0 else 0.0
                        z = abs(r_mean - b_mean) / (b_std if b_std > 1e-6 else 1.0)
                        psi = abs(r_mean - b_mean) / (abs(b_mean) + 1.0)
                        drifted = bool(z >= 2.0 or psi >= 0.2)
                        sev = _severity(max(z, psi * 10.0))
                        alerted = bool(drifted and precision_drop >= 0.08)
                        conn.execute(
                            """
                            INSERT INTO t0_feature_drift
                            (id, feature_name, psi, ks_stat, ks_p, severity, drifted, precision_drop, alerted)
                            VALUES (nextval('t0_feature_drift_id_seq'),
                                    ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                feature_name,
                                float(psi),
                                float(z),
                                0.01 if drifted else 0.5,
                                sev,
                                drifted,
                                float(precision_drop),
                                alerted,
                            ],
                        )
                finally:
                    conn.close()
            except Exception as e:
                logger.error(f"[T0 drift] monitor error: {e}")

    threading.Thread(target=_loop, daemon=True, name="t0-drift-monitor").start()


def _save_signal_history(conn: duckdb.DuckDBPyConnection, pool_id: int, evaluated: list) -> None:
    try:
        for row in evaluated:
            for s in row.get("signals", []):
                if not s.get("has_signal"):
                    continue
                sig_type = str(s.get("type") or "")
                dup = conn.execute(
                    """
                    SELECT id FROM signal_history
                    WHERE pool_id = ? AND ts_code = ? AND signal_type = ?
                      AND triggered_at > CURRENT_TIMESTAMP - INTERVAL '300 seconds'
                    LIMIT 1
                    """,
                    [pool_id, row.get("ts_code"), sig_type],
                ).fetchone()
                if dup:
                    continue
                conn.execute(
                    """
                    INSERT INTO signal_history
                    (id, pool_id, ts_code, name, signal_type, strength, message, price, pct_chg, triggered_at)
                    VALUES (nextval('signal_history_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        pool_id,
                        row.get("ts_code", ""),
                        row.get("name", ""),
                        sig_type,
                        float(s.get("strength", 0) or 0),
                        str(s.get("message", "") or ""),
                        float(row.get("price", 0) or 0),
                        float(row.get("pct_chg", 0) or 0),
                        datetime.datetime.fromtimestamp(int(s.get("triggered_at") or time.time())),
                    ],
                )
                if pool_id == 2:
                    try:
                        _quality_track_queue.put_nowait(
                            {
                                "ts_code": row.get("ts_code", ""),
                                "signal_type": sig_type,
                                "direction": str(s.get("direction", "buy") or "buy"),
                                "trigger_time": float(int(s.get("triggered_at") or time.time())),
                                "trigger_price": float(s.get("price", row.get("price", 0)) or 0),
                            }
                        )
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"save signal history failed: {e}")


def _build_member_data(conn: duckdb.DuckDBPyConnection, pool_id: int) -> list[dict]:
    members = conn.execute(
        "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
        [pool_id],
    ).fetchall()
    if not members:
        return []
    provider = get_tick_provider()
    out = []
    for ts_code, name in members:
        tick = provider.get_tick(ts_code)
        daily = {}
        try:
            row = conn.execute(
                """
                SELECT boll_upper_qfq AS boll_upper,
                       boll_mid_qfq AS boll_mid,
                       boll_lower_qfq AS boll_lower,
                       rsi_qfq_6 AS rsi6,
                       ma_qfq_5 AS ma5,
                       ma_qfq_10 AS ma10,
                       volume_ratio AS volume_ratio
                FROM stk_factor_pro
                WHERE ts_code = ?
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                [ts_code],
            ).fetchone()
            if row:
                daily = {
                    "boll_upper": row[0],
                    "boll_mid": row[1],
                    "boll_lower": row[2],
                    "rsi6": row[3],
                    "ma5": row[4],
                    "ma10": row[5],
                    "volume_ratio": row[6],
                }
        except Exception:
            pass

        m = {
            "ts_code": ts_code,
            "name": name,
            "price": tick.get("price", 0),
            "pct_chg": tick.get("pct_chg", 0),
            **{k: (v if v is not None else 0) for k, v in daily.items()},
        }

        if pool_id == 1:
            bids = tick.get("bids") or []
            asks = tick.get("asks") or []
            bid_vol = sum(v for _, v in bids[:5]) if bids else 0
            ask_vol = sum(v for _, v in asks[:5]) if asks else 0
            pre_close = float(tick.get("pre_close", 0) or 0)
            m["bid_ask_ratio"] = round(bid_vol / ask_vol, 3) if ask_vol > 0 else None
            m["up_limit"] = pre_close * 1.1 if pre_close > 0 else None
            m["down_limit"] = pre_close * 0.9 if pre_close > 0 else None
            m["prev_price"] = None

        if pool_id == 2:
            try:
                bars = mootdx_client.get_minute_bars(ts_code, 240)
                if bars:
                    cum_amt = sum(b.get("amount", 0) for b in bars)
                    cum_vol = sum(b.get("volume", 0) for b in bars)
                    m["vwap"] = cum_amt / cum_vol if cum_vol > 0 else None
                    m["intraday_prices"] = [b.get("close", 0) for b in bars]
            except Exception:
                pass
            try:
                txns = provider.get_cached_transactions(ts_code)
                if txns is None:
                    txns = mootdx_client.get_transactions(ts_code, int(_TXN_ANALYZE_COUNT))
                if txns:
                    sells = [t for t in txns if t.get("direction", 0) == 1 and t.get("volume", 0) < 50]
                    ratio = len(sells) / len(txns)
                    if ratio > 0.4:
                        m["gub5_trend"] = "up"
                    elif ratio > 0.2:
                        m["gub5_trend"] = "flat"
                    else:
                        m["gub5_trend"] = "down"
            except Exception:
                pass
        out.append(m)
    return out


def _evaluate_signals_internal(pool_id: int) -> list:
    conn = _get_conn()
    try:
        members_data = _build_member_data(conn, pool_id)
        if not members_data:
            return []
        evaluated = sig.evaluate_pool(pool_id, members_data)
        _save_signal_history(conn, pool_id, evaluated)
        return evaluated
    except Exception as e:
        logger.error(f"evaluate signals internal failed pool={pool_id}: {e}")
        return []
    finally:
        conn.close()


def _evaluate_signals_fast_internal(pool_id: int, members: list, provider) -> list:
    allowed_types = POOL_SIGNAL_TYPES.get(pool_id, set())
    data = []
    for ts_code, name in members:
        entry = provider.get_cached_signals(ts_code)
        if entry is None:
            tick = provider.get_tick(ts_code)
            data.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "price": tick.get("price", 0),
                    "pct_chg": tick.get("pct_chg", 0),
                    "signals": [],
                    "evaluated_at": 0,
                }
            )
            continue
        e = dict(entry)
        e["name"] = name
        e["signals"] = [s for s in e.get("signals", []) if s.get("type") in allowed_types]
        data.append(e)
    return data


@router.get("/tick_provider")
def tick_provider_info():
    p = get_tick_provider()
    return {"name": p.name, "display_name": p.display_name}


@router.get("/ui_config")
def realtime_ui_config():
    return {"data": REALTIME_UI_CONFIG}


@router.get("/pool/1/observe_stats")
def pool1_observe_stats():
    provider = get_tick_provider()
    stats = provider.get_pool1_observe_stats()
    if not stats:
        return {
            "pool_id": 1,
            "provider": provider.name,
            "supported": False,
            "message": "observe stats not supported",
        }
    return {
        "pool_id": 1,
        "provider": provider.name,
        "supported": True,
        "trade_date": stats.get("trade_date"),
        "updated_at": stats.get("updated_at"),
        "updated_at_iso": stats.get("updated_at_iso"),
        "data": stats,
    }


@router.post("/refresh_daily_cache")
def refresh_daily_cache(pool_id: Optional[int] = None):
    n = _refresh_daily_factors_cache(pool_id)
    return {"refreshed": n, "pool_id": pool_id}


@router.get("/pools")
def list_pools():
    conn = _get_conn()
    try:
        result = []
        for pid, info in POOLS.items():
            cnt = conn.execute("SELECT COUNT(*) FROM monitor_pools WHERE pool_id = ?", [pid]).fetchone()[0]
            result.append({**info, "member_count": cnt})
        return {"data": result}
    finally:
        conn.close()


class MemberItem(BaseModel):
    ts_code: str
    name: str
    industry: Optional[str] = None
    note: Optional[str] = None


@router.get("/pool/{pool_id}/members")
def get_members(pool_id: int):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT ts_code, name, industry, added_at, note, sort_order
            FROM monitor_pools
            WHERE pool_id = ?
            ORDER BY sort_order, added_at
            """,
            [pool_id],
        ).fetchall()
        data = []
        for r in rows:
            data.append(
                {
                    "ts_code": r[0],
                    "name": r[1],
                    "industry": r[2],
                    "added_at": r[3].isoformat() if hasattr(r[3], "isoformat") else str(r[3]) if r[3] else None,
                    "note": r[4],
                    "sort_order": r[5],
                }
            )
        return {"pool_id": pool_id, "count": len(data), "data": data}
    finally:
        conn.close()


@router.post("/pool/{pool_id}/add")
def add_member(pool_id: int, item: MemberItem):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        exists = conn.execute(
            "SELECT 1 FROM monitor_pools WHERE pool_id = ? AND ts_code = ? LIMIT 1",
            [pool_id, item.ts_code],
        ).fetchone()
        if exists:
            return {"ok": False, "msg": f"{item.ts_code} already exists in pool {pool_id}"}
        conn.execute(
            """
            INSERT INTO monitor_pools (pool_id, ts_code, name, industry, added_at, note, sort_order)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 0)
            """,
            [pool_id, item.ts_code, item.name, item.industry, item.note],
        )
        _refresh_daily_factors_cache(pool_id=None)
        return {"ok": True, "msg": "added"}
    finally:
        conn.close()


@router.delete("/pool/{pool_id}/remove/{ts_code}")
def remove_member(pool_id: int, ts_code: str):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM monitor_pools WHERE pool_id = ? AND ts_code = ?", [pool_id, ts_code])
        _refresh_daily_factors_cache(pool_id=None)
        return {"ok": True, "msg": "removed"}
    finally:
        conn.close()


@router.put("/pool/{pool_id}/note/{ts_code}")
def update_note(pool_id: int, ts_code: str, note: str = ""):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE monitor_pools SET note = ? WHERE pool_id = ? AND ts_code = ?",
            [note, pool_id, ts_code],
        )
        return {"ok": True, "msg": "updated"}
    finally:
        conn.close()


@router.get("/pool/{pool_id}/signals_fast")
def evaluate_signals_fast(pool_id: int):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        members = conn.execute(
            "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
            [pool_id],
        ).fetchall()
    finally:
        conn.close()
    if not members:
        return {"pool_id": pool_id, "count": 0, "data": []}
    provider = get_tick_provider()
    data = _evaluate_signals_fast_internal(pool_id, members, provider)
    return {"pool_id": pool_id, "count": len(data), "data": data}


@router.get("/pool/{pool_id}/signals")
def evaluate_signals(pool_id: int):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    evaluated = _evaluate_signals_internal(pool_id)
    return {"pool_id": pool_id, "count": len(evaluated), "data": evaluated}


class BatchRequest(BaseModel):
    ts_codes: list[str]


@router.post("/batch/tick")
def batch_tick(req: BatchRequest):
    provider = get_tick_provider()
    out = {}
    for ts_code in req.ts_codes:
        try:
            out[ts_code] = provider.get_tick(ts_code)
        except Exception as e:
            out[ts_code] = {"error": str(e)}
    return {"data": out}


@router.post("/batch/minute")
def batch_minute(req: BatchRequest, n: int = Query(240, ge=1, le=480)):
    out = {}
    for ts_code in req.ts_codes:
        try:
            out[ts_code] = mootdx_client.get_minute_bars(ts_code, n)
        except Exception:
            out[ts_code] = []
    return {"data": out}


@router.post("/batch/transactions")
def batch_transactions(req: BatchRequest, n: int = Query(15, ge=1, le=500)):
    provider = get_tick_provider()
    out = {}
    for ts_code in req.ts_codes:
        try:
            txns = provider.get_cached_transactions(ts_code)
            if txns is None:
                txns = mootdx_client.get_transactions(ts_code, n)
            out[ts_code] = txns or []
        except Exception:
            out[ts_code] = []
    return {"data": out}


@router.get("/tick/{ts_code}")
def get_tick(ts_code: str):
    return get_tick_provider().get_tick(ts_code)


@router.get("/minute/{ts_code}")
def get_minute(ts_code: str, n: int = Query(240, ge=1, le=480)):
    try:
        return {"ts_code": ts_code, "data": mootdx_client.get_minute_bars(ts_code, n)}
    except Exception:
        return {"ts_code": ts_code, "data": []}


@router.get("/transactions/{ts_code}")
def get_transactions(ts_code: str, n: int = Query(15, ge=1, le=500)):
    provider = get_tick_provider()
    txns = provider.get_cached_transactions(ts_code)
    if txns is None:
        try:
            txns = mootdx_client.get_transactions(ts_code, n)
        except Exception:
            txns = []
    return {"ts_code": ts_code, "data": txns or []}


@router.get("/market_status")
def market_status():
    return _is_market_open()


@router.get("/indices")
def indices():
    provider = get_tick_provider()
    data = []
    for ts_code, name in INDICES:
        try:
            # Indices use dedicated index API first; fallback to active provider.
            try:
                t = mootdx_client.get_index_tick(ts_code)
            except Exception:
                t = provider.get_tick(ts_code)
            if float(t.get("price", 0) or 0) <= 0:
                t2 = provider.get_tick(ts_code)
                if float(t2.get("price", 0) or 0) > 0:
                    t = t2
            price = float(t.get("price", 0) or 0)
            pre_close = float(t.get("pre_close", 0) or 0)
            raw_pct_chg = float(t.get("pct_chg", 0) or 0)
            pct_chg = ((price - pre_close) / pre_close * 100) if (price > 0 and pre_close > 0) else raw_pct_chg
            data.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "price": price,
                    "pre_close": pre_close,
                    "pct_chg": pct_chg,
                    "amount": t.get("amount", 0),
                    "is_mock": t.get("is_mock", False),
                }
            )
        except Exception:
            data.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "price": 0,
                    "pre_close": 0,
                    "pct_chg": 0,
                    "amount": 0,
                    "is_mock": True,
                }
            )
    return {"data": data}


@router.get("/pool/{pool_id}/signal_history")
def get_signal_history(pool_id: int, hours: int = Query(24, ge=1, le=168)):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT id, pool_id, ts_code, name, signal_type, strength, message, price, pct_chg, triggered_at
            FROM signal_history
            WHERE pool_id = ?
              AND triggered_at >= CURRENT_TIMESTAMP - INTERVAL ? HOUR
            ORDER BY triggered_at DESC
            """,
            [pool_id, hours],
        ).fetchall()
        data = []
        for r in rows:
            data.append(
                {
                    "id": r[0],
                    "pool_id": r[1],
                    "ts_code": r[2],
                    "name": r[3],
                    "signal_type": r[4],
                    "strength": r[5],
                    "message": r[6],
                    "price": r[7],
                    "pct_chg": r[8],
                    "triggered_at": r[9].isoformat() if hasattr(r[9], "isoformat") else str(r[9]),
                }
            )
        return {"pool_id": pool_id, "hours": hours, "count": len(data), "data": data}
    finally:
        conn.close()


@router.get("/pool/2/quality_summary")
def get_t0_quality_summary(hours: int = Query(24, ge=1, le=168)):
    conn = _get_conn()
    try:
        h = int(hours)
        rows = conn.execute(
            f"""
            SELECT signal_type, market_phase, eval_horizon_sec, ret_bps, mfe_bps, mae_bps, direction_correct
            FROM t0_signal_quality
            WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '{h} hours'
            """
        ).fetchall()
        df_quality = pd.DataFrame(
            rows,
            columns=[
                "signal_type",
                "market_phase",
                "eval_horizon_sec",
                "ret_bps",
                "mfe_bps",
                "mae_bps",
                "direction_correct",
            ],
        )

        sig_rows = conn.execute(
            f"""
            SELECT ts_code, signal_type, triggered_at
            FROM signal_history
            WHERE pool_id = 2
              AND triggered_at >= CURRENT_TIMESTAMP - INTERVAL '{h} hours'
            ORDER BY ts_code, triggered_at
            """
        ).fetchall()
        df_signal = pd.DataFrame(sig_rows, columns=["ts_code", "signal_type", "triggered_at"])
        churn = _calc_churn_stats(df_signal, hours=h)
        return _build_t0_quality_payload(df_quality, hours=h, churn=churn)
    finally:
        conn.close()


@router.get("/pool/2/drift_status")
def get_t0_drift_status(days: int = Query(3, ge=1, le=30)):
    conn = _get_conn()
    try:
        d = int(days)
        rows = conn.execute(
            f"""
            SELECT checked_at, feature_name, psi, ks_stat, ks_p, severity, drifted, precision_drop, alerted
            FROM t0_feature_drift
            WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '{d} days'
            ORDER BY checked_at DESC
            """
        ).fetchall()
        if not rows:
            return {
                "days": d,
                "total_checks": 0,
                "has_active_alert": False,
                "latest_checked_at": None,
                "latest": {},
                "alerts": 0,
            }
        latest_at = rows[0][0]
        latest_rows = [r for r in rows if r[0] == latest_at]
        latest = {}
        for r in latest_rows:
            latest[str(r[1])] = {
                "psi": r[2],
                "ks_stat": r[3],
                "severity": r[5],
                "drifted": bool(r[6]),
                "precision_drop": r[7],
                "alerted": bool(r[8]),
            }
        return {
            "days": d,
            "total_checks": len(rows),
            "has_active_alert": any(bool(r[8]) for r in latest_rows),
            "latest_checked_at": latest_at.isoformat() if hasattr(latest_at, "isoformat") else str(latest_at),
            "latest": latest,
            "alerts": sum(1 for r in rows if bool(r[8])),
        }
    finally:
        conn.close()


def _nan_safe_json(obj):
    return json.dumps(obj, ensure_ascii=False, allow_nan=False)


def _is_db_lock_error(exc: Exception) -> bool:
    """Best-effort DuckDB file lock detection on Windows."""
    return _is_duckdb_lock_message(str(exc))


@router.websocket("/ws")
async def realtime_ws(ws: WebSocket):
    await ws.accept()
    subscribed_pool: Optional[int] = None
    push_task: Optional[asyncio.Task] = None

    async def send_msg(data: dict):
        try:
            await ws.send_text(_nan_safe_json(data))
        except Exception:
            pass

    async def push_loop(pool_id: int):
        last_market = None
        last_tx_push_at = 0.0
        members_cache: list[tuple[str, str]] = []
        last_members_load_at = 0.0
        last_lock_warn_at = 0.0
        while True:
            try:
                market = _is_market_open()
                if market != last_market:
                    last_market = market
                    await send_msg({"type": "market_status", **market})
                is_open = bool(market.get("is_open", False))

                now = time.time()
                members = members_cache
                should_refresh_members = (not members_cache) or (now - last_members_load_at >= 10.0)
                if should_refresh_members:
                    try:
                        conn = _get_conn()
                        try:
                            members = conn.execute(
                                "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
                                [pool_id],
                            ).fetchall()
                        finally:
                            conn.close()
                        members_cache = list(members or [])
                        last_members_load_at = now
                    except Exception as e:
                        # DB lock fallback: keep streaming with previous in-memory member snapshot.
                        if _is_db_lock_error(e) and members_cache:
                            members = members_cache
                            if now - last_lock_warn_at >= 30.0:
                                logger.warning(f"ws member refresh fallback to cache due DB lock: {e}")
                                last_lock_warn_at = now
                        else:
                            raise
                if not members:
                    await asyncio.sleep(5 if is_open else 30)
                    continue

                ts_codes = [m[0] for m in members]
                provider = get_tick_provider()
                provider.subscribe_symbols(ts_codes)

                tick_data = {}
                for code in ts_codes:
                    try:
                        tick_data[code] = provider.get_tick(code)
                    except Exception:
                        pass
                await send_msg({"type": "tick", "data": tick_data})

                try:
                    idx = indices().get("data", [])
                    await send_msg({"type": "indices", "data": idx})
                except Exception:
                    pass

                if is_open and (now - last_tx_push_at >= 2.0):
                    txn_data = {}
                    for code in ts_codes:
                        try:
                            txns = provider.get_cached_transactions(code)
                            if txns is None:
                                txns = mootdx_client.get_transactions(code, 15)
                            txn_data[code] = txns or []
                        except Exception:
                            pass
                    await send_msg({"type": "transactions", "data": txn_data})
                    last_tx_push_at = now

                try:
                    signal_result = _evaluate_signals_fast_internal(pool_id, members, provider)
                    await send_msg({"type": "signals", "pool_id": pool_id, "data": signal_result})
                except Exception as e:
                    logger.debug(f"ws signals failed: {e}")

                if is_open:
                    await asyncio.sleep(0.8)
                else:
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ws push loop error: {e}")
                await asyncio.sleep(3)

    try:
        await send_msg({"type": "connected"})
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                await send_msg({"type": "error", "message": "invalid json"})
                continue

            action = msg.get("action")
            if action == "ping":
                await send_msg({"type": "pong"})
                continue
            if action == "unsubscribe":
                if push_task and not push_task.done():
                    push_task.cancel()
                    push_task = None
                subscribed_pool = None
                await send_msg({"type": "unsubscribed"})
                continue
            if action == "subscribe":
                pool_id = int(msg.get("pool_id", 0))
                if pool_id not in POOLS:
                    await send_msg({"type": "error", "message": f"pool_id {pool_id} not found"})
                    continue
                if push_task and not push_task.done():
                    push_task.cancel()
                subscribed_pool = pool_id
                push_task = asyncio.create_task(push_loop(pool_id))
                try:
                    history_map = _load_tick_history_map(pool_id, limit=240)
                    await send_msg({"type": "tick_history", "data": history_map})
                except Exception:
                    pass
                await send_msg({"type": "subscribed", "pool_id": pool_id})
                continue
    except WebSocketDisconnect:
        pass
    finally:
        if push_task and not push_task.done():
            push_task.cancel()
        _ = subscribed_pool




