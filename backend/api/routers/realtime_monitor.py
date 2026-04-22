"""Realtime monitor router (recovered minimal stable version)."""
from __future__ import annotations

import asyncio
import datetime
import json
import os
import queue as _queue_mod
import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Optional

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from loguru import logger
from pydantic import BaseModel

from config import DB_PATH, REALTIME_DB_PATH, REALTIME_UI_CONFIG
from backend.data.duckdb_manager import open_duckdb_conn
try:
    from config import REALTIME_DB_WRITER_CONFIG
except Exception:
    REALTIME_DB_WRITER_CONFIG = {}
try:
    from config import REALTIME_RUNTIME_STATE_CONFIG
except Exception:
    REALTIME_RUNTIME_STATE_CONFIG = {}
try:
    from config import POOL1_SIGNAL_CONFIG
except Exception:
    POOL1_SIGNAL_CONFIG = {}
try:
    from config import T0_SIGNAL_CONFIG
except Exception:
    T0_SIGNAL_CONFIG = {}
try:
    from config import DYNAMIC_THRESHOLD_CLOSED_LOOP_CONFIG
except Exception:
    DYNAMIC_THRESHOLD_CLOSED_LOOP_CONFIG = {}
from backend.realtime import signals as sig
from backend.realtime.mootdx_client import client as mootdx_client
from backend.realtime.tick_provider import get_tick_provider
try:
    from backend.realtime.threshold_engine import infer_instrument_profile, calc_theoretical_limits
except Exception:
    infer_instrument_profile = None
    calc_theoretical_limits = None

try:
    import redis as _redis_mod
except Exception:
    _redis_mod = None

router = APIRouter()

# Keep txn sample size aligned with Layer2 analysis window.
_TXN_ANALYZE_COUNT = 50


POOLS = {
    1: {
        "pool_id": 1,
        "name": "Pool 1 Timing",
        "strategy": "Position Building",
        "desc": "left side + right side breakout",
        "signal_types": ["left_side_buy", "right_side_breakout", "timing_clear"],
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
    1: {"left_side_buy", "right_side_breakout", "timing_clear"},
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
_INDEX_LAST_GOOD: dict[str, dict] = {}
_indices_cache_lock = threading.Lock()
_indices_cache_data: list[dict] = []
_indices_cache_at: float = 0.0

_TABLE_READY = False
_TABLE_LOCK = threading.Lock()

_tick_persist_started = False
_txn_refresher_started = False
_quality_monitor_started = False
_drift_monitor_started = False
_threshold_closed_loop_started = False
_quality_track_queue: _queue_mod.Queue = _queue_mod.Queue(maxsize=10000)
_db_writer_started = False
_db_write_queue: _queue_mod.Queue = _queue_mod.Queue(maxsize=200000)
_db_write_drop_warn_at = 0.0
_DB_WRITER_FLUSH_SEC = float(REALTIME_DB_WRITER_CONFIG.get("flush_interval_sec", 0.5) or 0.5)
_DB_WRITER_MAX_BATCH = int(REALTIME_DB_WRITER_CONFIG.get("max_batch", 1000) or 1000)
_DB_WRITER_REDIS_CFG = REALTIME_DB_WRITER_CONFIG.get("redis", {}) if isinstance(REALTIME_DB_WRITER_CONFIG, dict) else {}
_DB_WRITER_REDIS_ENABLED = bool(_DB_WRITER_REDIS_CFG.get("enabled", False))
_DB_WRITER_REDIS_URL = str(_DB_WRITER_REDIS_CFG.get("url", os.getenv("REALTIME_DB_WRITER_REDIS_URL", "redis://127.0.0.1:6379/0")))
_DB_WRITER_REDIS_KEY = str(_DB_WRITER_REDIS_CFG.get("list_key", os.getenv("REALTIME_DB_WRITER_REDIS_LIST_KEY", "quant:realtime:db_write")))
_db_writer_redis = None
_db_writer_redis_warn_at = 0.0
_db_writer_redis_next_retry_at = 0.0
_RUNTIME_STATE_CFG = REALTIME_RUNTIME_STATE_CONFIG.get("redis", {}) if isinstance(REALTIME_RUNTIME_STATE_CONFIG, dict) else {}
_RUNTIME_STATE_REDIS_ENABLED = bool(_RUNTIME_STATE_CFG.get("enabled", False))
_RUNTIME_STATE_REDIS_URL = str(_RUNTIME_STATE_CFG.get("url", os.getenv("RUNTIME_STATE_REDIS_URL", _DB_WRITER_REDIS_URL)))
_RUNTIME_STATE_REDIS_KEY_PREFIX = str(_RUNTIME_STATE_CFG.get("key_prefix", os.getenv("RUNTIME_STATE_REDIS_KEY_PREFIX", "quant:runtime")))
_RUNTIME_STATE_REDIS_TTL_DAYS = int(_RUNTIME_STATE_CFG.get("ttl_days", os.getenv("RUNTIME_STATE_REDIS_TTL_DAYS", "7")) or 7)
_RUNTIME_T0_QUALITY_WINDOW_SIZE = int(_RUNTIME_STATE_CFG.get("t0_quality_window_size", os.getenv("RUNTIME_T0_QUALITY_WINDOW_SIZE", "500")) or 500)
_CLOSED_LOOP_CFG = DYNAMIC_THRESHOLD_CLOSED_LOOP_CONFIG if isinstance(DYNAMIC_THRESHOLD_CLOSED_LOOP_CONFIG, dict) else {}
_CLOSED_LOOP_ENABLED = bool(_CLOSED_LOOP_CFG.get("enabled", False))
_CLOSED_LOOP_INTERVAL_MIN = float(_CLOSED_LOOP_CFG.get("interval_min", 15) or 15)
_CLOSED_LOOP_WINDOW_HOURS = int(_CLOSED_LOOP_CFG.get("window_hours", 24) or 24)
_CLOSED_LOOP_MIN_SAMPLES = int(_CLOSED_LOOP_CFG.get("min_samples", 20) or 20)
_CLOSED_LOOP_SCORE_OBS_T0 = float(_CLOSED_LOOP_CFG.get("score_observe_t0", 55) or 55)
_CLOSED_LOOP_SCORE_OBS_POOL1 = float(_CLOSED_LOOP_CFG.get("score_observe_pool1", 55) or 55)
_CLOSED_LOOP_TARGETS = _CLOSED_LOOP_CFG.get("targets", {}) if isinstance(_CLOSED_LOOP_CFG.get("targets"), dict) else {}
_CLOSED_LOOP_STEPS = _CLOSED_LOOP_CFG.get("steps", {}) if isinstance(_CLOSED_LOOP_CFG.get("steps"), dict) else {}
_runtime_state_redis = None
_runtime_state_redis_warn_at = 0.0
_runtime_state_redis_next_retry_at = 0.0
_REALTIME_DB = REALTIME_DB_PATH
_DATA_DB = DB_PATH
_DB_READ_POOL_SIZE = 8
_DB_DATA_READ_POOL_SIZE = 4
_db_read_pool: _queue_mod.Queue = _queue_mod.Queue(maxsize=_DB_READ_POOL_SIZE)
_db_data_read_pool: _queue_mod.Queue = _queue_mod.Queue(maxsize=_DB_DATA_READ_POOL_SIZE)
_metrics_lock = threading.Lock()
_runtime_metrics: dict[str, float] = {
    "db_lock_open_rw_total": 0,
    "db_lock_open_ro_total": 0,
    "db_lock_writer_total": 0,
    "db_read_pool_hit_total": 0,
    "db_read_pool_miss_total": 0,
    "db_data_read_pool_hit_total": 0,
    "db_data_read_pool_miss_total": 0,
    "db_writer_queue_drop_total": 0,
    "db_writer_batch_tx_total": 0,
    "db_writer_batch_tx_fail_total": 0,
    "db_redis_enqueue_total": 0,
    "db_redis_dequeue_total": 0,
    "db_redis_enqueue_fail_total": 0,
    "db_redis_dequeue_fail_total": 0,
    "runtime_state_redis_write_total": 0,
    "runtime_state_redis_write_fail_total": 0,
    "t0_quality_redis_write_total": 0,
    "t0_quality_redis_write_fail_total": 0,
    "threshold_calib_write_total": 0,
    "threshold_calib_write_fail_total": 0,
    "ws_member_cache_fallback_total": 0,
    "ws_signals_fail_total": 0,
    "ws_push_loop_error_total": 0,
    "ws_tick_full_push_total": 0,
    "ws_tick_delta_push_total": 0,
    "ws_tick_delta_keys_total": 0,
    "ws_signal_full_push_total": 0,
    "ws_signal_delta_push_total": 0,
    "ws_signal_delta_rows_total": 0,
    "ws_tx_full_push_total": 0,
    "ws_tx_delta_push_total": 0,
    "ws_tx_delta_codes_total": 0,
    "ws_indices_push_total": 0,
    "ws_indices_skip_total": 0,
    "ws_cycle_ms": 0,
    "ws_tick_fetch_ms": 0,
    "ws_indices_fetch_ms": 0,
    "db_write_skip_non_trading_total": 0,
}

_HEALTH_ALERT_ENABLED = os.getenv("RUNTIME_HEALTH_ALERT_ENABLED", "1").lower() in ("1", "true", "yes", "on")
_HEALTH_ALERT_CONSECUTIVE = int(os.getenv("RUNTIME_HEALTH_ALERT_CONSECUTIVE", "3"))
_HEALTH_ALERT_COOLDOWN_SEC = int(os.getenv("RUNTIME_HEALTH_ALERT_COOLDOWN_SEC", "300"))
_HEALTH_ALERT_MAX_EVENTS = int(os.getenv("RUNTIME_HEALTH_ALERT_MAX_EVENTS", "200"))
_FAST_SLOW_HEALTH_ENABLED = os.getenv("RUNTIME_HEALTH_FAST_SLOW_ENABLED", "1").lower() in ("1", "true", "yes", "on")
_FAST_SLOW_HEALTH_POOL_ID = int(os.getenv("RUNTIME_HEALTH_FAST_SLOW_POOL_ID", "1") or 1)
_FAST_SLOW_HEALTH_WARN_RATIO = float(os.getenv("RUNTIME_HEALTH_FAST_SLOW_WARN_RATIO", "0.05") or 0.05)
_FAST_SLOW_HEALTH_CRIT_RATIO = float(os.getenv("RUNTIME_HEALTH_FAST_SLOW_CRIT_RATIO", "0.10") or 0.10)
_FAST_SLOW_HEALTH_CACHE_TTL_SEC = float(os.getenv("RUNTIME_HEALTH_FAST_SLOW_CACHE_TTL_SEC", "20") or 20)
_FAST_SLOW_TREND_ENABLED = os.getenv("RUNTIME_FAST_SLOW_TREND_ENABLED", "1").lower() in ("1", "true", "yes", "on")
_FAST_SLOW_TREND_MIN_INTERVAL_SEC = float(os.getenv("RUNTIME_FAST_SLOW_TREND_MIN_INTERVAL_SEC", "30") or 30)
_FAST_SLOW_TREND_RETENTION_HOURS = int(os.getenv("RUNTIME_FAST_SLOW_TREND_RETENTION_HOURS", "72") or 72)
_FAST_SLOW_TREND_MAX_POINTS = int(os.getenv("RUNTIME_FAST_SLOW_TREND_MAX_POINTS", "2000") or 2000)
_WS_OPEN_INTERVAL_SEC = float(os.getenv("REALTIME_WS_OPEN_INTERVAL_SEC", "0.5") or 0.5)
_WS_MEMBER_REFRESH_SEC = float(os.getenv("REALTIME_WS_MEMBER_REFRESH_SEC", "10") or 10)
_WS_INDICES_INTERVAL_SEC = float(os.getenv("REALTIME_WS_INDICES_INTERVAL_SEC", "3") or 3)
_WS_TX_INTERVAL_SEC = float(os.getenv("REALTIME_WS_TX_INTERVAL_SEC", "2") or 2)
_WS_GM_MISS_TICK_FALLBACK_MAX = int(os.getenv("REALTIME_WS_GM_MISS_TICK_FALLBACK_MAX", "2") or 2)
_WS_TICK_FULL_SNAPSHOT_SEC = float(os.getenv("REALTIME_WS_TICK_FULL_SNAPSHOT_SEC", "5") or 5)
_WS_SIGNAL_FULL_SNAPSHOT_SEC = float(os.getenv("REALTIME_WS_SIGNAL_FULL_SNAPSHOT_SEC", "5") or 5)
_WS_TX_FULL_SNAPSHOT_SEC = float(os.getenv("REALTIME_WS_TX_FULL_SNAPSHOT_SEC", "8") or 8)
_WS_PUSH_INDICES_ENABLED = os.getenv("REALTIME_WS_PUSH_INDICES_ENABLED", "0").lower() in ("1", "true", "yes", "on")
_INDICES_CACHE_TTL_SEC = float(os.getenv("REALTIME_INDICES_CACHE_TTL_SEC", "2") or 2)
_health_alert_lock = threading.Lock()
_health_alert_state: dict = {
    "last_level": "green",
    "non_green_streak": 0,
    "yellow_streak": 0,
    "red_streak": 0,
    "last_alert_at": 0.0,
    "events": [],
}
_fast_slow_health_lock = threading.Lock()
_fast_slow_health_cache: dict = {
    "at": 0.0,
    "pool_id": 0,
    "payload": None,
}
_fast_slow_trend_lock = threading.Lock()
_fast_slow_trend_mem: dict[int, deque] = {}
_fast_slow_trend_last_ts: dict[int, float] = {}
_threshold_calibration_last: dict = {}
_P1_DEDUP_CFG = POOL1_SIGNAL_CONFIG.get("dedup_sec", {}) if isinstance(POOL1_SIGNAL_CONFIG, dict) else {}
_T0_DEDUP_CFG = T0_SIGNAL_CONFIG.get("dedup_sec", {}) if isinstance(T0_SIGNAL_CONFIG, dict) else {}
_P1_VOL_PACE_CFG = POOL1_SIGNAL_CONFIG.get("volume_pace", {}) if isinstance(POOL1_SIGNAL_CONFIG, dict) else {}
_P1_VOL_PACE_MIN_PROGRESS = float(_P1_VOL_PACE_CFG.get("min_progress", 0.15) or 0.15)
_P1_VOL_PACE_SHRINK_TH = float(_P1_VOL_PACE_CFG.get("shrink_th", 0.70) or 0.70)
_P1_VOL_PACE_EXPAND_TH = float(_P1_VOL_PACE_CFG.get("expand_th", 1.30) or 1.30)
_P1_VOL_PACE_SURGE_TH = float(_P1_VOL_PACE_CFG.get("surge_th", 2.00) or 2.00)
_T0_VOL_PACE_CFG = T0_SIGNAL_CONFIG.get("volume_pace", {}) if isinstance(T0_SIGNAL_CONFIG, dict) else {}
_T0_VOL_PACE_MIN_PROGRESS = float(_T0_VOL_PACE_CFG.get("min_progress", 0.12) or 0.12)
_T0_VOL_PACE_SHRINK_TH = float(_T0_VOL_PACE_CFG.get("shrink_th", 0.70) or 0.70)
_T0_VOL_PACE_EXPAND_TH = float(_T0_VOL_PACE_CFG.get("expand_th", 1.30) or 1.30)
_T0_VOL_PACE_SURGE_TH = float(_T0_VOL_PACE_CFG.get("surge_th", 2.00) or 2.00)


def _is_duckdb_lock_message(msg: str) -> bool:
    m = (msg or "").lower()
    return ("cannot open file" in m) and ("used by another process" in m or "进程无法访问" in m)


def _metric_inc(name: str, value: float = 1.0) -> None:
    with _metrics_lock:
        _runtime_metrics[name] = float(_runtime_metrics.get(name, 0.0) + value)


def _metric_set(name: str, value: float) -> None:
    with _metrics_lock:
        _runtime_metrics[name] = float(value)


def _metrics_snapshot() -> dict:
    with _metrics_lock:
        return dict(_runtime_metrics)


def _normalize_dt(v):
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    return v


def _parse_dt(v):
    if isinstance(v, datetime.datetime):
        return v
    if isinstance(v, str) and v:
        try:
            return datetime.datetime.fromisoformat(v)
        except Exception:
            return None
    return None


def _to_epoch_seconds(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return 0.0
    dt = _parse_dt(v)
    if isinstance(dt, datetime.datetime):
        try:
            return float(dt.timestamp())
        except Exception:
            return 0.0
    if isinstance(v, str) and v:
        try:
            return float(v)
        except Exception:
            return 0.0
    return 0.0


def _to_iso_datetime_text(v) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    dt = _parse_dt(v)
    if isinstance(dt, datetime.datetime):
        return dt.isoformat()
    if isinstance(v, (int, float)):
        try:
            return datetime.datetime.fromtimestamp(float(v)).isoformat()
        except Exception:
            return str(v)
    return str(v)


def _compact_history_snapshot(signal_obj: Optional[dict]) -> Optional[str]:
    if not isinstance(signal_obj, dict):
        return None
    details = signal_obj.get("details") if isinstance(signal_obj.get("details"), dict) else {}
    threshold = details.get("threshold") if isinstance(details.get("threshold"), dict) else {}
    trend_guard = details.get("trend_guard") if isinstance(details.get("trend_guard"), dict) else {}
    compact = {
        "type": str(signal_obj.get("type") or ""),
        "direction": str(signal_obj.get("direction") or ""),
        "strength": float(signal_obj.get("strength", 0) or 0),
        "message": str(signal_obj.get("message", "") or ""),
        "veto": str(details.get("veto") or ""),
        "observe_only": bool(details.get("observe_only", False)),
        "score_status": str(details.get("score_status") or ""),
        "trigger_items": list(details.get("trigger_items") or []) if isinstance(details.get("trigger_items"), list) else [],
        "confirm_items": list(details.get("confirm_items") or []) if isinstance(details.get("confirm_items"), list) else [],
        "bearish_confirms": list(details.get("bearish_confirms") or []) if isinstance(details.get("bearish_confirms"), list) else [],
        "volume_pace_ratio": details.get("volume_pace_ratio"),
        "volume_pace_state": details.get("volume_pace_state"),
        "pct_chg": details.get("pct_chg"),
        "bid_ask_ratio": details.get("bid_ask_ratio"),
        "robust_zscore": details.get("robust_zscore"),
        "v_power_divergence": bool(details.get("v_power_divergence", False)),
        "ask_wall_building": bool(details.get("ask_wall_building", False)),
        "ask_wall_absorb": bool(details.get("ask_wall_absorb", False)),
        "ask_wall_absorb_ratio": details.get("ask_wall_absorb_ratio"),
        "bid_wall_break": bool(details.get("bid_wall_break", False)),
        "bid_wall_break_ratio": details.get("bid_wall_break_ratio"),
        "real_buy_fading": bool(details.get("real_buy_fading", False)),
        "super_order_bias": details.get("super_order_bias"),
        "super_net_flow_bps": details.get("super_net_flow_bps"),
        "trend_guard_override": bool(details.get("trend_guard_override", False)),
        "threshold": {
            "threshold_version": threshold.get("threshold_version"),
            "market_phase": threshold.get("market_phase"),
            "regime": threshold.get("regime"),
            "board_segment": threshold.get("board_segment"),
            "security_type": threshold.get("security_type"),
            "listing_stage": threshold.get("listing_stage"),
            "price_limit_pct": threshold.get("price_limit_pct"),
            "risk_warning": threshold.get("risk_warning"),
            "volume_drought": threshold.get("volume_drought"),
            "board_seal_env": threshold.get("board_seal_env"),
            "seal_side": threshold.get("seal_side"),
            "profile_override": threshold.get("profile_override"),
            "observer_only": bool(threshold.get("observer_only", False)),
            "blocked": bool(threshold.get("blocked", False)),
        },
        "trend_guard": {
            "active": bool(trend_guard.get("active", False)),
            "trend_up": bool(trend_guard.get("trend_up", False)),
            "pct_chg": trend_guard.get("pct_chg"),
            "required_bearish_confirms": trend_guard.get("required_bearish_confirms"),
            "surge_absorb_guard": bool(trend_guard.get("surge_absorb_guard", False)),
            "guard_reasons": list(trend_guard.get("guard_reasons") or []) if isinstance(trend_guard.get("guard_reasons"), list) else [],
            "structure_reasons": list(trend_guard.get("structure_reasons") or []) if isinstance(trend_guard.get("structure_reasons"), list) else [],
            "negative_flags": list(trend_guard.get("negative_flags") or []) if isinstance(trend_guard.get("negative_flags"), list) else [],
            "bearish_confirms": list(trend_guard.get("bearish_confirms") or []) if isinstance(trend_guard.get("bearish_confirms"), list) else [],
        },
    }
    try:
        return json.dumps(compact, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except Exception:
        return None


def _serialize_write_payload(kind: str, payload):
    if kind == "quality_rows":
        out = []
        for row in payload or []:
            r = list(row)
            if len(r) > 3:
                r[3] = _normalize_dt(r[3])
            out.append(r)
        return out
    if kind == "signal_rows":
        out = []
        for row in payload or []:
            d = dict(row)
            d["triggered_at"] = _normalize_dt(d.get("triggered_at"))
            out.append(d)
        return out
    return payload


def _deserialize_write_payload(kind: str, payload):
    if kind == "quality_rows":
        out = []
        for row in payload or []:
            r = list(row)
            if len(r) > 3:
                dt = _parse_dt(r[3])
                if dt is not None:
                    r[3] = dt
            out.append(tuple(r))
        return out
    if kind == "signal_rows":
        out = []
        for row in payload or []:
            d = dict(row)
            dt = _parse_dt(d.get("triggered_at"))
            if dt is not None:
                d["triggered_at"] = dt
            out.append(d)
        return out
    return payload


def _get_db_writer_redis():
    global _db_writer_redis, _db_writer_redis_warn_at, _db_writer_redis_next_retry_at
    if not _DB_WRITER_REDIS_ENABLED or _redis_mod is None:
        return None
    if _db_writer_redis is not None:
        return _db_writer_redis
    now = time.time()
    if now < _db_writer_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_DB_WRITER_REDIS_URL, decode_responses=True)
        cli.ping()
        _db_writer_redis = cli
        logger.info(f"[DB writer] redis enabled key={_DB_WRITER_REDIS_KEY}")
        return _db_writer_redis
    except Exception as e:
        _db_writer_redis_next_retry_at = now + 5.0
        if now - _db_writer_redis_warn_at >= 30.0:
            _db_writer_redis_warn_at = now
            logger.warning(f"[DB writer] redis unavailable, fallback local queue: {e}")
        return None


def _redis_enqueue(kind: str, payload) -> bool:
    cli = _get_db_writer_redis()
    if cli is None:
        return False
    try:
        msg = json.dumps(
            {"kind": kind, "payload": _serialize_write_payload(kind, payload)},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        cli.rpush(_DB_WRITER_REDIS_KEY, msg)
        _metric_inc("db_redis_enqueue_total")
        return True
    except Exception:
        _metric_inc("db_redis_enqueue_fail_total")
        return False


def _redis_dequeue_batch(max_items: int) -> list[tuple[str, object]]:
    cli = _get_db_writer_redis()
    if cli is None:
        return []
    out: list[tuple[str, object]] = []
    for _ in range(max_items):
        try:
            raw = cli.lpop(_DB_WRITER_REDIS_KEY)
        except Exception:
            _metric_inc("db_redis_dequeue_fail_total")
            break
        if not raw:
            break
        try:
            obj = json.loads(raw)
            kind = str(obj.get("kind") or "")
            payload = _deserialize_write_payload(kind, obj.get("payload"))
            if kind:
                out.append((kind, payload))
        except Exception:
            _metric_inc("db_redis_dequeue_fail_total")
            continue
    if out:
        _metric_inc("db_redis_dequeue_total", float(len(out)))
    return out


def _runtime_state_ttl_sec() -> int:
    return max(1, int(_RUNTIME_STATE_REDIS_TTL_DAYS)) * 24 * 3600


def _trade_date_from_dt(v: Optional[datetime.datetime] = None) -> str:
    dt = v if isinstance(v, datetime.datetime) else datetime.datetime.now()
    return dt.strftime("%Y%m%d")


def _get_runtime_state_redis():
    global _runtime_state_redis, _runtime_state_redis_warn_at, _runtime_state_redis_next_retry_at
    if not _RUNTIME_STATE_REDIS_ENABLED or _redis_mod is None:
        return None
    if _runtime_state_redis is not None:
        return _runtime_state_redis
    now = time.time()
    if now < _runtime_state_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_RUNTIME_STATE_REDIS_URL, decode_responses=True)
        cli.ping()
        _runtime_state_redis = cli
        logger.info(f"[RuntimeState] redis enabled prefix={_RUNTIME_STATE_REDIS_KEY_PREFIX}")
        return _runtime_state_redis
    except Exception as e:
        _runtime_state_redis_next_retry_at = now + 5.0
        if now - _runtime_state_redis_warn_at >= 30.0:
            _runtime_state_redis_warn_at = now
            logger.warning(f"[RuntimeState] redis unavailable: {e}")
        return None


def _runtime_state_write_metrics(metrics: dict) -> None:
    cli = _get_runtime_state_redis()
    if cli is None:
        return
    trade_date = _trade_date_from_dt()
    key = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:metrics:{trade_date}"
    now_iso = datetime.datetime.now().isoformat()
    mapping: dict[str, str] = {"updated_at": now_iso}
    for k, v in (metrics or {}).items():
        if isinstance(v, bool):
            mapping[str(k)] = "1" if v else "0"
        elif isinstance(v, (int, float, str)):
            mapping[str(k)] = str(v)
        else:
            try:
                mapping[str(k)] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                mapping[str(k)] = str(v)
    try:
        pipe = cli.pipeline(transaction=False)
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, _runtime_state_ttl_sec())
        pipe.execute()
        _metric_inc("runtime_state_redis_write_total")
    except Exception:
        _metric_inc("runtime_state_redis_write_fail_total")


def _t0_quality_ingest_redis(rows: list[tuple]) -> None:
    """Persist T0 quality aggregates + sliding windows to Redis by trade_date."""
    cli = _get_runtime_state_redis()
    if cli is None or not rows:
        return
    ttl = _runtime_state_ttl_sec()
    max_win = max(20, int(_RUNTIME_T0_QUALITY_WINDOW_SIZE))
    try:
        pipe = cli.pipeline(transaction=False)
        for r in rows:
            if not r or len(r) < 13:
                continue
            ts_code = str(r[0] or "")
            signal_type = str(r[1] or "")
            horizon = int(r[5] or 0)
            ret_bps = float(r[7] or 0.0)
            mfe_bps = float(r[8] or 0.0)
            mae_bps = float(r[9] or 0.0)
            is_correct = 1 if bool(r[10]) else 0
            trigger_time = r[3] if isinstance(r[3], datetime.datetime) else None
            channel = str((r[12] if len(r) > 12 else "") or "unknown")
            signal_source = str((r[13] if len(r) > 13 else "") or "unknown")
            trade_date = _trade_date_from_dt(trigger_time)

            k_agg = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:t0_quality:agg:{trade_date}"
            k_win = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:t0_quality:window:{trade_date}:{signal_type}:{horizon}"

            pipe.hincrby(k_agg, "total", 1)
            pipe.hincrby(k_agg, "correct", is_correct)
            pipe.hincrbyfloat(k_agg, "ret_bps_sum", ret_bps)
            pipe.hincrbyfloat(k_agg, "mfe_bps_sum", mfe_bps)
            pipe.hincrbyfloat(k_agg, "mae_bps_sum", mae_bps)

            pipe.hincrby(k_agg, f"h:{horizon}:total", 1)
            pipe.hincrby(k_agg, f"h:{horizon}:correct", is_correct)
            pipe.hincrby(k_agg, f"s:{signal_type}:total", 1)
            pipe.hincrby(k_agg, f"s:{signal_type}:correct", is_correct)
            pipe.hincrby(k_agg, f"s:{signal_type}:h:{horizon}:total", 1)
            pipe.hincrby(k_agg, f"s:{signal_type}:h:{horizon}:correct", is_correct)
            pipe.hincrby(k_agg, f"c:{channel}:total", 1)
            pipe.hincrby(k_agg, f"c:{channel}:correct", is_correct)
            pipe.hincrby(k_agg, f"src:{signal_source}:total", 1)
            pipe.hincrby(k_agg, f"src:{signal_source}:correct", is_correct)
            pipe.hincrby(k_agg, f"c:{channel}:src:{signal_source}:total", 1)
            pipe.hincrby(k_agg, f"c:{channel}:src:{signal_source}:correct", is_correct)
            pipe.hset(k_agg, mapping={"updated_at": datetime.datetime.now().isoformat()})
            pipe.expire(k_agg, ttl)

            win_payload = json.dumps(
                {
                    "ts_code": ts_code,
                    "signal_type": signal_type,
                    "horizon": horizon,
                    "ret_bps": round(ret_bps, 2),
                    "mfe_bps": round(mfe_bps, 2),
                    "mae_bps": round(mae_bps, 2),
                    "correct": bool(is_correct),
                    "trigger_time": trigger_time.isoformat() if isinstance(trigger_time, datetime.datetime) else None,
                    "channel": channel,
                    "signal_source": signal_source,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            pipe.lpush(k_win, win_payload)
            pipe.ltrim(k_win, 0, max_win - 1)
            pipe.expire(k_win, ttl)
        pipe.execute()
        _metric_inc("t0_quality_redis_write_total", float(len(rows)))
    except Exception:
        _metric_inc("t0_quality_redis_write_fail_total")


def _phase_from_dt(dt_obj: datetime.datetime) -> str:
    try:
        hhmm = int(dt_obj.hour) * 100 + int(dt_obj.minute)
        if 930 <= hhmm < 1000:
            return "open"
        if 1000 <= hhmm < 1130:
            return "morning"
        if 1300 <= hhmm < 1430:
            return "afternoon"
        if 1430 <= hhmm <= 1500:
            return "close"
    except Exception:
        pass
    return "off_session"


def _vol_regime_from_vol20(vol_20: Optional[float]) -> str:
    try:
        v = float(vol_20) if vol_20 is not None else -1.0
    except Exception:
        v = -1.0
    high = float(os.getenv("DYNAMIC_VOL_REGIME_HIGH", "0.30") or 0.30)
    mid = float(os.getenv("DYNAMIC_VOL_REGIME_MID", "0.15") or 0.15)
    if v < 0:
        return "mid_vol"
    if v >= high:
        return "high_vol"
    if v >= mid:
        return "mid_vol"
    return "low_vol"


def _load_latest_vol20_map(ts_codes: list[str]) -> dict[str, float]:
    if not ts_codes:
        return {}
    out: dict[str, float] = {}
    try:
        uniq = sorted({str(x) for x in ts_codes if x})
        if not uniq:
            return out
        placeholders = ",".join(["?"] * len(uniq))
        with _data_ro_conn_ctx() as conn:
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
            if not vol_col:
                return out
            rows = conn.execute(
                f"""
                WITH latest AS (
                    SELECT ts_code, MAX(trade_date) AS max_date
                    FROM stk_factor_pro
                    WHERE ts_code IN ({placeholders})
                    GROUP BY ts_code
                )
                SELECT f.ts_code, f.{vol_col} AS vol_20
                FROM stk_factor_pro f
                JOIN latest l ON f.ts_code = l.ts_code AND f.trade_date = l.max_date
                """,
                uniq,
            ).fetchall()
            for ts_code, v in rows:
                try:
                    out[str(ts_code)] = float(v) if v is not None else -1.0
                except Exception:
                    out[str(ts_code)] = -1.0
    except Exception:
        return out
    return out


def _build_threshold_closed_loop_snapshot(window_hours: int) -> dict:
    now_dt = datetime.datetime.now()
    since_dt = now_dt - datetime.timedelta(hours=int(window_hours))
    signal_rows: list[tuple] = []
    quality_rows: list[tuple] = []
    industry_map: dict[str, str] = {}
    instrument_profile_map: dict[str, dict] = {}
    try:
        with _ro_conn_ctx() as conn:
            signal_rows = conn.execute(
                """
                SELECT pool_id, ts_code, signal_type, strength, triggered_at
                FROM signal_history
                WHERE triggered_at >= ?
                  AND signal_type IN ('left_side_buy','right_side_breakout','timing_clear','positive_t','reverse_t')
                """,
                [since_dt],
            ).fetchall()
            quality_rows = conn.execute(
                """
                SELECT ts_code, signal_type, market_phase, direction_correct, trigger_time
                FROM t0_signal_quality
                WHERE trigger_time >= ?
                """,
                [since_dt],
            ).fetchall()
            inds = conn.execute(
                """
                SELECT ts_code, MAX(COALESCE(industry, '')) AS industry
                FROM monitor_pools
                GROUP BY ts_code
                """
            ).fetchall()
            for ts_code, industry in inds:
                industry_map[str(ts_code)] = str(industry or "")
    except Exception as e:
        return {
            "ok": False,
            "checked_at": now_dt.isoformat(),
            "window_hours": int(window_hours),
            "error": str(e),
            "rows": [],
        }

    ts_codes = [str(r[1]) for r in signal_rows] + [str(r[0]) for r in quality_rows]
    uniq_codes = sorted({c for c in ts_codes if c})
    if uniq_codes and infer_instrument_profile is not None:
        try:
            with _data_ro_conn_ctx() as data_conn:
                placeholders = ",".join(["?"] * len(uniq_codes))
                meta_rows = data_conn.execute(
                    f"""
                    SELECT ts_code, name, market, list_date
                    FROM stock_basic
                    WHERE ts_code IN ({placeholders})
                    """,
                    uniq_codes,
                ).fetchall()
            for ts_code, name, market_name, list_date in meta_rows:
                profile = infer_instrument_profile(
                    str(ts_code or ""),
                    name=name,
                    market_name=market_name,
                    list_date=list_date,
                    ts_epoch=int(now_dt.timestamp()),
                )
                instrument_profile_map[str(ts_code)] = profile
        except Exception:
            instrument_profile_map = {}
    vol_map = _load_latest_vol20_map(ts_codes)
    aggr: dict[tuple[str, str, str, str, str, str, str], dict] = {}

    for pool_id, ts_code, signal_type, strength, triggered_at in signal_rows:
        ts_code = str(ts_code or "")
        signal_type = str(signal_type or "")
        tdt = triggered_at if isinstance(triggered_at, datetime.datetime) else now_dt
        phase = _phase_from_dt(tdt)
        industry = str(industry_map.get(ts_code, "") or "*")
        regime = _vol_regime_from_vol20(vol_map.get(ts_code))
        profile = instrument_profile_map.get(ts_code, {})
        board_segment = str(profile.get("board_segment") or "*")
        security_type = str(profile.get("security_type") or "*")
        listing_stage = str(profile.get("listing_stage") or "*")
        key = (signal_type, phase, regime, industry, board_segment, security_type, listing_stage)
        d = aggr.setdefault(
            key,
            {
                "signal_type": signal_type,
                "market_phase": phase,
                "regime": regime,
                "industry": industry,
                "board_segment": board_segment,
                "security_type": security_type,
                "listing_stage": listing_stage,
                "trigger_count": 0,
                "pass_count": 0,
                "hit_count": 0,
                "eval_count": 0,
                "symbols": set(),
            },
        )
        d["trigger_count"] += 1
        d["symbols"].add(ts_code)
        try:
            s_val = float(strength or 0)
        except Exception:
            s_val = 0.0
        pass_th = _CLOSED_LOOP_SCORE_OBS_POOL1 if int(pool_id or 0) == 1 else _CLOSED_LOOP_SCORE_OBS_T0
        if s_val >= pass_th:
            d["pass_count"] += 1

    for ts_code, signal_type, market_phase, direction_correct, _trigger_time in quality_rows:
        ts_code = str(ts_code or "")
        signal_type = str(signal_type or "")
        phase = str(market_phase or "off_session")
        industry = str(industry_map.get(ts_code, "") or "*")
        regime = _vol_regime_from_vol20(vol_map.get(ts_code))
        profile = instrument_profile_map.get(ts_code, {})
        board_segment = str(profile.get("board_segment") or "*")
        security_type = str(profile.get("security_type") or "*")
        listing_stage = str(profile.get("listing_stage") or "*")
        key = (signal_type, phase, regime, industry, board_segment, security_type, listing_stage)
        d = aggr.setdefault(
            key,
            {
                "signal_type": signal_type,
                "market_phase": phase,
                "regime": regime,
                "industry": industry,
                "board_segment": board_segment,
                "security_type": security_type,
                "listing_stage": listing_stage,
                "trigger_count": 0,
                "pass_count": 0,
                "hit_count": 0,
                "eval_count": 0,
                "symbols": set(),
            },
        )
        d["eval_count"] += 1
        if bool(direction_correct):
            d["hit_count"] += 1

    rows: list[dict] = []
    for _, d in aggr.items():
        symbols = max(1, len(d["symbols"]))
        trigger_count = int(d["trigger_count"])
        pass_count = int(d["pass_count"])
        eval_count = int(d["eval_count"])
        hit_count = int(d["hit_count"])
        trigger_rate = float(trigger_count) / float(symbols)
        pass_rate = (float(pass_count) / float(trigger_count)) if trigger_count > 0 else None
        hit_rate = (float(hit_count) / float(eval_count)) if eval_count > 0 else None
        samples = max(trigger_count, eval_count)
        rows.append(
            {
                "signal_type": d["signal_type"],
                "market_phase": d["market_phase"],
                "regime": d["regime"],
                "industry": d["industry"],
                "board_segment": d["board_segment"],
                "security_type": d["security_type"],
                "listing_stage": d["listing_stage"],
                "symbols": symbols,
                "trigger_count": trigger_count,
                "pass_count": pass_count,
                "eval_count": eval_count,
                "hit_count": hit_count,
                "trigger_rate": round(trigger_rate, 4),
                "pass_rate": round(pass_rate, 4) if pass_rate is not None else None,
                "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
                "samples": int(samples),
            }
        )

    rows.sort(key=lambda x: (x["signal_type"], x["market_phase"], x["regime"], x["industry"], x["board_segment"], x["security_type"], x["listing_stage"]))
    return {
        "ok": True,
        "checked_at": now_dt.isoformat(),
        "window_hours": int(window_hours),
        "rows": rows,
    }


def _build_threshold_calibration_rules(snapshot: dict) -> list[dict]:
    rows = snapshot.get("rows", []) if isinstance(snapshot, dict) else []
    if not isinstance(rows, list):
        return []
    min_samples = max(1, int(_CLOSED_LOOP_MIN_SAMPLES))
    trigger_low = float(_CLOSED_LOOP_TARGETS.get("trigger_per_symbol_low", 0.3) or 0.3)
    trigger_high = float(_CLOSED_LOOP_TARGETS.get("trigger_per_symbol_high", 3.0) or 3.0)
    pass_low = float(_CLOSED_LOOP_TARGETS.get("pass_rate_low", 0.35) or 0.35)
    hit_low = float(_CLOSED_LOOP_TARGETS.get("hit_rate_low", 0.48) or 0.48)
    hit_high = float(_CLOSED_LOOP_TARGETS.get("hit_rate_high", 0.62) or 0.62)

    step_pos = float(_CLOSED_LOOP_STEPS.get("positive_t_bias_vwap", 0.1) or 0.1)
    step_rev = float(_CLOSED_LOOP_STEPS.get("reverse_t_z", 0.1) or 0.1)
    step_left = float(_CLOSED_LOOP_STEPS.get("left_near_lower", 0.03) or 0.03)
    step_right = float(_CLOSED_LOOP_STEPS.get("right_eps", 0.0003) or 0.0003)

    rules: list[dict] = []
    for r in rows:
        try:
            samples = int(r.get("samples", 0) or 0)
            if samples < min_samples:
                continue
            sig = str(r.get("signal_type") or "")
            phase = str(r.get("market_phase") or "*")
            regime = str(r.get("regime") or "*")
            industry = str(r.get("industry") or "*")
            board_segment = str(r.get("board_segment") or "*")
            security_type = str(r.get("security_type") or "*")
            listing_stage = str(r.get("listing_stage") or "*")
            trigger_rate = float(r.get("trigger_rate", 0) or 0)
            pass_rate = float(r.get("pass_rate", 0) or 0)
            hit_rate = r.get("hit_rate")
            hit_rate = float(hit_rate) if hit_rate is not None else None
        except Exception:
            continue

        bad = (pass_rate < pass_low) or (trigger_rate > trigger_high) or (hit_rate is not None and hit_rate < hit_low)
        good = (trigger_rate < trigger_low) and (hit_rate is not None and hit_rate > hit_high)
        if not (bad or good):
            continue

        delta: dict = {}
        reason = "tighten" if bad else "loosen"
        if sig == "positive_t":
            delta["bias_vwap_th"] = -step_pos if bad else step_pos
        elif sig == "reverse_t":
            delta["z_th"] = step_rev if bad else -step_rev
        elif sig == "left_side_buy":
            delta["near_lower_th"] = -step_left if bad else step_left
        elif sig == "right_side_breakout":
            eps_delta = step_right if bad else -step_right
            delta["eps_mid"] = eps_delta
            delta["eps_upper"] = eps_delta
        else:
            continue

        rules.append(
            {
                "signal_type": sig,
                "market_phase": phase,
                "regime": regime,
                "industry": industry,
                "board_segment": board_segment,
                "security_type": security_type,
                "listing_stage": listing_stage,
                "delta": delta,
                "reason": reason,
                "metrics": {
                    "samples": samples,
                    "trigger_rate": round(trigger_rate, 4),
                    "pass_rate": round(pass_rate, 4),
                    "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
                },
            }
        )
    return rules


def _write_threshold_calibration_payload(payload: dict) -> bool:
    cli = _get_runtime_state_redis()
    if cli is None:
        return False
    key_latest = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:threshold:calibration:latest"
    key_hist = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:threshold:calibration:history:{_trade_date_from_dt()}"
    ttl = _runtime_state_ttl_sec()
    try:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        pipe = cli.pipeline(transaction=False)
        pipe.set(key_latest, raw, ex=ttl)
        pipe.lpush(key_hist, raw)
        pipe.ltrim(key_hist, 0, 199)
        pipe.expire(key_hist, ttl)
        pipe.execute()
        _metric_inc("threshold_calib_write_total")
        return True
    except Exception:
        _metric_inc("threshold_calib_write_fail_total")
        return False


def _start_threshold_closed_loop(interval_min: float = 15.0) -> None:
    global _threshold_closed_loop_started, _threshold_calibration_last
    if _threshold_closed_loop_started or not _CLOSED_LOOP_ENABLED:
        return
    _threshold_closed_loop_started = True
    sec = max(30.0, float(interval_min) * 60.0)

    def _loop() -> None:
        logger.info(f"[ThresholdCalib] closed-loop started interval={sec:.0f}s")
        while True:
            try:
                snap = _build_threshold_closed_loop_snapshot(_CLOSED_LOOP_WINDOW_HOURS)
                rules = _build_threshold_calibration_rules(snap) if bool(snap.get("ok")) else []
                payload = {
                    "ok": bool(snap.get("ok")),
                    "checked_at": datetime.datetime.now().isoformat(),
                    "window_hours": int(_CLOSED_LOOP_WINDOW_HOURS),
                    "rows": snap.get("rows", []),
                    "rules": rules,
                    "summary": {
                        "row_count": len(snap.get("rows", []) if isinstance(snap.get("rows"), list) else []),
                        "rule_count": len(rules),
                    },
                }
                _threshold_calibration_last = payload
                _write_threshold_calibration_payload(payload)
            except Exception as e:
                logger.error(f"[ThresholdCalib] closed-loop error: {e}")
            time.sleep(sec)

    threading.Thread(target=_loop, daemon=True, name="threshold-closed-loop").start()


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
                channel VARCHAR,
                signal_source VARCHAR,
                strength DOUBLE,
                message VARCHAR,
                price DOUBLE,
                pct_chg DOUBLE,
                details_json VARCHAR,
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
                channel VARCHAR,
                signal_source VARCHAR,
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
        # Backward-compatible schema migration for existing DBs.
        for ddl in (
            "ALTER TABLE signal_history ADD COLUMN channel VARCHAR",
            "ALTER TABLE signal_history ADD COLUMN signal_source VARCHAR",
            "ALTER TABLE signal_history ADD COLUMN details_json VARCHAR",
            "ALTER TABLE t0_signal_quality ADD COLUMN channel VARCHAR",
            "ALTER TABLE t0_signal_quality ADD COLUMN signal_source VARCHAR",
        ):
            try:
                conn.execute(ddl)
            except Exception:
                pass
        for ddl in (
            "CREATE INDEX IF NOT EXISTS idx_monitor_pools_pool_order ON monitor_pools(pool_id, sort_order, added_at)",
            "CREATE INDEX IF NOT EXISTS idx_signal_history_pool_code_type_ts ON signal_history(pool_id, ts_code, signal_type, triggered_at)",
            "CREATE INDEX IF NOT EXISTS idx_signal_history_triggered_at ON signal_history(triggered_at)",
            "CREATE INDEX IF NOT EXISTS idx_tick_history_code_ts ON tick_history(ts_code, ts)",
            "CREATE INDEX IF NOT EXISTS idx_t0_quality_created_at ON t0_signal_quality(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_t0_quality_code_type_time ON t0_signal_quality(ts_code, signal_type, trigger_time, eval_horizon_sec)",
            "CREATE INDEX IF NOT EXISTS idx_t0_quality_channel_source_time ON t0_signal_quality(channel, signal_source, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_t0_drift_checked_at ON t0_feature_drift(checked_at)",
            "CREATE INDEX IF NOT EXISTS idx_t0_drift_feature_checked ON t0_feature_drift(feature_name, checked_at)",
        ):
            try:
                conn.execute(ddl)
            except Exception:
                pass
        _bootstrap_monitor_pools_from_data_db(conn)
        _TABLE_READY = True


def _bootstrap_monitor_pools_from_data_db(rt_conn: duckdb.DuckDBPyConnection) -> None:
    """One-time bootstrap: copy monitor_pools from cold DB when hot DB is empty."""
    try:
        if os.path.abspath(_REALTIME_DB) == os.path.abspath(_DATA_DB):
            return
    except Exception:
        pass
    try:
        c = rt_conn.execute("SELECT COUNT(*) FROM monitor_pools").fetchone()
        if c and int(c[0] or 0) > 0:
            return
    except Exception:
        return
    data_conn = None
    try:
        data_conn = open_duckdb_conn(_DATA_DB, retries=2, base_sleep_sec=0.05)
        rows = data_conn.execute(
            """
            SELECT pool_id, ts_code, name, industry, added_at, note, sort_order
            FROM monitor_pools
            ORDER BY pool_id, sort_order, added_at
            """
        ).fetchall()
        if not rows:
            return
        rt_conn.executemany(
            """
            INSERT INTO monitor_pools (pool_id, ts_code, name, industry, added_at, note, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        logger.info(f"[RealtimeDB] bootstrapped monitor_pools from data DB rows={len(rows)}")
    except Exception:
        # best effort; keep startup resilient
        return
    finally:
        if data_conn is not None:
            try:
                data_conn.close()
            except Exception:
                pass


def _get_conn() -> duckdb.DuckDBPyConnection:
    conn = open_duckdb_conn(
        _REALTIME_DB,
        retries=3,
        base_sleep_sec=0.08,
        on_retry=lambda _attempt, _exc: _metric_inc("db_lock_open_rw_total"),
    )
    _ensure_table(conn)
    return conn


def _new_ro_conn() -> duckdb.DuckDBPyConnection:
    return open_duckdb_conn(
        _REALTIME_DB,
        retries=3,
        base_sleep_sec=0.05,
        on_retry=lambda _attempt, _exc: _metric_inc("db_lock_open_ro_total"),
    )


def _acquire_ro_conn() -> duckdb.DuckDBPyConnection:
    try:
        conn = _db_read_pool.get_nowait()
        _metric_inc("db_read_pool_hit_total")
        return conn
    except _queue_mod.Empty:
        _metric_inc("db_read_pool_miss_total")
        return _new_ro_conn()


def _release_ro_conn(conn: Optional[duckdb.DuckDBPyConnection]) -> None:
    if conn is None:
        return
    try:
        _db_read_pool.put_nowait(conn)
    except _queue_mod.Full:
        try:
            conn.close()
        except Exception:
            pass


def _new_data_ro_conn() -> duckdb.DuckDBPyConnection:
    return open_duckdb_conn(
        _DATA_DB,
        retries=3,
        base_sleep_sec=0.05,
        on_retry=lambda _attempt, _exc: _metric_inc("db_lock_open_ro_total"),
    )


def _acquire_data_ro_conn() -> duckdb.DuckDBPyConnection:
    try:
        conn = _db_data_read_pool.get_nowait()
        _metric_inc("db_data_read_pool_hit_total")
        return conn
    except _queue_mod.Empty:
        _metric_inc("db_data_read_pool_miss_total")
        return _new_data_ro_conn()


def _release_data_ro_conn(conn: Optional[duckdb.DuckDBPyConnection]) -> None:
    if conn is None:
        return
    try:
        _db_data_read_pool.put_nowait(conn)
    except _queue_mod.Full:
        try:
            conn.close()
        except Exception:
            pass


@contextmanager
def _ro_conn_ctx():
    conn = _acquire_ro_conn()
    try:
        yield conn
    finally:
        _release_ro_conn(conn)


@contextmanager
def _data_ro_conn_ctx():
    conn = _acquire_data_ro_conn()
    try:
        yield conn
    finally:
        _release_data_ro_conn(conn)


def _enqueue_db_write(kind: str, payload) -> None:
    global _db_write_drop_warn_at
    if kind in {"tick_rows", "signal_rows", "quality_rows", "drift_rows"}:
        if not _is_market_open().get("is_open", False):
            _metric_inc("db_write_skip_non_trading_total")
            return
    if _redis_enqueue(kind, payload):
        return
    try:
        _db_write_queue.put_nowait((kind, payload))
    except _queue_mod.Full:
        _metric_inc("db_writer_queue_drop_total")
        now = time.time()
        if now - _db_write_drop_warn_at >= 5.0:
            _db_write_drop_warn_at = now
            logger.warning(f"[DB writer] queue full, dropped kind={kind}")


def _enqueue_quality_track(task: dict) -> None:
    try:
        _quality_track_queue.put_nowait(task)
    except Exception:
        pass


def _collect_signal_rows(pool_id: int, evaluated: list) -> list[dict]:
    rows: list[dict] = []
    now_ts = int(time.time())
    for row in evaluated or []:
        ts_code = str(row.get("ts_code", "") or "")
        if not ts_code:
            continue
        for s in row.get("signals", []) or []:
            if not s.get("has_signal"):
                continue
            sig_type = str(s.get("type") or "")
            if not sig_type:
                continue
            trigger_ts = int(s.get("triggered_at") or now_ts)
            rows.append(
                {
                    "pool_id": int(pool_id),
                    "ts_code": ts_code,
                    "name": str(row.get("name", "") or ""),
                    "signal_type": sig_type,
                    "channel": str(
                        s.get("channel")
                        or (s.get("details") or {}).get("channel")
                        or ("pool1_timing" if int(pool_id) == 1 else "pool2_t0")
                    ),
                    "signal_source": str(
                        s.get("signal_source")
                        or (s.get("details") or {}).get("signal_source")
                        or "runtime"
                    ),
                    "strength": float(s.get("strength", 0) or 0),
                    "message": str(s.get("message", "") or ""),
                    "price": float(row.get("price", 0) or 0),
                    "pct_chg": float(row.get("pct_chg", 0) or 0),
                    "details_json": _compact_history_snapshot(s),
                    "triggered_at": datetime.datetime.fromtimestamp(trigger_ts),
                    "direction": str(s.get("direction", "buy") or "buy"),
                    "trigger_time": float(trigger_ts),
                    "trigger_price": float(s.get("price", row.get("price", 0)) or 0),
                }
            )
    return rows


def _build_tick_row(ts_code: str, name: str, tick: dict) -> tuple:
    now_ts = int(time.time())
    t = tick or {}
    return (
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


def _enqueue_gm_signal_history(ts_code: str, tick_item: dict, fired_signals: list) -> None:
    if not _is_market_open().get("is_open", False):
        return
    if not fired_signals:
        return
    rows_by_pool: dict[int, list[dict]] = {1: [], 2: []}
    for s in fired_signals:
        if not isinstance(s, dict):
            continue
        if not s.get("has_signal", True):
            continue
        if not s.get("is_new", False):
            continue
        sig_type = str(s.get("type") or "")
        if not sig_type:
            continue
        raw_pools = s.get("_pool_ids")
        pools: list[int]
        if isinstance(raw_pools, (list, tuple, set)):
            pools = []
            for pid in raw_pools:
                try:
                    p = int(pid)
                except Exception:
                    continue
                if p in POOL_SIGNAL_TYPES:
                    pools.append(p)
        else:
            pools = [1, 2]
        for pid in pools:
            if sig_type not in POOL_SIGNAL_TYPES.get(pid, set()):
                continue
            rows_by_pool[pid].append(
                {
                    "ts_code": ts_code,
                    "name": str(tick_item.get("name", "") or ""),
                    "price": float(tick_item.get("price", 0) or 0),
                    "pct_chg": float(tick_item.get("pct_chg", 0) or 0),
                    "signals": [s],
                }
            )
    for pid, rows in rows_by_pool.items():
        if rows:
            _save_signal_history(pid, rows)


def _process_db_batch(conn: duckdb.DuckDBPyConnection, batch: list[tuple[str, object]]) -> list[dict]:
    tick_rows: list[tuple] = []
    quality_rows: list[tuple] = []
    drift_rows: list[tuple] = []
    signal_rows: list[dict] = []
    quality_tasks: list[dict] = []

    for kind, payload in batch:
        if kind == "tick_rows":
            tick_rows.extend(payload or [])
        elif kind == "quality_rows":
            quality_rows.extend(payload or [])
        elif kind == "drift_rows":
            drift_rows.extend(payload or [])
        elif kind == "signal_rows":
            signal_rows.extend(payload or [])

    if tick_rows:
        conn.executemany(
            """
            INSERT INTO tick_history
            (ts_code, name, price, open, high, low, pre_close, volume, amount, pct_chg, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tick_rows,
        )

    if signal_rows:
        for r in signal_rows:
            pool_id = int(r.get("pool_id", 0) or 0)
            ts_code = str(r.get("ts_code", "") or "")
            sig_type = str(r.get("signal_type", "") or "")
            triggered_at = r.get("triggered_at")
            if pool_id <= 0 or not ts_code or not sig_type or triggered_at is None:
                continue
            if isinstance(triggered_at, datetime.datetime):
                dedup_since = triggered_at - datetime.timedelta(seconds=_signal_history_dedup_seconds(pool_id, sig_type))
            else:
                dedup_since = datetime.datetime.now() - datetime.timedelta(
                    seconds=_signal_history_dedup_seconds(pool_id, sig_type)
                )
            dup = conn.execute(
                """
                SELECT id FROM signal_history
                WHERE pool_id = ? AND ts_code = ? AND signal_type = ?
                  AND triggered_at > ?
                LIMIT 1
                """,
                [pool_id, ts_code, sig_type, dedup_since],
            ).fetchone()
            if dup:
                continue
            conn.execute(
                """
                INSERT INTO signal_history
                (id, pool_id, ts_code, name, signal_type, channel, signal_source, strength, message, price, pct_chg, details_json, triggered_at)
                VALUES (nextval('signal_history_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    pool_id,
                    ts_code,
                    r.get("name", ""),
                    sig_type,
                    str(r.get("channel", "") or ""),
                    str(r.get("signal_source", "") or ""),
                    float(r.get("strength", 0) or 0),
                    str(r.get("message", "") or ""),
                    float(r.get("price", 0) or 0),
                    float(r.get("pct_chg", 0) or 0),
                    str(r.get("details_json", "") or "") or None,
                    triggered_at,
                ],
            )
            if pool_id == 2:
                quality_tasks.append(
                    {
                        "ts_code": ts_code,
                        "signal_type": sig_type,
                        "direction": str(r.get("direction", "buy") or "buy"),
                        "trigger_time": float(r.get("trigger_time", time.time()) or time.time()),
                        "trigger_price": float(r.get("trigger_price", r.get("price", 0)) or 0),
                        "channel": str(r.get("channel", "pool2_t0") or "pool2_t0"),
                        "signal_source": str(r.get("signal_source", "runtime") or "runtime"),
                    }
                )

    if quality_rows:
        normalized_quality_rows: list[tuple] = []
        for r in quality_rows:
            if not isinstance(r, (list, tuple)):
                continue
            row = list(r)
            # Backward compatibility:
            # old format: 12 columns (without channel/signal_source)
            # new format: 14 columns.
            if len(row) < 12:
                continue
            if len(row) == 12:
                row.extend(["pool2_t0", "runtime"])
            elif len(row) == 13:
                row.append("runtime")
            if len(row) > 14:
                row = row[:14]
            normalized_quality_rows.append(tuple(row))
        if not normalized_quality_rows:
            quality_rows = []
        else:
            quality_rows = normalized_quality_rows
    if quality_rows:
        conn.executemany(
            """
            INSERT INTO t0_signal_quality
            (id, ts_code, signal_type, direction, trigger_time, trigger_price,
             eval_horizon_sec, eval_price, ret_bps, mfe_bps, mae_bps,
             direction_correct, market_phase, channel, signal_source)
            VALUES (nextval('t0_signal_quality_id_seq'),
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            quality_rows,
        )
        _t0_quality_ingest_redis(quality_rows)

    if drift_rows:
        conn.executemany(
            """
            INSERT INTO t0_feature_drift
            (id, feature_name, psi, ks_stat, ks_p, severity, drifted, precision_drop, alerted)
            VALUES (nextval('t0_feature_drift_id_seq'),
                    ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            drift_rows,
        )
    return quality_tasks


def _start_db_writer(flush_interval: float = _DB_WRITER_FLUSH_SEC, max_batch: int = _DB_WRITER_MAX_BATCH) -> None:
    global _db_writer_started
    if _db_writer_started:
        return
    _db_writer_started = True

    def _loop() -> None:
        logger.info(f"[DB writer] started flush={flush_interval}s batch={max_batch}")
        pending: list[tuple[str, object]] = []
        while True:
            try:
                if not pending:
                    pending.extend(_redis_dequeue_batch(max_batch))
                if not pending:
                    kind, payload = _db_write_queue.get(timeout=flush_interval)
                    pending.append((kind, payload))
                while len(pending) < max_batch:
                    need = max_batch - len(pending)
                    if need > 0:
                        pending.extend(_redis_dequeue_batch(need))
                        if len(pending) >= max_batch:
                            break
                    try:
                        pending.append(_db_write_queue.get_nowait())
                    except _queue_mod.Empty:
                        break
                if not pending:
                    continue
                conn = _get_conn()
                try:
                    conn.execute("BEGIN")
                    quality_tasks = _process_db_batch(conn, pending)
                    conn.execute("COMMIT")
                    for task in quality_tasks:
                        _enqueue_quality_track(task)
                    _metric_inc("db_writer_batch_tx_total")
                    pending.clear()
                except Exception:
                    _metric_inc("db_writer_batch_tx_fail_total")
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise
                finally:
                    conn.close()
            except _queue_mod.Empty:
                continue
            except Exception as e:
                if _is_db_lock_error(e):
                    _metric_inc("db_lock_writer_total")
                    logger.warning(f"[DB writer] lock retry: {e}")
                else:
                    logger.error(f"[DB writer] error: {e}")
                time.sleep(0.3)

    threading.Thread(target=_loop, daemon=True, name="db-writer").start()


def _is_market_open() -> dict:
    now = datetime.datetime.now()
    weekday = now.weekday()
    t = now.hour * 100 + now.minute
    if weekday >= 5:
        return {"is_open": False, "status": "weekend", "next_open": "next weekday 09:15"}
    if 915 <= t < 925:
        return {"is_open": True, "status": "pre_auction", "desc": "pre auction"}
    # Morning continuous session ends at 11:30; 11:30-13:00 should be treated as closed.
    if 930 <= t < 1130:
        return {"is_open": True, "status": "morning_session", "desc": "morning session"}
    if 1130 <= t < 1300:
        return {"is_open": False, "status": "lunch_break", "desc": "lunch break"}
    if 1300 <= t <= 1500:
        return {"is_open": True, "status": "afternoon_session", "desc": "afternoon session"}
    if t > 1500:
        return {"is_open": False, "status": "after_close", "desc": "after close"}
    return {"is_open": False, "status": "before_open", "desc": "before open"}


def _intraday_progress_ratio(ts_epoch: Optional[int] = None) -> float:
    """Continuous-auction progress ratio in [0,1] for A-share sessions."""
    try:
        dt = datetime.datetime.fromtimestamp(int(ts_epoch)) if ts_epoch else datetime.datetime.now()
    except Exception:
        dt = datetime.datetime.now()
    if dt.weekday() >= 5:
        return 0.0
    hm = dt.hour * 60 + dt.minute
    morning_start = 9 * 60 + 30
    morning_end = 11 * 60 + 30
    afternoon_start = 13 * 60
    afternoon_end = 15 * 60
    if hm < morning_start:
        traded = 0.0
    elif hm < morning_end:
        traded = float(hm - morning_start)
    elif hm < afternoon_start:
        traded = 120.0
    elif hm < afternoon_end:
        traded = 120.0 + float(hm - afternoon_start)
    else:
        traded = 240.0
    return max(0.0, min(1.0, traded / 240.0))


def _resolve_volume_pace_state(pool_id: int, pace_ratio: Optional[float], given_state: Optional[str] = None) -> str:
    if isinstance(given_state, str) and given_state.strip():
        s = given_state.strip().lower()
        if s in {"unknown", "shrink", "normal", "expand", "surge"}:
            return s
    if pace_ratio is None:
        return "unknown"
    try:
        p = float(pace_ratio)
    except Exception:
        return "unknown"
    if p <= 0:
        return "unknown"
    if int(pool_id) == 1:
        shrink_th = _P1_VOL_PACE_SHRINK_TH
        expand_th = _P1_VOL_PACE_EXPAND_TH
        surge_th = _P1_VOL_PACE_SURGE_TH
    else:
        shrink_th = _T0_VOL_PACE_SHRINK_TH
        expand_th = _T0_VOL_PACE_EXPAND_TH
        surge_th = _T0_VOL_PACE_SURGE_TH
    if p >= surge_th:
        return "surge"
    if p >= expand_th:
        return "expand"
    if p < shrink_th:
        return "shrink"
    return "normal"


def _calc_intraday_volume_pace_member(
    tick: dict,
    prev_day_volume: Optional[float],
    vol5_median_volume: Optional[float],
    pool_id: int,
) -> dict:
    """Compute member-level intraday volume pace aligned with fast path."""
    ts_epoch = int(tick.get("timestamp") or time.time())
    progress_ratio = _intraday_progress_ratio(ts_epoch)
    try:
        cum_volume = float(tick.get("volume", 0) or 0)
    except Exception:
        cum_volume = 0.0
    try:
        prev_vol = float(prev_day_volume or 0)
    except Exception:
        prev_vol = 0.0
    try:
        med5_vol = float(vol5_median_volume or 0)
    except Exception:
        med5_vol = 0.0
    min_progress = _P1_VOL_PACE_MIN_PROGRESS if int(pool_id) == 1 else _T0_VOL_PACE_MIN_PROGRESS
    ratio = None
    ratio_prev = None
    ratio_med5 = None
    state = "unknown"
    baseline_volume = 0.0
    baseline_mode = "none"
    if prev_vol > 0 and med5_vol > 0:
        baseline_volume = 0.5 * prev_vol + 0.5 * med5_vol
        baseline_mode = "blend_prev_med5"
    elif prev_vol > 0:
        baseline_volume = prev_vol
        baseline_mode = "prev_day"
    elif med5_vol > 0:
        baseline_volume = med5_vol
        baseline_mode = "med5"
    if baseline_volume > 0 and progress_ratio > 0 and progress_ratio >= min_progress:
        ratio = (cum_volume / baseline_volume) / progress_ratio
        if prev_vol > 0:
            ratio_prev = (cum_volume / prev_vol) / progress_ratio
        if med5_vol > 0:
            ratio_med5 = (cum_volume / med5_vol) / progress_ratio
        if ratio > 0:
            state = _resolve_volume_pace_state(pool_id, ratio, None)
        else:
            ratio = None
            state = "unknown"
    return {
        "ratio": ratio,
        "ratio_prev": ratio_prev,
        "ratio_med5": ratio_med5,
        "state": state,
        "progress_ratio": progress_ratio,
        "cum_volume": cum_volume,
        "prev_day_volume": prev_vol,
        "vol5_median_volume": med5_vol,
        "baseline_volume": baseline_volume,
        "baseline_mode": baseline_mode,
        "min_progress": min_progress,
    }


def _get_latest_factor_date() -> Optional[str]:
    with _data_ro_conn_ctx() as conn:
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


def _refresh_daily_factors_cache(pool_id: Optional[int] = None) -> int:
    provider = get_tick_provider()
    try:
        with _ro_conn_ctx() as rt_conn:
            pool_rows = rt_conn.execute("SELECT ts_code, pool_id, industry FROM monitor_pools").fetchall()
        stock_pool_map: dict[str, set] = {}
        stock_industry_map: dict[str, str] = {}
        for ts_code, pid, industry in pool_rows:
            stock_pool_map.setdefault(ts_code, set()).add(int(pid))
            if ts_code and ts_code not in stock_industry_map:
                stock_industry_map[str(ts_code)] = str(industry or "")
        instrument_profile_map: dict[str, dict] = {}
        ts_codes = list(stock_pool_map.keys()) if pool_id is None else [
            code for code, pools in stock_pool_map.items() if int(pool_id) in pools
        ]
        if ts_codes:
            try:
                with _data_ro_conn_ctx() as data_conn:
                    placeholders = ",".join(["?"] * len(ts_codes))
                    meta_rows = data_conn.execute(
                        f"""
                        SELECT ts_code, name, market, list_date
                        FROM stock_basic
                        WHERE ts_code IN ({placeholders})
                        """,
                        ts_codes,
                    ).fetchall()
                for ts_code, name, market_name, list_date in meta_rows:
                    if infer_instrument_profile is None:
                        continue
                    profile = infer_instrument_profile(
                        str(ts_code or ""),
                        name=name,
                        market_name=market_name,
                        list_date=list_date,
                        ts_epoch=int(time.time()),
                    )
                    profile["name"] = str(name or "")
                    instrument_profile_map[str(ts_code)] = profile
            except Exception:
                instrument_profile_map = {}
        try:
            provider.bulk_update_stock_pools(stock_pool_map)
        except Exception:
            pass
        try:
            provider.bulk_update_stock_industry(stock_industry_map)
        except Exception:
            pass
        try:
            provider.bulk_update_instrument_profiles(instrument_profile_map)
        except Exception:
            pass

        # 启动与全量刷新时，统一订阅 Pool1+Pool2 股票（去重后）。
        # 这样即使前端尚未连接 WS，也能提前建立行情订阅。
        if pool_id is None:
            try:
                target_codes = sorted(
                    {
                        str(code)
                        for code, pools in stock_pool_map.items()
                        if code and ({1, 2} & set(pools or set()))
                    }
                )
                if target_codes:
                    provider.subscribe_symbols(target_codes)
                    logger.info(f"[Layer2] startup merged subscription synced: {len(target_codes)}")
            except Exception as e:
                logger.warning(f"[Layer2] startup merged subscription failed: {e}")

        if not ts_codes:
            return 0

        with _data_ro_conn_ctx() as data_conn:
            # Compatible column fallback: vol_20/vol20/vol and atr_14/atr14/atr.
            try:
                schema_rows = data_conn.execute("PRAGMA table_info('stk_factor_pro')").fetchall()
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
            ma20_col = "ma_qfq_20" if "ma_qfq_20" in columns else None

            vol_expr = f"f.{vol_col}" if vol_col else "NULL"
            atr_expr = f"f.{atr_col}" if atr_col else "NULL"
            ma20_expr = f"f.{ma20_col}" if ma20_col else "NULL"
            try:
                daily_schema_rows = data_conn.execute("PRAGMA table_info('daily')").fetchall()
                daily_columns = {str(r[1]).lower() for r in daily_schema_rows}
            except Exception:
                daily_columns = set()
            daily_vol_col = None
            for c in ("vol", "volume", "trade_vol", "total_vol"):
                if c in daily_columns:
                    daily_vol_col = c
                    break
            daily_vol_scale = 100.0 if daily_vol_col == "vol" else 1.0

            placeholders = ",".join(["?"] * len(ts_codes))
            rows = data_conn.execute(
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
                       {ma20_expr} AS ma20,
                       f.volume_ratio AS volume_ratio,
                       {vol_expr} AS vol_20,
                       {atr_expr} AS atr_14
                FROM stk_factor_pro f
                JOIN latest l ON f.ts_code = l.ts_code AND f.trade_date = l.max_date
                """,
                ts_codes,
            ).fetchall()
            prev_day_volume_map: dict[str, float] = {}
            vol5_median_volume_map: dict[str, float] = {}
            if daily_vol_col:
                daily_rows = data_conn.execute(
                    f"""
                    WITH ranked_daily AS (
                        SELECT ts_code,
                               {daily_vol_col} AS vol_raw,
                               ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                        FROM daily
                        WHERE ts_code IN ({placeholders})
                    ),
                    agg_daily AS (
                        SELECT ts_code,
                               MAX(CASE WHEN rn = 1 THEN vol_raw END) AS prev_day_volume,
                               median(vol_raw) AS vol5_median_volume
                        FROM ranked_daily
                        WHERE rn <= 5
                        GROUP BY ts_code
                    )
                    SELECT ts_code, prev_day_volume, vol5_median_volume
                    FROM agg_daily
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, prev_vol, med5_vol in daily_rows:
                    try:
                        base_vol = float(prev_vol) if prev_vol is not None else 0.0
                        prev_day_volume_map[str(ts_code)] = base_vol * daily_vol_scale
                    except Exception:
                        prev_day_volume_map[str(ts_code)] = 0.0
                    try:
                        base_med5 = float(med5_vol) if med5_vol is not None else 0.0
                        vol5_median_volume_map[str(ts_code)] = base_med5 * daily_vol_scale
                    except Exception:
                        vol5_median_volume_map[str(ts_code)] = 0.0

        factors_map: dict[str, dict] = {}
        for r in rows:
            factors_map[r[0]] = {
                "boll_upper": r[1] or 0,
                "boll_mid": r[2] or 0,
                "boll_lower": r[3] or 0,
                "rsi6": r[4],
                "ma5": r[5],
                "ma10": r[6],
                "ma20": r[7],
                "volume_ratio": r[8],
                "vol_20": r[9],
                "atr_14": r[10],
                "prev_day_volume": prev_day_volume_map.get(str(r[0]), 0.0),
                "vol5_median_volume": vol5_median_volume_map.get(str(r[0]), 0.0),
            }
        try:
            provider.bulk_update_daily_factors(factors_map)
        except Exception:
            pass

        # Pool1 筹码特征链路：cyq_perf -> provider.bulk_update_chip_features
        chip_map: dict[str, dict] = {}
        try:
            with _data_ro_conn_ctx() as data_conn:
                try:
                    cyq_schema = data_conn.execute("PRAGMA table_info('cyq_perf')").fetchall()
                    cyq_cols = {str(r[1]).lower() for r in cyq_schema}
                except Exception:
                    cyq_cols = set()

                cost5_col = next((c for c in ("cost_5pct", "cost5") if c in cyq_cols), None)
                cost95_col = next((c for c in ("cost_95pct", "cost95") if c in cyq_cols), None)
                winner_col = next((c for c in ("winner_rate", "win_rate") if c in cyq_cols), None)

                if cost5_col and cost95_col and winner_col:
                    placeholders = ",".join(["?"] * len(ts_codes))
                    cyq_rows = data_conn.execute(
                        f"""
                        WITH latest AS (
                            SELECT ts_code, MAX(trade_date) AS max_date
                            FROM cyq_perf
                            WHERE ts_code IN ({placeholders})
                            GROUP BY ts_code
                        )
                        SELECT c.ts_code,
                               c.{cost5_col} AS cost_5pct,
                               c.{cost95_col} AS cost_95pct,
                               c.{winner_col} AS winner_rate
                        FROM cyq_perf c
                        JOIN latest l ON c.ts_code = l.ts_code AND c.trade_date = l.max_date
                        """,
                        ts_codes,
                    ).fetchall()
                    for ts_code, c5, c95, winner in cyq_rows:
                        try:
                            c5f = float(c5) if c5 is not None else 0.0
                            c95f = float(c95) if c95 is not None else 0.0
                            wf = float(winner) if winner is not None else 0.0
                            mid = (c95f + c5f) / 2.0
                            conc_pct = (abs(c95f - c5f) / abs(mid) * 100.0) if abs(mid) > 1e-9 else 0.0
                            chip_map[str(ts_code)] = {
                                "cost_5pct": c5f,
                                "cost_95pct": c95f,
                                "winner_rate": wf,
                                "chip_concentration_pct": conc_pct,
                            }
                        except Exception:
                            continue
        except Exception:
            chip_map = {}

        if chip_map:
            try:
                provider.bulk_update_chip_features(chip_map)
            except Exception:
                pass
        return len(factors_map)
    except Exception as e:
        logger.warning(f"refresh_daily_factors_cache failed: {e}")
        return 0
        


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


def _signal_history_dedup_seconds(pool_id: int, signal_type: str) -> int:
    sig = str(signal_type or "")
    if int(pool_id) == 1:
        try:
            return max(1, int(_P1_DEDUP_CFG.get(sig, 300) or 300))
        except Exception:
            return 300
    if int(pool_id) == 2:
        try:
            return max(1, int(_T0_DEDUP_CFG.get(sig, 120) or 120))
        except Exception:
            return 120
    return 300


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
            "by_channel": {},
            "by_source": {},
            "by_channel_source": {},
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
    if "channel" not in d.columns:
        d["channel"] = "unknown"
    if "signal_source" not in d.columns:
        d["signal_source"] = "unknown"
    d["channel"] = d["channel"].fillna("").astype(str)
    d["signal_source"] = d["signal_source"].fillna("").astype(str)
    d.loc[d["channel"].str.len() == 0, "channel"] = "unknown"
    d.loc[d["signal_source"].str.len() == 0, "signal_source"] = "unknown"

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

    by_channel: dict[str, dict] = {}
    channels = sorted({str(x) for x in d5["channel"].dropna().tolist()})
    for c in channels:
        pc = d5[d5["channel"] == c]
        a = agg(pc)
        by_channel[c] = {
            "count": a["count"],
            "precision_5m": a["precision"],
            "avg_ret_bps": a["avg_ret_bps"],
        }

    by_source: dict[str, dict] = {}
    sources = sorted({str(x) for x in d5["signal_source"].dropna().tolist()})
    for s in sources:
        ps = d5[d5["signal_source"] == s]
        a = agg(ps)
        by_source[s] = {
            "count": a["count"],
            "precision_5m": a["precision"],
            "avg_ret_bps": a["avg_ret_bps"],
        }

    by_channel_source: dict[str, dict] = {}
    grouped = d5.groupby(["channel", "signal_source"], dropna=False)
    for (c, s), part in grouped:
        cc = str(c or "unknown")
        ss = str(s or "unknown")
        a = agg(part)
        key = f"{cc}|{ss}"
        by_channel_source[key] = {
            "channel": cc,
            "signal_source": ss,
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
        "by_channel": by_channel,
        "by_source": by_source,
        "by_channel_source": by_channel_source,
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
    with _ro_conn_ctx() as conn:
        members = conn.execute(
            "SELECT ts_code FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
            [pool_id],
        ).fetchall()
        out: dict[str, list] = {}
        for (ts_code,) in members:
            rows = conn.execute(
                """
                SELECT ts, price, volume, amount
                FROM (
                    SELECT ts, price, volume, amount
                    FROM tick_history
                    WHERE ts_code = ?
                    ORDER BY ts DESC
                    LIMIT ?
                ) t
                ORDER BY ts ASC
                """,
                [ts_code, int(limit)],
            ).fetchall()
            if rows:
                out[ts_code] = [
                    {"time": int(r[0]), "price": r[1], "volume": r[2], "amount": r[3]}
                    for r in rows
                ]
        return out


def _start_tick_persistence() -> None:
    global _tick_persist_started
    if _tick_persist_started:
        return
    _tick_persist_started = True
    _start_db_writer()

    def _loop() -> None:
        logger.info("[Layer3] tick persistence thread started")
        provider = get_tick_provider()
        gm_persist_queue = None
        if getattr(provider, "name", "") == "gm":
            try:
                from backend.realtime.gm_tick import init_persist_queue
                gm_persist_queue = init_persist_queue(maxsize=100000)
                logger.info("[Layer3] gm persist queue consumer enabled")
            except Exception as e:
                logger.warning(f"[Layer3] gm persist queue init failed, fallback polling: {e}")
        while True:
            try:
                if gm_persist_queue is not None:
                    try:
                        kind, ts_code, item, extra = gm_persist_queue.get(timeout=1.0)
                    except _queue_mod.Empty:
                        continue
                    if not _is_market_open().get("is_open", False):
                        # Drain queue but skip persistence during non-trading periods.
                        continue
                    if kind == "tick":
                        row = _build_tick_row(str(ts_code), str((item or {}).get("name", "") or ""), item or {})
                        _enqueue_db_write("tick_rows", [row])
                    elif kind == "signal":
                        _enqueue_gm_signal_history(str(ts_code), item or {}, extra or [])
                    continue

                if not _is_market_open().get("is_open", False):
                    time.sleep(30.0)
                    continue
                with _ro_conn_ctx() as conn:
                    members = conn.execute("SELECT DISTINCT ts_code, name FROM monitor_pools").fetchall()
                if not members:
                    time.sleep(5.0)
                    continue
                rows = []
                for ts_code, name in members:
                    try:
                        t = provider.get_tick(ts_code)
                        rows.append(_build_tick_row(str(ts_code), str(name or ""), t))
                    except Exception:
                        pass
                if rows:
                    _enqueue_db_write("tick_rows", rows)
            except Exception as e:
                logger.warning(f"[Layer3] tick persistence error: {e}")
            time.sleep(0.05 if gm_persist_queue is not None else 5.0)

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
                with _ro_conn_ctx() as conn:
                    rows = conn.execute("SELECT DISTINCT ts_code FROM monitor_pools").fetchall()
                    ts_codes = [r[0] for r in rows]
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
                if not _is_market_open().get("is_open", False):
                    # Non-trading session: clear stale pending tasks to avoid
                    # cross-session carry-over and unnecessary IO.
                    pending.clear()
                    while True:
                        try:
                            _quality_track_queue.get_nowait()
                        except _queue_mod.Empty:
                            break
                    time.sleep(max(interval, 5.0))
                    continue
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
                    channel = str(task.get("channel", "pool2_t0") or "pool2_t0")
                    signal_source = str(task.get("signal_source", "runtime") or "runtime")
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
                                        channel,
                                        signal_source,
                                    )
                                )
                        except Exception:
                            pass
                        done.add(h)

                    if len(done) < len(horizons) and now < (trigger_time + max(horizons) + 60):
                        keep.append(task)

                pending = keep

                if rows_to_insert:
                    _enqueue_db_write("quality_rows", rows_to_insert)
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
                if not _is_market_open().get("is_open", False):
                    continue
                with _ro_conn_ctx() as conn:
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

                    drift_rows = []
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
                        drift_rows.append(
                            (
                                feature_name,
                                float(psi),
                                float(z),
                                0.01 if drifted else 0.5,
                                sev,
                                drifted,
                                float(precision_drop),
                                alerted,
                            )
                        )
                    if drift_rows:
                        _enqueue_db_write("drift_rows", drift_rows)
            except Exception as e:
                logger.error(f"[T0 drift] monitor error: {e}")

    threading.Thread(target=_loop, daemon=True, name="t0-drift-monitor").start()


def _save_signal_history(pool_id: int, evaluated: list) -> None:
    if not _is_market_open().get("is_open", False):
        return
    try:
        rows = _collect_signal_rows(pool_id, evaluated)
        if rows:
            _enqueue_db_write("signal_rows", rows)
    except Exception as e:
        logger.debug(f"save signal history failed: {e}")


def _build_member_data(conn: duckdb.DuckDBPyConnection, pool_id: int) -> list[dict]:
    members = conn.execute(
        "SELECT ts_code, name, industry FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
        [pool_id],
    ).fetchall()
    if not members:
        return []
    ts_codes = [str(m[0]) for m in members]
    daily_map: dict[str, dict] = {}
    chip_map: dict[str, dict] = {}
    prev_day_volume_map: dict[str, float] = {}
    vol5_median_volume_map: dict[str, float] = {}
    instrument_profile_map: dict[str, dict] = {}
    try:
        placeholders = ",".join(["?"] * len(ts_codes))
        with _data_ro_conn_ctx() as dconn:
            try:
                schema_rows = dconn.execute("PRAGMA table_info('stk_factor_pro')").fetchall()
                columns = {str(r[1]).lower() for r in schema_rows}
            except Exception:
                columns = set()
            ma20_expr = "f.ma_qfq_20" if "ma_qfq_20" in columns else "NULL"
            rows = dconn.execute(
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
                       {ma20_expr} AS ma20,
                       f.volume_ratio AS volume_ratio
                FROM stk_factor_pro f
                JOIN latest l ON f.ts_code = l.ts_code AND f.trade_date = l.max_date
                """,
                ts_codes,
                ).fetchall()
            try:
                daily_schema_rows = dconn.execute("PRAGMA table_info('daily')").fetchall()
                daily_cols = {str(r[1]).lower() for r in daily_schema_rows}
            except Exception:
                daily_cols = set()
            daily_vol_col = next((c for c in ("vol", "volume", "trade_vol", "total_vol") if c in daily_cols), None)
            daily_vol_scale = 100.0 if daily_vol_col == "vol" else 1.0
            if daily_vol_col:
                vol_rows = dconn.execute(
                    f"""
                    WITH ranked_daily AS (
                        SELECT ts_code,
                               {daily_vol_col} AS vol_raw,
                               ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                        FROM daily
                        WHERE ts_code IN ({placeholders})
                    ),
                    agg_daily AS (
                        SELECT ts_code,
                               MAX(CASE WHEN rn = 1 THEN vol_raw END) AS prev_day_volume,
                               median(vol_raw) AS vol5_median_volume
                        FROM ranked_daily
                        WHERE rn <= 5
                        GROUP BY ts_code
                    )
                    SELECT ts_code, prev_day_volume, vol5_median_volume
                    FROM agg_daily
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, prev_vol, med5_vol in vol_rows:
                    try:
                        base_vol = float(prev_vol) if prev_vol is not None else 0.0
                        prev_day_volume_map[str(ts_code)] = base_vol * daily_vol_scale
                    except Exception:
                        prev_day_volume_map[str(ts_code)] = 0.0
                    try:
                        base_med5 = float(med5_vol) if med5_vol is not None else 0.0
                        vol5_median_volume_map[str(ts_code)] = base_med5 * daily_vol_scale
                    except Exception:
                        vol5_median_volume_map[str(ts_code)] = 0.0
            # Optional: cyq_perf chip features for Pool1 consistency.
            try:
                cyq_schema = dconn.execute("PRAGMA table_info('cyq_perf')").fetchall()
                cyq_cols = {str(r[1]).lower() for r in cyq_schema}
            except Exception:
                cyq_cols = set()
            cost5_col = next((c for c in ("cost_5pct", "cost5") if c in cyq_cols), None)
            cost95_col = next((c for c in ("cost_95pct", "cost95") if c in cyq_cols), None)
            winner_col = next((c for c in ("winner_rate", "win_rate") if c in cyq_cols), None)
            if cost5_col and cost95_col and winner_col:
                cyq_rows = dconn.execute(
                    f"""
                    WITH latest AS (
                        SELECT ts_code, MAX(trade_date) AS max_date
                        FROM cyq_perf
                        WHERE ts_code IN ({placeholders})
                        GROUP BY ts_code
                    )
                    SELECT c.ts_code,
                           c.{cost5_col} AS cost_5pct,
                           c.{cost95_col} AS cost_95pct,
                           c.{winner_col} AS winner_rate
                    FROM cyq_perf c
                    JOIN latest l ON c.ts_code = l.ts_code AND c.trade_date = l.max_date
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, c5, c95, winner in cyq_rows:
                    try:
                        c5f = float(c5) if c5 is not None else 0.0
                        c95f = float(c95) if c95 is not None else 0.0
                        wf = float(winner) if winner is not None else 0.0
                        mid = (c95f + c5f) / 2.0
                        conc_pct = (abs(c95f - c5f) / abs(mid) * 100.0) if abs(mid) > 1e-9 else 0.0
                        chip_map[str(ts_code)] = {
                            "winner_rate": wf,
                            "chip_concentration_pct": conc_pct,
                        }
                    except Exception:
                        continue
            try:
                meta_rows = dconn.execute(
                    f"""
                    SELECT ts_code, name, market, list_date
                    FROM stock_basic
                    WHERE ts_code IN ({placeholders})
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, sb_name, market_name, list_date in meta_rows:
                    if infer_instrument_profile is None:
                        continue
                    profile = infer_instrument_profile(
                        str(ts_code or ""),
                        name=sb_name,
                        market_name=market_name,
                        list_date=list_date,
                        ts_epoch=int(time.time()),
                    )
                    profile["name"] = str(sb_name or "")
                    instrument_profile_map[str(ts_code)] = profile
            except Exception:
                instrument_profile_map = {}
        for r in rows:
            daily_map[str(r[0])] = {
                "boll_upper": r[1],
                "boll_mid": r[2],
                "boll_lower": r[3],
                "rsi6": r[4],
                "ma5": r[5],
                "ma10": r[6],
                "ma20": r[7],
                "volume_ratio": r[8],
                "prev_day_volume": prev_day_volume_map.get(str(r[0]), 0.0),
                "vol5_median_volume": vol5_median_volume_map.get(str(r[0]), 0.0),
            }
    except Exception:
        pass
    provider = get_tick_provider()
    out = []
    for ts_code, name, industry in members:
        tick = provider.get_tick(ts_code)
        daily = dict(daily_map.get(str(ts_code), {}))
        instrument_profile = dict(instrument_profile_map.get(str(ts_code), {}) or {})

        m = {
            "ts_code": ts_code,
            "name": name,
            "industry": str(industry or ""),
            "instrument_profile": instrument_profile,
            "market_name": instrument_profile.get("market_name"),
            "list_date": instrument_profile.get("list_date"),
            "board_segment": instrument_profile.get("board_segment"),
            "security_type": instrument_profile.get("security_type"),
            "risk_warning": bool(instrument_profile.get("risk_warning")),
            "listing_stage": instrument_profile.get("listing_stage"),
            "listing_days": instrument_profile.get("listing_days"),
            "price_limit_pct": instrument_profile.get("price_limit_pct"),
            "price": tick.get("price", 0),
            "pct_chg": tick.get("pct_chg", 0),
            **{k: (v if v is not None else 0) for k, v in daily.items()},
        }
        pace = _calc_intraday_volume_pace_member(
            tick=tick,
            prev_day_volume=m.get("prev_day_volume"),
            vol5_median_volume=m.get("vol5_median_volume"),
            pool_id=pool_id,
        )
        m["volume_pace_ratio"] = pace.get("ratio")
        m["volume_pace_ratio_prev"] = pace.get("ratio_prev")
        m["volume_pace_ratio_med5"] = pace.get("ratio_med5")
        m["volume_pace_state"] = pace.get("state")
        m["volume_pace_progress"] = pace.get("progress_ratio")
        m["volume_pace_baseline_volume"] = pace.get("baseline_volume")
        m["volume_pace_baseline_mode"] = pace.get("baseline_mode")
        m["cum_volume"] = pace.get("cum_volume")

        if pool_id == 1:
            bids = tick.get("bids") or []
            asks = tick.get("asks") or []
            bid_vol = sum(v for _, v in bids[:5]) if bids else 0
            ask_vol = sum(v for _, v in asks[:5]) if asks else 0
            pre_close = float(tick.get("pre_close", 0) or 0)
            m["bid_ask_ratio"] = round(bid_vol / ask_vol, 3) if ask_vol > 0 else None
            if pre_close > 0 and calc_theoretical_limits is not None:
                m["up_limit"], m["down_limit"] = calc_theoretical_limits(pre_close, instrument_profile.get("price_limit_pct"))
            else:
                m["up_limit"] = pre_close * 1.1 if pre_close > 0 else None
                m["down_limit"] = pre_close * 0.9 if pre_close > 0 else None
            prev_price = None
            try:
                prev_price = provider.get_prev_price(ts_code)
            except Exception:
                prev_price = None
            if prev_price is not None and prev_price > 0:
                m["prev_price"] = float(prev_price)
            else:
                m["prev_price"] = None
            try:
                pos = provider.get_pool1_position_state(ts_code)
            except Exception:
                pos = None
            if isinstance(pos, dict):
                status = str(pos.get("status") or "observe")
                m["pool1_position_status"] = status
                m["pool1_holding_days"] = float(pos.get("holding_days", 0.0) or 0.0)
                m["pool1_in_holding"] = bool(status == "holding")
            else:
                m["pool1_position_status"] = "observe"
                m["pool1_holding_days"] = 0.0
                m["pool1_in_holding"] = False
            chip = chip_map.get(str(ts_code), {})
            if chip:
                m["winner_rate"] = chip.get("winner_rate")
                m["chip_concentration_pct"] = chip.get("chip_concentration_pct")
            try:
                bars_1m = provider.get_cached_bars(ts_code)
                if not bars_1m:
                    bars_1m = mootdx_client.get_minute_bars(ts_code, 240)
                if bars_1m:
                    m["minute_bars_1m"] = bars_1m
                    rv, rinfo = sig.compute_pool1_resonance_60m(bars_1m, fallback=False)
                    if isinstance(rinfo, dict):
                        rinfo = dict(rinfo)
                        rinfo.setdefault("source", "1m_aggregate")
                    m["resonance_60m"] = bool(rv)
                    m["resonance_60m_info"] = rinfo
            except Exception:
                pass

        if pool_id == 2:
            try:
                bars = mootdx_client.get_minute_bars(ts_code, 240)
                if bars:
                    cum_amt = sum(b.get("amount", 0) for b in bars)
                    cum_vol = sum(b.get("volume", 0) for b in bars)
                    m["vwap"] = cum_amt / cum_vol if cum_vol > 0 else None
                    m["intraday_prices"] = [b.get("close", 0) for b in bars]
                    m["volume_pace_progress"] = m.get("volume_pace_progress")
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
    try:
        with _ro_conn_ctx() as conn:
            members_data = _build_member_data(conn, pool_id)
            if not members_data:
                return []
            evaluated = sig.evaluate_pool(pool_id, members_data)
            _save_signal_history(pool_id, evaluated)
            return evaluated
    except Exception as e:
        logger.error(f"evaluate signals internal failed pool={pool_id}: {e}")
        return []
    


def _evaluate_signals_fast_internal(pool_id: int, members: list, provider, tick_hint: Optional[dict] = None) -> list:
    allowed_types = POOL_SIGNAL_TYPES.get(pool_id, set())
    data = []
    hint = tick_hint or {}
    keep_pool1_until_eod = bool(POOL1_SIGNAL_CONFIG.get("keep_signals_until_eod", True))
    today_date = datetime.datetime.now().date()

    def _pool1_keep_today_signal(sig: dict) -> bool:
        if int(pool_id) != 1 or not keep_pool1_until_eod:
            return True
        if not isinstance(sig, dict):
            return False
        ts = sig.get("triggered_at")
        try:
            if isinstance(ts, datetime.datetime):
                sig_date = ts.date()
            elif isinstance(ts, datetime.date):
                sig_date = ts
            else:
                sig_date = datetime.datetime.fromtimestamp(int(float(ts or 0))).date()
            return sig_date == today_date
        except Exception:
            return False

    def _attach_pool1_position_fields(row: dict, code: str) -> None:
        if int(pool_id) != 1:
            return
        status = "observe"
        holding_days = 0.0
        try:
            pos = provider.get_pool1_position_state(code)
            if isinstance(pos, dict):
                status = str(pos.get("status") or "observe").strip().lower()
                if status not in ("holding", "observe"):
                    status = "observe"
                holding_days = float(pos.get("holding_days", 0.0) or 0.0)
        except Exception:
            pass
        row["pool1_position_status"] = status
        row["pool1_holding_days"] = round(max(0.0, holding_days), 4)
        row["pool1_in_holding"] = bool(status == "holding")

    for ts_code, name in members:
        entry = provider.get_cached_signals(ts_code)
        if entry is None:
            tick = hint.get(ts_code)
            if not tick:
                try:
                    tick = provider.get_cached_tick(ts_code)
                except Exception:
                    tick = None
            if not tick:
                if str(getattr(provider, "name", "")) == "gm":
                    tick = _empty_tick_payload(ts_code)
                else:
                    tick = provider.get_tick(ts_code)
            row = {
                "ts_code": ts_code,
                "name": name,
                "price": tick.get("price", 0),
                "pct_chg": tick.get("pct_chg", 0),
                "signals": [],
                "evaluated_at": 0,
            }
            _attach_pool1_position_fields(row, ts_code)
            data.append(row)
            continue
        e = dict(entry)
        e["name"] = name
        e["signals"] = [
            s for s in e.get("signals", [])
            if s.get("type") in allowed_types and _pool1_keep_today_signal(s)
        ]
        _attach_pool1_position_fields(e, ts_code)
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
    storage_check = {
        "expected": str(stats.get("storage_expected") or ("redis" if bool(stats.get("redis_enabled", False)) else "memory")),
        "source": str(stats.get("storage_source") or "memory"),
        "verified": bool(stats.get("storage_verified", False)),
        "degraded": bool(stats.get("storage_degraded", False)),
        "note": str(stats.get("storage_note") or ""),
        "redis_enabled": bool(stats.get("redis_enabled", False)),
        "redis_ready": bool(stats.get("redis_ready", False)),
    }
    return {
        "pool_id": 1,
        "provider": provider.name,
        "supported": True,
        "trade_date": stats.get("trade_date"),
        "updated_at": stats.get("updated_at"),
        "updated_at_iso": stats.get("updated_at_iso"),
        "storage_check": storage_check,
        "data": stats,
    }


@router.get("/pool/1/position_summary")
def pool1_position_summary():
    provider = get_tick_provider()
    with _ro_conn_ctx() as conn:
        rows = conn.execute(
            """
            SELECT ts_code, name
            FROM monitor_pools
            WHERE pool_id = 1
            ORDER BY sort_order, added_at
            """
        ).fetchall()
    total = len(rows)
    holding = 0
    observe = 0
    holding_days_arr: list[float] = []
    transitions_today = 0
    today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_ts = int(today_start.timestamp())
    holdings_detail: list[dict] = []
    for r in rows:
        ts_code = str(r[0] or "")
        name = str(r[1] or "")
        pos = {}
        try:
            got = provider.get_pool1_position_state(ts_code)
            if isinstance(got, dict):
                pos = got
        except Exception:
            pos = {}
        status = str(pos.get("status") or "observe").strip().lower()
        if status not in ("observe", "holding"):
            status = "observe"
        hd = float(pos.get("holding_days", 0.0) or 0.0)
        last_buy_at = int(pos.get("last_buy_at", 0) or 0)
        last_sell_at = int(pos.get("last_sell_at", 0) or 0)
        if last_buy_at >= today_ts:
            transitions_today += 1
        if last_sell_at >= today_ts:
            transitions_today += 1
        if status == "holding":
            holding += 1
            holding_days_arr.append(max(0.0, hd))
            holdings_detail.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "holding_days": round(max(0.0, hd), 4),
                    "last_buy_at": last_buy_at,
                    "last_buy_type": str(pos.get("last_buy_type") or ""),
                    "last_buy_price": float(pos.get("last_buy_price", 0.0) or 0.0),
                }
            )
        else:
            observe += 1
    holdings_detail.sort(key=lambda x: float(x.get("holding_days", 0.0) or 0.0), reverse=True)
    avg_holding_days = (sum(holding_days_arr) / len(holding_days_arr)) if holding_days_arr else 0.0
    max_holding_days = max(holding_days_arr) if holding_days_arr else 0.0
    storage = None
    try:
        s = provider.get_pool1_position_storage_status()
        if isinstance(s, dict):
            storage = s
    except Exception:
        storage = None
    return {
        "pool_id": 1,
        "provider": provider.name,
        "checked_at": datetime.datetime.now().isoformat(),
        "summary": {
            "member_count": int(total),
            "holding_count": int(holding),
            "observe_count": int(observe),
            "holding_ratio": round((holding / total), 4) if total > 0 else 0.0,
            "avg_holding_days": round(avg_holding_days, 4),
            "max_holding_days": round(max_holding_days, 4),
            "transitions_today": int(transitions_today),
        },
        "storage": storage or {},
        "holdings": holdings_detail[:50],
    }


@router.post("/refresh_daily_cache")
def refresh_daily_cache(pool_id: Optional[int] = None):
    n = _refresh_daily_factors_cache(pool_id)
    return {"refreshed": n, "pool_id": pool_id}


@router.get("/pools")
def list_pools():
    with _ro_conn_ctx() as conn:
        result = []
        for pid, info in POOLS.items():
            cnt = conn.execute("SELECT COUNT(*) FROM monitor_pools WHERE pool_id = ?", [pid]).fetchone()[0]
            result.append({**info, "member_count": cnt})
        return {"data": result}


class MemberItem(BaseModel):
    ts_code: str
    name: str
    industry: Optional[str] = None
    note: Optional[str] = None


@router.get("/pool/{pool_id}/members")
def get_members(pool_id: int):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    with _ro_conn_ctx() as conn:
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
    with _ro_conn_ctx() as conn:
        members = conn.execute(
            "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
            [pool_id],
        ).fetchall()
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


def _load_indices_snapshot(force_refresh: bool = False) -> list[dict]:
    global _indices_cache_data, _indices_cache_at
    now = time.time()
    ttl = max(0.2, float(_INDICES_CACHE_TTL_SEC))
    if not force_refresh:
        with _indices_cache_lock:
            if _indices_cache_data and (now - float(_indices_cache_at)) < ttl:
                return list(_indices_cache_data)

    data = []
    now_ts = int(now)
    for ts_code, name in INDICES:
        try:
            source = "mootdx_index"
            try:
                t = mootdx_client.get_index_tick(ts_code)
            except Exception:
                t = {}
                source = "index_fetch_error"

            price = float(t.get("price", 0) or 0)
            pre_close = float(t.get("pre_close", 0) or 0)
            raw_pct_chg = float(t.get("pct_chg", 0) or 0)
            valid = price > 0 and pre_close > 0

            # Avoid stock/index code collision fallback; prefer last-good index cache.
            if not valid:
                cached = _INDEX_LAST_GOOD.get(ts_code)
                if cached and float(cached.get("price", 0) or 0) > 0 and float(cached.get("pre_close", 0) or 0) > 0:
                    t = dict(cached)
                    price = float(t.get("price", 0) or 0)
                    pre_close = float(t.get("pre_close", 0) or 0)
                    raw_pct_chg = float(t.get("pct_chg", 0) or 0)
                    source = "cache_last_good"
                    valid = True

            if pre_close <= 0 and price > 0 and abs(raw_pct_chg) > 1e-9 and abs(raw_pct_chg) < 90:
                try:
                    pre_close = price / (1.0 + raw_pct_chg / 100.0)
                except Exception:
                    pre_close = 0.0
            pct_chg = ((price - pre_close) / pre_close * 100) if (price > 0 and pre_close > 0) else raw_pct_chg
            updated_at = int(t.get("timestamp", 0) or 0)
            if updated_at <= 0:
                updated_at = now_ts

            if price > 0 and pre_close > 0:
                _INDEX_LAST_GOOD[ts_code] = {
                    "price": price,
                    "pre_close": pre_close,
                    "pct_chg": pct_chg,
                    "amount": float(t.get("amount", 0) or 0),
                    "timestamp": updated_at,
                    "is_mock": bool(t.get("is_mock", False)),
                }

            data.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "price": price,
                    "pre_close": pre_close,
                    "pct_chg": pct_chg,
                    "amount": t.get("amount", 0),
                    "is_mock": t.get("is_mock", False),
                    "source": source,
                    "updated_at": updated_at,
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
                    "source": "error_fallback",
                    "updated_at": int(time.time()),
                }
            )
    with _indices_cache_lock:
        _indices_cache_data = list(data)
        _indices_cache_at = float(time.time())
    return data


@router.get("/indices")
def indices():
    return {"data": _load_indices_snapshot(force_refresh=False)}


@router.get("/pool/{pool_id}/signal_history")
def get_signal_history(
    pool_id: int,
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(200, ge=1, le=2000),
):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    since_dt = datetime.datetime.now() - datetime.timedelta(hours=int(hours))
    with _ro_conn_ctx() as conn:
        rows = conn.execute(
            """
            SELECT id, pool_id, ts_code, name, signal_type, channel, signal_source, strength, message, price, pct_chg, triggered_at
            FROM signal_history
            WHERE pool_id = ?
              AND triggered_at >= ?
            ORDER BY triggered_at DESC
            LIMIT ?
            """,
            [pool_id, since_dt, int(limit)],
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
                    "channel": r[5],
                    "signal_source": r[6],
                    "strength": r[7],
                    "message": r[8],
                    "price": r[9],
                    "pct_chg": r[10],
                    "triggered_at": r[11].isoformat() if hasattr(r[11], "isoformat") else str(r[11]),
                }
            )
        return {"pool_id": pool_id, "hours": hours, "limit": limit, "count": len(data), "data": data}


@router.get("/pool/2/quality_summary")
def get_t0_quality_summary(hours: int = Query(24, ge=1, le=168)):
    with _ro_conn_ctx() as conn:
        h = int(hours)
        rows = conn.execute(
            f"""
            SELECT signal_type, market_phase, eval_horizon_sec, ret_bps, mfe_bps, mae_bps, direction_correct,
                   COALESCE(channel, '') AS channel,
                   COALESCE(signal_source, '') AS signal_source
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
                "channel",
                "signal_source",
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


@router.get("/pool/2/drift_status")
def get_t0_drift_status(days: int = Query(3, ge=1, le=30)):
    with _ro_conn_ctx() as conn:
        d = int(days)
        rows = conn.execute(
            f"""
            SELECT checked_at, feature_name, psi, ks_stat, ks_p, severity, drifted, precision_drop, alerted
            FROM t0_feature_drift
            WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '{d} days'
            ORDER BY checked_at DESC
            """
        ).fetchall()
        split_rows = conn.execute(
            f"""
            WITH recent AS (
                SELECT
                    COALESCE(channel, 'unknown') AS channel,
                    COALESCE(signal_source, 'unknown') AS signal_source,
                    COUNT(*) AS recent_cnt,
                    AVG(CASE WHEN direction_correct THEN 1.0 ELSE 0.0 END) AS recent_precision,
                    AVG(ret_bps) AS recent_ret_bps
                FROM t0_signal_quality
                WHERE eval_horizon_sec = 300
                  AND created_at >= CURRENT_TIMESTAMP - INTERVAL '{d} days'
                GROUP BY 1,2
            ),
            baseline AS (
                SELECT
                    COALESCE(channel, 'unknown') AS channel,
                    COALESCE(signal_source, 'unknown') AS signal_source,
                    COUNT(*) AS base_cnt,
                    AVG(CASE WHEN direction_correct THEN 1.0 ELSE 0.0 END) AS base_precision,
                    AVG(ret_bps) AS base_ret_bps
                FROM t0_signal_quality
                WHERE eval_horizon_sec = 300
                  AND created_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '7 days'
                                      AND CURRENT_TIMESTAMP - INTERVAL '3 days'
                GROUP BY 1,2
            )
            SELECT
                COALESCE(r.channel, b.channel) AS channel,
                COALESCE(r.signal_source, b.signal_source) AS signal_source,
                COALESCE(r.recent_cnt, 0) AS recent_cnt,
                r.recent_precision,
                r.recent_ret_bps,
                COALESCE(b.base_cnt, 0) AS base_cnt,
                b.base_precision,
                b.base_ret_bps
            FROM recent r
            FULL OUTER JOIN baseline b
              ON r.channel = b.channel
             AND r.signal_source = b.signal_source
            ORDER BY recent_cnt DESC, base_cnt DESC, channel, signal_source
            """
        ).fetchall()
        quality_split = []
        for sr in split_rows:
            channel = str(sr[0] or "unknown")
            signal_source = str(sr[1] or "unknown")
            recent_cnt = int(sr[2] or 0)
            recent_precision = _round_or_none(sr[3], 4)
            recent_ret_bps = _round_or_none(sr[4], 2)
            base_cnt = int(sr[5] or 0)
            base_precision = _round_or_none(sr[6], 4)
            base_ret_bps = _round_or_none(sr[7], 2)
            precision_drop = None
            if base_precision is not None and recent_precision is not None:
                precision_drop = _round_or_none(base_precision - recent_precision, 4)
            quality_split.append(
                {
                    "channel": channel,
                    "signal_source": signal_source,
                    "recent_count": recent_cnt,
                    "recent_precision_5m": recent_precision,
                    "recent_avg_ret_bps": recent_ret_bps,
                    "baseline_count": base_cnt,
                    "baseline_precision_5m": base_precision,
                    "baseline_avg_ret_bps": base_ret_bps,
                    "precision_drop": precision_drop,
                }
            )
        if not rows:
            return {
                "days": d,
                "total_checks": 0,
                "has_active_alert": False,
                "latest_checked_at": None,
                "latest": {},
                "alerts": 0,
                "quality_split_by_channel_source": quality_split,
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
            "quality_split_by_channel_source": quality_split,
        }


@router.get("/runtime/t0_reverse_replay")
def runtime_t0_reverse_replay(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(120, ge=1, le=1000),
    fee_bps: float = Query(16.0, ge=0, le=200),
):
    h = int(hours)
    lim = int(limit)
    fee = float(fee_bps)
    since_dt = datetime.datetime.now() - datetime.timedelta(hours=h)
    with _ro_conn_ctx() as conn:
        hist_rows = conn.execute(
            """
            SELECT id, ts_code, name, signal_type, channel, signal_source, strength, message, price, pct_chg, details_json, triggered_at
            FROM signal_history
            WHERE pool_id = 2
              AND signal_type = 'reverse_t'
              AND triggered_at >= ?
            ORDER BY triggered_at DESC
            LIMIT ?
            """,
            [since_dt, lim],
        ).fetchall()
        q_rows = conn.execute(
            """
            SELECT ts_code, signal_type, direction, trigger_time, trigger_price, eval_price,
                   ret_bps, mfe_bps, mae_bps, direction_correct, market_phase,
                   COALESCE(channel, '') AS channel,
                   COALESCE(signal_source, '') AS signal_source,
                   created_at
            FROM t0_signal_quality
            WHERE signal_type = 'reverse_t'
              AND eval_horizon_sec = 300
              AND created_at >= ?
            ORDER BY trigger_time DESC
            """,
            [since_dt],
        ).fetchall()

    quality_by_code: dict[str, list[dict]] = {}
    for r in q_rows:
        item = {
            "ts_code": str(r[0] or ""),
            "signal_type": str(r[1] or ""),
            "direction": str(r[2] or ""),
            "trigger_time": _to_epoch_seconds(r[3]),
            "trigger_time_iso": _to_iso_datetime_text(r[3]),
            "trigger_price": float(r[4] or 0),
            "eval_price": float(r[5] or 0),
            "ret_bps": float(r[6] or 0),
            "mfe_bps": float(r[7] or 0),
            "mae_bps": float(r[8] or 0),
            "direction_correct": bool(r[9]),
            "market_phase": str(r[10] or ""),
            "channel": str(r[11] or ""),
            "signal_source": str(r[12] or ""),
            "created_at": r[13].isoformat() if hasattr(r[13], "isoformat") else str(r[13]),
            "_used": False,
        }
        quality_by_code.setdefault(item["ts_code"], []).append(item)

    def _classify_reverse_row(match: Optional[dict]) -> tuple[str, str]:
        if not isinstance(match, dict):
            return "pending", "等待5分钟质量评估"
        ret_bps = float(match.get("ret_bps", 0.0) or 0.0)
        direction_correct = bool(match.get("direction_correct", False))
        if (not direction_correct) or ret_bps < 0:
            return "bad_sell", "卖出后价格继续上行，疑似误卖/漏杀"
        if ret_bps < fee:
            return "weak_edge", "方向虽对但边际不足，可能覆盖不了手续费"
        return "good_hit", "反T命中，5分钟收益覆盖成本"

    rows: list[dict] = []
    summary = {
        "signals": 0,
        "matched_quality": 0,
        "pending": 0,
        "bad_sell": 0,
        "weak_edge": 0,
        "good_hit": 0,
    }
    tolerance_sec = 180.0
    for r in hist_rows:
        hist_id = r[0]
        ts_code = str(r[1] or "")
        name = str(r[2] or "")
        details_raw = r[10]
        triggered_at = r[11]
        trigger_dt = triggered_at if isinstance(triggered_at, datetime.datetime) else None
        trigger_ts = trigger_dt.timestamp() if trigger_dt is not None else 0.0
        history_snapshot = None
        if isinstance(details_raw, str) and details_raw:
            try:
                parsed_snapshot = json.loads(details_raw)
                if isinstance(parsed_snapshot, dict):
                    history_snapshot = parsed_snapshot
            except Exception:
                history_snapshot = None
        match = None
        best_diff = None
        for cand in quality_by_code.get(ts_code, []):
            if cand.get("_used"):
                continue
            diff = abs(float(cand.get("trigger_time", 0) or 0) - trigger_ts)
            if diff > tolerance_sec:
                continue
            if best_diff is None or diff < best_diff:
                best_diff = diff
                match = cand
        if isinstance(match, dict):
            match["_used"] = True

        classification, classification_reason = _classify_reverse_row(match)
        summary["signals"] += 1
        if classification in summary:
            summary[classification] += 1
        if isinstance(match, dict):
            summary["matched_quality"] += 1

        rows.append(
            {
                "id": hist_id,
                "ts_code": ts_code,
                "name": name,
                "signal_type": str(r[3] or ""),
                "channel": str(r[4] or ""),
                "signal_source": str(r[5] or ""),
                "strength": float(r[6] or 0),
                "message": str(r[7] or ""),
                "price": float(r[8] or 0),
                "pct_chg": float(r[9] or 0),
                "history_snapshot": history_snapshot,
                "triggered_at": trigger_dt.isoformat() if trigger_dt else str(triggered_at),
                "quality": None if not isinstance(match, dict) else {
                    "trigger_time": _to_epoch_seconds(match.get("trigger_time", 0)),
                    "trigger_time_iso": _to_iso_datetime_text(match.get("trigger_time", "")),
                    "trigger_price": float(match.get("trigger_price", 0) or 0),
                    "eval_price": float(match.get("eval_price", 0) or 0),
                    "ret_bps": float(match.get("ret_bps", 0) or 0),
                    "mfe_bps": float(match.get("mfe_bps", 0) or 0),
                    "mae_bps": float(match.get("mae_bps", 0) or 0),
                    "direction_correct": bool(match.get("direction_correct", False)),
                    "market_phase": str(match.get("market_phase", "") or ""),
                    "channel": str(match.get("channel", "") or ""),
                    "signal_source": str(match.get("signal_source", "") or ""),
                    "created_at": str(match.get("created_at", "") or ""),
                    "time_diff_sec": round(float(best_diff or 0.0), 3),
                },
                "classification": classification,
                "classification_reason": classification_reason,
                "fee_bps": fee,
            }
        )

    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "hours": h,
        "limit": lim,
        "fee_bps": fee,
        "summary": summary,
        "count": len(rows),
        "rows": rows,
    }


def _nan_safe_json(obj):
    return json.dumps(obj, ensure_ascii=False, allow_nan=False)


def _empty_tick_payload(ts_code: str) -> dict:
    return {
        "ts_code": ts_code,
        "name": "",
        "price": 0.0,
        "open": 0.0,
        "high": 0.0,
        "low": 0.0,
        "pre_close": 0.0,
        "volume": 0,
        "amount": 0.0,
        "pct_chg": 0.0,
        "timestamp": int(time.time()),
        "bids": [],
        "asks": [],
        "is_mock": False,
        "data_unavailable": True,
    }


def _tick_ws_fingerprint(t: Optional[dict]) -> tuple:
    if not isinstance(t, dict):
        return (0, 0, 0, 0, 0, 0, 0)
    return (
        float(t.get("price", 0) or 0),
        float(t.get("pct_chg", 0) or 0),
        int(t.get("volume", 0) or 0),
        float(t.get("amount", 0) or 0),
        float(t.get("open", 0) or 0),
        float(t.get("high", 0) or 0),
        float(t.get("low", 0) or 0),
    )


def _signal_item_ws_fingerprint(s: Optional[dict]) -> tuple:
    if not isinstance(s, dict):
        return ()
    return (
        str(s.get("type") or ""),
        bool(s.get("has_signal", False)),
        str(s.get("direction") or ""),
        float(s.get("strength", 0) or 0),
        float(s.get("current_strength", 0) or 0),
        str(s.get("state") or ""),
        int(s.get("triggered_at", 0) or 0),
        int(s.get("age_sec", 0) or 0),
        str(s.get("expire_reason") or ""),
        float(s.get("price", 0) or 0),
        str(s.get("message") or ""),
        str(s.get("channel") or ""),
        str(s.get("signal_source") or ""),
    )


def _signal_row_ws_fingerprint(row: Optional[dict]) -> tuple:
    if not isinstance(row, dict):
        return ()
    sigs = row.get("signals") or []
    fp_list = []
    if isinstance(sigs, list):
        for s in sigs:
            fp_list.append(_signal_item_ws_fingerprint(s))
    fp_list = sorted(fp_list)
    return (
        str(row.get("ts_code") or ""),
        float(row.get("price", 0) or 0),
        float(row.get("pct_chg", 0) or 0),
        tuple(fp_list),
    )


def _txn_row_ws_fingerprint(txns: Optional[list]) -> tuple:
    arr = txns if isinstance(txns, list) else []
    out = []
    for t in arr:
        if not isinstance(t, dict):
            continue
        out.append(
            (
                str(t.get("time") or ""),
                float(t.get("price", 0) or 0),
                int(t.get("volume", 0) or 0),
                int(t.get("direction", 0) or 0),
            )
        )
    return tuple(out)


def _is_db_lock_error(exc: Exception) -> bool:
    """Best-effort DuckDB file lock detection on Windows."""
    return _is_duckdb_lock_message(str(exc))


@router.get("/runtime/db_metrics")
def runtime_db_metrics():
    m = _metrics_snapshot()
    m["db_realtime_path"] = _REALTIME_DB
    m["db_data_path"] = _DATA_DB
    m["db_read_pool_size"] = int(_db_read_pool.qsize())
    m["db_read_pool_capacity"] = int(_DB_READ_POOL_SIZE)
    m["db_data_read_pool_size"] = int(_db_data_read_pool.qsize())
    m["db_data_read_pool_capacity"] = int(_DB_DATA_READ_POOL_SIZE)
    m["db_write_queue_size"] = int(_db_write_queue.qsize())
    m["db_write_queue_capacity"] = 200000
    m["db_writer_flush_sec"] = float(_DB_WRITER_FLUSH_SEC)
    m["db_writer_max_batch"] = int(_DB_WRITER_MAX_BATCH)
    m["db_writer_redis_enabled"] = bool(_DB_WRITER_REDIS_ENABLED and _redis_mod is not None)
    if m["db_writer_redis_enabled"]:
        cli = _get_db_writer_redis()
        m["db_writer_redis_ready"] = bool(cli is not None)
        if cli is not None:
            try:
                m["db_writer_redis_queue_size"] = int(cli.llen(_DB_WRITER_REDIS_KEY))
            except Exception:
                m["db_writer_redis_queue_size"] = None
    else:
        m["db_writer_redis_ready"] = False
        m["db_writer_redis_queue_size"] = None
    m["runtime_state_redis_enabled"] = bool(_RUNTIME_STATE_REDIS_ENABLED and _redis_mod is not None)
    if m["runtime_state_redis_enabled"]:
        cli2 = _get_runtime_state_redis()
        m["runtime_state_redis_ready"] = bool(cli2 is not None)
    else:
        m["runtime_state_redis_ready"] = False
    m["threshold_closed_loop_enabled"] = bool(_CLOSED_LOOP_ENABLED)
    m["threshold_closed_loop_started"] = bool(_threshold_closed_loop_started)
    try:
        p = get_tick_provider()
        fn = getattr(p, "get_signal_perf_stats", None)
        if callable(fn):
            perf = fn() or {}
            if isinstance(perf, dict) and perf:
                for k, v in perf.items():
                    m[f"signal_perf_{k}"] = v
    except Exception:
        pass
    _runtime_state_write_metrics(m)
    return {"data": m}


@router.get("/runtime/t0_quality_live")
def runtime_t0_quality_live(
    trade_date: Optional[str] = Query(None, description="YYYYMMDD, default today"),
    signal_type: Optional[str] = Query(None, description="positive_t|reverse_t"),
    horizon_sec: Optional[int] = Query(None, description="e.g. 60/180/300"),
    window_limit: int = Query(50, ge=1, le=500),
):
    td = trade_date.strip() if isinstance(trade_date, str) and trade_date.strip() else _trade_date_from_dt()
    sig = signal_type.strip() if isinstance(signal_type, str) and signal_type.strip() else None
    hz = int(horizon_sec) if isinstance(horizon_sec, int) else None
    wlim = int(window_limit) if isinstance(window_limit, int) else 50
    wlim = max(1, min(500, wlim))
    cli = _get_runtime_state_redis()
    if cli is None:
        return {
            "redis_ready": False,
            "trade_date": td,
            "storage_source": "memory",
            "aggregate": {},
            "window": {"count": 0, "items": []},
        }

    k_agg = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:t0_quality:agg:{td}"
    agg_raw = {}
    try:
        agg_raw = cli.hgetall(k_agg) or {}
    except Exception:
        agg_raw = {}

    def _to_i(x):
        try:
            return int(float(x))
        except Exception:
            return 0

    def _to_f(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    total = _to_i(agg_raw.get("total"))
    correct = _to_i(agg_raw.get("correct"))
    ret_sum = _to_f(agg_raw.get("ret_bps_sum"))
    mfe_sum = _to_f(agg_raw.get("mfe_bps_sum"))
    mae_sum = _to_f(agg_raw.get("mae_bps_sum"))
    aggregate = {
        "total": total,
        "correct": correct,
        "precision": round((correct / total), 4) if total > 0 else None,
        "ret_bps_avg": round((ret_sum / total), 2) if total > 0 else None,
        "mfe_bps_avg": round((mfe_sum / total), 2) if total > 0 else None,
        "mae_bps_avg": round((mae_sum / total), 2) if total > 0 else None,
        "updated_at": agg_raw.get("updated_at"),
    }
    by_channel: dict[str, dict] = {}
    by_source: dict[str, dict] = {}
    by_channel_source: dict[str, dict] = {}
    for k in list(agg_raw.keys()):
        if isinstance(k, str) and k.startswith("c:") and k.endswith(":total") and ":src:" not in k:
            channel = k[2:-6]
            c_total = _to_i(agg_raw.get(k))
            c_correct = _to_i(agg_raw.get(f"c:{channel}:correct"))
            by_channel[channel] = {
                "total": c_total,
                "correct": c_correct,
                "precision": round((c_correct / c_total), 4) if c_total > 0 else None,
            }
        if isinstance(k, str) and k.startswith("src:") and k.endswith(":total"):
            source = k[4:-6]
            s_total = _to_i(agg_raw.get(k))
            s_correct = _to_i(agg_raw.get(f"src:{source}:correct"))
            by_source[source] = {
                "total": s_total,
                "correct": s_correct,
                "precision": round((s_correct / s_total), 4) if s_total > 0 else None,
            }
        if isinstance(k, str) and k.startswith("c:") and ":src:" in k and k.endswith(":total"):
            mid = k[2:-6]
            idx = mid.find(":src:")
            if idx > 0:
                channel = mid[:idx]
                source = mid[idx + 5 :]
                cs_total = _to_i(agg_raw.get(k))
                cs_correct = _to_i(agg_raw.get(f"c:{channel}:src:{source}:correct"))
                by_channel_source[f"{channel}|{source}"] = {
                    "channel": channel,
                    "signal_source": source,
                    "total": cs_total,
                    "correct": cs_correct,
                    "precision": round((cs_correct / cs_total), 4) if cs_total > 0 else None,
                }
    if by_channel:
        aggregate["by_channel"] = by_channel
    if by_source:
        aggregate["by_source"] = by_source
    if by_channel_source:
        aggregate["by_channel_source"] = by_channel_source

    if isinstance(hz, int):
        h_total = _to_i(agg_raw.get(f"h:{hz}:total"))
        h_correct = _to_i(agg_raw.get(f"h:{hz}:correct"))
        aggregate["horizon"] = {
            "sec": int(hz),
            "total": h_total,
            "correct": h_correct,
            "precision": round((h_correct / h_total), 4) if h_total > 0 else None,
        }
    if isinstance(sig, str) and sig:
        s_total = _to_i(agg_raw.get(f"s:{sig}:total"))
        s_correct = _to_i(agg_raw.get(f"s:{sig}:correct"))
        aggregate["signal_type"] = {
            "name": sig,
            "total": s_total,
            "correct": s_correct,
            "precision": round((s_correct / s_total), 4) if s_total > 0 else None,
        }
        if isinstance(hz, int):
            sh_total = _to_i(agg_raw.get(f"s:{sig}:h:{hz}:total"))
            sh_correct = _to_i(agg_raw.get(f"s:{sig}:h:{hz}:correct"))
            aggregate["signal_type_horizon"] = {
                "name": sig,
                "sec": int(hz),
                "total": sh_total,
                "correct": sh_correct,
                "precision": round((sh_correct / sh_total), 4) if sh_total > 0 else None,
            }

    items = []
    if sig and isinstance(hz, int):
        k_win = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:t0_quality:window:{td}:{sig}:{hz}"
        try:
            raws = cli.lrange(k_win, 0, max(0, int(wlim) - 1))
            for raw in raws or []:
                try:
                    items.append(json.loads(raw))
                except Exception:
                    continue
        except Exception:
            items = []

    return {
        "redis_ready": True,
        "trade_date": td,
        "storage_source": "redis",
        "window_size": int(_RUNTIME_T0_QUALITY_WINDOW_SIZE),
        "aggregate": aggregate,
        "window": {
            "count": len(items),
            "items": items,
        },
    }


@router.get("/runtime/threshold_calibration_snapshot")
def runtime_threshold_calibration_snapshot():
    cli = _get_runtime_state_redis()
    key_latest = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:threshold:calibration:latest"
    if cli is not None:
        try:
            raw = cli.get(key_latest)
            if raw:
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    payload["storage_source"] = "redis"
                    return payload
        except Exception:
            pass
    payload = dict(_threshold_calibration_last) if isinstance(_threshold_calibration_last, dict) else {}
    if not payload:
        return {
            "ok": False,
            "checked_at": datetime.datetime.now().isoformat(),
            "message": "threshold calibration snapshot not ready",
            "storage_source": "memory",
        }
    payload["storage_source"] = "memory"
    return payload


def _table_probe(conn: duckdb.DuckDBPyConnection, table_name: str) -> tuple[bool, Optional[str]]:
    try:
        conn.execute(f'SELECT 1 FROM "{table_name}" LIMIT 1').fetchone()
        return True, None
    except Exception as e:
        return False, str(e)


@router.get("/runtime/selfcheck")
def runtime_selfcheck():
    now = datetime.datetime.now()
    checks: list[dict] = []
    ok_all = True

    rt_conn = None
    data_conn = None
    try:
        try:
            rt_conn = _get_conn()
            checks.append({"name": "realtime_db_open", "ok": True, "db": _REALTIME_DB})
        except Exception as e:
            ok_all = False
            checks.append({"name": "realtime_db_open", "ok": False, "db": _REALTIME_DB, "error": str(e)})

        try:
            data_conn = open_duckdb_conn(_DATA_DB, retries=2, base_sleep_sec=0.05)
            checks.append({"name": "data_db_open", "ok": True, "db": _DATA_DB})
        except Exception as e:
            ok_all = False
            checks.append({"name": "data_db_open", "ok": False, "db": _DATA_DB, "error": str(e)})

        if rt_conn is not None:
            for t in ("monitor_pools", "tick_history", "signal_history", "t0_signal_quality", "t0_feature_drift"):
                ok, err = _table_probe(rt_conn, t)
                checks.append({"name": f"realtime_table:{t}", "ok": ok, "error": err})
                if not ok:
                    ok_all = False
            try:
                c = rt_conn.execute("SELECT COUNT(*) FROM monitor_pools").fetchone()
                checks.append({"name": "realtime_pool_member_count", "ok": True, "value": int(c[0] or 0)})
            except Exception as e:
                ok_all = False
                checks.append({"name": "realtime_pool_member_count", "ok": False, "error": str(e)})

        if data_conn is not None:
            for t in ("stock_basic", "daily", "daily_basic", "stk_factor_pro", "trade_cal"):
                ok, err = _table_probe(data_conn, t)
                checks.append({"name": f"data_table:{t}", "ok": ok, "error": err})
                if not ok:
                    ok_all = False
            try:
                row = data_conn.execute("SELECT MAX(trade_date) FROM stk_factor_pro").fetchone()
                latest = row[0] if row else None
                checks.append(
                    {
                        "name": "data_latest_factor_date",
                        "ok": bool(latest),
                        "value": (latest.isoformat() if hasattr(latest, "isoformat") else str(latest)) if latest else None,
                    }
                )
            except Exception as e:
                ok_all = False
                checks.append({"name": "data_latest_factor_date", "ok": False, "error": str(e)})

        market = _is_market_open()
        checks.append({"name": "market_status", "ok": True, "value": market})

        return {
            "ok": ok_all,
            "checked_at": now.isoformat(),
            "checks": checks,
        }
    finally:
        if rt_conn is not None:
            try:
                rt_conn.close()
            except Exception:
                pass
        if data_conn is not None:
            try:
                data_conn.close()
            except Exception:
                pass


@router.get("/runtime/channel_split_stats")
def runtime_channel_split_stats(hours: int = Query(24, ge=1, le=240)):
    """
    统计近 N 小时 Pool1/Pool2 的通道分布，检测是否存在串池污染。
    """
    since_ts = int(time.time()) - int(hours) * 3600
    since_dt = datetime.datetime.fromtimestamp(since_ts)
    rows_out: list[dict] = []
    conflicts: list[dict] = []
    try:
        with _ro_conn_ctx() as conn:
            rows = conn.execute(
                """
                SELECT
                    pool_id,
                    COALESCE(channel, '') AS channel,
                    signal_type,
                    COUNT(*) AS cnt,
                    COUNT(DISTINCT ts_code) AS symbols,
                    MIN(triggered_at) AS min_ts,
                    MAX(triggered_at) AS max_ts
                FROM signal_history
                WHERE triggered_at >= ?
                GROUP BY 1,2,3
                ORDER BY pool_id, channel, cnt DESC
                """,
                [since_dt],
            ).fetchall()
            src_rows = conn.execute(
                """
                SELECT
                    pool_id,
                    COALESCE(channel, '') AS channel,
                    COALESCE(signal_source, '') AS signal_source,
                    COUNT(*) AS cnt
                FROM signal_history
                WHERE triggered_at >= ?
                GROUP BY 1,2,3
                ORDER BY pool_id, channel, cnt DESC
                """,
                [since_dt],
            ).fetchall()
    except Exception as e:
        return {
            "ok": False,
            "checked_at": datetime.datetime.now().isoformat(),
            "hours": int(hours),
            "error": str(e),
        }

    for r in rows:
        pool_id = int(r[0] or 0)
        channel = str(r[1] or "")
        signal_type = str(r[2] or "")
        cnt = int(r[3] or 0)
        symbols = int(r[4] or 0)
        min_dt = r[5]
        max_dt = r[6]
        expected_channel = "pool1_timing" if pool_id == 1 else ("pool2_t0" if pool_id == 2 else "")
        conflict = bool(expected_channel and channel and channel != expected_channel)
        item = {
            "pool_id": pool_id,
            "channel": channel,
            "expected_channel": expected_channel,
            "signal_type": signal_type,
            "count": cnt,
            "symbols": symbols,
            "first_at": min_dt.isoformat() if hasattr(min_dt, "isoformat") else (str(min_dt) if min_dt else None),
            "last_at": max_dt.isoformat() if hasattr(max_dt, "isoformat") else (str(max_dt) if max_dt else None),
            "channel_conflict": conflict,
        }
        rows_out.append(item)
        if conflict:
            conflicts.append(item)

    source_rows = [
        {
            "pool_id": int(r[0] or 0),
            "channel": str(r[1] or ""),
            "signal_source": str(r[2] or ""),
            "count": int(r[3] or 0),
        }
        for r in src_rows
    ]

    return {
        "ok": len(conflicts) == 0,
        "checked_at": datetime.datetime.now().isoformat(),
        "hours": int(hours),
        "summary": {
            "groups": len(rows_out),
            "source_groups": len(source_rows),
            "conflicts": len(conflicts),
        },
        "conflicts": conflicts,
        "rows": rows_out,
        "source_rows": source_rows,
    }


@router.get("/runtime/fast_slow_diff")
def runtime_fast_slow_diff(
    pool_id: int = Query(1, ge=1, le=2),
    limit: int = Query(200, ge=1, le=2000),
):
    """
    Fast/slow signal path consistency check.
    - fast: provider cached signal path
    - slow: rebuild member context + evaluate_pool
    """
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")

    with _ro_conn_ctx() as conn:
        members = conn.execute(
            "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
            [pool_id],
        ).fetchall()
    if not members:
        return {
            "ok": True,
            "checked_at": datetime.datetime.now().isoformat(),
            "pool_id": int(pool_id),
            "summary": {"members": 0, "mismatch": 0, "exact_match": 0},
            "rows": [],
            "field_missing": {},
        }

    provider = get_tick_provider()
    fast_rows = _evaluate_signals_fast_internal(pool_id, members, provider)

    with _ro_conn_ctx() as conn:
        member_ctx = _build_member_data(conn, pool_id)
    slow_rows = sig.evaluate_pool(pool_id, member_ctx) if member_ctx else []

    def _signal_map(row: Optional[dict]) -> dict[str, dict]:
        if not row:
            return {}
        out: dict[str, dict] = {}
        for s in row.get("signals", []) or []:
            if not isinstance(s, dict):
                continue
            if s.get("has_signal") is False:
                continue
            t = str(s.get("type") or "")
            if t:
                out[t] = s
        return out

    def _active_types(row: Optional[dict]) -> set[str]:
        return set(_signal_map(row).keys())

    def _norm_value(v):
        if isinstance(v, float):
            return round(float(v), 6)
        if isinstance(v, dict):
            return {str(k): _norm_value(x) for k, x in sorted(v.items(), key=lambda kv: str(kv[0]))}
        if isinstance(v, list):
            return [_norm_value(x) for x in v]
        return v

    def _value_changed(a, b) -> bool:
        try:
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                return abs(float(a) - float(b)) > 1e-6
            return _norm_value(a) != _norm_value(b)
        except Exception:
            return str(a) != str(b)

    def _extract_conditions(signal_obj: Optional[dict]) -> list[str]:
        if not isinstance(signal_obj, dict):
            return []
        details = signal_obj.get("details")
        if isinstance(details, dict):
            cond = details.get("conditions")
            if isinstance(cond, list):
                return [str(x).strip() for x in cond if str(x).strip()]
        msg = str(signal_obj.get("message") or "")
        if not msg:
            return []
        # message schema usually: "<title> | 条件1 + 条件2 + 确认:..."
        tail = msg.split("|", 1)[1] if "|" in msg else msg
        return [p.strip() for p in tail.split("+") if p and p.strip()]

    trigger_keys_by_type: dict[str, list[str]] = {
        "left_side_buy": ["dist_to_lower_pct", "near_lower_th_pct", "rsi6", "pct_chg", "resonance_60m"],
        "right_side_breakout": ["cross_mid", "cross_upper", "volume_ratio", "ma5", "ma10", "rsi6", "resonance_60m"],
        "timing_clear": ["holding_days", "break_mid", "break_ma10", "break_ma20", "trend_reversal", "early_risk_exit", "bid_ask_ratio"],
        "positive_t": ["boll_break", "bias_vwap", "gub5_transition", "bid_ask_ratio", "lure_short", "ask_wall_absorb"],
        "reverse_t": ["boll_over", "robust_zscore", "v_power_divergence", "ask_wall_building", "real_buy_fading"],
    }
    threshold_extra_keys_by_type: dict[str, list[str]] = {
        "left_side_buy": ["near_lower_th_pct"],
        "right_side_breakout": [],
        "timing_clear": [],
        "positive_t": [],
        "reverse_t": [],
    }

    def _trigger_diff(signal_type: str, fast_sig: dict, slow_sig: dict) -> dict:
        fd = fast_sig.get("details") if isinstance(fast_sig.get("details"), dict) else {}
        sd = slow_sig.get("details") if isinstance(slow_sig.get("details"), dict) else {}
        f_conds = _extract_conditions(fast_sig)
        s_conds = _extract_conditions(slow_sig)
        cond_only_fast = sorted(list(set(f_conds) - set(s_conds)))
        cond_only_slow = sorted(list(set(s_conds) - set(f_conds)))
        detail_delta: dict[str, dict] = {}
        for k in trigger_keys_by_type.get(signal_type, []):
            fv = fd.get(k)
            sv = sd.get(k)
            if _value_changed(fv, sv):
                detail_delta[k] = {"fast": _norm_value(fv), "slow": _norm_value(sv)}
        return {
            "fast_conditions": f_conds,
            "slow_conditions": s_conds,
            "condition_only_fast": cond_only_fast,
            "condition_only_slow": cond_only_slow,
            "detail_delta": detail_delta,
            "has_diff": bool(cond_only_fast or cond_only_slow or detail_delta),
        }

    def _threshold_diff(signal_type: str, fast_sig: dict, slow_sig: dict) -> dict:
        fd = fast_sig.get("details") if isinstance(fast_sig.get("details"), dict) else {}
        sd = slow_sig.get("details") if isinstance(slow_sig.get("details"), dict) else {}
        f_th = fd.get("threshold") if isinstance(fd.get("threshold"), dict) else {}
        s_th = sd.get("threshold") if isinstance(sd.get("threshold"), dict) else {}
        delta: dict[str, dict] = {}
        for k in sorted(set(f_th.keys()) | set(s_th.keys())):
            fv = f_th.get(k)
            sv = s_th.get(k)
            if _value_changed(fv, sv):
                delta[str(k)] = {"fast": _norm_value(fv), "slow": _norm_value(sv)}
        f_extra: dict[str, object] = {}
        s_extra: dict[str, object] = {}
        for k in threshold_extra_keys_by_type.get(signal_type, []):
            fv = fd.get(k)
            sv = sd.get(k)
            if fv is not None:
                f_extra[k] = _norm_value(fv)
            if sv is not None:
                s_extra[k] = _norm_value(sv)
            if _value_changed(fv, sv):
                delta[k] = {"fast": _norm_value(fv), "slow": _norm_value(sv)}
        return {
            "fast_threshold": _norm_value(f_th),
            "slow_threshold": _norm_value(s_th),
            "fast_threshold_extra": f_extra,
            "slow_threshold_extra": s_extra,
            "delta": delta,
            "has_diff": bool(delta),
        }

    fast_map = {str(r.get("ts_code")): r for r in fast_rows}
    slow_map = {str(r.get("ts_code")): r for r in slow_rows}

    # Context completeness diagnostics (slow path input fields).
    # Use pool-specific required fields to avoid false positives.
    field_missing: dict[str, int] = {}
    required_fields: list[str] = []
    market_open = bool(_is_market_open().get("is_open", False))
    if int(pool_id) == 1:
        required_fields = [
            "ma20",
            "bid_ask_ratio",
            "up_limit",
            "down_limit",
            "winner_rate",
            "chip_concentration_pct",
            "pool1_in_holding",
            "pool1_holding_days",
        ]
        # prev_price/resonance are runtime-sensitive intraday fields;
        # only treat as required in open session.
        if market_open:
            required_fields.extend(["prev_price", "resonance_60m"])
    member_ctx_map = {str(m.get("ts_code") or ""): m for m in (member_ctx or []) if isinstance(m, dict)}

    def _missing_fields_for_code(code: str) -> list[str]:
        m = member_ctx_map.get(code, {})
        out: list[str] = []
        for field in required_fields:
            v = m.get(field) if isinstance(m, dict) else None
            miss = v is None
            if isinstance(v, (int, float)) and field in ("up_limit", "down_limit") and float(v) <= 0:
                miss = True
            if miss:
                out.append(field)
                field_missing[field] = field_missing.get(field, 0) + 1
        return out

    rows: list[dict] = []
    mismatch = 0
    exact = 0
    condition_mismatch = 0
    threshold_mismatch = 0
    missing_field_members = 0
    full_exact_match = 0
    for ts_code, name in members:
        code = str(ts_code)
        f = fast_map.get(code, {})
        s = slow_map.get(code, {})
        f_sig_map = _signal_map(f)
        s_sig_map = _signal_map(s)
        f_types = _active_types(f)
        s_types = _active_types(s)
        only_fast = sorted(list(f_types - s_types))
        only_slow = sorted(list(s_types - f_types))
        same = (not only_fast) and (not only_slow)  # keep backward-compatible semantics
        if same:
            exact += 1
        else:
            mismatch += 1

        missing_fields = _missing_fields_for_code(code)
        if missing_fields:
            missing_field_members += 1

        common_types = sorted(list(f_types & s_types))
        trigger_condition_diff: dict[str, dict] = {}
        threshold_diff: dict[str, dict] = {}
        for sig_type in common_types:
            f_sig = f_sig_map.get(sig_type, {})
            s_sig = s_sig_map.get(sig_type, {})
            td = _trigger_diff(sig_type, f_sig, s_sig)
            if td.get("has_diff"):
                trigger_condition_diff[sig_type] = td
            thd = _threshold_diff(sig_type, f_sig, s_sig)
            if thd.get("has_diff"):
                threshold_diff[sig_type] = thd

        if trigger_condition_diff:
            condition_mismatch += 1
        if threshold_diff:
            threshold_mismatch += 1

        full_match = bool(same and not missing_fields and not trigger_condition_diff and not threshold_diff)
        if full_match:
            full_exact_match += 1

        rows.append(
            {
                "ts_code": code,
                "name": str(name or ""),
                "fast_types": sorted(list(f_types)),
                "slow_types": sorted(list(s_types)),
                "only_fast": only_fast,
                "only_slow": only_slow,
                "fast_price": float(f.get("price", 0) or 0),
                "slow_price": float(s.get("price", 0) or 0),
                "price_delta": round(float(f.get("price", 0) or 0) - float(s.get("price", 0) or 0), 6),
                "match": same,
                "full_match": full_match,
                "missing_fields": missing_fields,
                "signal_type_diff": {
                    "only_fast": only_fast,
                    "only_slow": only_slow,
                    "common": common_types,
                },
                "trigger_condition_diff": trigger_condition_diff,
                "threshold_diff": threshold_diff,
            }
        )

    rows.sort(key=lambda x: (x.get("match", True), x.get("ts_code", "")))
    rows = rows[: int(limit)]

    ok = mismatch == 0
    return {
        "ok": ok,
        "checked_at": datetime.datetime.now().isoformat(),
        "pool_id": int(pool_id),
        "provider": provider.name,
        "summary": {
            "members": len(members),
            "mismatch": mismatch,
            "exact_match": exact,
            "mismatch_ratio": round((mismatch / len(members)) if members else 0.0, 6),
            "full_exact_match": full_exact_match,
            "full_mismatch": len(members) - full_exact_match,
            "missing_field_members": missing_field_members,
            "condition_mismatch_members": condition_mismatch,
            "threshold_mismatch_members": threshold_mismatch,
        },
        "rows": rows,
        "field_missing": field_missing,
    }


def _t0_reverse_diag_sort_key(row: dict) -> tuple:
    status = str(row.get("status") or "")
    priority_map = {
        "triggered": 0,
        "surge_guard_veto": 1,
        "guard_veto": 2,
        "trigger_no_confirm": 3,
        "trigger_score_filtered": 4,
        "trigger_observe_only": 5,
        "candidate": 6,
        "idle": 7,
        "off_session_skip": 8,
        "error": 9,
    }
    try:
        strength = float(row.get("strength", 0) or 0)
    except Exception:
        strength = 0.0
    try:
        pct_chg_abs = abs(float(row.get("pct_chg", 0) or 0))
    except Exception:
        pct_chg_abs = 0.0
    return (priority_map.get(status, 99), -strength, -pct_chg_abs, str(row.get("ts_code") or ""))


def _build_t0_reverse_diag_row(member: dict, provider) -> dict:
    from backend.realtime import gm_tick as gm_runtime
    from backend.realtime.threshold_engine import build_signal_context, get_thresholds

    m = dict(member or {})
    ts_code = str(m.get("ts_code") or "")
    name = str(m.get("name") or "")
    if not ts_code:
        return {"status": "error", "error": "missing_ts_code"}
    market_open = bool(_is_market_open().get("is_open", False))

    tick = {}
    try:
        tick = provider.get_cached_tick(ts_code) or {}
    except Exception:
        tick = {}
    if not tick:
        try:
            tick = provider.get_tick(ts_code) or {}
        except Exception:
            tick = {}
    if not isinstance(tick, dict) or not tick:
        return {
            "ts_code": ts_code,
            "name": name,
            "status": "error",
            "error": "tick_unavailable",
        }

    price = float(tick.get("price", m.get("price", 0)) or 0)
    pct_chg = float(tick.get("pct_chg", m.get("pct_chg", 0)) or 0)
    if price > 0:
        m["price"] = price
    m["pct_chg"] = pct_chg
    if (not market_open) and price <= 0:
        return {
            "ts_code": ts_code,
            "name": name,
            "price": round(price, 3),
            "pct_chg": round(pct_chg, 3),
            "status": "off_session_skip",
            "has_signal": False,
            "observe_only": False,
            "strength": 0.0,
            "message": "",
            "veto": None,
            "main_rally_guard": False,
            "trend_guard": {"enabled": True, "reason": "off_session_skip"},
            "trigger_items": [],
            "confirm_items": [],
            "bearish_confirm_count": 0,
            "threshold": {},
            "momentum_divergence": {"reason": "off_session_skip"},
            "market_structure": {},
            "feature_snapshot": {
                "trend_up": False,
                "boll_over": False,
                "robust_zscore": None,
                "bid_ask_ratio": None,
                "volume_pace_ratio": None,
                "volume_pace_state": "unknown",
                "main_rally_guard": False,
                "pct_chg": pct_chg,
            },
        }

    txns = None
    try:
        txns = provider.get_cached_transactions(ts_code)
    except Exception:
        txns = None
    if txns is None:
        try:
            txns = mootdx_client.get_transactions(ts_code, int(_TXN_ANALYZE_COUNT))
        except Exception:
            txns = None
    txns = txns or []

    ms = gm_runtime._analyze_market_structure(tick, txns)
    state = gm_runtime._update_intraday_state(ts_code, tick)
    vwap = m.get("vwap")
    if vwap in (None, 0):
        vwap = gm_runtime._compute_vwap(state)
    gub5_trend = m.get("gub5_trend") or gm_runtime._compute_gub5_trend(state)
    gub5_transition = gm_runtime._compute_gub5_transition(state)
    pw = state.get("price_window") or []
    intraday_prices = m.get("intraday_prices") or [p for _, p, _ in pw]
    v_power_divergence, vpower_info = gm_runtime._detect_v_power_divergence(state, txns)

    ma5 = float(m.get("ma5", 0) or 0)
    ma10 = float(m.get("ma10", 0) or 0)
    ma20 = float(m.get("ma20", 0) or 0)
    trend_up = bool(ma5 > ma10 > ma20 > 0)

    feat = sig.build_t0_features(
        tick_price=price,
        boll_lower=m.get("boll_lower", 0),
        boll_upper=m.get("boll_upper", 0),
        vwap=vwap,
        intraday_prices=intraday_prices,
        pct_chg=pct_chg,
        trend_up=trend_up,
        gub5_trend=gub5_trend,
        gub5_transition=gub5_transition,
        bid_ask_ratio=m.get("bid_ask_ratio"),
        lure_short=bool(ms.get("lure_short", False)),
        wash_trade=bool(ms.get("wash_trade", False)),
        spread_abnormal=bool(m.get("spread_abnormal", False)),
        v_power_divergence=bool(v_power_divergence),
        ask_wall_building=bool(ms.get("lure_long", False)),
        real_buy_fading=bool(ms.get("active_buy_fading", False)),
        wash_trade_severe=bool(ms.get("wash_trade", False) and abs(float(ms.get("big_order_bias", 0.0) or 0.0)) < 0.05),
        liquidity_drain=bool(m.get("liquidity_drain", False)),
        ask_wall_absorb_ratio=ms.get("ask_wall_absorb_ratio"),
        bid_wall_break_ratio=ms.get("bid_wall_break_ratio"),
        spoofing_vol_ratio=m.get("spoofing_vol_ratio"),
        big_order_bias=ms.get("big_order_bias"),
        big_net_flow_bps=ms.get("big_net_flow_bps"),
        super_order_bias=ms.get("super_order_bias"),
        super_net_flow_bps=ms.get("super_net_flow_bps"),
        volume_pace_ratio=m.get("volume_pace_ratio"),
        volume_pace_state=m.get("volume_pace_state"),
    )

    ctx = build_signal_context(
        ts_code,
        2,
        tick=tick,
        daily=m,
        intraday={
            "vwap": feat.get("vwap"),
            "bias_vwap": feat.get("bias_vwap"),
            "robust_zscore": feat.get("robust_zscore"),
            "gub5_ratio": m.get("gub5_ratio"),
            "gub5_transition": feat.get("gub5_transition"),
            "volume_pace_ratio": feat.get("volume_pace_ratio"),
            "volume_pace_state": feat.get("volume_pace_state"),
            "progress_ratio": m.get("volume_pace_progress"),
        },
        market={
            "industry": m.get("industry"),
            "vol_20": m.get("vol_20"),
            "atr_14": m.get("atr_14"),
            "name": m.get("name"),
            "market_name": m.get("market_name"),
            "list_date": m.get("list_date"),
            "instrument_profile": m.get("instrument_profile"),
        },
    )
    thresholds = get_thresholds("reverse_t", ctx)
    rev = sig.detect_reverse_t(
        tick_price=feat["tick_price"],
        boll_upper=feat["boll_upper"],
        vwap=feat["vwap"],
        intraday_prices=intraday_prices,
        pct_chg=feat.get("pct_chg"),
        trend_up=bool(feat.get("trend_up", False)),
        bid_ask_ratio=feat["bid_ask_ratio"],
        v_power_divergence=feat["v_power_divergence"],
        ask_wall_building=feat["ask_wall_building"],
        real_buy_fading=feat["real_buy_fading"],
        wash_trade_severe=feat["wash_trade_severe"],
        liquidity_drain=feat["liquidity_drain"],
        ask_wall_absorb=bool(feat.get("ask_wall_absorb", False)),
        ask_wall_absorb_ratio=feat.get("ask_wall_absorb_ratio"),
        bid_wall_break=bool(feat.get("bid_wall_break", False)),
        bid_wall_break_ratio=feat.get("bid_wall_break_ratio"),
        boll_over=feat["boll_over"],
        spoofing_suspected=feat["spoofing_suspected"],
        big_order_bias=feat["big_order_bias"],
        super_order_bias=feat.get("super_order_bias"),
        super_net_flow_bps=feat.get("super_net_flow_bps"),
        robust_zscore=feat["robust_zscore"],
        volume_pace_ratio=feat.get("volume_pace_ratio"),
        volume_pace_state=feat.get("volume_pace_state"),
        main_rally_guard=bool(feat.get("main_rally_guard", False)),
        main_rally_info=feat.get("main_rally_info"),
        ts_code=ts_code,
        thresholds=thresholds,
    )

    z_th = None
    try:
        z_th = float((thresholds or {}).get("z_th"))
    except Exception:
        z_th = None
    trigger_items: list[str] = []
    if bool(feat.get("boll_over", False)):
        trigger_items.append("boll_over")
    try:
        robust_z = float(feat.get("robust_zscore")) if feat.get("robust_zscore") is not None else None
    except Exception:
        robust_z = None
    if robust_z is not None and z_th is not None and robust_z > z_th:
        trigger_items.append("robust_zscore")

    confirm_items: list[str] = []
    if bool(feat.get("v_power_divergence", False)):
        confirm_items.append("v_power_divergence")
    if bool(feat.get("ask_wall_building", False)):
        confirm_items.append("ask_wall_building")
    if bool(feat.get("bid_wall_break", False)):
        confirm_items.append("bid_wall_break")
    if bool(feat.get("real_buy_fading", False)):
        confirm_items.append("real_buy_fading")
    try:
        if feat.get("super_order_bias") is not None and float(feat.get("super_order_bias")) <= -0.20:
            confirm_items.append("super_order_bias")
    except Exception:
        pass
    try:
        if feat.get("super_net_flow_bps") is not None and float(feat.get("super_net_flow_bps")) <= -120.0:
            confirm_items.append("super_net_flow_bps")
    except Exception:
        pass

    details = rev.get("details") if isinstance(rev.get("details"), dict) else {}
    veto = str(details.get("veto") or "")
    has_signal = bool(rev.get("has_signal", False))
    observe_only = bool(details.get("observe_only", False))
    strength = float(rev.get("strength", 0) or 0)

    if has_signal:
        status = "trigger_observe_only" if observe_only else "triggered"
    elif veto == "trend_surge_absorb_guard":
        status = "surge_guard_veto"
    elif veto == "trend_extension_guard":
        status = "guard_veto"
    elif trigger_items and not confirm_items:
        status = "trigger_no_confirm"
    elif trigger_items and confirm_items:
        status = "trigger_score_filtered"
    elif bool(feat.get("main_rally_guard", False)):
        status = "candidate"
    else:
        status = "idle"

    return {
        "ts_code": ts_code,
        "name": name,
        "price": round(price, 3),
        "pct_chg": round(pct_chg, 3),
        "status": status,
        "has_signal": has_signal,
        "observe_only": observe_only,
        "strength": round(strength, 2),
        "message": str(rev.get("message", "") or ""),
        "veto": veto or None,
        "main_rally_guard": bool(feat.get("main_rally_guard", False)),
        "trend_guard": feat.get("main_rally_info") if isinstance(feat.get("main_rally_info"), dict) else {},
        "trigger_items": trigger_items,
        "confirm_items": confirm_items,
        "bearish_confirm_count": len(confirm_items),
        "threshold": details.get("threshold") if isinstance(details.get("threshold"), dict) else thresholds,
        "momentum_divergence": dict(vpower_info or {}),
        "market_structure": {
            "tag": ms.get("tag"),
            "big_order_bias": ms.get("big_order_bias"),
            "big_net_flow_bps": ms.get("big_net_flow_bps"),
            "super_order_bias": ms.get("super_order_bias"),
            "super_net_flow_bps": ms.get("super_net_flow_bps"),
            "bid_ask_ratio": ms.get("bid_ask_ratio"),
            "ask_wall_absorb_ratio": ms.get("ask_wall_absorb_ratio"),
            "bid_wall_break_ratio": ms.get("bid_wall_break_ratio"),
            "ask_wall_absorbed": bool(ms.get("ask_wall_absorbed", False)),
            "bid_wall_broken": bool(ms.get("bid_wall_broken", False)),
            "active_buy_fading": bool(ms.get("active_buy_fading", False)),
            "pull_up_not_stable": bool(ms.get("pull_up_not_stable", False)),
            "peak_gain_pct": ms.get("peak_gain_pct"),
            "retrace_from_high_pct": ms.get("retrace_from_high_pct"),
        },
        "feature_snapshot": {
            "trend_up": trend_up,
            "boll_over": bool(feat.get("boll_over", False)),
            "robust_zscore": feat.get("robust_zscore"),
            "bid_ask_ratio": feat.get("bid_ask_ratio"),
            "volume_pace_ratio": feat.get("volume_pace_ratio"),
            "volume_pace_state": feat.get("volume_pace_state"),
            "main_rally_guard": bool(feat.get("main_rally_guard", False)),
            "pct_chg": feat.get("pct_chg"),
        },
    }


@router.get("/runtime/t0_reverse_diagnostics")
def runtime_t0_reverse_diagnostics(
    limit: int = Query(50, ge=1, le=500),
    interesting_only: bool = Query(True),
):
    with _ro_conn_ctx() as conn:
        members_data = _build_member_data(conn, 2)
    provider = get_tick_provider()
    rows: list[dict] = []
    for m in members_data or []:
        try:
            rows.append(_build_t0_reverse_diag_row(m, provider))
        except Exception as e:
            rows.append(
                {
                    "ts_code": str((m or {}).get("ts_code") or ""),
                    "name": str((m or {}).get("name") or ""),
                    "status": "error",
                    "error": str(e),
                }
            )

    total_rows = len(rows)
    summary = {
        "members": total_rows,
        "triggered": 0,
        "observe_only": 0,
        "surge_guard_veto": 0,
        "guard_veto": 0,
        "main_rally_guard": 0,
        "trigger_no_confirm": 0,
        "trigger_score_filtered": 0,
        "candidate": 0,
        "off_session_skip": 0,
        "error": 0,
    }
    for row in rows:
        status = str(row.get("status") or "")
        if status == "triggered":
            summary["triggered"] += 1
        elif status == "trigger_observe_only":
            summary["triggered"] += 1
            summary["observe_only"] += 1
        elif status == "surge_guard_veto":
            summary["surge_guard_veto"] += 1
        elif status == "guard_veto":
            summary["guard_veto"] += 1
        elif status == "trigger_no_confirm":
            summary["trigger_no_confirm"] += 1
        elif status == "trigger_score_filtered":
            summary["trigger_score_filtered"] += 1
        elif status == "candidate":
            summary["candidate"] += 1
        elif status == "off_session_skip":
            summary["off_session_skip"] += 1
        elif status == "error":
            summary["error"] += 1
        if bool(row.get("main_rally_guard", False)):
            summary["main_rally_guard"] += 1

    if interesting_only:
        rows = [r for r in rows if str(r.get("status") or "") not in {"idle", "off_session_skip"}]
    rows.sort(key=_t0_reverse_diag_sort_key)

    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "interesting_only": bool(interesting_only),
        "summary": summary,
        "rows_total": total_rows,
        "count": len(rows[: int(limit)]),
        "rows": rows[: int(limit)],
    }


@router.get("/runtime/consistency_check")
def runtime_consistency_check(
    pool_id: int = Query(1, ge=1, le=2),
    limit: int = Query(200, ge=1, le=2000),
):
    """
    Alias of fast/slow consistency diagnostics.
    Output includes:
    - field missing
    - signal type diff
    - trigger condition diff
    - threshold diff
    """
    return runtime_fast_slow_diff(pool_id=pool_id, limit=limit)


def _fast_slow_trend_key(pool_id: int) -> str:
    return f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:fast_slow:trend:pool:{int(pool_id)}"


def _append_fast_slow_trend_snapshot(snap: dict, now_ts: Optional[float] = None) -> None:
    """
    Persist fast/slow mismatch snapshots for trend view.
    Storage priority: Redis (when enabled), with in-memory fallback always kept.
    """
    if not _FAST_SLOW_TREND_ENABLED or not isinstance(snap, dict):
        return
    ts_now = float(now_ts) if isinstance(now_ts, (int, float)) else time.time()
    pool_id = int(snap.get("pool_id", 0) or 0)
    if pool_id <= 0:
        return

    min_interval = max(1.0, float(_FAST_SLOW_TREND_MIN_INTERVAL_SEC))
    with _fast_slow_trend_lock:
        last_ts = float(_fast_slow_trend_last_ts.get(pool_id, 0.0) or 0.0)
        if last_ts > 0 and (ts_now - last_ts) < min_interval:
            return
        _fast_slow_trend_last_ts[pool_id] = ts_now

    point = {
        "pool_id": pool_id,
        "at": int(ts_now),
        "at_iso": datetime.datetime.fromtimestamp(ts_now).isoformat(),
        "mismatch_ratio": round(float(snap.get("mismatch_ratio", 0.0) or 0.0), 6),
        "mismatch": int(snap.get("mismatch", 0) or 0),
        "members": int(snap.get("members", 0) or 0),
        "error": snap.get("error"),
    }

    with _fast_slow_trend_lock:
        dq = _fast_slow_trend_mem.get(pool_id)
        if dq is None:
            dq = deque(maxlen=max(200, int(_FAST_SLOW_TREND_MAX_POINTS)))
            _fast_slow_trend_mem[pool_id] = dq
        dq.append(point)

    cli = _get_runtime_state_redis()
    if cli is None:
        return
    key = _fast_slow_trend_key(pool_id)
    retention_sec = max(3600, int(_FAST_SLOW_TREND_RETENTION_HOURS) * 3600)
    ttl = max(_runtime_state_ttl_sec(), retention_sec + 24 * 3600)
    try:
        raw = json.dumps(point, ensure_ascii=False, separators=(",", ":"))
        pipe = cli.pipeline(transaction=False)
        pipe.zadd(key, {raw: int(ts_now)})
        pipe.zremrangebyscore(key, 0, int(ts_now) - retention_sec)
        pipe.expire(key, ttl)
        pipe.execute()
        # hard cap size (oldest first)
        try:
            zc = int(cli.zcard(key) or 0)
            max_pts = max(200, int(_FAST_SLOW_TREND_MAX_POINTS))
            if zc > max_pts:
                cli.zremrangebyrank(key, 0, zc - max_pts - 1)
        except Exception:
            pass
    except Exception:
        pass


@router.get("/runtime/fast_slow_trend")
def runtime_fast_slow_trend(
    pool_id: int = Query(1, ge=1, le=2),
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(500, ge=10, le=5000),
):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    now_ts = int(time.time())
    since_ts = now_ts - int(hours) * 3600
    lim = max(10, min(5000, int(limit)))

    points: list[dict] = []
    storage = "memory"
    cli = _get_runtime_state_redis()
    if cli is not None:
        key = _fast_slow_trend_key(int(pool_id))
        try:
            raws = cli.zrangebyscore(key, since_ts, now_ts)
            if raws:
                if len(raws) > lim:
                    raws = raws[-lim:]
                for raw in raws:
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict):
                            points.append(obj)
                    except Exception:
                        continue
                storage = "redis"
        except Exception:
            points = []

    # Fallback to in-memory snapshots when redis not available or empty.
    if not points:
        with _fast_slow_trend_lock:
            dq = _fast_slow_trend_mem.get(int(pool_id))
            arr = list(dq) if dq is not None else []
        points = [p for p in arr if int(p.get("at", 0) or 0) >= since_ts]
        if len(points) > lim:
            points = points[-lim:]
        storage = "memory"

    latest = points[-1] if points else None
    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "pool_id": int(pool_id),
        "hours": int(hours),
        "count": len(points),
        "latest": latest,
        "storage_source": storage,
        "points": points,
    }


def _get_fast_slow_health_snapshot(pool_id: int) -> dict:
    now_ts = time.time()
    ttl = max(1.0, float(_FAST_SLOW_HEALTH_CACHE_TTL_SEC))
    with _fast_slow_health_lock:
        cached_at = float(_fast_slow_health_cache.get("at", 0.0) or 0.0)
        cached_pool = int(_fast_slow_health_cache.get("pool_id", 0) or 0)
        cached_payload = _fast_slow_health_cache.get("payload")
        if cached_pool == int(pool_id) and cached_payload and (now_ts - cached_at) <= ttl:
            snap = dict(cached_payload)
            snap["cached"] = True
            snap["cache_age_sec"] = round(max(0.0, now_ts - cached_at), 3)
            return snap

    try:
        diff = runtime_fast_slow_diff(pool_id=int(pool_id), limit=2000)
        summary = diff.get("summary", {}) if isinstance(diff, dict) else {}
        payload = {
            "ok": bool(diff.get("ok", False)) if isinstance(diff, dict) else False,
            "pool_id": int(pool_id),
            "members": int(summary.get("members", 0) or 0),
            "mismatch": int(summary.get("mismatch", 0) or 0),
            "mismatch_ratio": float(summary.get("mismatch_ratio", 0.0) or 0.0),
            "checked_at": diff.get("checked_at") if isinstance(diff, dict) else None,
            "error": None,
            "cached": False,
            "cache_age_sec": 0.0,
        }
    except Exception as e:
        payload = {
            "ok": False,
            "pool_id": int(pool_id),
            "members": 0,
            "mismatch": 0,
            "mismatch_ratio": 0.0,
            "checked_at": datetime.datetime.now().isoformat(),
            "error": str(e),
            "cached": False,
            "cache_age_sec": 0.0,
        }

    with _fast_slow_health_lock:
        _fast_slow_health_cache["at"] = now_ts
        _fast_slow_health_cache["pool_id"] = int(pool_id)
        _fast_slow_health_cache["payload"] = dict(payload)
    # record trend only for fresh computations (not cache hit path)
    _append_fast_slow_trend_snapshot(payload, now_ts=now_ts)
    return payload


def _health_level(ok: bool, warn: bool = False) -> str:
    if not ok:
        return "red"
    if warn:
        return "yellow"
    return "green"


def _resolve_health_profile(market: dict, override: Optional[str]) -> str:
    if override in {"open", "normal", "closed"}:
        return str(override)
    if not bool((market or {}).get("is_open", False)):
        return "closed"
    status = str((market or {}).get("status", ""))
    if status == "pre_auction":
        return "open"
    now = datetime.datetime.now()
    hhmm = now.hour * 100 + now.minute
    if 930 <= hhmm < 1000:
        return "open"
    return "normal"


def _health_thresholds(profile: str) -> dict:
    if profile == "open":
        return {
            "queue_warn_ratio": 0.30,
            "queue_crit_ratio": 0.60,
            "tick_ok_sec": 20,
            "tick_warn_sec": 60,
            "signal_ok_sec": 180,
            "signal_warn_sec": 420,
            "closed_tick_warn_sec": 24 * 3600,
        }
    if profile == "closed":
        return {
            "queue_warn_ratio": 0.70,
            "queue_crit_ratio": 0.90,
            "tick_ok_sec": 30,
            "tick_warn_sec": 120,
            "signal_ok_sec": 300,
            "signal_warn_sec": 900,
            "closed_tick_warn_sec": 24 * 3600,
        }
    return {
        "queue_warn_ratio": 0.50,
        "queue_crit_ratio": 0.80,
        "tick_ok_sec": 30,
        "tick_warn_sec": 120,
        "signal_ok_sec": 300,
        "signal_warn_sec": 900,
        "closed_tick_warn_sec": 24 * 3600,
    }


def _maybe_record_health_alert(payload: dict) -> dict:
    now_ts = time.time()
    level = str(payload.get("level", "green"))
    triggered = False
    snapshot = {}
    with _health_alert_lock:
        st = _health_alert_state
        st["last_level"] = level
        if level == "green":
            st["non_green_streak"] = 0
            st["yellow_streak"] = 0
            st["red_streak"] = 0
        elif level == "yellow":
            st["non_green_streak"] = int(st.get("non_green_streak", 0)) + 1
            st["yellow_streak"] = int(st.get("yellow_streak", 0)) + 1
            st["red_streak"] = 0
        else:
            st["non_green_streak"] = int(st.get("non_green_streak", 0)) + 1
            st["red_streak"] = int(st.get("red_streak", 0)) + 1
            st["yellow_streak"] = 0

        can_alert = (
            _HEALTH_ALERT_ENABLED
            and level in {"yellow", "red"}
            and int(st.get("non_green_streak", 0)) >= max(1, _HEALTH_ALERT_CONSECUTIVE)
            and (now_ts - float(st.get("last_alert_at", 0.0))) >= max(1, _HEALTH_ALERT_COOLDOWN_SEC)
        )
        if can_alert:
            event = {
                "at": datetime.datetime.fromtimestamp(now_ts).isoformat(),
                "level": level,
                "non_green_streak": int(st.get("non_green_streak", 0)),
                "yellow_streak": int(st.get("yellow_streak", 0)),
                "red_streak": int(st.get("red_streak", 0)),
                "profile": payload.get("profile"),
                "market": payload.get("market"),
                "summary": payload.get("summary"),
            }
            events = list(st.get("events", []))
            events.append(event)
            if len(events) > max(10, _HEALTH_ALERT_MAX_EVENTS):
                events = events[-max(10, _HEALTH_ALERT_MAX_EVENTS):]
            st["events"] = events
            st["last_alert_at"] = now_ts
            triggered = True
            try:
                logger.warning(
                    "[HealthAlert] level={} streak={} profile={} summary={}",
                    level,
                    st.get("non_green_streak", 0),
                    payload.get("profile"),
                    payload.get("summary"),
                )
            except Exception:
                pass
        snapshot = {
            "last_level": st.get("last_level"),
            "non_green_streak": int(st.get("non_green_streak", 0)),
            "yellow_streak": int(st.get("yellow_streak", 0)),
            "red_streak": int(st.get("red_streak", 0)),
            "last_alert_at": (
                datetime.datetime.fromtimestamp(float(st.get("last_alert_at", 0.0))).isoformat()
                if float(st.get("last_alert_at", 0.0)) > 0
                else None
            ),
            "events_count": len(st.get("events", [])),
        }
    return {"triggered": triggered, "state": snapshot}


def _build_runtime_health_summary(
    profile_override: Optional[str] = None,
    record_alert: bool = True,
    simulate_market_open: Optional[bool] = None,
) -> dict:
    now = datetime.datetime.now()
    market = _is_market_open()
    is_open = bool(market.get("is_open", False))
    effective_is_open = is_open if simulate_market_open is None else bool(simulate_market_open)
    profile = _resolve_health_profile(market, profile_override)
    th = _health_thresholds(profile)

    metrics = runtime_db_metrics().get("data", {})
    selfcheck = runtime_selfcheck()
    checks = selfcheck.get("checks", []) or []
    ok_all = bool(selfcheck.get("ok", False))

    check_map: dict[str, dict] = {str(c.get("name")): c for c in checks if isinstance(c, dict)}
    pool_cnt = int((check_map.get("realtime_pool_member_count") or {}).get("value") or 0)

    queue_size = int(metrics.get("db_write_queue_size", 0) or 0)
    queue_cap = int(metrics.get("db_write_queue_capacity", 1) or 1)
    queue_ratio = (float(queue_size) / float(queue_cap)) if queue_cap > 0 else 0.0
    queue_warn = queue_ratio >= float(th.get("queue_warn_ratio", 0.5))
    queue_ok = queue_ratio < float(th.get("queue_crit_ratio", 0.8))

    redis_enabled = bool(metrics.get("db_writer_redis_enabled", False))
    redis_ready = bool(metrics.get("db_writer_redis_ready", False))
    redis_ok = (not redis_enabled) or redis_ready
    state_redis_enabled = bool(metrics.get("runtime_state_redis_enabled", False))
    state_redis_ready = bool(metrics.get("runtime_state_redis_ready", False))
    state_redis_ok = (not state_redis_enabled) or state_redis_ready

    latest_signal_age_sec = None
    latest_signal_eval_age_sec = None
    latest_signal_eval_samples = 0
    latest_tick_age_sec = None
    rt_conn = None
    member_codes: list[str] = []
    try:
        rt_conn = _get_conn()
        try:
            r = rt_conn.execute("SELECT MAX(triggered_at) FROM signal_history").fetchone()
            if r and r[0]:
                dt = r[0]
                if hasattr(dt, "timestamp"):
                    latest_signal_age_sec = max(0, int(now.timestamp() - dt.timestamp()))
        except Exception:
            pass
        try:
            r2 = rt_conn.execute("SELECT MAX(ts) FROM tick_history").fetchone()
            if r2 and r2[0]:
                ts = int(r2[0] or 0)
                if ts > 10**12:
                    ts //= 1000
                latest_tick_age_sec = max(0, int(now.timestamp() - ts))
        except Exception:
            pass
        try:
            mrows = rt_conn.execute(
                """
                SELECT DISTINCT ts_code
                FROM monitor_pools
                ORDER BY ts_code
                LIMIT 500
                """
            ).fetchall()
            member_codes = [str(r[0]) for r in (mrows or []) if r and r[0]]
        except Exception:
            member_codes = []
    except Exception:
        pass
    finally:
        if rt_conn is not None:
            try:
                rt_conn.close()
            except Exception:
                pass

    # Signal freshness should reflect whether Layer2 signal engine is still evaluating,
    # not whether a *new* signal was recently triggered.
    if member_codes:
        try:
            provider = get_tick_provider()
        except Exception:
            provider = None
        if provider is not None:
            latest_eval_ts = 0
            sample_cnt = 0
            for code in member_codes:
                try:
                    entry = provider.get_cached_signals(code)
                except Exception:
                    entry = None
                if not isinstance(entry, dict):
                    continue
                ev = entry.get("evaluated_at")
                if ev is None:
                    continue
                try:
                    ev_ts = int(float(ev))
                    if ev_ts > 10**12:
                        ev_ts //= 1000
                    if ev_ts > 0:
                        sample_cnt += 1
                        if ev_ts > latest_eval_ts:
                            latest_eval_ts = ev_ts
                except Exception:
                    continue
            if latest_eval_ts > 0:
                latest_signal_eval_age_sec = max(0, int(now.timestamp() - int(latest_eval_ts)))
                latest_signal_eval_samples = int(sample_cnt)

    # Freshness thresholds: profile-based.
    signal_fresh_age = latest_signal_eval_age_sec if latest_signal_eval_age_sec is not None else latest_signal_age_sec
    signal_fresh_basis = "signal_eval" if latest_signal_eval_age_sec is not None else ("signal_event" if latest_signal_age_sec is not None else "none")
    if effective_is_open:
        tick_ok = latest_tick_age_sec is not None and latest_tick_age_sec <= int(th.get("tick_ok_sec", 30))
        tick_warn = latest_tick_age_sec is not None and int(th.get("tick_ok_sec", 30)) < latest_tick_age_sec <= int(th.get("tick_warn_sec", 120))
        signal_ok = signal_fresh_age is not None and signal_fresh_age <= int(th.get("signal_ok_sec", 300))
        signal_warn = signal_fresh_age is not None and int(th.get("signal_ok_sec", 300)) < signal_fresh_age <= int(th.get("signal_warn_sec", 900))
    else:
        # After close, missing/old tick is expected and should not turn health red.
        tick_ok = True
        tick_warn = latest_tick_age_sec is not None and latest_tick_age_sec > int(th.get("closed_tick_warn_sec", 24 * 3600))
        signal_ok = True  # after close no strict signal freshness requirement
        signal_warn = False

    fast_slow_item = None
    if _FAST_SLOW_HEALTH_ENABLED and int(_FAST_SLOW_HEALTH_POOL_ID) in POOLS:
        snap = _get_fast_slow_health_snapshot(int(_FAST_SLOW_HEALTH_POOL_ID))
        members = int(snap.get("members", 0) or 0)
        ratio = float(snap.get("mismatch_ratio", 0.0) or 0.0)
        mismatch = int(snap.get("mismatch", 0) or 0)
        err = snap.get("error")

        # Health policy:
        # - compute error -> yellow (not red), avoid transient heavy-check false negatives
        # - ratio >= crit -> red
        # - ratio >= warn -> yellow
        # - members too small -> yellow (diagnostic weak)
        if err:
            fs_ok = True
            fs_warn = True
        else:
            fs_ok = ratio < float(_FAST_SLOW_HEALTH_CRIT_RATIO)
            fs_warn = (ratio >= float(_FAST_SLOW_HEALTH_WARN_RATIO) and fs_ok) or (members > 0 and members < 10)

        fast_slow_item = {
            "name": f"fast_slow_consistency_pool{int(_FAST_SLOW_HEALTH_POOL_ID)}",
            "ok": fs_ok,
            "level": _health_level(fs_ok, warn=fs_warn),
            "detail": {
                "pool_id": int(_FAST_SLOW_HEALTH_POOL_ID),
                "members": members,
                "mismatch": mismatch,
                "mismatch_ratio": round(ratio, 6),
                "warn_ratio": float(_FAST_SLOW_HEALTH_WARN_RATIO),
                "crit_ratio": float(_FAST_SLOW_HEALTH_CRIT_RATIO),
                "checked_at": snap.get("checked_at"),
                "cached": bool(snap.get("cached", False)),
                "cache_age_sec": snap.get("cache_age_sec"),
                "error": err,
            },
        }

    items = [
        {
            "name": "db_connectivity",
            "ok": ok_all,
            "level": _health_level(ok_all),
            "detail": "selfcheck",
        },
        {
            "name": "pool_members",
            "ok": pool_cnt > 0,
            "level": _health_level(pool_cnt > 0, warn=(pool_cnt > 0 and pool_cnt < 10)),
            "detail": {"count": pool_cnt},
        },
        {
            "name": "writer_queue",
            "ok": queue_ok,
            "level": _health_level(queue_ok, warn=queue_warn and queue_ok),
            "detail": {"size": queue_size, "capacity": queue_cap, "ratio": round(queue_ratio, 4)},
        },
        {
            "name": "writer_redis",
            "ok": redis_ok,
            "level": _health_level(redis_ok),
            "detail": {"enabled": redis_enabled, "ready": redis_ready},
        },
        {
            "name": "runtime_state_redis",
            "ok": state_redis_ok,
            "level": _health_level(state_redis_ok),
            "detail": {"enabled": state_redis_enabled, "ready": state_redis_ready},
        },
        {
            "name": "tick_freshness",
            "ok": tick_ok,
            "level": _health_level(tick_ok, warn=tick_warn),
            "detail": {
                "age_sec": latest_tick_age_sec,
                "market_open": is_open,
                "effective_market_open": effective_is_open,
            },
        },
        {
            "name": "signal_freshness",
            "ok": signal_ok,
            "level": _health_level(signal_ok, warn=signal_warn),
            "detail": {
                "age_sec": signal_fresh_age,
                "basis": signal_fresh_basis,
                "signal_event_age_sec": latest_signal_age_sec,
                "signal_eval_age_sec": latest_signal_eval_age_sec,
                "signal_eval_samples": latest_signal_eval_samples,
                "market_open": is_open,
                "effective_market_open": effective_is_open,
            },
        },
    ]
    if fast_slow_item is not None:
        items.append(fast_slow_item)

    reds = sum(1 for x in items if x["level"] == "red")
    yellows = sum(1 for x in items if x["level"] == "yellow")
    overall = "red" if reds > 0 else ("yellow" if yellows > 0 else "green")
    thresholds_ext = dict(th)
    if _FAST_SLOW_HEALTH_ENABLED and int(_FAST_SLOW_HEALTH_POOL_ID) in POOLS:
        thresholds_ext.update(
            {
                "fast_slow_pool_id": int(_FAST_SLOW_HEALTH_POOL_ID),
                "fast_slow_warn_ratio": float(_FAST_SLOW_HEALTH_WARN_RATIO),
                "fast_slow_crit_ratio": float(_FAST_SLOW_HEALTH_CRIT_RATIO),
                "fast_slow_cache_ttl_sec": float(_FAST_SLOW_HEALTH_CACHE_TTL_SEC),
            }
        )
    payload = {
        "ok": overall != "red",
        "level": overall,
        "checked_at": now.isoformat(),
        "profile": profile,
        "simulation": {
            "simulate_market_open": simulate_market_open,
            "effective_market_open": effective_is_open,
        },
        "thresholds": thresholds_ext,
        "market": market,
        "summary": {
            "green": sum(1 for x in items if x["level"] == "green"),
            "yellow": yellows,
            "red": reds,
        },
        "items": items,
    }
    if record_alert:
        payload["alert"] = _maybe_record_health_alert(payload)
    return payload


@router.get("/runtime/health_summary")
def runtime_health_summary(
    profile: Optional[str] = Query(None, description="override: open|normal|closed"),
    simulate_open: bool = Query(False, description="simulate market open branch for threshold checks"),
):
    p = profile.strip().lower() if isinstance(profile, str) else None
    if p not in (None, "open", "normal", "closed"):
        raise HTTPException(status_code=400, detail="invalid profile, expected open|normal|closed")
    sim = True if (isinstance(simulate_open, bool) and simulate_open) else None
    return _build_runtime_health_summary(profile_override=p, record_alert=True, simulate_market_open=sim)


@router.get("/runtime/health_pressure")
def runtime_health_pressure():
    return {
        "checked_at": datetime.datetime.now().isoformat(),
        "profiles": {
            "open": _build_runtime_health_summary(
                profile_override="open",
                record_alert=False,
                simulate_market_open=True,
            ),
            "normal": _build_runtime_health_summary(
                profile_override="normal",
                record_alert=False,
                simulate_market_open=True,
            ),
            "closed": _build_runtime_health_summary(
                profile_override="closed",
                record_alert=False,
                simulate_market_open=False,
            ),
        },
    }


@router.get("/runtime/health_alerts")
def runtime_health_alerts(limit: int = Query(20, ge=1, le=200)):
    lim = int(limit) if isinstance(limit, int) else 20
    lim = max(1, min(200, lim))
    with _health_alert_lock:
        st = dict(_health_alert_state)
        events = list(st.get("events", []))
    events = events[-lim:]
    return {
        "enabled": _HEALTH_ALERT_ENABLED,
        "consecutive": _HEALTH_ALERT_CONSECUTIVE,
        "cooldown_sec": _HEALTH_ALERT_COOLDOWN_SEC,
        "state": {
            "last_level": st.get("last_level", "green"),
            "non_green_streak": int(st.get("non_green_streak", 0)),
            "yellow_streak": int(st.get("yellow_streak", 0)),
            "red_streak": int(st.get("red_streak", 0)),
            "last_alert_at": (
                datetime.datetime.fromtimestamp(float(st.get("last_alert_at", 0.0))).isoformat()
                if float(st.get("last_alert_at", 0.0)) > 0
                else None
            ),
            "events_count": len(st.get("events", [])),
        },
        "events": events,
    }


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
        last_tx_full_at = 0.0
        last_tx_fp: dict[str, tuple] = {}
        last_indices_push_at = 0.0
        last_tick_full_at = 0.0
        last_tick_fp: dict[str, tuple] = {}
        last_signal_full_at = 0.0
        last_signal_fp: dict[str, tuple] = {}
        indices_cache: list = []
        members_cache: list[tuple[str, str]] = []
        last_members_load_at = 0.0
        last_lock_warn_at = 0.0
        member_source = "cache_reuse"
        while True:
            try:
                cycle_t0 = time.perf_counter()
                market = _is_market_open()
                if market != last_market:
                    last_market = market
                    await send_msg({"type": "market_status", **market})
                is_open = bool(market.get("is_open", False))

                now = time.time()
                members = members_cache
                should_refresh_members = (not members_cache) or (now - last_members_load_at >= _WS_MEMBER_REFRESH_SEC)
                if should_refresh_members:
                    try:
                        with _ro_conn_ctx() as conn:
                            members = conn.execute(
                                "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
                                [pool_id],
                            ).fetchall()
                        members_cache = list(members or [])
                        last_members_load_at = now
                        member_source = "db"
                    except Exception as e:
                        # DB lock fallback: keep streaming with previous in-memory member snapshot.
                        if _is_db_lock_error(e) and members_cache:
                            _metric_inc("ws_member_cache_fallback_total")
                            members = members_cache
                            member_source = "cache_fallback"
                            if now - last_lock_warn_at >= 30.0:
                                logger.warning(f"ws member refresh fallback to cache due DB lock: {e}")
                                last_lock_warn_at = now
                        else:
                            raise
                else:
                    member_source = "cache_reuse"
                if not members:
                    await asyncio.sleep(5 if is_open else 30)
                    continue

                ts_codes = [m[0] for m in members]
                provider = get_tick_provider()
                provider.subscribe_symbols(ts_codes)

                tick_data = {}
                miss_codes: list[str] = []
                tick_t0 = time.perf_counter()
                for code in ts_codes:
                    try:
                        cached_tick = provider.get_cached_tick(code)
                    except Exception:
                        cached_tick = None
                    if cached_tick:
                        tick_data[code] = cached_tick
                    else:
                        miss_codes.append(code)

                if miss_codes:
                    if str(getattr(provider, "name", "")) == "gm":
                        fallback_cap = max(0, int(_WS_GM_MISS_TICK_FALLBACK_MAX))
                        fallback_codes = miss_codes[:fallback_cap]
                        for code in fallback_codes:
                            try:
                                tick_data[code] = provider.get_tick(code)
                            except Exception:
                                tick_data[code] = _empty_tick_payload(code)
                        for code in miss_codes[fallback_cap:]:
                            tick_data[code] = _empty_tick_payload(code)
                    else:
                        for code in miss_codes:
                            try:
                                tick_data[code] = provider.get_tick(code)
                            except Exception:
                                tick_data[code] = _empty_tick_payload(code)
                _metric_set("ws_tick_fetch_ms", round((time.perf_counter() - tick_t0) * 1000.0, 2))
                changed_tick_codes: list[str] = []
                full_snapshot = (not last_tick_fp) or (now - last_tick_full_at >= max(1.0, float(_WS_TICK_FULL_SNAPSHOT_SEC)))
                if full_snapshot:
                    await send_msg({"type": "tick", "data": tick_data, "full_snapshot": True})
                    last_tick_fp = {code: _tick_ws_fingerprint(tk) for code, tk in tick_data.items()}
                    last_tick_full_at = now
                    changed_tick_codes = list(tick_data.keys())
                    _metric_inc("ws_tick_full_push_total")
                    _metric_inc("ws_tick_delta_keys_total", float(len(tick_data)))
                else:
                    # 清理已不在当前池中的旧键，避免指纹表无限增长
                    for old_code in list(last_tick_fp.keys()):
                        if old_code not in tick_data:
                            del last_tick_fp[old_code]
                    delta_tick_data: dict[str, dict] = {}
                    for code, tk in tick_data.items():
                        fp = _tick_ws_fingerprint(tk)
                        if last_tick_fp.get(code) != fp:
                            delta_tick_data[code] = tk
                            last_tick_fp[code] = fp
                    changed_tick_codes = list(delta_tick_data.keys())
                    if delta_tick_data:
                        await send_msg({"type": "tick", "data": delta_tick_data, "full_snapshot": False})
                        _metric_inc("ws_tick_delta_push_total")
                        _metric_inc("ws_tick_delta_keys_total", float(len(delta_tick_data)))

                if _WS_PUSH_INDICES_ENABLED and (now - last_indices_push_at >= _WS_INDICES_INTERVAL_SEC):
                    idx_t0 = time.perf_counter()
                    try:
                        idx = _load_indices_snapshot(force_refresh=False)
                        if isinstance(idx, list) and idx:
                            indices_cache = idx
                    except Exception:
                        pass
                    _metric_set("ws_indices_fetch_ms", round((time.perf_counter() - idx_t0) * 1000.0, 2))
                    last_indices_push_at = now
                    if indices_cache:
                        await send_msg({"type": "indices", "data": indices_cache})
                        _metric_inc("ws_indices_push_total")
                elif (not _WS_PUSH_INDICES_ENABLED) and (now - last_indices_push_at >= _WS_INDICES_INTERVAL_SEC):
                    _metric_inc("ws_indices_skip_total")
                    last_indices_push_at = now

                if is_open and (now - last_tx_push_at >= _WS_TX_INTERVAL_SEC):
                    for old_code in list(last_tx_fp.keys()):
                        if old_code not in ts_codes:
                            del last_tx_fp[old_code]
                    tx_full_snapshot = (not last_tx_fp) or (
                        now - last_tx_full_at >= max(1.0, float(_WS_TX_FULL_SNAPSHOT_SEC))
                    )
                    tx_codes = ts_codes if tx_full_snapshot else changed_tick_codes
                    txn_data = {}
                    for code in tx_codes:
                        try:
                            txns = provider.get_cached_transactions(code)
                            if txns is None:
                                txns = mootdx_client.get_transactions(code, 15)
                            txn_data[code] = txns or []
                        except Exception:
                            pass
                    if tx_full_snapshot:
                        await send_msg({"type": "transactions", "data": txn_data, "full_snapshot": True})
                        last_tx_fp = {code: _txn_row_ws_fingerprint(v) for code, v in txn_data.items()}
                        last_tx_full_at = now
                        _metric_inc("ws_tx_full_push_total")
                        _metric_inc("ws_tx_delta_codes_total", float(len(txn_data)))
                    else:
                        delta_txn_data: dict[str, list] = {}
                        for code, rows in txn_data.items():
                            fp = _txn_row_ws_fingerprint(rows)
                            if last_tx_fp.get(code) != fp:
                                delta_txn_data[code] = rows
                                last_tx_fp[code] = fp
                        if delta_txn_data:
                            await send_msg({"type": "transactions", "data": delta_txn_data, "full_snapshot": False})
                            _metric_inc("ws_tx_delta_push_total")
                            _metric_inc("ws_tx_delta_codes_total", float(len(delta_txn_data)))
                    last_tx_push_at = now

                try:
                    signal_result = _evaluate_signals_fast_internal(pool_id, members, provider, tick_data)
                    signal_full_snapshot = (not last_signal_fp) or (
                        now - last_signal_full_at >= max(1.0, float(_WS_SIGNAL_FULL_SNAPSHOT_SEC))
                    )
                    if signal_full_snapshot:
                        await send_msg(
                            {
                                "type": "signals",
                                "pool_id": pool_id,
                                "data": signal_result,
                                "full_snapshot": True,
                                "meta": {
                                    "member_source": member_source,
                                    "ws_member_cache_fallback_total": int(
                                        _metrics_snapshot().get("ws_member_cache_fallback_total", 0)
                                    ),
                                },
                            }
                        )
                        last_signal_fp = {
                            str(r.get("ts_code") or ""): _signal_row_ws_fingerprint(r)
                            for r in (signal_result or [])
                            if isinstance(r, dict) and str(r.get("ts_code") or "")
                        }
                        last_signal_full_at = now
                        _metric_inc("ws_signal_full_push_total")
                        _metric_inc("ws_signal_delta_rows_total", float(len(signal_result or [])))
                    else:
                        cur_signal_fp: dict[str, tuple] = {}
                        delta_signal_rows: list[dict] = []
                        for r in (signal_result or []):
                            if not isinstance(r, dict):
                                continue
                            code = str(r.get("ts_code") or "")
                            if not code:
                                continue
                            fp = _signal_row_ws_fingerprint(r)
                            cur_signal_fp[code] = fp
                            if last_signal_fp.get(code) != fp:
                                delta_signal_rows.append(r)
                        for old_code in list(last_signal_fp.keys()):
                            if old_code not in cur_signal_fp:
                                del last_signal_fp[old_code]
                        for c, fp in cur_signal_fp.items():
                            last_signal_fp[c] = fp
                        if delta_signal_rows:
                            await send_msg(
                                {
                                    "type": "signals",
                                    "pool_id": pool_id,
                                    "data": delta_signal_rows,
                                    "full_snapshot": False,
                                    "meta": {
                                        "member_source": member_source,
                                        "ws_member_cache_fallback_total": int(
                                            _metrics_snapshot().get("ws_member_cache_fallback_total", 0)
                                        ),
                                    },
                                }
                            )
                            _metric_inc("ws_signal_delta_push_total")
                            _metric_inc("ws_signal_delta_rows_total", float(len(delta_signal_rows)))
                except Exception as e:
                    _metric_inc("ws_signals_fail_total")
                    logger.debug(f"ws signals failed: {e}")

                if is_open:
                    _metric_set("ws_cycle_ms", round((time.perf_counter() - cycle_t0) * 1000.0, 2))
                    await asyncio.sleep(max(0.2, float(_WS_OPEN_INTERVAL_SEC)))
                else:
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                _metric_inc("ws_push_loop_error_total")
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




