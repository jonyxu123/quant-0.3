"""Shared realtime runtime context, constants, and pure helpers."""
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


# Keep txn sample size aligned with Layer2 analysis window.
_TXN_ANALYZE_COUNT = 50


POOLS = {
    1: {
        "pool_id": 1,
        "name": "Pool 1 主线择时",
        "strategy": "日线结构 + 中周期确认 + 盘中验证 + 持仓状态管理",
        "desc": "主账户主线择时池：决定建仓、持有与从持仓转回观望",
        "signal_types": ["left_side_buy", "right_side_breakout", "timing_clear"],
    },
    2: {
        "pool_id": 2,
        "name": "Pool 2 T+0",
        "strategy": "盘中做T",
        "desc": "正T买入 + 反T卖出",
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
            "market_phase_detail": threshold.get("market_phase_detail"),
            "session_policy": threshold.get("session_policy"),
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
            "session_override": threshold.get("session_override"),
            "observer_only": bool(threshold.get("observer_only", False)),
            "observer_reason": threshold.get("observer_reason"),
            "blocked": bool(threshold.get("blocked", False)),
            "blocked_reason": threshold.get("blocked_reason"),
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
def _runtime_state_ttl_sec() -> int:
    return max(1, int(_RUNTIME_STATE_REDIS_TTL_DAYS)) * 24 * 3600
def _trade_date_from_dt(v: Optional[datetime.datetime] = None) -> str:
    dt = v if isinstance(v, datetime.datetime) else datetime.datetime.now()
    return dt.strftime("%Y%m%d")
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
def _is_market_open() -> dict:
    now = datetime.datetime.now()
    weekday = now.weekday()
    t = now.hour * 100 + now.minute
    if weekday >= 5:
        return {"is_open": False, "status": "weekend", "next_open": "next weekday 09:15"}
    if 915 <= t < 930:
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


def _is_signal_processing_allowed(ts_epoch: Optional[int] = None) -> bool:
    try:
        now = datetime.datetime.fromtimestamp(int(ts_epoch)) if ts_epoch else datetime.datetime.now()
    except Exception:
        now = datetime.datetime.now()
    if now.weekday() >= 5:
        return False
    hhmm = now.hour * 100 + now.minute
    if 915 <= hhmm < 930:
        return False
    return True
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


__all__ = [name for name in globals() if not name.startswith("__")]
