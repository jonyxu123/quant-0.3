"""gm runtime shared state, configs, and helper functions."""
from __future__ import annotations

import datetime as _dt
import multiprocessing
import secrets
import statistics
import threading
import time
import random
import os
import json
from collections import deque
from multiprocessing.managers import BaseManager
from queue import Queue as _StdQueue, Empty as _QueueEmpty
from typing import Optional
from loguru import logger

from backend.realtime.tick_provider import TickProvider
from backend.realtime.mootdx_client import client as mootdx_client

try:
    from config import POOL1_SIGNAL_CONFIG as _P1_CFG
except Exception:
    _P1_CFG = {}

try:
    from config import T0_SIGNAL_CONFIG as _T0_CFG
except Exception:
    _T0_CFG = {}

try:
    from config import POOL1_OBSERVE_STORAGE_CONFIG as _P1_OBS_CFG
except Exception:
    _P1_OBS_CFG = {}

try:
    import redis as _redis_mod
except Exception:
    _redis_mod = None

try:
    from backend.realtime.threshold_engine import (
        build_signal_context as _build_signal_context,
        get_thresholds as _get_thresholds,
        infer_instrument_profile as _infer_instrument_profile,
        calc_theoretical_limits as _calc_theoretical_limits,
        resolve_market_phase_detail as _resolve_market_phase_detail,
        resolve_session_policy as _resolve_session_policy,
    )
except Exception:
    _build_signal_context = None
    _get_thresholds = None
    _infer_instrument_profile = None
    _calc_theoretical_limits = None
    _resolve_market_phase_detail = None
    _resolve_session_policy = None

# 尝试导入 gm
try:
    from gm.api import *  # type: ignore
    _GM_AVAILABLE = True
except ImportError:
    _GM_AVAILABLE = False
    logger.warning("gm 未安装，Tick Provider 将返回空数据。可安装：pip install gm")


# ============================================================
# 工具函数
# ============================================================
def _ts_code_to_gm_symbol(ts_code: str) -> str:
    """将 '600519.SH' 转换为掘金格式 'SHSE.600519'。"""
    code, suffix = ts_code.split('.')
    if suffix.upper() == 'SH':
        return f'SHSE.{code}'
    elif suffix.upper() == 'SZ':
        return f'SZSE.{code}'
    elif suffix.upper() == 'BJ':
        return f'BJSE.{code}'
    return f'{suffix.upper()}.{code}'


def _gm_symbol_to_ts_code(symbol: str) -> str:
    """将掘金格式 'SHSE.600519' 转换为 '600519.SH'。"""
    exchange, code = symbol.split('.')
    if exchange == 'SHSE':
        return f'{code}.SH'
    elif exchange == 'SZSE':
        return f'{code}.SZ'
    elif exchange == 'BJSE':
        return f'{code}.BJ'
    return f'{code}.{exchange}'


def _is_cn_trading_time_now() -> bool:
    """A-share intraday trading window (includes pre-auction 09:15-09:25)."""
    now = _dt.datetime.now()
    if now.weekday() >= 5:
        return False
    hhmm = now.hour * 100 + now.minute
    if 915 <= hhmm < 925:
        return True
    # Morning session ends at 11:30; from 11:30 to 13:00 is lunch break.
    if 930 <= hhmm < 1130:
        return True
    if 1300 <= hhmm <= 1500:
        return True
    return False


def _is_signal_processing_allowed(ts_epoch: Optional[int] = None) -> bool:
    try:
        dt = _dt.datetime.fromtimestamp(int(ts_epoch)) if ts_epoch else _dt.datetime.now()
    except Exception:
        dt = _dt.datetime.now()
    if dt.weekday() >= 5:
        return False
    hhmm = dt.hour * 100 + dt.minute
    if 915 <= hhmm < 930:
        return False
    return True


def _mock_tick(ts_code: str) -> dict:
    """鐢熸垚 mock tick 鏁版嵁"""
    base = 10 + random.uniform(-2, 2)
    return {
        'ts_code': ts_code, 'name': 'MOCK', 'price': round(base, 2),
        'open': round(base * 0.99, 2), 'high': round(base * 1.02, 2),
        'low': round(base * 0.97, 2), 'pre_close': round(base * 0.98, 2),
        'volume': random.randint(10000, 1000000),
        'amount': random.uniform(1e6, 1e8),
        'pct_chg': round(random.uniform(-3, 3), 2),
        'timestamp': int(time.time()),
        'bids': [(round(base - i * 0.01, 2), random.randint(100, 5000)) for i in range(1, 6)],
        'asks': [(round(base + i * 0.01, 2), random.randint(100, 5000)) for i in range(1, 6)],
        'is_mock': True,
    }


def _with_pre_close_fallback(ts_code: str, tick: dict) -> dict:
    """
    Backfill missing pre_close/pct_chg for gm cached ticks.
    Priority:
    1) in-memory hint cache
    2) mootdx single quote fallback
    """
    if not isinstance(tick, dict):
        return tick

    price = float(tick.get('price', 0) or 0)
    pre_close = float(tick.get('pre_close', 0) or 0)
    now = time.time()

    if pre_close > 0:
        _main_preclose_hints[ts_code] = {'pre_close': pre_close, 'at': now}
        if price > 0:
            out = dict(tick)
            out['pct_chg'] = (price - pre_close) / pre_close * 100
            return out
        return tick

    hint = _main_preclose_hints.get(ts_code)
    if hint:
        hint_pc = float(hint.get('pre_close', 0) or 0)
        hint_at = float(hint.get('at', 0) or 0)
        if hint_pc > 0 and now - hint_at <= _PRECLOSE_HINT_TTL_SEC:
            out = dict(tick)
            out['pre_close'] = hint_pc
            if price > 0:
                out['pct_chg'] = (price - hint_pc) / hint_pc * 100
            return out

    try:
        mt = mootdx_client.get_tick(ts_code)
        mt_pc = float(mt.get('pre_close', 0) or 0)
        if mt_pc > 0:
            _main_preclose_hints[ts_code] = {'pre_close': mt_pc, 'at': now}
            out = dict(tick)
            out['pre_close'] = mt_pc
            if price > 0:
                out['pct_chg'] = (price - mt_pc) / mt_pc * 100
            return out
    except Exception:
        pass
    return tick


def _derive_price_from_order_book(tick: dict) -> tuple[float, str]:
    """Derive indicative price from L1 order book when trade price is unavailable."""
    bids = tick.get('bids') or []
    asks = tick.get('asks') or []
    bid1 = 0.0
    ask1 = 0.0
    try:
        if bids and len(bids) > 0:
            bid1 = float((bids[0] or (0, 0))[0] or 0)
    except Exception:
        bid1 = 0.0
    try:
        if asks and len(asks) > 0:
            ask1 = float((asks[0] or (0, 0))[0] or 0)
    except Exception:
        ask1 = 0.0
    if bid1 > 0 and ask1 > 0:
        return (bid1 + ask1) / 2.0, 'bidask_mid'
    if bid1 > 0:
        return bid1, 'bid1'
    if ask1 > 0:
        return ask1, 'ask1'
    return 0.0, 'none'


# ============================================================
# 信号衰减与状态机工具函数
# ============================================================
def _get_market_phase(ts_epoch: int) -> str:
    """根据 epoch 判断当前交易时段：open/morning/afternoon/close。"""
    dt = _dt.datetime.fromtimestamp(ts_epoch)
    hm = dt.hour * 60 + dt.minute
    if 570 <= hm < 600:    # 09:30-10:00
        return 'open'
    elif 600 <= hm < 690:  # 10:00-11:30
        return 'morning'
    elif 780 <= hm < 870:  # 13:00-14:30
        return 'afternoon'
    elif 870 <= hm < 900:  # 14:30-15:00
        return 'close'
    return 'morning'       # 默认兜底


def _get_market_phase_detail(ts_epoch: int) -> str:
    """更细粒度时段：open_strict / lunch_break / close_reduce 等。"""
    if _resolve_market_phase_detail is not None:
        try:
            return str(_resolve_market_phase_detail(int(ts_epoch)) or 'off_session')
        except Exception:
            pass
    dt = _dt.datetime.fromtimestamp(int(ts_epoch))
    hm = dt.hour * 60 + dt.minute
    if 555 <= hm < 570:
        return 'pre_auction'
    if 570 <= hm < 575:
        return 'open_strict'
    if 575 <= hm < 600:
        return 'open'
    if 600 <= hm < 690:
        return 'morning'
    if 690 <= hm < 780:
        return 'lunch_break'
    if 780 <= hm < 870:
        return 'afternoon'
    if 870 <= hm < 897:
        return 'close'
    if 897 <= hm < 900:
        return 'close_reduce'
    return 'off_session'


def _get_session_policy(ts_epoch: int) -> str:
    """盘中行为策略标签：open_strict / lunch_pause / close_reduce / none。"""
    if _resolve_session_policy is not None:
        try:
            return str(_resolve_session_policy(int(ts_epoch)) or 'none')
        except Exception:
            pass
    detail = _get_market_phase_detail(ts_epoch)
    if detail == 'pre_auction':
        return 'auction_pause'
    if detail == 'open_strict':
        return 'open_strict'
    if detail == 'lunch_break':
        return 'lunch_pause'
    if detail == 'close_reduce':
        return 'close_reduce'
    return 'none'


def _trading_minutes_elapsed(start_ts: int, end_ts: int) -> float:
    """计算两个时间戳间的交易分钟数，自动扣除午休（11:30-13:00）。"""
    if end_ts <= start_ts:
        return 0.0
    elapsed = (end_ts - start_ts) / 60.0
    start_dt = _dt.datetime.fromtimestamp(start_ts)
    end_dt = _dt.datetime.fromtimestamp(end_ts)
    start_hm = start_dt.hour * 60 + start_dt.minute
    end_hm = end_dt.hour * 60 + end_dt.minute
    # 午休 11:30-13:00 = 90 分钟非交易时段
    noon_start, noon_end = 690, 780
    overlap = 0.0
    if start_hm < noon_end and end_hm > noon_start:
        overlap = min(end_hm, noon_end) - max(start_hm, noon_start)
        overlap = max(0.0, overlap)
    return max(0.0, elapsed - overlap)


def _phase_decay_lambda(phase: str, signal_type: str) -> float:
    """返回给定时段的时间衰减参数 lambda（用于信号强度衰减）。"""
    try:
        from config import T0_SIGNAL_CONFIG
        lam = T0_SIGNAL_CONFIG['decay'].get(phase, 0.10)
    except Exception:
        lam = 0.10
    # reverse_t 在开盘/尾盘噪音更大，衰减稍快
    if signal_type == 'reverse_t' and phase in ('open', 'close'):
        lam *= 1.3
    return lam


def _pool1_half_life_lambda(signal_type: str) -> Optional[float]:
    """Pool1 独立半衰期配置转换为 lambda（可选）。"""
    half_life = _P1_HALF_LIFE_CFG.get(signal_type)
    if half_life is None:
        return None
    try:
        hl = float(half_life)
        if hl <= 0:
            return None
        import math
        return math.log(2) / hl
    except Exception:
        return None


def _decay_factor(signal_type: str, signal_ts: int, now_ts: int) -> float:
    """
    计算 [0,1] 衰减因子：factor = exp(-lambda * trading_minutes_elapsed)。
    """
    import math
    trading_min = _trading_minutes_elapsed(signal_ts, now_ts)
    p1_lambda = _pool1_half_life_lambda(signal_type)
    if p1_lambda is not None:
        return math.exp(-p1_lambda * trading_min)
    phase = _get_market_phase(now_ts)
    lam = _phase_decay_lambda(phase, signal_type)
    return math.exp(-lam * trading_min)


def _dedup_seconds_for_signal(signal_type: str, pools: set) -> int:
    """按信号类型返回去重窗口（秒）。Pool1 由独立配置覆盖。"""
    if signal_type in ('left_side_buy', 'right_side_breakout', 'timing_clear'):
        return int(_P1_DEDUP_CFG.get(signal_type, 300) or 300)
    if signal_type in ('positive_t', 'reverse_t'):
        return int(_T0_DEDUP_CFG.get(signal_type, 120) or 120)
    dedup_seconds = 300
    for pool_id in pools:
        dedup_seconds = min(dedup_seconds, SIGNAL_DEDUP_SECONDS_POOL.get(pool_id, 300))
    return dedup_seconds


def _pool1_resonance_60m_proxy(daily: dict, price: float, prev_price: Optional[float]) -> bool:
    """Pool1 60m 共振代理判断。"""
    cfg = _P1_CFG.get('resonance_60m', {}) if isinstance(_P1_CFG, dict) else {}
    if not cfg.get('enabled', False):
        return True
    ma5 = float(daily.get('ma5', 0) or 0)
    ma10 = float(daily.get('ma10', 0) or 0)
    boll_mid = float(daily.get('boll_mid', 0) or 0)
    if ma5 <= ma10 or boll_mid <= 0 or prev_price is None or prev_price <= 0:
        return False
    return prev_price <= boll_mid and price > boll_mid


def _record_signal_perf(elapsed_ms: float) -> None:
    ms = float(max(0.0, elapsed_ms))
    _signal_perf_window.append(ms)
    _signal_perf_stats["count"] = float(_signal_perf_stats.get("count", 0.0) + 1.0)
    _signal_perf_stats["sum_ms"] = float(_signal_perf_stats.get("sum_ms", 0.0) + ms)
    _signal_perf_stats["max_ms"] = float(max(_signal_perf_stats.get("max_ms", 0.0), ms))
    _signal_perf_stats["last_ms"] = ms
    if ms >= _SIGNAL_PERF_SLOW_MS:
        _signal_perf_stats["slow_count"] = float(_signal_perf_stats.get("slow_count", 0.0) + 1.0)


def _record_consumer_batch_perf(raw_cnt: int, proc_cnt: int, *, backlog: int = 0, target: int = 0) -> None:
    r = float(max(0, int(raw_cnt)))
    p = float(max(0, int(proc_cnt)))
    _consumer_perf_stats["batch_count"] = float(_consumer_perf_stats.get("batch_count", 0.0) + 1.0)
    _consumer_perf_stats["batch_sum_raw"] = float(_consumer_perf_stats.get("batch_sum_raw", 0.0) + r)
    _consumer_perf_stats["batch_sum_proc"] = float(_consumer_perf_stats.get("batch_sum_proc", 0.0) + p)
    _consumer_perf_stats["batch_last_raw"] = r
    _consumer_perf_stats["batch_last_proc"] = p
    _consumer_perf_stats["batch_last_backlog"] = float(max(0, int(backlog)))
    _consumer_perf_stats["batch_last_target"] = float(max(0, int(target)))
    if r > p:
        _consumer_perf_stats["coalesced_drop_total"] = float(
            _consumer_perf_stats.get("coalesced_drop_total", 0.0) + (r - p)
        )
    if target > _CONSUMER_BATCH_MAX:
        _consumer_perf_stats["backlog_drain_batches"] = float(
            _consumer_perf_stats.get("backlog_drain_batches", 0.0) + 1.0
        )


def get_signal_perf_stats() -> dict:
    arr = list(_signal_perf_window)
    count = int(_signal_perf_stats.get("count", 0.0) or 0)
    avg_ms = float(_signal_perf_stats.get("sum_ms", 0.0) / count) if count > 0 else 0.0
    max_ms = float(_signal_perf_stats.get("max_ms", 0.0) or 0.0)
    last_ms = float(_signal_perf_stats.get("last_ms", 0.0) or 0.0)
    slow_count = int(_signal_perf_stats.get("slow_count", 0.0) or 0)
    out = {
        "count": count,
        "avg_ms": round(avg_ms, 3),
        "max_ms": round(max_ms, 3),
        "last_ms": round(last_ms, 3),
        "slow_ms_threshold": float(_SIGNAL_PERF_SLOW_MS),
        "slow_count": slow_count,
        "slow_ratio": round((slow_count / count), 4) if count > 0 else 0.0,
        "window_size": len(arr),
        "queue_size": int(_main_queue.qsize()),
        "queue_capacity": int(getattr(_main_queue, "maxsize", 0) or 0),
        "resonance_refresh_queue_size": int(_pool1_resonance_refresh_queue.qsize()),
    }
    bcount = int(_consumer_perf_stats.get("batch_count", 0.0) or 0)
    if bcount > 0:
        avg_raw = float(_consumer_perf_stats.get("batch_sum_raw", 0.0) / bcount)
        avg_proc = float(_consumer_perf_stats.get("batch_sum_proc", 0.0) / bcount)
        out["consumer_batch_count"] = bcount
        out["consumer_batch_avg_raw"] = round(avg_raw, 3)
        out["consumer_batch_avg_proc"] = round(avg_proc, 3)
        out["consumer_batch_last_raw"] = int(_consumer_perf_stats.get("batch_last_raw", 0.0) or 0)
        out["consumer_batch_last_proc"] = int(_consumer_perf_stats.get("batch_last_proc", 0.0) or 0)
        out["consumer_batch_last_backlog"] = int(_consumer_perf_stats.get("batch_last_backlog", 0.0) or 0)
        out["consumer_batch_last_target"] = int(_consumer_perf_stats.get("batch_last_target", 0.0) or 0)
        out["consumer_backlog_drain_batches"] = int(_consumer_perf_stats.get("backlog_drain_batches", 0.0) or 0)
        out["consumer_coalesced_drop_total"] = int(_consumer_perf_stats.get("coalesced_drop_total", 0.0) or 0)
    else:
        out["consumer_batch_count"] = 0
        out["consumer_batch_avg_raw"] = 0.0
        out["consumer_batch_avg_proc"] = 0.0
        out["consumer_batch_last_raw"] = 0
        out["consumer_batch_last_proc"] = 0
        out["consumer_batch_last_backlog"] = 0
        out["consumer_batch_last_target"] = 0
        out["consumer_backlog_drain_batches"] = 0
        out["consumer_coalesced_drop_total"] = 0
    out["consumer_batch_max"] = int(_CONSUMER_BATCH_MAX)
    out["consumer_drain_max"] = int(_CONSUMER_DRAIN_MAX)
    out["consumer_backlog_drain_trigger"] = int(_CONSUMER_BACKLOG_DRAIN_TRIGGER)
    if arr:
        s = sorted(arr)
        n = len(s)

        def _pct(p: float) -> float:
            if n <= 1:
                return float(s[0])
            idx = int(round((n - 1) * p))
            idx = max(0, min(n - 1, idx))
            return float(s[idx])

        out["p50_ms"] = round(_pct(0.50), 3)
        out["p95_ms"] = round(_pct(0.95), 3)
        out["p99_ms"] = round(_pct(0.99), 3)
    return out


def _start_pool1_resonance_worker() -> None:
    global _pool1_resonance_worker_started
    if _pool1_resonance_worker_started:
        return
    with _pool1_resonance_worker_lock:
        if _pool1_resonance_worker_started:
            return
        _pool1_resonance_worker_started = True

        def _loop():
            while True:
                try:
                    ts_code = _pool1_resonance_refresh_queue.get(timeout=1.0)
                except _QueueEmpty:
                    continue
                except Exception:
                    continue
                try:
                    if not ts_code:
                        continue
                    tick = _main_cache.get(ts_code) or {}
                    daily = _main_daily_cache.get(ts_code) or {}
                    price = float(tick.get("price", 0) or 0)
                    prev_price = _main_prev_price.get(ts_code)
                    now_ts = float(time.time())
                    t0 = time.perf_counter()
                    try:
                        from backend.realtime import signals as sig
                        bars_1m = mootdx_client.get_minute_bars(ts_code, 240)
                        if bars_1m:
                            val, info = sig.compute_pool1_resonance_60m(bars_1m, fallback=False)
                            info = dict(info or {})
                            info["source"] = "1m_aggregate_async"
                            info["async"] = True
                            info["calc_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)
                            _pool1_resonance_cache[ts_code] = {
                                "value": bool(val),
                                "info": info,
                                "updated_at": now_ts,
                            }
                            continue
                    except Exception as e:
                        logger.debug(f"pool1 60m共振异步计算失败 {ts_code}: {e}")
                    proxy_val = _pool1_resonance_60m_proxy(daily, price, prev_price)
                    info = {
                        "enabled": True,
                        "source": "proxy_fallback_async",
                        "reason": "minute_bars_unavailable",
                        "async": True,
                        "calc_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    }
                    _pool1_resonance_cache[ts_code] = {
                        "value": bool(proxy_val),
                        "info": info,
                        "updated_at": now_ts,
                    }
                finally:
                    _pool1_resonance_refresh_pending.discard(str(ts_code))

        threading.Thread(target=_loop, daemon=True, name="pool1-resonance-worker").start()


def _schedule_pool1_resonance_refresh(ts_code: str, now_ts: float) -> bool:
    if not _POOL1_RESONANCE_ASYNC_ENABLED:
        return False
    code = str(ts_code or "")
    if not code:
        return False
    last = float(_pool1_resonance_refresh_last_at.get(code, 0.0) or 0.0)
    if now_ts - last < max(0.5, float(_POOL1_RESONANCE_ASYNC_MIN_GAP_SEC)):
        return False
    if code in _pool1_resonance_refresh_pending:
        return False
    _start_pool1_resonance_worker()
    _pool1_resonance_refresh_pending.add(code)
    _pool1_resonance_refresh_last_at[code] = now_ts
    try:
        _pool1_resonance_refresh_queue.put_nowait(code)
        return True
    except Exception:
        _pool1_resonance_refresh_pending.discard(code)
        return False


def _pool1_resonance_60m(ts_code: str, daily: dict, price: float, prev_price: Optional[float], now_ts: Optional[float] = None) -> tuple[bool, dict]:
    """
    Pool1 60m 共振计算（缓存优先）。
    1) 优先命中缓存（减少 IO）。
    2) 缓存过期后，聚合 1m 数据计算 60m 共振；失败时走代理兜底。
    """
    if not _P1_RESONANCE_ENABLED:
        return True, {'enabled': False, 'source': 'disabled'}

    now_ts = float(now_ts if now_ts is not None else time.time())
    refresh_sec = max(30, int(_P1_RESONANCE_REFRESH_SEC))
    cache = _pool1_resonance_cache.get(ts_code)
    if cache:
        age = now_ts - float(cache.get('updated_at', 0) or 0)
        if age < refresh_sec:
            info = dict(cache.get('info') or {})
            info['source'] = 'cache'
            info['cache_age_sec'] = round(age, 1)
            return bool(cache.get('value', False)), info
        # 缓存过期：异步刷新时，直接返回旧值避免阻塞主信号线程
        if _POOL1_RESONANCE_ASYNC_ENABLED:
            queued = _schedule_pool1_resonance_refresh(ts_code, now_ts)
            info = dict(cache.get('info') or {})
            info['source'] = 'cache_stale'
            info['cache_age_sec'] = round(age, 1)
            info['refresh_queued'] = bool(queued)
            return bool(cache.get('value', False)), info

    if _POOL1_RESONANCE_ASYNC_ENABLED:
        queued = _schedule_pool1_resonance_refresh(ts_code, now_ts)
        # 冷启动时先走代理，分钟线结果由异步线程回填
        proxy_val = _pool1_resonance_60m_proxy(daily, price, prev_price)
        info = {'enabled': True, 'source': 'proxy_coldstart_async', 'refresh_queued': bool(queued)}
        _pool1_resonance_cache[ts_code] = {
            'value': bool(proxy_val),
            'info': info,
            'updated_at': now_ts,
        }
        return bool(proxy_val), info

    try:
        from backend.realtime import signals as sig
        bars_1m = mootdx_client.get_minute_bars(ts_code, 240)
        if bars_1m:
            val, info = sig.compute_pool1_resonance_60m(bars_1m, fallback=False)
            info = dict(info or {})
            info['source'] = '1m_aggregate'
            _pool1_resonance_cache[ts_code] = {
                'value': bool(val),
                'info': info,
                'updated_at': now_ts,
            }
            return bool(val), info
    except Exception as e:
        logger.debug(f"pool1 60m共振计算失败 {ts_code}: {e}")

    # 无法获取分钟线时，使用代理逻辑兜底
    proxy_val = _pool1_resonance_60m_proxy(daily, price, prev_price)
    info = {'enabled': True, 'source': 'proxy_fallback', 'reason': 'minute_bars_unavailable'}
    _pool1_resonance_cache[ts_code] = {
        'value': bool(proxy_val),
        'info': info,
        'updated_at': now_ts,
    }
    return bool(proxy_val), info


def _update_signal_state(ts_code: str, signal: dict, now: int) -> dict:
    """更新信号状态并返回包含 state/age/current_strength 的信号副本。生命周期：active -> decaying -> expired。"""
    try:
        from config import T0_SIGNAL_CONFIG
        decay_cfg = T0_SIGNAL_CONFIG['decay']
        hard_expiry_min = decay_cfg.get('hard_expiry_min', 12)
        min_floor = decay_cfg.get('min_floor', 0.15)
    except Exception:
        hard_expiry_min, min_floor = 12, 0.15

    signal_type = signal.get('type', '')
    states = _main_signal_state_cache.setdefault(ts_code, {})

    initial_strength = signal.get('strength', 50)
    triggered_at = signal.get('triggered_at', now)
    is_new = signal.get('is_new', False)

    if signal_type not in states or is_new:
        # 首次进入状态机
        states[signal_type] = {
            'state': 'active',
            'triggered_at': triggered_at,
            'trigger_price': signal.get('price', 0),
            'initial_strength': initial_strength,
            'current_strength': float(initial_strength),
            'last_update_at': now,
            'expire_reason': '',
        }

    st = states[signal_type]
    trading_min = _trading_minutes_elapsed(st['triggered_at'], now)
    factor = _decay_factor(signal_type, st['triggered_at'], now)
    current_strength = st['initial_strength'] * factor
    st['current_strength'] = round(current_strength, 1)
    st['last_update_at'] = now

    # Pool1：可配置为“当日持续展示，不因时间衰减自动消失”，直到新信号覆盖或跨日重置。
    pool_ids = signal.get('_pool_ids')
    is_pool1_signal = False
    if isinstance(pool_ids, list):
        is_pool1_signal = any(int(pid) == 1 for pid in pool_ids if isinstance(pid, (int, str)) and str(pid).strip().isdigit())
    if not is_pool1_signal and signal_type in ('left_side_buy', 'right_side_breakout', 'timing_clear'):
        is_pool1_signal = True
    trigger_dt = _dt.datetime.fromtimestamp(int(st.get('triggered_at', now)))
    now_dt = _dt.datetime.fromtimestamp(int(now))
    same_day = (trigger_dt.date() == now_dt.date())
    keep_until_eod = bool(_P1_KEEP_SIGNALS_UNTIL_EOD and is_pool1_signal and same_day)

    # 状态迁移
    if st['state'] != 'expired':
        if keep_until_eod:
            # 当日仅展示衰减态，不因 timeout/weak 删除。
            st['expire_reason'] = ''
            if current_strength < 0.6 * st['initial_strength']:
                st['state'] = 'decaying'
            else:
                st['state'] = 'active'
        elif trading_min >= hard_expiry_min:
            st['state'] = 'expired'
            st['expire_reason'] = 'timeout'
        elif current_strength < min_floor * st['initial_strength']:
            st['state'] = 'expired'
            st['expire_reason'] = 'weak'
        elif current_strength < 0.6 * st['initial_strength']:
            st['state'] = 'decaying'

    s = dict(signal)
    s['state'] = st['state']
    s['age_sec'] = int(now - st['triggered_at'])
    s['current_strength'] = st['current_strength']
    s['expire_reason'] = st['expire_reason']
    return s


def _expire_reversed_signal_state(ts_code: str, signal_type: str) -> None:
    """当出现反向信号时，将上一方向信号立即标记为 expired(reversed)。"""
    states = _main_signal_state_cache.get(ts_code, {})
    if signal_type in states and states[signal_type]['state'] != 'expired':
        states[signal_type]['state'] = 'expired'
        states[signal_type]['expire_reason'] = 'reversed'


# ============================================================
# ============================================================
# 项目路径
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
_STRATEGY_FILE = os.path.join(_PROJECT_ROOT, '_gm_strategy.py')  # gm 策略脚本路径
# ============================================================
# 三层结构（生产/消费/读取）
# ------------------------------------------------------------
# Layer 1（生产）：gm on_tick -> shared_queue.put()
# Layer 2（消费线程，主进程）：queue.get() -> shared_cache[ts] = tick
# Layer 3（读取）：FastAPI / Provider 从 cache 获取
#
# 通过 BaseManager TCP server 共享 Queue 与 dict。
# 主进程保留对象，子进程通过 TCP 代理连接访问。
_tick_lock = threading.Lock()  # 线程锁
_subscribed_codes: set[str] = set()  # 已订阅 ts_code
_gm_context = None

# 订阅列表文件（供 gm 策略脚本 init 恢复订阅）
_SUBS_FILE = os.path.join(_PROJECT_ROOT, '.gm_tick_subs.txt')

_main_queue: _StdQueue = _StdQueue(maxsize=50000)   # (ts_code, tick_dict) 队列
_main_cache: dict[str, dict] = {}                   # ts_code -> tick dict
_main_preclose_hints: dict[str, dict] = {}          # ts_code -> {'pre_close': float, 'at': epoch}
_PRECLOSE_HINT_TTL_SEC = 300
_GM_MISS_FALLBACK_ENABLED = os.getenv("GM_MISS_FALLBACK_ENABLED", "1").lower() in ("1", "true", "yes", "on")
_GM_MISS_FALLBACK_WHEN_OPEN = os.getenv("GM_MISS_FALLBACK_WHEN_OPEN", "0").lower() in ("1", "true", "yes", "on")
_GM_MISS_FALLBACK_TTL_SEC = float(os.getenv("GM_MISS_FALLBACK_TTL_SEC", "5") or 5.0)
_main_miss_fallback_at: dict[str, float] = {}

_main_daily_cache: dict[str, dict] = {}             # ts_code -> 日线指标
_main_chip_cache: dict[str, dict] = {}              # ts_code -> 筹码特征
_main_signal_cache: dict[str, dict] = {}            # ts_code -> 实时信号缓存
_main_stock_pools: dict[str, set] = {}              # ts_code -> {pool_id, ...}
_main_stock_industry: dict[str, str] = {}           # ts_code -> industry
_main_stock_industry_code: dict[str, str] = {}      # ts_code -> eastmoney industry board code
_main_stock_concepts: dict[str, list[str]] = {}     # ts_code -> [concept_name, ...]
_main_stock_core_concept: dict[str, str] = {}       # ts_code -> core concept_name
_main_stock_concept_codes: dict[str, list[str]] = {}  # ts_code -> [board_code, ...]
_main_stock_core_concept_code: dict[str, str] = {}    # ts_code -> core board_code
_main_concept_snapshot: dict[str, dict] = {}        # concept_name -> ecology snapshot
_main_industry_snapshot: dict[str, dict] = {}       # industry_name -> ecology snapshot
_main_instrument_profile: dict[str, dict] = {}      # ts_code -> instrument profile
# 信号去重：(ts_code, signal_type) -> 上次触发 timestamp
_main_signal_last_fire: dict[tuple, int] = {}
SIGNAL_DEDUP_SECONDS_POOL: dict[int, int] = {
    1: 300,  # Pool1 去重窗口（秒）
    2: 120,  # Pool2 去重窗口（秒，T+0 更敏捷）
}
REVERSE_SIGNAL_PAIRS = {
    'positive_t': ('reverse_t',),
    'reverse_t': ('positive_t',),
    'left_side_buy': ('timing_clear',),
    'right_side_breakout': ('timing_clear',),
    'timing_clear': ('left_side_buy', 'right_side_breakout'),
}

_P1_DEDUP_CFG = _P1_CFG.get('dedup_sec', {}) if isinstance(_P1_CFG, dict) else {}
_T0_DEDUP_CFG = _T0_CFG.get('dedup_sec', {}) if isinstance(_T0_CFG, dict) else {}
_P1_HALF_LIFE_CFG = _P1_CFG.get('decay_half_life_min', {}) if isinstance(_P1_CFG, dict) else {}
_P1_KEEP_SIGNALS_UNTIL_EOD = bool(_P1_CFG.get('keep_signals_until_eod', True))
_P1_RESONANCE_CFG = _P1_CFG.get('resonance_60m', {}) if isinstance(_P1_CFG, dict) else {}
_P1_RESONANCE_ENABLED = bool(_P1_RESONANCE_CFG.get('enabled', False))
_P1_RESONANCE_REFRESH_SEC = int(_P1_RESONANCE_CFG.get('refresh_sec', 120) or 120)
_P1_VOL_PACE_CFG = _P1_CFG.get('volume_pace', {}) if isinstance(_P1_CFG, dict) else {}
_P1_VOL_PACE_MIN_PROGRESS = float(_P1_VOL_PACE_CFG.get('min_progress', 0.15) or 0.15)
_P1_VOL_PACE_SHRINK_TH = float(_P1_VOL_PACE_CFG.get('shrink_th', 0.70) or 0.70)
_P1_VOL_PACE_EXPAND_TH = float(_P1_VOL_PACE_CFG.get('expand_th', 1.30) or 1.30)
_P1_VOL_PACE_SURGE_TH = float(_P1_VOL_PACE_CFG.get('surge_th', 2.00) or 2.00)
_T0_VOL_PACE_CFG = _T0_CFG.get('volume_pace', {}) if isinstance(_T0_CFG, dict) else {}
_T0_VOL_PACE_MIN_PROGRESS = float(_T0_VOL_PACE_CFG.get('min_progress', 0.12) or 0.12)
_T0_VOL_PACE_SHRINK_TH = float(_T0_VOL_PACE_CFG.get('shrink_th', 0.70) or 0.70)
_T0_VOL_PACE_EXPAND_TH = float(_T0_VOL_PACE_CFG.get('expand_th', 1.30) or 1.30)
_T0_VOL_PACE_SURGE_TH = float(_T0_VOL_PACE_CFG.get('surge_th', 2.00) or 2.00)
_T0_MICRO_CFG = _T0_CFG.get('microstructure', {}) if isinstance(_T0_CFG, dict) else {}
_T0_LURE_LONG_BIG_BIAS_MIN = float(_T0_MICRO_CFG.get('lure_long_big_bias_min', 0.25) or 0.25)
_T0_LURE_LONG_BIDASK_MAX = float(_T0_MICRO_CFG.get('lure_long_bidask_max', 0.85) or 0.85)
_T0_LURE_LONG_PULLUP_MIN_PCT = float(_T0_MICRO_CFG.get('lure_long_pullup_min_pct', 0.25) or 0.25)
_T0_LURE_LONG_RETRACE_MIN_PCT = float(_T0_MICRO_CFG.get('lure_long_retrace_min_pct', 0.12) or 0.12)
_T0_LURE_LONG_FADE_RATIO_MAX = float(_T0_MICRO_CFG.get('lure_long_fade_ratio_max', 0.80) or 0.80)
_T0_LURE_LONG_NET_BPS_DROP = float(_T0_MICRO_CFG.get('lure_long_net_bps_drop', 30.0) or 30.0)
_T0_DIV_FLOW_RATIO_MAX = float(_T0_MICRO_CFG.get('divergence_flow_ratio_max', 0.85) or 0.85)
_T0_DIV_NET_BPS_DROP = float(_T0_MICRO_CFG.get('divergence_net_bps_drop', 25.0) or 25.0)
_T0_DIV_PRICE_EPS_PCT = float(_T0_MICRO_CFG.get('divergence_price_eps_pct', 0.05) or 0.05)

# 每个池允许的信号类型（择时池 vs T+0 池）
POOL_SIGNAL_TYPES: dict[int, set] = {
     1: {'left_side_buy', 'right_side_breakout', 'timing_clear'},   # Pool1 择时建仓+清仓
     2: {'positive_t', 'reverse_t'},                # Pool2 T+0 日内
}

POOL_CHANNEL_NAME: dict[int, str] = {
    1: 'pool1_timing',
    2: 'pool2_t0',
}

SIGNAL_CHANNEL_SOURCE: dict[int, str] = {
    1: 'tick+daily+chip+resonance60m',
    2: 'tick+intraday_state+microstructure',
}

# Layer2 T+0 日内状态缓存，用于 VWAP/GUB5/Z-Score 特征
# _intraday_state[ts_code] = {
#   'date': 'YYYY-MM-DD',
#   'cum_volume': int, 'cum_amount': float,    # 累计成交 -> VWAP
#   'last_volume': int,                        # 用于计算增量成交量
#   'price_window': deque[(ts, price, vol)],  # 最近 N 分钟 tick
#   'gub5_small_sells_5min': deque[ts],       # 最近 5 分钟小单卖出
# }
_intraday_state: dict[str, dict] = {}
INTRADAY_WINDOW_SECONDS = 20 * 60                    # 20 分钟价格滚动窗口
GUB5_WINDOW_SECONDS = 5 * 60                         # 5 分钟 GUB5 滚动窗口

# Layer 2: recent transactions cache shared by Pool1/Pool2.
# _main_recent_txns[ts_code] = [{time, price, volume, direction}, ...]
_main_recent_txns: dict[str, list] = {}

# Large-order thresholds (hand count) for transaction structure analysis.
BIG_ORDER_THRESHOLD = int(os.getenv("GM_BIG_ORDER_THRESHOLD", "500") or 500)
SUPER_BIG_ORDER_THRESHOLD = int(os.getenv("GM_SUPER_BIG_ORDER_THRESHOLD", "2000") or 2000)
if SUPER_BIG_ORDER_THRESHOLD <= BIG_ORDER_THRESHOLD:
    SUPER_BIG_ORDER_THRESHOLD = max(BIG_ORDER_THRESHOLD * 3, BIG_ORDER_THRESHOLD + 1)
WALL_LEVELS = int(os.getenv("GM_WALL_LEVELS", "3") or 3)
WALL_LEVELS = max(1, min(5, WALL_LEVELS))
ASK_WALL_ABSORB_TH = float(os.getenv("GM_ASK_WALL_ABSORB_TH", "0.85") or 0.85)
BID_WALL_BREAK_TH = float(os.getenv("GM_BID_WALL_BREAK_TH", "0.85") or 0.85)
WALL_SUPER_WEIGHT = float(os.getenv("GM_WALL_SUPER_WEIGHT", "0.5") or 0.5)
WALL_NET_FLOW_BPS_TH = float(os.getenv("GM_WALL_NET_FLOW_BPS_TH", "30") or 30.0)
# Number of recent transactions kept for analysis.
TXN_ANALYZE_COUNT = 50

# Layer2 信号状态缓存
#   'state':            'active' | 'decaying' | 'expired',
#   'triggered_at': int,       # 首次触发时间戳（epoch）
#   'initial_strength': float,
#   'current_strength': float,
#   'last_update_at':   int,
#   'expire_reason':    str,       # '' | 'timeout' | 'reversed' | 'weak'
# }}
_main_signal_state_cache: dict[str, dict] = {}
_main_prev_price: dict[str, float] = {}
_P1_POS_CFG = _P1_CFG.get('position_state', {}) if isinstance(_P1_CFG, dict) else {}
_P1_POS_ENABLED = bool(_P1_POS_CFG.get('enabled', True))
_P1_POS_FILE = os.path.join(_PROJECT_ROOT, str(_P1_POS_CFG.get('file', '.pool1_position_state.json')))
_P1_POS_FILE_MAX_ITEMS = int(_P1_POS_CFG.get('max_items', 5000) or 5000)
_P1_POS_FILE_FALLBACK = bool(_P1_POS_CFG.get('file_fallback', True))
_P1_POS_REDIS_CFG = _P1_POS_CFG.get('redis', {}) if isinstance(_P1_POS_CFG, dict) else {}
_P1_POS_REDIS_ENABLED = bool(_P1_POS_REDIS_CFG.get('enabled', False))
_P1_POS_REDIS_URL = str(_P1_POS_REDIS_CFG.get('url', 'redis://127.0.0.1:6379/0'))
_P1_POS_REDIS_KEY_PREFIX = str(_P1_POS_REDIS_CFG.get('key_prefix', 'quant:pool1:position'))
_P1_POS_REDIS_TTL_DAYS = int(_P1_POS_REDIS_CFG.get('ttl_days', 14) or 14)
_pool1_position_state: dict[str, dict] = {}
_pool1_position_state_loaded = False
_pool1_position_state_lock = threading.Lock()
_pool1_position_redis_cli = None
_pool1_position_redis_warn_at = 0.0
_pool1_position_redis_next_retry_at = 0.0
_pool1_position_last_source = 'memory'
_POOL1_BUY_SIGNAL_TYPES = ('left_side_buy', 'right_side_breakout')
_POOL1_SELL_SIGNAL_TYPES = ('timing_clear',)



_pool1_resonance_cache: dict[str, dict] = {}
_POOL1_RESONANCE_ASYNC_ENABLED = os.getenv("POOL1_RESONANCE_ASYNC_ENABLED", "1").lower() in ("1", "true", "yes", "on")
_POOL1_RESONANCE_ASYNC_MIN_GAP_SEC = float(os.getenv("POOL1_RESONANCE_ASYNC_MIN_GAP_SEC", "5") or 5)
_pool1_resonance_refresh_queue: _StdQueue = _StdQueue(maxsize=2048)
_pool1_resonance_refresh_pending: set[str] = set()
_pool1_resonance_refresh_last_at: dict[str, float] = {}
_pool1_resonance_worker_started = False
_pool1_resonance_worker_lock = threading.Lock()

_SIGNAL_PERF_WINDOW_MAX = int(os.getenv("SIGNAL_PERF_WINDOW_MAX", "2000") or 2000)
_SIGNAL_PERF_SLOW_MS = float(os.getenv("SIGNAL_PERF_SLOW_MS", "15") or 15)
_signal_perf_window: deque[float] = deque(maxlen=max(200, _SIGNAL_PERF_WINDOW_MAX))
_signal_perf_stats: dict[str, float] = {
    "count": 0.0,
    "sum_ms": 0.0,
    "max_ms": 0.0,
    "last_ms": 0.0,
    "slow_count": 0.0,
}
_CONSUMER_BATCH_MAX = int(os.getenv("GM_CONSUMER_BATCH_MAX", "256") or 256)
_CONSUMER_COALESCE_ENABLED = os.getenv("GM_CONSUMER_COALESCE_ENABLED", "1").lower() in ("1", "true", "yes", "on")
_CONSUMER_COALESCE_MIN_BATCH = int(os.getenv("GM_CONSUMER_COALESCE_MIN_BATCH", "8") or 8)
_CONSUMER_DRAIN_MAX = int(os.getenv("GM_CONSUMER_DRAIN_MAX", "2048") or 2048)
_CONSUMER_BACKLOG_DRAIN_TRIGGER = int(os.getenv("GM_CONSUMER_BACKLOG_DRAIN_TRIGGER", "512") or 512)
_consumer_perf_stats: dict[str, float] = {
    "batch_count": 0.0,
    "batch_sum_raw": 0.0,
    "batch_sum_proc": 0.0,
    "batch_last_raw": 0.0,
    "batch_last_proc": 0.0,
    "batch_last_backlog": 0.0,
    "batch_last_target": 0.0,
    "backlog_drain_batches": 0.0,
    "coalesced_drop_total": 0.0,
}

# Layer3 持久化队列（消费线程 -> writer）
_persist_queue: Optional[_StdQueue] = None
_TICK_MGR_AUTHKEY: bytes = secrets.token_bytes(16)
_TICK_MGR_ADDRESS: tuple = ('127.0.0.1', 0)          # 0 = 自动分配端口

_manager_server = None
_consumer_thread_started = False
_signal_state_refresh_started = False
_SIGNAL_STATE_REFRESH_ENABLED = os.getenv("SIGNAL_STATE_REFRESH_ENABLED", "1").lower() in ("1", "true", "yes", "on")
_SIGNAL_STATE_REFRESH_INTERVAL_SEC = float(os.getenv("SIGNAL_STATE_REFRESH_INTERVAL_SEC", "10") or 10)
_SIGNAL_STATE_REFRESH_POLICIES = {"lunch_pause", "close_reduce"}

# Pool1 两阶段统计（轻量观测）
_POOL1_REJECT_DEFAULTS = (
    'stage1_fail',
    'veto_lure_long',
    'veto_wash_trade',
    'limit_magnet_block',
    'resonance_missing',
    'confirm_missing',
)
_POOL1_SIGNAL_TYPES = ('left_side_buy', 'right_side_breakout')
_POOL1_OBS_WINDOWS_SEC = {'5m': 5 * 60, '15m': 15 * 60, '1h': 60 * 60}
_POOL1_OBS_EVENTS_MAX = max(2000, int(os.getenv("POOL1_OBS_EVENTS_MAX", "40000") or 40000))


def _pool1_threshold_observe_default() -> dict:
    return {
        sig_type: {
            'total': 0,
            'blocked': 0,
            'observer_only': 0,
            'by_bucket': {},
            'blocked_by_bucket': {},
            'observer_by_bucket': {},
            'by_version': {},
            'by_source': {},
            'blocked_reasons': {},
            'current': {},
        }
        for sig_type in _POOL1_SIGNAL_TYPES
    }


_pool1_observe = {
    'trade_date': '',
    'updated_at': 0,
    'screen_total': 0,
    'screen_pass': 0,
    'stage2_triggered': 0,
    'reject_counts': {},
    'reject_by_signal_type': {k: {} for k in _POOL1_SIGNAL_TYPES},
    'reject_by_industry': {},
    'reject_by_regime': {},
    'reject_by_board_segment': {},
    'reject_by_security_type': {},
    'reject_by_listing_stage': {},
    'threshold_observe': _pool1_threshold_observe_default(),
}
_pool1_observe_recent_events = deque(maxlen=_POOL1_OBS_EVENTS_MAX)
_pool1_observe_last_source = 'memory'

_P1_OBS_REDIS_CFG = _P1_OBS_CFG.get('redis', {}) if isinstance(_P1_OBS_CFG, dict) else {}
_P1_OBS_REDIS_ENABLED = bool(_P1_OBS_REDIS_CFG.get('enabled', False))
_P1_OBS_REDIS_URL = str(_P1_OBS_REDIS_CFG.get('url', 'redis://127.0.0.1:6379/0'))
_P1_OBS_REDIS_KEY_PREFIX = str(_P1_OBS_REDIS_CFG.get('key_prefix', 'quant:pool1:observe'))
_P1_OBS_REDIS_TTL_DAYS = int(_P1_OBS_REDIS_CFG.get('ttl_days', 7) or 7)
_P1_OBS_REDIS_FLUSH_SEC = float(_P1_OBS_REDIS_CFG.get('flush_sec', os.getenv("POOL1_OBS_REDIS_FLUSH_SEC", "2")) or 2)
_P1_OBS_REDIS_THRESHOLD_FLUSH_SEC = float(
    _P1_OBS_REDIS_CFG.get('threshold_flush_sec', os.getenv("POOL1_OBS_REDIS_THRESHOLD_FLUSH_SEC", "8")) or 8
)
_pool1_observe_redis_cli = None
_pool1_observe_redis_warn_at = 0.0
_pool1_observe_redis_next_retry_at = 0.0
_pool1_observe_redis_last_flush_at = 0.0
_pool1_observe_redis_last_threshold_flush_at = 0.0


__all__ = [name for name in globals() if not name.startswith("__")]
