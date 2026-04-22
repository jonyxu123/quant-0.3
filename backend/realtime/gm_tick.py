"""
掘金量化(gm) Tick Provider（subscribe 订阅模式，多进程版）。
工作原理：
1) _gm_strategy.py 作为 gm.run() 入口脚本，在独立进程中启动行情订阅。
2) on_tick 写入共享队列/缓存；GmTickProvider.get_tick() 读取最新数据。
注意：gm.run() 必须在主线程运行，因此使用 multiprocessing.Process 隔离。
symbol 使用 'SHSE.600519' 格式。
文档：
https://www.myquant.cn/docs2/sdk/python/API介绍/数据订阅.html#subscribe-行情订阅
"""
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
    )
except Exception:
    _build_signal_context = None
    _get_thresholds = None
    _infer_instrument_profile = None
    _calc_theoretical_limits = None

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
    """Pool2 半衰期配置（分钟）转换为 lambda。"""
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
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
 #   'last_volume': int,                          # 用于计算增量成交量
 #   'price_window': deque[(ts, price, vol)],    # 最近 N 分钟 tick
 #   'gub5_small_sells_5min': deque[ts],         # 最近 5 分钟小单卖出
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


def _pool1_position_empty(now_ts: Optional[int] = None) -> dict:
    ts = int(now_ts or time.time())
    return {
        'status': 'observe',
        'updated_at': ts,
        'last_buy_at': 0,
        'last_buy_price': 0.0,
        'last_buy_type': '',
        'last_sell_at': 0,
        'last_sell_price': 0.0,
        'last_sell_type': '',
        'signal_seq': 0,
    }


def _pool1_hold_days(entry: dict, now_ts: Optional[int] = None) -> float:
    if not isinstance(entry, dict):
        return 0.0
    if str(entry.get('status') or 'observe') != 'holding':
        return 0.0
    last_buy_at = int(entry.get('last_buy_at', 0) or 0)
    if last_buy_at <= 0:
        return 0.0
    ts = int(now_ts or time.time())
    return max(0.0, float(ts - last_buy_at) / 86400.0)


def _pool1_position_redis_key() -> str:
    return f"{_P1_POS_REDIS_KEY_PREFIX}:state"


def _get_pool1_position_redis():
    global _pool1_position_redis_cli, _pool1_position_redis_warn_at, _pool1_position_redis_next_retry_at
    if not _P1_POS_REDIS_ENABLED or _redis_mod is None:
        return None
    if _pool1_position_redis_cli is not None:
        return _pool1_position_redis_cli
    now = time.time()
    if now < _pool1_position_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_P1_POS_REDIS_URL, decode_responses=True)
        cli.ping()
        _pool1_position_redis_cli = cli
        logger.info(f"Pool1 position Redis enabled: {_P1_POS_REDIS_KEY_PREFIX}")
        return _pool1_position_redis_cli
    except Exception as e:
        _pool1_position_redis_next_retry_at = now + 5.0
        if now - _pool1_position_redis_warn_at >= 30.0:
            _pool1_position_redis_warn_at = now
            logger.warning(f"Pool1 position Redis unavailable, fallback file/memory: {e}")
        return None


def _normalize_pool1_position_entry(item: dict, now_ts: Optional[int] = None) -> dict:
    ent = _pool1_position_empty(now_ts=now_ts)
    if isinstance(item, dict):
        ent.update(
            {
                'status': str(item.get('status') or 'observe'),
                'updated_at': int(item.get('updated_at', 0) or 0),
                'last_buy_at': int(item.get('last_buy_at', 0) or 0),
                'last_buy_price': float(item.get('last_buy_price', 0.0) or 0.0),
                'last_buy_type': str(item.get('last_buy_type') or ''),
                'last_sell_at': int(item.get('last_sell_at', 0) or 0),
                'last_sell_price': float(item.get('last_sell_price', 0.0) or 0.0),
                'last_sell_type': str(item.get('last_sell_type') or ''),
                'signal_seq': int(item.get('signal_seq', 0) or 0),
            }
        )
    if ent['status'] not in {'observe', 'holding'}:
        ent['status'] = 'observe'
    return ent


def _pool1_position_read_redis() -> Optional[dict[str, dict]]:
    cli = _get_pool1_position_redis()
    if cli is None:
        return None
    key = _pool1_position_redis_key()
    try:
        raw = cli.hgetall(key) or {}
    except Exception:
        return None
    if not raw:
        return None
    loaded: dict[str, dict] = {}
    for code, text in raw.items():
        if not isinstance(code, str) or not code:
            continue
        if not isinstance(text, str) or not text:
            continue
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        loaded[code] = _normalize_pool1_position_entry(parsed)
    return loaded if loaded else None


def _pool1_position_write_redis_entry(code: str, entry: dict) -> bool:
    cli = _get_pool1_position_redis()
    if cli is None:
        return False
    key = _pool1_position_redis_key()
    try:
        payload = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
        pipe = cli.pipeline(transaction=True)
        pipe.hset(key, code, payload)
        ttl = max(1, int(_P1_POS_REDIS_TTL_DAYS)) * 24 * 3600
        pipe.expire(key, ttl)
        pipe.execute()
        return True
    except Exception:
        return False


def _pool1_position_write_redis_bulk(payload: dict[str, dict]) -> bool:
    cli = _get_pool1_position_redis()
    if cli is None:
        return False
    key = _pool1_position_redis_key()
    try:
        pipe = cli.pipeline(transaction=True)
        pipe.delete(key)
        for code, entry in payload.items():
            if not isinstance(code, str) or not code or not isinstance(entry, dict):
                continue
            text = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
            pipe.hset(key, code, text)
        ttl = max(1, int(_P1_POS_REDIS_TTL_DAYS)) * 24 * 3600
        pipe.expire(key, ttl)
        pipe.execute()
        return True
    except Exception:
        return False


def _load_pool1_position_state() -> None:
    global _pool1_position_state_loaded, _pool1_position_last_source
    if _pool1_position_state_loaded:
        return
    _pool1_position_state_loaded = True
    if not _P1_POS_ENABLED:
        return
    # 优先从 Redis 恢复（跨进程一致性更好）
    redis_loaded = _pool1_position_read_redis()
    if isinstance(redis_loaded, dict) and redis_loaded:
        _pool1_position_state.update(redis_loaded)
        _pool1_position_last_source = 'redis'
        logger.info(f"Pool1 持仓状态已从 Redis 加载: {len(redis_loaded)}")
        return
    if not _P1_POS_FILE_FALLBACK:
        _pool1_position_last_source = 'memory'
        return
    try:
        if not os.path.exists(_P1_POS_FILE):
            return
        with open(_P1_POS_FILE, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return
        loaded: dict[str, dict] = {}
        for code, item in raw.items():
            if not isinstance(code, str) or not code or not isinstance(item, dict):
                continue
            loaded[code] = _normalize_pool1_position_entry(item)
        _pool1_position_state.update(loaded)
        if loaded:
            _pool1_position_last_source = 'file'
            logger.info(f"Pool1 持仓状态已从文件加载: {len(loaded)}")
            # 文件恢复后尽量回写 Redis，供后续进程复用。
            if _P1_POS_REDIS_ENABLED:
                _pool1_position_write_redis_bulk(loaded)
    except Exception as e:
        logger.warning(f"Pool1 持仓状态加载失败: {e}")


def _persist_pool1_position_state(*, changed_code: Optional[str] = None, changed_entry: Optional[dict] = None) -> None:
    global _pool1_position_last_source
    if not _P1_POS_ENABLED:
        return
    if _P1_POS_REDIS_ENABLED:
        redis_ok = False
        if isinstance(changed_code, str) and changed_code and isinstance(changed_entry, dict):
            redis_ok = _pool1_position_write_redis_entry(changed_code, changed_entry)
        else:
            try:
                with _pool1_position_state_lock:
                    payload = {k: dict(v) for k, v in _pool1_position_state.items() if isinstance(v, dict)}
                redis_ok = _pool1_position_write_redis_bulk(payload)
            except Exception:
                redis_ok = False
        if redis_ok:
            _pool1_position_last_source = 'redis'
        elif not _P1_POS_FILE_FALLBACK:
            _pool1_position_last_source = 'memory'
    if not _P1_POS_FILE_FALLBACK:
        return
    try:
        with _pool1_position_state_lock:
            items = list(_pool1_position_state.items())
        if len(items) > _P1_POS_FILE_MAX_ITEMS:
            items.sort(key=lambda kv: int((kv[1] or {}).get('updated_at', 0) or 0), reverse=True)
            items = items[:_P1_POS_FILE_MAX_ITEMS]
        payload = {k: v for k, v in items if isinstance(v, dict)}
        tmp = _P1_POS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
        os.replace(tmp, _P1_POS_FILE)
        if _pool1_position_last_source != 'redis':
            _pool1_position_last_source = 'file'
    except Exception as e:
        logger.debug(f"Pool1 持仓状态持久化失败: {e}")


def _pool1_get_position_state(ts_code: str, now_ts: Optional[int] = None) -> dict:
    _load_pool1_position_state()
    code = str(ts_code or '')
    ts = int(now_ts or time.time())
    with _pool1_position_state_lock:
        ent = _pool1_position_state.get(code)
        if not isinstance(ent, dict):
            ent = _pool1_position_empty(ts)
            _pool1_position_state[code] = ent
        snapshot = dict(ent)
    snapshot['holding_days'] = round(_pool1_hold_days(snapshot, ts), 4)
    return snapshot


def _pool1_set_position_state(
    ts_code: str,
    status: str,
    *,
    now_ts: Optional[int] = None,
    signal_type: str = '',
    signal_price: Optional[float] = None,
) -> dict:
    _load_pool1_position_state()
    code = str(ts_code or '')
    ts = int(now_ts or time.time())
    st = 'holding' if str(status or '') == 'holding' else 'observe'
    with _pool1_position_state_lock:
        ent = _pool1_position_state.get(code)
        if not isinstance(ent, dict):
            ent = _pool1_position_empty(ts)
        ent['status'] = st
        ent['updated_at'] = ts
        ent['signal_seq'] = int(ent.get('signal_seq', 0) or 0) + 1
        px = float(signal_price or 0.0)
        if st == 'holding':
            ent['last_buy_at'] = ts
            ent['last_buy_price'] = px
            ent['last_buy_type'] = str(signal_type or '')
        else:
            ent['last_sell_at'] = ts
            ent['last_sell_price'] = px
            ent['last_sell_type'] = str(signal_type or '')
        _pool1_position_state[code] = ent
        persisted = dict(ent)
        out = dict(ent)
    out['holding_days'] = round(_pool1_hold_days(out, ts), 4)
    _persist_pool1_position_state(changed_code=code, changed_entry=persisted)
    return out


def get_pool1_position_state(ts_code: str) -> Optional[dict]:
    try:
        return _pool1_get_position_state(ts_code)
    except Exception:
        return None


def get_pool1_position_storage_status() -> dict:
    try:
        redis_ready = bool(_get_pool1_position_redis() is not None) if _P1_POS_REDIS_ENABLED else False
    except Exception:
        redis_ready = False
    return {
        'enabled': bool(_P1_POS_ENABLED),
        'redis_enabled': bool(_P1_POS_REDIS_ENABLED),
        'redis_ready': bool(redis_ready),
        'file_fallback': bool(_P1_POS_FILE_FALLBACK),
        'source': str(_pool1_position_last_source or 'memory'),
    }


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


def _pool1_observe_trade_date() -> str:
    """当前统计交易日（本地日期口径）。"""
    return _dt.datetime.now().strftime('%Y-%m-%d')


def _pool1_observe_redis_key(trade_date: str) -> str:
    return f"{_P1_OBS_REDIS_KEY_PREFIX}:{trade_date}"


def _get_pool1_observe_redis():
    global _pool1_observe_redis_cli, _pool1_observe_redis_warn_at, _pool1_observe_redis_next_retry_at
    if not _P1_OBS_REDIS_ENABLED or _redis_mod is None:
        return None
    if _pool1_observe_redis_cli is not None:
        return _pool1_observe_redis_cli
    now = time.time()
    if now < _pool1_observe_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_P1_OBS_REDIS_URL, decode_responses=True)
        cli.ping()
        _pool1_observe_redis_cli = cli
        logger.info(f"Pool1 observe Redis enabled: {_P1_OBS_REDIS_KEY_PREFIX}")
        return _pool1_observe_redis_cli
    except Exception as e:
        _pool1_observe_redis_next_retry_at = now + 5.0
        if now - _pool1_observe_redis_warn_at >= 30.0:
            _pool1_observe_redis_warn_at = now
            logger.warning(f"Pool1 observe Redis unavailable, fallback memory: {e}")
        return None


def _pool1_observe_read_redis(trade_date: str) -> Optional[dict]:
    cli = _get_pool1_observe_redis()
    if cli is None:
        return None
    key = _pool1_observe_redis_key(trade_date)
    try:
        raw = cli.hgetall(key) or {}
    except Exception:
        return None
    if not raw:
        return None
    reject_counts: dict[str, int] = {}
    threshold_observe: dict = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not k.startswith('reject:'):
            continue
        rk = k.split(':', 1)[1]
        try:
            rv = int(v or 0)
        except Exception:
            rv = 0
        if rk:
            reject_counts[rk] = rv
    try:
        th_raw = raw.get('threshold_observe_json')
        if isinstance(th_raw, str) and th_raw.strip():
            parsed = json.loads(th_raw)
            if isinstance(parsed, dict):
                threshold_observe = parsed
    except Exception:
        threshold_observe = {}
    return {
        'trade_date': raw.get('trade_date') or trade_date,
        'updated_at': int(raw.get('updated_at') or 0),
        'screen_total': int(raw.get('screen_total') or 0),
        'screen_pass': int(raw.get('screen_pass') or 0),
        'stage2_triggered': int(raw.get('stage2_triggered') or 0),
        'reject_counts': reject_counts,
        'threshold_observe': threshold_observe if isinstance(threshold_observe, dict) else {},
    }


def _normalize_reject_reason(reason: str) -> str:
    txt = str(reason or '').strip().lower().replace(' ', '_').replace('-', '_')
    safe = ''.join(ch for ch in txt if (ch.isalnum() or ch == '_'))
    return safe[:48] if safe else ''


def _normalize_bucket(value: str, default: str = 'unknown', max_len: int = 48) -> str:
    txt = str(value or '').strip()
    if not txt:
        return default
    txt = txt.replace(':', '_').replace('|', '/')
    return txt[:max_len]


def _count_inc(counter: dict, key: str, n: int = 1) -> None:
    counter[key] = int(counter.get(key, 0) or 0) + int(n)


def _nested_count_inc(counter: dict, k1: str, k2: str, n: int = 1) -> None:
    sub = counter.setdefault(k1, {})
    if not isinstance(sub, dict):
        sub = {}
        counter[k1] = sub
    sub[k2] = int(sub.get(k2, 0) or 0) + int(n)


def _sorted_count_map(counter: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    if not isinstance(counter, dict):
        return out
    items = []
    for k, v in counter.items():
        try:
            iv = int(v or 0)
        except Exception:
            iv = 0
        if iv > 0:
            items.append((str(k), iv))
    items.sort(key=lambda kv: int(kv[1]), reverse=True)
    for k, v in items:
        out[k] = int(v)
    return out


def _sorted_nested_count_map(counter: dict) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    if not isinstance(counter, dict):
        return out
    for k, sub in counter.items():
        sub_out = _sorted_count_map(sub if isinstance(sub, dict) else {})
        if sub_out:
            out[str(k)] = sub_out
    return out


def _pool1_threshold_bucket(th: Optional[dict]) -> str:
    if not isinstance(th, dict):
        return 'unknown|unknown|unknown'
    phase = _normalize_bucket(str(th.get('market_phase') or 'unknown'), default='unknown', max_len=24)
    regime = _normalize_bucket(str(th.get('regime') or 'unknown'), default='unknown', max_len=24)
    industry = _normalize_bucket(str(th.get('industry') or 'unknown'), default='unknown', max_len=32)
    return f'{phase}|{regime}|{industry}'


def _pool1_threshold_source(th: Optional[dict]) -> str:
    if not isinstance(th, dict):
        return 'unknown'
    cali = th.get('calibration')
    if isinstance(cali, dict) and bool(cali.get('applied')):
        return 'runtime_calibration'
    return 'dynamic_profile'


def _pool1_threshold_snapshot(th: Optional[dict]) -> dict:
    if not isinstance(th, dict):
        return {}
    out: dict[str, object] = {}
    for k in (
        'near_lower_th',
        'rsi_oversold',
        'eps_mid',
        'eps_upper',
        'breakout_max_offset_pct',
    ):
        if k in th:
            v = th.get(k)
            if isinstance(v, float):
                out[k] = round(v, 6)
            else:
                out[k] = v
    lf = th.get('limit_filter')
    if isinstance(lf, dict):
        out['limit_filter'] = {
            'distance_pct': lf.get('distance_pct'),
            'threshold_pct': lf.get('threshold_pct'),
            'action': lf.get('action'),
            'blocked': bool(lf.get('blocked')),
            'observer_only': bool(lf.get('observer_only')),
        }
    return out


def _pool1_threshold_blocked_reason(
    th: Optional[dict],
    signal_reject_reasons: Optional[list[str]] = None,
) -> str:
    reasons = [
        _normalize_reject_reason(x)
        for x in (signal_reject_reasons or [])
        if _normalize_reject_reason(x)
    ]
    for r in reasons:
        if r in {'limit_magnet_block', 'threshold_blocked'}:
            return r
    if not isinstance(th, dict):
        return ''
    if not bool(th.get('blocked')):
        return ''
    lf = th.get('limit_filter')
    if isinstance(lf, dict):
        act = str(lf.get('action') or '')
        if act == 'blocked':
            if bool(lf.get('limit_magnet')):
                return 'limit_magnet_block'
            return 'threshold_blocked'
    return 'threshold_blocked'


def _record_pool1_threshold_observe(
    threshold_info: Optional[dict[str, Optional[dict]]] = None,
    signal_rejects: Optional[dict[str, list[str]]] = None,
) -> None:
    by_sig = _pool1_observe.setdefault('threshold_observe', _pool1_threshold_observe_default())
    if not isinstance(by_sig, dict):
        by_sig = _pool1_threshold_observe_default()
        _pool1_observe['threshold_observe'] = by_sig
    now_iso = _dt.datetime.now().isoformat()
    for sig_type in _POOL1_SIGNAL_TYPES:
        th = None
        if isinstance(threshold_info, dict):
            th = threshold_info.get(sig_type)
        if not isinstance(th, dict):
            continue
        bucket = _pool1_threshold_bucket(th)
        version = str(th.get('threshold_version') or 'unknown')
        source = _pool1_threshold_source(th)
        blocked = bool(th.get('blocked'))
        observer_only = bool(th.get('observer_only'))
        blocked_reason = _pool1_threshold_blocked_reason(
            th,
            signal_reject_reasons=(signal_rejects or {}).get(sig_type) if isinstance(signal_rejects, dict) else None,
        )
        entry = by_sig.setdefault(
            sig_type,
            {
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
            },
        )
        entry['total'] = int(entry.get('total', 0) or 0) + 1
        if blocked:
            entry['blocked'] = int(entry.get('blocked', 0) or 0) + 1
        if observer_only:
            entry['observer_only'] = int(entry.get('observer_only', 0) or 0) + 1
        _count_inc(entry.setdefault('by_bucket', {}), bucket, 1)
        _count_inc(entry.setdefault('by_version', {}), version, 1)
        _count_inc(entry.setdefault('by_source', {}), source, 1)
        if blocked:
            _count_inc(entry.setdefault('blocked_by_bucket', {}), bucket, 1)
        if observer_only:
            _count_inc(entry.setdefault('observer_by_bucket', {}), bucket, 1)
        if blocked_reason:
            _count_inc(entry.setdefault('blocked_reasons', {}), blocked_reason, 1)
        entry['current'] = {
            'signal_type': sig_type,
            'bucket': bucket,
            'threshold_version': version,
            'threshold_source': source,
            'blocked': blocked,
            'observer_only': observer_only,
            'blocked_reason': blocked_reason,
            'snapshot': _pool1_threshold_snapshot(th),
            'updated_at': int(time.time()),
            'updated_at_iso': now_iso,
        }


def _pool1_observe_window_snapshot(now_ts: Optional[int] = None) -> dict:
    ts_now = int(now_ts if isinstance(now_ts, int) else time.time())
    events = list(_pool1_observe_recent_events)
    windows_out: dict[str, dict] = {}
    for win_key, sec in _POOL1_OBS_WINDOWS_SEC.items():
        lower = ts_now - int(sec)
        win_events = [e for e in events if int(e.get('ts', 0) or 0) >= lower]
        reject_counts: dict[str, int] = {}
        by_signal_type: dict[str, dict[str, int]] = {k: {} for k in _POOL1_SIGNAL_TYPES}
        by_industry: dict[str, dict[str, int]] = {}
        by_regime: dict[str, dict[str, int]] = {}
        for ev in win_events:
            reason = str(ev.get('reason') or '')
            if not reason:
                continue
            sig_type = str(ev.get('signal_type') or '')
            industry = _normalize_bucket(ev.get('industry') or '')
            regime = _normalize_bucket(ev.get('regime') or '')
            if sig_type == 'all':
                _count_inc(reject_counts, reason, 1)
                _nested_count_inc(by_industry, industry, reason, 1)
                _nested_count_inc(by_regime, regime, reason, 1)
            elif sig_type in _POOL1_SIGNAL_TYPES:
                _nested_count_inc(by_signal_type, sig_type, reason, 1)
        reject_counts_out = _sorted_count_map(reject_counts)
        top_rejects = sorted(reject_counts_out.items(), key=lambda kv: int(kv[1]), reverse=True)
        windows_out[win_key] = {
            'seconds': int(sec),
            'event_count': len(win_events),
            'reject_counts': reject_counts_out,
            'top_rejects': [{'reason': k, 'count': int(v)} for k, v in top_rejects if int(v) > 0][:6],
            'reject_by_signal_type': _sorted_nested_count_map(by_signal_type),
            'reject_by_industry': _sorted_nested_count_map(by_industry),
            'reject_by_regime': _sorted_nested_count_map(by_regime),
        }
    return windows_out


def _pool1_observe_write_redis(
    trade_date: str,
    stage1_pass: bool,
    stage2_triggered: bool,
    ts_now: int,
    reject_reasons: Optional[list[str]] = None,
    threshold_observe: Optional[dict] = None,
) -> bool:
    cli = _get_pool1_observe_redis()
    if cli is None:
        return False
    key = _pool1_observe_redis_key(trade_date)
    reasons = sorted({_normalize_reject_reason(r) for r in (reject_reasons or []) if _normalize_reject_reason(r)})
    try:
        pipe = cli.pipeline(transaction=True)
        pipe.hincrby(key, 'screen_total', 1)
        if stage1_pass:
            pipe.hincrby(key, 'screen_pass', 1)
            if stage2_triggered:
                pipe.hincrby(key, 'stage2_triggered', 1)
        for rr in reasons:
            pipe.hincrby(key, f'reject:{rr}', 1)
        if isinstance(threshold_observe, dict) and threshold_observe:
            try:
                pipe.hset(
                    key,
                    'threshold_observe_json',
                    json.dumps(threshold_observe, ensure_ascii=False, separators=(',', ':')),
                )
            except Exception:
                pass
        pipe.hset(key, mapping={'trade_date': trade_date, 'updated_at': int(ts_now)})
        ttl = max(1, int(_P1_OBS_REDIS_TTL_DAYS)) * 24 * 3600
        pipe.expire(key, ttl)
        pipe.execute()
        return True
    except Exception:
        return False


def _ensure_pool1_observe_day() -> None:
    """若跨日则重置统计，保证按交易日聚合。"""
    global _pool1_observe_last_source
    today = _pool1_observe_trade_date()
    if _pool1_observe.get('trade_date') == today:
        return
    _pool1_observe['trade_date'] = today
    _pool1_observe['updated_at'] = int(time.time())
    _pool1_observe['screen_total'] = 0
    _pool1_observe['screen_pass'] = 0
    _pool1_observe['stage2_triggered'] = 0
    _pool1_observe['reject_counts'] = {}
    _pool1_observe['reject_by_signal_type'] = {k: {} for k in _POOL1_SIGNAL_TYPES}
    _pool1_observe['reject_by_industry'] = {}
    _pool1_observe['reject_by_regime'] = {}
    _pool1_observe['threshold_observe'] = _pool1_threshold_observe_default()
    _pool1_observe_recent_events.clear()
    snap = _pool1_observe_read_redis(today)
    if snap:
        _pool1_observe.update(snap)
        _pool1_observe_last_source = 'redis'
    else:
        _pool1_observe_last_source = 'memory'


def _record_pool1_observe(
    stage1_pass: bool,
    stage2_triggered: bool,
    reject_reasons: Optional[list[str]] = None,
    signal_rejects: Optional[dict[str, list[str]]] = None,
    threshold_info: Optional[dict[str, Optional[dict]]] = None,
    industry: Optional[str] = None,
    regime: Optional[str] = None,
) -> None:
    global _pool1_observe_last_source, _pool1_observe_redis_last_flush_at, _pool1_observe_redis_last_threshold_flush_at
    if not _is_cn_trading_time_now():
        return
    _ensure_pool1_observe_day()
    _pool1_observe['screen_total'] += 1
    if stage1_pass:
        _pool1_observe['screen_pass'] += 1
        if stage2_triggered:
            _pool1_observe['stage2_triggered'] += 1
    rej_map = _pool1_observe.setdefault('reject_counts', {})
    reasons = sorted({_normalize_reject_reason(r) for r in (reject_reasons or []) if _normalize_reject_reason(r)})
    for rr in reasons:
        rej_map[rr] = int(rej_map.get(rr, 0) or 0) + 1
    # 按信号类型拆分
    by_sig = _pool1_observe.setdefault('reject_by_signal_type', {k: {} for k in _POOL1_SIGNAL_TYPES})
    sig_rejects_norm: dict[str, list[str]] = {}
    for sig_type in _POOL1_SIGNAL_TYPES:
        sub = []
        if isinstance(signal_rejects, dict):
            raw_sub = signal_rejects.get(sig_type)
            if isinstance(raw_sub, list):
                sub = [_normalize_reject_reason(x) for x in raw_sub if _normalize_reject_reason(x)]
        sub = sorted(set(sub))
        sig_rejects_norm[sig_type] = sub
        if sig_type not in by_sig or not isinstance(by_sig.get(sig_type), dict):
            by_sig[sig_type] = {}
        for rr in sub:
            _count_inc(by_sig[sig_type], rr, 1)
    # 按行业 / regime 拆分（按总 reject_reasons 口径）
    ind_bucket = _normalize_bucket(industry or '')
    reg_bucket = _normalize_bucket(regime or '')
    by_ind = _pool1_observe.setdefault('reject_by_industry', {})
    by_reg = _pool1_observe.setdefault('reject_by_regime', {})
    for rr in reasons:
        _nested_count_inc(by_ind, ind_bucket, rr, 1)
        _nested_count_inc(by_reg, reg_bucket, rr, 1)
    _record_pool1_threshold_observe(threshold_info=threshold_info, signal_rejects=sig_rejects_norm)
    ts_now = int(time.time())
    _pool1_observe['updated_at'] = ts_now
    # 记录近窗事件：all 口径 + 按信号类型口径
    for rr in reasons:
        _pool1_observe_recent_events.append({
            'ts': ts_now,
            'signal_type': 'all',
            'reason': rr,
            'industry': ind_bucket,
            'regime': reg_bucket,
        })
    for sig_type in _POOL1_SIGNAL_TYPES:
        for rr in sig_rejects_norm.get(sig_type, []):
            _pool1_observe_recent_events.append({
                'ts': ts_now,
                'signal_type': sig_type,
                'reason': rr,
                'industry': ind_bucket,
                'regime': reg_bucket,
            })
    # Redis 落库节流：内存实时累计，按时间窗口刷盘，避免每 tick IO 阻塞。
    now_f = float(time.time())
    need_flush = (now_f - float(_pool1_observe_redis_last_flush_at)) >= max(0.2, float(_P1_OBS_REDIS_FLUSH_SEC))
    # 有 stage2 命中时优先刷盘，保证实盘观察延迟可控。
    if stage2_triggered:
        need_flush = True
    if need_flush:
        include_threshold = (
            now_f - float(_pool1_observe_redis_last_threshold_flush_at)
        ) >= max(0.5, float(_P1_OBS_REDIS_THRESHOLD_FLUSH_SEC))
        threshold_payload = _pool1_observe.get('threshold_observe') if include_threshold else None
        if _pool1_observe_write_redis(
            _pool1_observe.get('trade_date') or _pool1_observe_trade_date(),
            stage1_pass,
            stage2_triggered,
            ts_now,
            reject_reasons=reasons,
            threshold_observe=threshold_payload,
        ):
            _pool1_observe_last_source = 'redis'
            _pool1_observe_redis_last_flush_at = now_f
            if include_threshold:
                _pool1_observe_redis_last_threshold_flush_at = now_f
        else:
            _pool1_observe_last_source = 'memory'


def _pool1_observe_summary() -> str:
    _ensure_pool1_observe_day()
    total = int(_pool1_observe.get('screen_total', 0) or 0)
    passed = int(_pool1_observe.get('screen_pass', 0) or 0)
    triggered = int(_pool1_observe.get('stage2_triggered', 0) or 0)
    if total <= 0:
        return "pool1无样本"
    pass_rate = (passed / total) * 100
    trigger_rate = (triggered / passed) * 100 if passed > 0 else 0.0
    reject_counts = _pool1_observe.get('reject_counts') or {}
    top_reason = ''
    if isinstance(reject_counts, dict) and reject_counts:
        try:
            rk, rv = sorted(reject_counts.items(), key=lambda kv: int(kv[1]), reverse=True)[0]
            if int(rv) > 0:
                top_reason = f" 主要拦截:{rk}({int(rv)})"
        except Exception:
            top_reason = ''
    return (
        f"pool1通过率 {pass_rate:.1f}%({passed}/{total}) "
        f"二阶段触发率 {trigger_rate:.1f}%({triggered}/{passed}){top_reason}"
    )


def get_pool1_observe_stats() -> dict:
    """返回 Pool1 两阶段统计快照（轻量接口，无 IO）。"""
    global _pool1_observe_last_source
    _ensure_pool1_observe_day()
    snap = _pool1_observe_read_redis(_pool1_observe.get('trade_date') or _pool1_observe_trade_date())
    if snap and int(snap.get('updated_at', 0) or 0) >= int(_pool1_observe.get('updated_at', 0) or 0):
        _pool1_observe.update(snap)
        _pool1_observe_last_source = 'redis'
    total = int(_pool1_observe.get('screen_total', 0) or 0)
    passed = int(_pool1_observe.get('screen_pass', 0) or 0)
    triggered = int(_pool1_observe.get('stage2_triggered', 0) or 0)
    reject_counts = _pool1_observe.get('reject_counts') or {}
    reject_by_signal_type = _pool1_observe.get('reject_by_signal_type') or {}
    reject_by_industry = _pool1_observe.get('reject_by_industry') or {}
    reject_by_regime = _pool1_observe.get('reject_by_regime') or {}
    updated_at = int(_pool1_observe.get('updated_at', 0) or 0)
    pass_rate = (passed / total) if total > 0 else 0.0
    trigger_rate = (triggered / passed) if passed > 0 else 0.0
    reject_counts_out = _sorted_count_map(reject_counts)
    for k in _POOL1_REJECT_DEFAULTS:
        reject_counts_out.setdefault(k, 0)
    reject_by_signal_type_out = _sorted_nested_count_map(reject_by_signal_type)
    for sig_type in _POOL1_SIGNAL_TYPES:
        reject_by_signal_type_out.setdefault(sig_type, {})
    reject_by_industry_out = _sorted_nested_count_map(reject_by_industry)
    reject_by_regime_out = _sorted_nested_count_map(reject_by_regime)
    threshold_raw = _pool1_observe.get('threshold_observe') or {}
    threshold_out: dict[str, dict] = {}
    for sig_type in _POOL1_SIGNAL_TYPES:
        e = threshold_raw.get(sig_type) if isinstance(threshold_raw, dict) else {}
        total_th = int((e or {}).get('total', 0) or 0)
        blocked_th = int((e or {}).get('blocked', 0) or 0)
        observer_th = int((e or {}).get('observer_only', 0) or 0)
        by_bucket = _sorted_count_map((e or {}).get('by_bucket') if isinstance(e, dict) else {})
        blocked_by_bucket = _sorted_count_map((e or {}).get('blocked_by_bucket') if isinstance(e, dict) else {})
        observer_by_bucket = _sorted_count_map((e or {}).get('observer_by_bucket') if isinstance(e, dict) else {})
        by_version = _sorted_count_map((e or {}).get('by_version') if isinstance(e, dict) else {})
        by_source = _sorted_count_map((e or {}).get('by_source') if isinstance(e, dict) else {})
        blocked_reasons = _sorted_count_map((e or {}).get('blocked_reasons') if isinstance(e, dict) else {})
        current = (e or {}).get('current') if isinstance(e, dict) else {}
        top_bucket = next(iter(by_bucket.keys()), '')
        threshold_out[sig_type] = {
            'total': total_th,
            'blocked': blocked_th,
            'observer_only': observer_th,
            'blocked_ratio': round((blocked_th / total_th), 4) if total_th > 0 else 0.0,
            'observer_ratio': round((observer_th / total_th), 4) if total_th > 0 else 0.0,
            'by_bucket': by_bucket,
            'blocked_by_bucket': blocked_by_bucket,
            'observer_by_bucket': observer_by_bucket,
            'by_version': by_version,
            'by_source': by_source,
            'blocked_reasons': blocked_reasons,
            'top_bucket': top_bucket,
            'current': current if isinstance(current, dict) else {},
        }
    reject_windows = _pool1_observe_window_snapshot(now_ts=updated_at if updated_at > 0 else None)
    top_rejects = sorted(reject_counts_out.items(), key=lambda kv: int(kv[1]), reverse=True)
    redis_enabled = bool(_P1_OBS_REDIS_ENABLED)
    redis_ready = bool(_get_pool1_observe_redis() is not None) if redis_enabled else False
    expected_source = 'redis' if redis_enabled else 'memory'
    actual_source = str(_pool1_observe_last_source or 'memory')
    if not redis_enabled:
        storage_verified = (actual_source == 'memory')
        storage_degraded = False
        storage_note = 'redis_disabled'
    elif not redis_ready:
        storage_verified = (actual_source == 'memory')
        storage_degraded = True
        storage_note = 'redis_unavailable_fallback'
    elif total <= 0:
        storage_verified = True
        storage_degraded = False
        storage_note = 'redis_ready_no_samples'
    else:
        storage_verified = (actual_source == 'redis')
        storage_degraded = not storage_verified
        storage_note = 'ok' if storage_verified else 'redis_ready_but_memory_source'
    return {
        'trade_date': _pool1_observe.get('trade_date') or _pool1_observe_trade_date(),
        'updated_at': updated_at,
        'updated_at_iso': _dt.datetime.fromtimestamp(updated_at).isoformat() if updated_at > 0 else None,
        'screen_total': total,
        'screen_pass': passed,
        'stage2_triggered': triggered,
        'pass_rate': round(pass_rate, 4),
        'trigger_rate': round(trigger_rate, 4),
        'reject_counts': reject_counts_out,
        'reject_by_signal_type': reject_by_signal_type_out,
        'reject_by_industry': reject_by_industry_out,
        'reject_by_regime': reject_by_regime_out,
        'threshold_observe': threshold_out,
        'reject_windows': reject_windows,
        'top_rejects': [{'reason': k, 'count': int(v)} for k, v in top_rejects if int(v) > 0][:6],
        'summary': _pool1_observe_summary(),
        'storage_source': actual_source,
        'storage_expected': expected_source,
        'storage_verified': bool(storage_verified),
        'storage_degraded': bool(storage_degraded),
        'storage_note': storage_note,
        'redis_enabled': redis_enabled,
        'redis_ready': redis_ready,
    }


class _TickSyncManager(BaseManager):
    """跨进程共享 Queue 与 Cache 的 Manager。"""
    pass


_TickSyncManager.register('get_queue', callable=lambda: _main_queue)
_TickSyncManager.register('get_cache', callable=lambda: _main_cache)


def _save_subscribed_codes():
    """保存订阅列表到文件（供 gm 策略脚本 init 读取）。"""
    try:
        with open(_SUBS_FILE, 'w') as f:
            f.write(','.join(_subscribed_codes))
    except Exception as e:
        logger.warning(f"保存订阅列表失败: {e}")


def _load_subscribed_codes():
    """从文件加载订阅列表。"""
    global _subscribed_codes
    try:
        if os.path.exists(_SUBS_FILE):
            with open(_SUBS_FILE, 'r') as f:
                codes = f.read().strip()
                if codes:
                    _subscribed_codes = set(codes.split(','))
    except Exception:
        _subscribed_codes = set()


def seed_subscribed_codes(ts_codes: list[str], overwrite: bool = True) -> int:
    """
    预写 gm 订阅文件（用于 gm.run() 启动前注入初始订阅列表）。
    返回写入后的订阅总数。
    """
    global _subscribed_codes
    cleaned = sorted(
        {
            str(c).strip()
            for c in (ts_codes or [])
            if c is not None and str(c).strip() and "." in str(c)
        }
    )
    if overwrite:
        _subscribed_codes = set(cleaned)
    else:
        _subscribed_codes.update(cleaned)
    _save_subscribed_codes()
    return len(_subscribed_codes)


def _start_manager_server():
    """启动 BaseManager TCP server（主进程调用一次）。"""
    global _manager_server, _TICK_MGR_ADDRESS
    if _manager_server is not None:
        return

    mgr = _TickSyncManager(address=_TICK_MGR_ADDRESS, authkey=_TICK_MGR_AUTHKEY)
    _manager_server = mgr.get_server()
    actual_addr = _manager_server.address  # (host, port)
    _TICK_MGR_ADDRESS = actual_addr

    # 通过环境变量传给 gm 子进程，使其连接此 server
    os.environ['GM_TICK_MGR_HOST'] = str(actual_addr[0])
    os.environ['GM_TICK_MGR_PORT'] = str(actual_addr[1])
    os.environ['GM_TICK_MGR_AUTHKEY'] = _TICK_MGR_AUTHKEY.hex()

    t = threading.Thread(target=_manager_server.serve_forever, daemon=True, name='tick-mgr-server')
    t.start()
    logger.info(f"TickManager TCP server: {actual_addr[0]}:{actual_addr[1]}")


def init_persist_queue(maxsize: int = 100000) -> _StdQueue:
    """初始化 Layer3 持久化队列，供 realtime_monitor writer 线程消费。"""
    global _persist_queue
    if _persist_queue is None:
        _persist_queue = _StdQueue(maxsize=maxsize)
        logger.info(f"Layer3 持久化队列已创建 (maxsize={maxsize})")
    return _persist_queue


def _update_intraday_state(ts_code: str, tick: dict) -> dict:
    """更新日内状态（VWAP/价格窗口/GUB5），返回 state 供信号计算使用。"""
    today = _dt.datetime.now().strftime('%Y-%m-%d')
    state = _intraday_state.get(ts_code)
    if state is None or state.get('date') != today:
        state = {
            'date': today,
            'cum_volume': 0,
            'cum_amount': 0.0,
            'last_volume': 0,
            'price_window': deque(),   # (ts, price, delta_volume)
            'gub5_window': deque(),    # (ts, small_sell_flag: 0/1)
        }
        _intraday_state[ts_code] = state

    now = int(tick.get('timestamp') or time.time())
    price = float(tick.get('price', 0) or 0)
    cum_volume = int(tick.get('volume', 0) or 0)
    cum_amount = float(tick.get('amount', 0) or 0)

    # 计算增量成交
    last_v = state['last_volume']
    delta_v = cum_volume - last_v if cum_volume >= last_v else cum_volume  # 当日重置
    state['last_volume'] = cum_volume
    state['cum_volume'] = cum_volume
    state['cum_amount'] = cum_amount

    # 滚动价格窗口
    pw = state['price_window']
    pw.append((now, price, delta_v))
    while pw and now - pw[0][0] > INTRADAY_WINDOW_SECONDS:
        pw.popleft()

    # GUB5：简化为“当前小单卖出占比”的滚动近似
    gw = state['gub5_window']
    small_sell = 0
    if delta_v > 0 and delta_v < 50000 and len(pw) >= 2 and pw[-1][1] < pw[-2][1]:
        small_sell = 1
    gw.append((now, small_sell))
    while gw and now - gw[0][0] > GUB5_WINDOW_SECONDS:
        gw.popleft()

    return state


def _compute_vwap(state: dict) -> Optional[float]:
    """日内 VWAP = cum_amount / cum_volume。"""
    if state['cum_volume'] > 0:
        return state['cum_amount'] / state['cum_volume']
    return None


def _compute_gub5_trend(state: dict) -> str:
    """GUB5 趋势（'up' | 'flat' | 'down'），基于近 5 分钟小单卖出占比。"""
    gw = state['gub5_window']
    if len(gw) < 10:
        return 'flat'
    ratio = sum(f for _, f in gw) / len(gw)
    if ratio > 0.4:
        return 'up'
    elif ratio > 0.2:
        return 'flat'
    else:
        return 'down'


def _compute_gub5_transition(state: dict) -> str:
    """GUB5 趋势转向检测：比较窗口前半段与后半段，返回 up->down 等标签。"""
    gw = state['gub5_window']
    if len(gw) < 20:
        return _compute_gub5_trend(state)
    half = len(gw) // 2
    items = list(gw)
    def _ratio(seg): return sum(f for _, f in seg) / len(seg) if seg else 0
    def _label(r): return 'up' if r > 0.4 else ('flat' if r > 0.2 else 'down')
    r_prev = _ratio(items[:half])
    r_curr = _ratio(items[half:])
    prev_label = _label(r_prev)
    curr_label = _label(r_curr)
    if prev_label == curr_label:
        return curr_label
    return f'{prev_label}->{curr_label}'


def _compute_bid_ask_ratio(tick: dict) -> Optional[float]:
    """
    五档买卖量比：sum(bid_vol 1-5) / sum(ask_vol 1-5)
    > 1.15 代表买盘承接偏强。"""
    bids = tick.get('bids') or []
    asks = tick.get('asks') or []
    bid_vol = sum(v for _, v in bids[:5]) if bids else 0
    ask_vol = sum(v for _, v in asks[:5]) if asks else 0
    if ask_vol <= 0:
        return None
    return round(bid_vol / ask_vol, 3)


def _compute_price_zscore(state: dict) -> Optional[float]:
    """当前价格相对最近 20 分钟价格分布的 Z-Score。"""
    pw = state['price_window']
    if len(pw) < 10:
        return None
    prices = [p for _, p, _ in pw]
    try:
        m = statistics.mean(prices[:-1])     # 向前窗口均值
        s = statistics.stdev(prices[:-1])
        if s == 0:
            return None
        return (prices[-1] - m) / s
    except Exception:
        return None


def _txn_flow_stats(txns: Optional[list]) -> dict:
    """Summarize active buy/sell flow for a transaction slice."""
    out = {
        'buy_amt': 0.0,
        'sell_amt': 0.0,
        'buy_share': 0.0,
        'sell_share': 0.0,
        'net_bps': 0.0,
        'start_price': 0.0,
        'last_price': 0.0,
        'peak_price': 0.0,
        'low_price': 0.0,
        'count': 0,
    }
    if not txns:
        return out
    prices: list[float] = []
    buy_amt = 0.0
    sell_amt = 0.0
    for t in txns:
        try:
            v = int(t.get('volume', 0) or 0)
            p = float(t.get('price', 0) or 0)
            d = int(t.get('direction', 0) or 0)
        except Exception:
            continue
        if v <= 0 or p <= 0:
            continue
        amt = p * v
        prices.append(p)
        if d == 0:
            buy_amt += amt
        else:
            sell_amt += amt
    total_amt = buy_amt + sell_amt
    out['buy_amt'] = round(buy_amt, 2)
    out['sell_amt'] = round(sell_amt, 2)
    out['count'] = len(prices)
    if total_amt > 0:
        out['buy_share'] = buy_amt / total_amt
        out['sell_share'] = sell_amt / total_amt
        out['net_bps'] = (buy_amt - sell_amt) / total_amt * 10000.0
    if prices:
        out['start_price'] = prices[0]
        out['last_price'] = prices[-1]
        out['peak_price'] = max(prices)
        out['low_price'] = min(prices)
    return out


def _intraday_progress_ratio(ts_epoch: Optional[int] = None) -> float:
    """
    连续竞价进度（09:30-11:30 + 13:00-15:00）归一化到 [0, 1]。
    午间休市保持在 0.5，收盘后固定为 1.0。
    """
    try:
        dt = _dt.datetime.fromtimestamp(int(ts_epoch)) if ts_epoch else _dt.datetime.now()
    except Exception:
        dt = _dt.datetime.now()
    if dt.weekday() >= 5:
        return 0.0
    hm = dt.hour * 60 + dt.minute
    morning_start = 9 * 60 + 30
    morning_end = 11 * 60 + 30
    afternoon_start = 13 * 60
    afternoon_end = 15 * 60
    total = 240.0
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
    return max(0.0, min(1.0, traded / total))


def _calc_intraday_volume_pace(
    tick: dict,
    daily: dict,
    *,
    min_progress: float,
    shrink_th: float,
    expand_th: float,
    surge_th: float,
) -> dict:
    """
    量能节奏：pace_ratio = (cum_volume / baseline_volume) / progress_ratio
    baseline_volume 采用昨量与近5日中位量的融合基准，降低单日异常失真：
      - 同时可用：baseline = 0.5 * prev_day_volume + 0.5 * vol5_median_volume
      - 单边可用：退化为可用项
    当进度不足 min_progress 时，返回 unknown（避免开盘早段噪声放大）。
    """
    ts_epoch = int(tick.get('timestamp') or time.time())
    progress_ratio = _intraday_progress_ratio(ts_epoch)
    try:
        cum_volume = float(tick.get('volume', 0) or 0)
    except Exception:
        cum_volume = 0.0
    try:
        prev_day_volume = float(daily.get('prev_day_volume', 0) or 0)
    except Exception:
        prev_day_volume = 0.0
    try:
        vol5_median_volume = float(daily.get('vol5_median_volume', 0) or 0)
    except Exception:
        vol5_median_volume = 0.0

    ratio = None
    ratio_prev = None
    ratio_med5 = None
    state = 'unknown'
    min_progress = max(0.0, float(min_progress or 0.0))
    baseline_volume = 0.0
    baseline_mode = 'none'
    if prev_day_volume > 0 and vol5_median_volume > 0:
        baseline_volume = 0.5 * prev_day_volume + 0.5 * vol5_median_volume
        baseline_mode = 'blend_prev_med5'
    elif prev_day_volume > 0:
        baseline_volume = prev_day_volume
        baseline_mode = 'prev_day'
    elif vol5_median_volume > 0:
        baseline_volume = vol5_median_volume
        baseline_mode = 'med5'

    if baseline_volume > 0 and progress_ratio > 0 and progress_ratio >= min_progress:
        ratio = (cum_volume / baseline_volume) / progress_ratio
        if prev_day_volume > 0:
            ratio_prev = (cum_volume / prev_day_volume) / progress_ratio
        if vol5_median_volume > 0:
            ratio_med5 = (cum_volume / vol5_median_volume) / progress_ratio
        if ratio > 0:
            if ratio >= float(surge_th):
                state = 'surge'
            elif ratio >= float(expand_th):
                state = 'expand'
            elif ratio < float(shrink_th):
                state = 'shrink'
            else:
                state = 'normal'
        else:
            ratio = None
    return {
        'ratio': ratio,
        'ratio_prev': ratio_prev,
        'ratio_med5': ratio_med5,
        'state': state,
        'progress_ratio': progress_ratio,
        'cum_volume': cum_volume,
        'prev_day_volume': prev_day_volume,
        'vol5_median_volume': vol5_median_volume,
        'baseline_volume': baseline_volume,
        'baseline_mode': baseline_mode,
        'min_progress': min_progress,
    }


def _pool1_daily_screening(daily: dict) -> tuple[bool, int, dict]:
    """Pool1 第一阶段（日线筛选 + 加分）。"""
    ma5 = float(daily.get('ma5', 0) or 0)
    ma10 = float(daily.get('ma10', 0) or 0)
    ma20 = float(daily.get('ma20', 0) or 0)
    rsi6_val = daily.get('rsi6')
    rsi6 = float(rsi6_val) if rsi6_val is not None else None
    volume_ratio_val = daily.get('volume_ratio')
    volume_ratio = float(volume_ratio_val) if volume_ratio_val is not None else None

    trend_ok = ma5 > ma10 > ma20 > 0
    momentum_ok = (rsi6 is not None) and (35 <= rsi6 <= 70)
    volume_ok = (volume_ratio is not None) and (volume_ratio >= 1.0)

    passed = trend_ok or momentum_ok
    bonus = 0
    if trend_ok:
        bonus += 10
    if momentum_ok:
        bonus += 10
    if volume_ok:
        bonus += 5

    return passed, min(25, bonus), {
        'trend_ok': trend_ok,
        'momentum_ok': momentum_ok,
        'volume_ok': volume_ok,
        'bonus': min(25, bonus),
    }


def _attach_signal_channel_meta(signal: dict, pool_id: int) -> dict:
    if not isinstance(signal, dict):
        return signal
    channel = POOL_CHANNEL_NAME.get(int(pool_id), f'pool{pool_id}')
    source = SIGNAL_CHANNEL_SOURCE.get(int(pool_id), 'unknown')
    signal['channel'] = channel
    signal['signal_source'] = source
    signal['_pool_ids'] = [int(pool_id)]
    details = signal.setdefault('details', {})
    details.setdefault('channel', channel)
    details.setdefault('signal_source', source)
    details.setdefault('pool_id', int(pool_id))
    return signal


def _compute_signals_for_tick(tick: dict, daily: dict, ts_code: str = '') -> list[dict]:
    """Layer2 信号计算入口：按股票所属池分流。
    - Pool1(择时): 左/右买入 + 持仓态清仓（timing_clear）
    - Pool2(T+0): positive_t + reverse_t（tick + 日内 VWAP/GUB5/Z-score）
    同一股票可同时命中两个池，分别计算。
    """
    from backend.realtime import signals as sig

    price = float(tick.get('price', 0) or 0)
    pct_chg = float(tick.get('pct_chg', 0) or 0)
    boll_upper = float(daily.get('boll_upper', 0) or 0)
    boll_mid = float(daily.get('boll_mid', 0) or 0)
    boll_lower = float(daily.get('boll_lower', 0) or 0)
    rsi6 = daily.get('rsi6')
    volume_ratio = daily.get('volume_ratio')
    prev_price = _main_prev_price.get(ts_code)

    if price <= 0 or boll_upper <= 0:
        return []

    # 榄忔腑鐖昏櫩鎮ｈ棔
    pools = _main_stock_pools.get(ts_code, set())
    if not pools:
        return []

    # 微观结构分析较频繁，单次复用结果避免重复扫描逐笔数据。
    txns_recent = _main_recent_txns.get(ts_code)
    ms_shared = _analyze_market_structure(tick, txns_recent)
    ms_pool1 = ms_shared
    ms_pool2 = ms_shared
    bid_ask_ratio = _compute_bid_ask_ratio(tick)
    up_limit = None
    down_limit = None
    pre_close = float(tick.get('pre_close', 0) or 0)
    instrument_profile = dict(_main_instrument_profile.get(ts_code, {}) or {})
    if pre_close > 0 and _calc_theoretical_limits is not None:
        up_limit, down_limit = _calc_theoretical_limits(pre_close, instrument_profile.get('price_limit_pct'))
    elif pre_close > 0:
        up_limit = pre_close * 1.1
        down_limit = pre_close * 0.9

    daily_with_limits = dict(daily)
    if up_limit is not None:
        daily_with_limits['up_limit'] = up_limit
    if down_limit is not None:
        daily_with_limits['down_limit'] = down_limit
    p1_vol_pace = _calc_intraday_volume_pace(
        tick=tick,
        daily=daily,
        min_progress=_P1_VOL_PACE_MIN_PROGRESS,
        shrink_th=_P1_VOL_PACE_SHRINK_TH,
        expand_th=_P1_VOL_PACE_EXPAND_TH,
        surge_th=_P1_VOL_PACE_SURGE_TH,
    )
    t0_vol_pace = _calc_intraday_volume_pace(
        tick=tick,
        daily=daily,
        min_progress=_T0_VOL_PACE_MIN_PROGRESS,
        shrink_th=_T0_VOL_PACE_SHRINK_TH,
        expand_th=_T0_VOL_PACE_EXPAND_TH,
        surge_th=_T0_VOL_PACE_SURGE_TH,
    )

    def _build_ctx(pool_id: int, intraday: Optional[dict] = None) -> Optional[dict]:
        if _build_signal_context is None:
            return None
        try:
            pace = p1_vol_pace if int(pool_id) == 1 else t0_vol_pace
            intraday_payload = {
                'volume_pace_ratio': pace.get('ratio'),
                'volume_pace_state': pace.get('state'),
                'progress_ratio': pace.get('progress_ratio'),
            }
            if isinstance(intraday, dict):
                intraday_payload.update(intraday)
            ctx = _build_signal_context(
                ts_code=ts_code,
                pool_id=pool_id,
                tick=tick,
                daily=daily_with_limits,
                intraday=intraday_payload,
                market={
                    'vol_20': daily.get('vol_20'),
                    'atr_14': daily.get('atr_14'),
                    'industry': _main_stock_industry.get(ts_code, ''),
                    'name': tick.get('name'),
                    'market_name': instrument_profile.get('market_name'),
                    'list_date': instrument_profile.get('list_date'),
                    'instrument_profile': instrument_profile,
                },
            )
            # Pool1 limit-magnet distance now follows dynamic threshold engine config.
            return ctx
        except Exception as e:
            logger.debug(f"build_signal_context 异常 {ts_code}/{pool_id}: {e}")
            return None

    def _get_th(signal_type: str, ctx: Optional[dict]) -> Optional[dict]:
        if _get_thresholds is None or ctx is None:
            return None
        try:
            return _get_thresholds(signal_type, ctx)
        except Exception as e:
            logger.debug(f"get_thresholds 异常 {ts_code}/{signal_type}: {e}")
            return None

    fired_pool1: list[dict] = []
    fired_pool2: list[dict] = []

    # ===== Pool1：择时建仓 =====
    if 1 in pools:
        now_ts = int(tick.get('timestamp') or time.time())
        pos_state_before = _pool1_get_position_state(ts_code, now_ts=now_ts)
        in_holding = str(pos_state_before.get('status') or 'observe') == 'holding'
        holding_days_before = float(pos_state_before.get('holding_days', 0.0) or 0.0)
        stage1_pass, stage1_bonus, stage1_info = _pool1_daily_screening(daily)
        stage2_triggered = False
        stage2_rejects: set[str] = set()
        stage2_rejects_by_signal: dict[str, set[str]] = {k: set() for k in _POOL1_SIGNAL_TYPES}
        p1_ctx = None
        p1_left_th = None
        p1_right_th = None
        p1_clear_th = None
        p1_regime = ''
        if isinstance(p1_left_th, dict):
            p1_regime = str(p1_left_th.get('regime') or '')
        if not p1_regime and isinstance(p1_right_th, dict):
            p1_regime = str(p1_right_th.get('regime') or '')
        p1_industry = str(_main_stock_industry.get(ts_code, '') or '')
        chip_bonus = 0
        chip_info = {'source': 'skipped_stage1'}
        resonance_60m = True
        resonance_60m_info = {'enabled': bool(_P1_RESONANCE_ENABLED), 'source': 'skipped_stage1'}

        if in_holding:
            # 已持仓：关闭买入触发，仅评估确认清仓信号。
            p1_ctx = _build_ctx(pool_id=1)
            p1_clear_th = _get_th('timing_clear', p1_ctx)
            if isinstance(p1_clear_th, dict):
                p1_regime = str(p1_clear_th.get('regime') or '')
            resonance_60m, resonance_60m_info = _pool1_resonance_60m(ts_code, daily, price, prev_price)
            clear_sig = sig.detect_timing_clear(
                price=price,
                boll_mid=boll_mid,
                ma5=daily.get('ma5'),
                ma10=daily.get('ma10'),
                ma20=daily.get('ma20'),
                rsi6=rsi6,
                pct_chg=pct_chg,
                volume_ratio=volume_ratio,
                bid_ask_ratio=bid_ask_ratio,
                resonance_60m=resonance_60m,
                big_order_bias=ms_pool1.get('big_order_bias'),
                super_order_bias=ms_pool1.get('super_order_bias'),
                big_net_flow_bps=ms_pool1.get('big_net_flow_bps'),
                super_net_flow_bps=ms_pool1.get('super_net_flow_bps'),
                volume_pace_ratio=p1_vol_pace.get('ratio'),
                volume_pace_state=p1_vol_pace.get('state'),
                in_holding=True,
                holding_days=holding_days_before,
                thresholds=p1_clear_th,
            )
            if clear_sig.get('has_signal'):
                clear_sig = sig.finalize_pool1_signal(clear_sig, thresholds=p1_clear_th)
            if clear_sig.get('has_signal'):
                clear_sig = _attach_signal_channel_meta(clear_sig, pool_id=1)
                clear_sig.setdefault('details', {})['pool1_stage'] = {
                    'daily_screening': {'skipped': True, 'reason': 'in_holding_mode'},
                    'stage2_triggered': True,
                    'chip': {'source': 'skipped_holding_mode'},
                    'resonance_60m': bool(resonance_60m),
                    'resonance_60m_info': resonance_60m_info,
                    'position_mode': 'holding',
                }
                fired_pool1.append(clear_sig)
        else:
            if stage1_pass:
                p1_ctx = _build_ctx(pool_id=1)
                p1_left_th = _get_th('left_side_buy', p1_ctx)
                p1_right_th = _get_th('right_side_breakout', p1_ctx)
                if isinstance(p1_left_th, dict):
                    p1_regime = str(p1_left_th.get('regime') or '')
                if not p1_regime and isinstance(p1_right_th, dict):
                    p1_regime = str(p1_right_th.get('regime') or '')

                # Pool1 第二阶段（盘口确认）通过后再叠加筹码与共振加分
                chip_feat = dict(_main_chip_cache.get(ts_code, {}))
                chip_feat.update({
                    'chip_concentration_pct': daily.get('chip_concentration_pct'),
                    'winner_rate': daily.get('winner_rate'),
                })
                chip_bonus, chip_info = sig.compute_pool1_chip_bonus(chip_feat)
                resonance_60m, resonance_60m_info = _pool1_resonance_60m(ts_code, daily, price, prev_price)

                left = sig.detect_left_side_buy(
                    price=price, boll_upper=boll_upper, boll_mid=boll_mid, boll_lower=boll_lower,
                    rsi6=rsi6, pct_chg=pct_chg,
                    bid_ask_ratio=bid_ask_ratio,
                    lure_long=ms_pool1.get('lure_long', False),
                    wash_trade=ms_pool1.get('wash_trade', False),
                    up_limit=up_limit, down_limit=down_limit,
                    resonance_60m=resonance_60m,
                    volume_pace_ratio=p1_vol_pace.get('ratio'),
                    volume_pace_state=p1_vol_pace.get('state'),
                    thresholds=p1_left_th,
                )
                if left.get('has_signal'):
                    left['strength'] = min(100, int(left.get('strength', 0)) + stage1_bonus + chip_bonus)
                    left = sig.finalize_pool1_signal(left, thresholds=p1_left_th)
                if left.get('has_signal'):
                    left = _attach_signal_channel_meta(left, pool_id=1)
                    left.setdefault('details', {})['pool1_stage'] = {
                        'daily_screening': stage1_info,
                        'stage2_triggered': True,
                        'chip': chip_info,
                        'resonance_60m': bool(resonance_60m),
                        'resonance_60m_info': resonance_60m_info,
                        'position_mode': 'observe',
                    }
                    fired_pool1.append(left)
                    stage2_triggered = True
                else:
                    l_reason = (left.get('details') or {}).get('reject_reason') if isinstance(left, dict) else None
                    if l_reason:
                        stage2_rejects.add(str(l_reason))
                        stage2_rejects_by_signal['left_side_buy'].add(str(l_reason))

                right = sig.detect_right_side_breakout(
                    price=price, boll_upper=boll_upper, boll_mid=boll_mid, boll_lower=boll_lower,
                    volume_ratio=volume_ratio,
                    ma5=daily.get('ma5'), ma10=daily.get('ma10'), rsi6=rsi6,
                    prev_price=prev_price,
                    bid_ask_ratio=bid_ask_ratio,
                    lure_long=ms_pool1.get('lure_long', False),
                    wash_trade=ms_pool1.get('wash_trade', False),
                    up_limit=up_limit, down_limit=down_limit,
                    resonance_60m=resonance_60m,
                    volume_pace_ratio=p1_vol_pace.get('ratio'),
                    volume_pace_state=p1_vol_pace.get('state'),
                    thresholds=p1_right_th,
                )
                if right.get('has_signal'):
                    right['strength'] = min(100, int(right.get('strength', 0)) + stage1_bonus + chip_bonus)
                    right = sig.finalize_pool1_signal(right, thresholds=p1_right_th)
                if right.get('has_signal'):
                    right = _attach_signal_channel_meta(right, pool_id=1)
                    right.setdefault('details', {})['pool1_stage'] = {
                        'daily_screening': stage1_info,
                        'stage2_triggered': True,
                        'chip': chip_info,
                        'resonance_60m': bool(resonance_60m),
                        'resonance_60m_info': resonance_60m_info,
                        'position_mode': 'observe',
                    }
                    fired_pool1.append(right)
                    stage2_triggered = True
                else:
                    r_reason = (right.get('details') or {}).get('reject_reason') if isinstance(right, dict) else None
                    if r_reason:
                        stage2_rejects.add(str(r_reason))
                        stage2_rejects_by_signal['right_side_breakout'].add(str(r_reason))
            else:
                stage2_rejects.add('stage1_fail')

            if stage1_pass and (not stage2_triggered) and (not stage2_rejects):
                stage2_rejects.add('stage2_not_triggered')
            _record_pool1_observe(
                stage1_pass=stage1_pass,
                stage2_triggered=stage2_triggered,
                reject_reasons=sorted(stage2_rejects),
                signal_rejects={k: sorted(v) for k, v in stage2_rejects_by_signal.items() if v},
                threshold_info={
                    'left_side_buy': p1_left_th if isinstance(p1_left_th, dict) else None,
                    'right_side_breakout': p1_right_th if isinstance(p1_right_th, dict) else None,
                },
                industry=p1_industry,
                regime=p1_regime,
            )

        # Pool1 持仓状态机迁移：observe -> holding -> observe
        def _pool1_actionable(s: dict) -> bool:
            if not isinstance(s, dict) or not s.get('has_signal'):
                return False
            details = s.get('details') if isinstance(s.get('details'), dict) else {}
            return not bool(details.get('observe_only', False))

        pos_state_after = pos_state_before
        transition = None
        if in_holding:
            sells = [
                s for s in fired_pool1
                if str(s.get('type') or '') in _POOL1_SELL_SIGNAL_TYPES and _pool1_actionable(s)
            ]
            if sells:
                best_sell = max(sells, key=lambda x: float(x.get('strength', 0) or 0))
                pos_state_after = _pool1_set_position_state(
                    ts_code,
                    'observe',
                    now_ts=now_ts,
                    signal_type=str(best_sell.get('type') or ''),
                    signal_price=float(best_sell.get('price', price) or price),
                )
                transition = 'holding->observe'
        else:
            buys = [
                s for s in fired_pool1
                if str(s.get('type') or '') in _POOL1_BUY_SIGNAL_TYPES and _pool1_actionable(s)
            ]
            if buys:
                best_buy = max(buys, key=lambda x: float(x.get('strength', 0) or 0))
                pos_state_after = _pool1_set_position_state(
                    ts_code,
                    'holding',
                    now_ts=now_ts,
                    signal_type=str(best_buy.get('type') or ''),
                    signal_price=float(best_buy.get('price', price) or price),
                )
                transition = 'observe->holding'

        for s in fired_pool1:
            d = s.setdefault('details', {})
            d['pool1_position'] = {
                'status_before': pos_state_before.get('status', 'observe'),
                'status_after': pos_state_after.get('status', pos_state_before.get('status', 'observe')),
                'holding_days_before': round(holding_days_before, 4),
                'holding_days_after': round(float(pos_state_after.get('holding_days', holding_days_before) or 0.0), 4),
                'transition': transition,
            }

    # ===== Pool2：T+0 日内 =====
    if 2 in pools and ts_code:
        state = _update_intraday_state(ts_code, tick)
        vwap = _compute_vwap(state)
        gub5 = _compute_gub5_trend(state)
        gub5_transition = _compute_gub5_transition(state)
        bid_ask_ratio = _compute_bid_ask_ratio(tick)

        pw = state.get('price_window')
        intraday_prices = [p for _, p, _ in pw] if pw else None
        v_power_divergence, vpower_info = _detect_v_power_divergence(state, txns_recent)

        wash_trade = ms_pool2.get('wash_trade', False)
        lure_short = ms_pool2.get('lure_short', False)
        big_order_bias = ms_pool2.get('big_order_bias', 0.0)
        real_buy_fading = bool(ms_pool2.get('active_buy_fading', False))
        wash_trade_severe = wash_trade and abs(big_order_bias) < 0.05

        try:
            feat = sig.build_t0_features(
                tick_price=price,
                boll_lower=boll_lower,
                boll_upper=boll_upper,
                vwap=vwap,
                intraday_prices=intraday_prices,
                pct_chg=pct_chg,
                trend_up=bool(
                    (float(daily.get('ma5', 0) or 0) > float(daily.get('ma10', 0) or 0) > float(daily.get('ma20', 0) or 0))
                    and float(daily.get('ma20', 0) or 0) > 0
                ),
                gub5_trend=gub5,
                gub5_transition=gub5_transition,
                bid_ask_ratio=bid_ask_ratio,
                lure_short=lure_short,
                wash_trade=wash_trade,
                spread_abnormal=False,
                v_power_divergence=v_power_divergence,
                ask_wall_building=ms_pool2.get('lure_long', False),
                real_buy_fading=real_buy_fading,
                wash_trade_severe=wash_trade_severe,
                liquidity_drain=False,
                ask_wall_absorb_ratio=ms_pool2.get('ask_wall_absorb_ratio'),
                bid_wall_break_ratio=ms_pool2.get('bid_wall_break_ratio'),
                big_order_bias=big_order_bias,
                big_net_flow_bps=ms_pool2.get('big_net_flow_bps'),
                super_order_bias=ms_pool2.get('super_order_bias'),
                super_net_flow_bps=ms_pool2.get('super_net_flow_bps'),
                volume_pace_ratio=t0_vol_pace.get('ratio'),
                volume_pace_state=t0_vol_pace.get('state'),
            )
        except Exception as e:
            logger.debug(f"build_t0_features 异常 {ts_code}: {e}")
            feat = None

        if feat is not None:
            t0_ctx = _build_ctx(
                pool_id=2,
                intraday={
                    'vwap': vwap,
                    'bias_vwap': feat.get('bias_vwap'),
                    'robust_zscore': feat.get('robust_zscore'),
                    'gub5_ratio': None,
                    'gub5_transition': gub5_transition,
                },
            )
            t0_pos_th = _get_th('positive_t', t0_ctx)
            t0_rev_th = _get_th('reverse_t', t0_ctx)
            try:
                pos_t = sig.detect_positive_t(
                    tick_price=feat['tick_price'],
                    boll_lower=feat['boll_lower'],
                    vwap=feat['vwap'],
                    gub5_trend=feat['gub5_trend'],
                    gub5_transition=feat['gub5_transition'],
                    bid_ask_ratio=feat['bid_ask_ratio'],
                    lure_short=feat['lure_short'],
                    wash_trade=feat['wash_trade'],
                    spread_abnormal=feat['spread_abnormal'],
                    boll_break=feat['boll_break'],
                    ask_wall_absorb=feat['ask_wall_absorb'],
                    ask_wall_absorb_ratio=feat.get('ask_wall_absorb_ratio'),
                    spoofing_suspected=feat['spoofing_suspected'],
                    bias_vwap=feat['bias_vwap'],
                    super_order_bias=feat.get('super_order_bias'),
                    super_net_flow_bps=feat.get('super_net_flow_bps'),
                    volume_pace_ratio=feat.get('volume_pace_ratio'),
                    volume_pace_state=feat.get('volume_pace_state'),
                    ts_code=ts_code,
                    thresholds=t0_pos_th,
                )
                if pos_t.get('has_signal'):
                    fired_pool2.append(_attach_signal_channel_meta(pos_t, pool_id=2))
            except Exception as e:
                logger.debug(f"positive_t 计算异常 {ts_code}: {e}")

            try:
                rev_t = sig.detect_reverse_t(
                    tick_price=feat['tick_price'],
                    boll_upper=feat['boll_upper'],
                    vwap=feat['vwap'],
                    intraday_prices=intraday_prices,
                    pct_chg=feat.get('pct_chg'),
                    trend_up=bool(feat.get('trend_up', False)),
                    bid_ask_ratio=feat['bid_ask_ratio'],
                    v_power_divergence=feat['v_power_divergence'],
                    ask_wall_building=feat['ask_wall_building'],
                    real_buy_fading=feat['real_buy_fading'],
                    wash_trade_severe=feat['wash_trade_severe'],
                    liquidity_drain=feat['liquidity_drain'],
                    ask_wall_absorb=bool(feat.get('ask_wall_absorb', False)),
                    ask_wall_absorb_ratio=feat.get('ask_wall_absorb_ratio'),
                    bid_wall_break=bool(feat.get('bid_wall_break', False)),
                    bid_wall_break_ratio=feat.get('bid_wall_break_ratio'),
                    boll_over=feat['boll_over'],
                    spoofing_suspected=feat['spoofing_suspected'],
                    big_order_bias=feat['big_order_bias'],
                    super_order_bias=feat.get('super_order_bias'),
                    super_net_flow_bps=feat.get('super_net_flow_bps'),
                    robust_zscore=feat['robust_zscore'],
                    volume_pace_ratio=feat.get('volume_pace_ratio'),
                    volume_pace_state=feat.get('volume_pace_state'),
                    main_rally_guard=bool(feat.get('main_rally_guard', False)),
                    main_rally_info=feat.get('main_rally_info'),
                    ts_code=ts_code,
                    thresholds=t0_rev_th,
                )
                if rev_t and rev_t.get('has_signal'):
                    fired_pool2.append(_attach_signal_channel_meta(rev_t, pool_id=2))
            except Exception as e:
                logger.debug(f"reverse_t 计算异常 {ts_code}: {e}")

    # Pool1/Pool2 双通道后处理：Pool1 不再叠加统一盘口增量，Pool2 保留微观增量。
    for s in fired_pool1:
        d = s.setdefault('details', {})
        d['market_structure'] = {
            'tag': ms_pool1['tag'],
            'big_order_bias': round(ms_pool1['big_order_bias'], 3),
            'big_net_flow_bps': round(float(ms_pool1.get('big_net_flow_bps', 0.0) or 0.0), 2),
            'super_order_bias': round(float(ms_pool1.get('super_order_bias', 0.0) or 0.0), 3),
            'super_net_flow_bps': round(float(ms_pool1.get('super_net_flow_bps', 0.0) or 0.0), 2),
            'bid_ask_ratio': round(ms_pool1['bid_ask_ratio'], 2),
            'ask_wall_absorb_ratio': round(float(ms_pool1.get('ask_wall_absorb_ratio', 0.0) or 0.0), 3),
            'bid_wall_break_ratio': round(float(ms_pool1.get('bid_wall_break_ratio', 0.0) or 0.0), 3),
            'ask_wall_absorbed': bool(ms_pool1.get('ask_wall_absorbed', False)),
            'bid_wall_broken': bool(ms_pool1.get('bid_wall_broken', False)),
            'lure_long': ms_pool1['lure_long'],
            'lure_short': ms_pool1['lure_short'],
            'wash_trade': ms_pool1['wash_trade'],
            'strength_delta': 0,
            'channel_adjust': 'pool1_no_extra_delta',
        }
        d['volume_pace'] = {
            'ratio': p1_vol_pace.get('ratio'),
            'ratio_prev': p1_vol_pace.get('ratio_prev'),
            'ratio_med5': p1_vol_pace.get('ratio_med5'),
            'state': p1_vol_pace.get('state'),
            'baseline_mode': p1_vol_pace.get('baseline_mode'),
            'baseline_volume': p1_vol_pace.get('baseline_volume'),
            'prev_day_volume': p1_vol_pace.get('prev_day_volume'),
            'vol5_median_volume': p1_vol_pace.get('vol5_median_volume'),
            'progress_ratio': p1_vol_pace.get('progress_ratio'),
        }
        if instrument_profile:
            d['instrument_profile'] = dict(instrument_profile)
    for s in fired_pool2:
        direction = s.get('direction', 'buy')
        delta = ms_pool2['strength_delta'] if direction == 'buy' else -ms_pool2['strength_delta']
        original = s.get('strength', 0)
        s['strength'] = max(0, min(100, original + delta))
        d = s.setdefault('details', {})
        d['market_structure'] = {
            'tag': ms_pool2['tag'],
            'big_order_bias': round(ms_pool2['big_order_bias'], 3),
            'big_net_flow_bps': round(float(ms_pool2.get('big_net_flow_bps', 0.0) or 0.0), 2),
            'super_order_bias': round(float(ms_pool2.get('super_order_bias', 0.0) or 0.0), 3),
            'super_net_flow_bps': round(float(ms_pool2.get('super_net_flow_bps', 0.0) or 0.0), 2),
            'bid_ask_ratio': round(ms_pool2['bid_ask_ratio'], 2),
            'ask_wall_absorb_ratio': round(float(ms_pool2.get('ask_wall_absorb_ratio', 0.0) or 0.0), 3),
            'bid_wall_break_ratio': round(float(ms_pool2.get('bid_wall_break_ratio', 0.0) or 0.0), 3),
            'ask_wall_absorbed': bool(ms_pool2.get('ask_wall_absorbed', False)),
            'bid_wall_broken': bool(ms_pool2.get('bid_wall_broken', False)),
            'lure_long': ms_pool2['lure_long'],
            'lure_short': ms_pool2['lure_short'],
            'wash_trade': ms_pool2['wash_trade'],
            'active_buy_fading': bool(ms_pool2.get('active_buy_fading', False)),
            'pull_up_not_stable': bool(ms_pool2.get('pull_up_not_stable', False)),
            'front_buy_share': round(float(ms_pool2.get('front_buy_share', 0.0) or 0.0), 4),
            'back_buy_share': round(float(ms_pool2.get('back_buy_share', 0.0) or 0.0), 4),
            'front_net_bps': round(float(ms_pool2.get('front_net_bps', 0.0) or 0.0), 2),
            'back_net_bps': round(float(ms_pool2.get('back_net_bps', 0.0) or 0.0), 2),
            'peak_gain_pct': round(float(ms_pool2.get('peak_gain_pct', 0.0) or 0.0), 4),
            'retrace_from_high_pct': round(float(ms_pool2.get('retrace_from_high_pct', 0.0) or 0.0), 4),
            'strength_delta': delta,
            'channel_adjust': 'pool2_micro_delta',
        }
        d['volume_pace'] = {
            'ratio': t0_vol_pace.get('ratio'),
            'ratio_prev': t0_vol_pace.get('ratio_prev'),
            'ratio_med5': t0_vol_pace.get('ratio_med5'),
            'state': t0_vol_pace.get('state'),
            'baseline_mode': t0_vol_pace.get('baseline_mode'),
            'baseline_volume': t0_vol_pace.get('baseline_volume'),
            'prev_day_volume': t0_vol_pace.get('prev_day_volume'),
            'vol5_median_volume': t0_vol_pace.get('vol5_median_volume'),
            'progress_ratio': t0_vol_pace.get('progress_ratio'),
        }
        if instrument_profile:
            d['instrument_profile'] = dict(instrument_profile)
        d['momentum_divergence'] = dict(vpower_info or {})
        if ms_pool2['tag'] != 'normal':
            tag_map = {
                'lure_long': '诱多回落',
                'lure_short': '诱空回补',
                'wash': '对倒震荡',
                'real_buy': '主力流入',
                'real_sell': '主力流出',
            }
            s['message'] = s.get('message', '') + f" [{tag_map.get(ms_pool2['tag'], ms_pool2['tag'])}]"

    fired: list[dict] = fired_pool1 + fired_pool2

    if ts_code and price > 0:
        _main_prev_price[ts_code] = price

    return fired


def _analyze_market_structure(tick: dict, txns: Optional[list]) -> dict:
    """
    基于逐笔成交 + 五档盘口综合识别主力行为。
    分类包括：lure_long（诱多）、lure_short（诱空）、wash_trade（对倒）。
    若不属于以上异常，则识别 real_buy / real_sell（真实流入/流出）。
    
      - real_buy / real_sell（大单真实流入/流出）

    返回：{
        'big_buy_vol', 'big_sell_vol', 'big_order_bias' ->[-1, 1],
        'bid_vol', 'ask_vol', 'bid_ask_ratio',
        'ask_wall_absorb_ratio', 'bid_wall_break_ratio',
        'ask_wall_absorbed', 'bid_wall_broken',
        'lure_long', 'lure_short', 'wash_trade',
        'tag': 'normal'|'lure_long'|'lure_short'|'wash'|'real_buy'|'real_sell',
        'strength_delta': int ->[-30, +30]  # 对买入信号的分值增量
    """
    result = {
        'big_buy_vol': 0, 'big_sell_vol': 0, 'big_order_bias': 0.0,
        'big_buy_amt': 0.0, 'big_sell_amt': 0.0, 'big_net_flow_bps': 0.0,
        'super_buy_vol': 0, 'super_sell_vol': 0, 'super_order_bias': 0.0,
        'super_buy_amt': 0.0, 'super_sell_amt': 0.0, 'super_net_flow_bps': 0.0,
        'bid_vol': 0, 'ask_vol': 0, 'bid_ask_ratio': 1.0,
        'ask_wall_absorb_ratio': 0.0, 'bid_wall_break_ratio': 0.0,
        'ask_wall_absorbed': False, 'bid_wall_broken': False,
        'active_buy_fading': False, 'pull_up_not_stable': False,
        'front_buy_share': 0.0, 'back_buy_share': 0.0,
        'front_net_bps': 0.0, 'back_net_bps': 0.0,
        'peak_gain_pct': 0.0, 'retrace_from_high_pct': 0.0,
        'lure_long': False, 'lure_short': False, 'wash_trade': False,
        'tag': 'normal', 'strength_delta': 0,
    }

    # 1. 五档盘口压力
    bids = tick.get('bids') or []
    asks = tick.get('asks') or []
    bid_vol = sum(int(v or 0) for _, v in bids[:WALL_LEVELS])
    ask_vol = sum(int(v or 0) for _, v in asks[:WALL_LEVELS])
    result['bid_vol'] = bid_vol
    result['ask_vol'] = ask_vol
    result['bid_ask_ratio'] = (bid_vol / ask_vol) if ask_vol > 0 else 1.0

    # 2. 逐笔大单分析
    if not txns:
        return result
    recent = txns[-TXN_ANALYZE_COUNT:]
    big_buy_vol = 0
    big_sell_vol = 0
    big_buy_cnt = 0
    big_sell_cnt = 0
    big_buy_amt = 0.0
    big_sell_amt = 0.0
    super_buy_vol = 0
    super_sell_vol = 0
    super_buy_amt = 0.0
    super_sell_amt = 0.0
    big_buy_prices: list[float] = []
    big_sell_prices: list[float] = []
    all_prices: list[float] = []
    total_vol = 0
    total_amt = 0.0
    wall_window = min(30, len(recent)) if recent else 0
    wall_start = max(0, len(recent) - wall_window)
    wall_buy_big = 0
    wall_sell_big = 0
    wall_buy_super = 0
    wall_sell_super = 0

    for idx, t in enumerate(recent):
        v = int(t.get('volume', 0) or 0)
        p = float(t.get('price', 0) or 0)
        d = int(t.get('direction', 0) or 0)  # 0=buy, 1=sell
        amt = p * v
        total_vol += v
        total_amt += amt
        all_prices.append(p)
        if v >= BIG_ORDER_THRESHOLD:
            if d == 0:
                big_buy_vol += v
                big_buy_cnt += 1
                big_buy_amt += amt
                big_buy_prices.append(p)
            else:
                big_sell_vol += v
                big_sell_cnt += 1
                big_sell_amt += amt
                big_sell_prices.append(p)
        if idx >= wall_start and v >= BIG_ORDER_THRESHOLD:
            if d == 0:
                wall_buy_big += v
            else:
                wall_sell_big += v
        if v >= SUPER_BIG_ORDER_THRESHOLD:
            if d == 0:
                super_buy_vol += v
                super_buy_amt += amt
                if idx >= wall_start:
                    wall_buy_super += v
            else:
                super_sell_vol += v
                super_sell_amt += amt
                if idx >= wall_start:
                    wall_sell_super += v

    total_big = big_buy_vol + big_sell_vol
    total_super = super_buy_vol + super_sell_vol
    result['big_buy_vol'] = big_buy_vol
    result['big_sell_vol'] = big_sell_vol
    result['big_order_bias'] = (big_buy_vol - big_sell_vol) / total_big if total_big > 0 else 0.0
    result['big_buy_amt'] = round(big_buy_amt, 2)
    result['big_sell_amt'] = round(big_sell_amt, 2)
    result['big_net_flow_bps'] = (
        round((big_buy_amt - big_sell_amt) / total_amt * 10000.0, 2)
        if total_amt > 0 else 0.0
    )
    result['super_buy_vol'] = super_buy_vol
    result['super_sell_vol'] = super_sell_vol
    result['super_order_bias'] = (super_buy_vol - super_sell_vol) / total_super if total_super > 0 else 0.0
    result['super_buy_amt'] = round(super_buy_amt, 2)
    result['super_sell_amt'] = round(super_sell_amt, 2)
    result['super_net_flow_bps'] = (
        round((super_buy_amt - super_sell_amt) / total_amt * 10000.0, 2)
        if total_amt > 0 else 0.0
    )
    wall_buy_eff = float(wall_buy_big) + float(WALL_SUPER_WEIGHT) * float(wall_buy_super)
    wall_sell_eff = float(wall_sell_big) + float(WALL_SUPER_WEIGHT) * float(wall_sell_super)
    ask_absorb_ratio = (wall_buy_eff / ask_vol) if ask_vol > 0 else 0.0
    bid_break_ratio = (wall_sell_eff / bid_vol) if bid_vol > 0 else 0.0
    result['ask_wall_absorb_ratio'] = round(float(ask_absorb_ratio), 4)
    result['bid_wall_break_ratio'] = round(float(bid_break_ratio), 4)

    price_drift_pct = 0.0
    if wall_window >= 2 and recent:
        try:
            p_first = float(recent[wall_start].get('price', 0) or 0)
            p_last = float(recent[-1].get('price', 0) or 0)
            if p_first > 0 and p_last > 0:
                price_drift_pct = (p_last - p_first) / p_first * 100.0
        except Exception:
            price_drift_pct = 0.0
    result['price_drift_pct'] = round(float(price_drift_pct), 4)
    ask_wall_absorbed = bool(
        ask_absorb_ratio >= ASK_WALL_ABSORB_TH
        and (result['big_order_bias'] > 0.15 or result['super_order_bias'] > 0.12 or result['big_net_flow_bps'] >= WALL_NET_FLOW_BPS_TH)
        and price_drift_pct >= -0.10
    )
    bid_wall_broken = bool(
        bid_break_ratio >= BID_WALL_BREAK_TH
        and (result['big_order_bias'] < -0.15 or result['super_order_bias'] < -0.12 or result['big_net_flow_bps'] <= -WALL_NET_FLOW_BPS_TH)
        and price_drift_pct <= 0.10
    )
    result['ask_wall_absorbed'] = ask_wall_absorbed
    result['bid_wall_broken'] = bid_wall_broken

    tail = list(recent[-min(20, len(recent)):]) if recent else []
    if len(tail) >= 8:
        half = max(1, len(tail) // 2)
        front_stats = _txn_flow_stats(tail[:half])
        back_stats = _txn_flow_stats(tail[half:])
        front_buy_share = float(front_stats.get('buy_share', 0.0) or 0.0)
        back_buy_share = float(back_stats.get('buy_share', 0.0) or 0.0)
        front_net_bps = float(front_stats.get('net_bps', 0.0) or 0.0)
        back_net_bps = float(back_stats.get('net_bps', 0.0) or 0.0)
        start_price = float(front_stats.get('start_price', 0.0) or 0.0)
        peak_price = max(
            float(front_stats.get('peak_price', 0.0) or 0.0),
            float(back_stats.get('peak_price', 0.0) or 0.0),
            float(back_stats.get('last_price', 0.0) or 0.0),
        )
        last_price = float(back_stats.get('last_price', 0.0) or 0.0)
        peak_gain_pct = ((peak_price - start_price) / start_price * 100.0) if start_price > 0 and peak_price > 0 else 0.0
        retrace_from_high_pct = ((peak_price - last_price) / peak_price * 100.0) if peak_price > 0 and last_price > 0 else 0.0
        pull_up_not_stable = bool(
            peak_gain_pct >= _T0_LURE_LONG_PULLUP_MIN_PCT
            and retrace_from_high_pct >= _T0_LURE_LONG_RETRACE_MIN_PCT
        )
        active_buy_fading = bool(
            front_buy_share > 0
            and back_buy_share < front_buy_share * _T0_LURE_LONG_FADE_RATIO_MAX
            and back_net_bps < front_net_bps - _T0_LURE_LONG_NET_BPS_DROP
        )
        result['front_buy_share'] = round(front_buy_share, 4)
        result['back_buy_share'] = round(back_buy_share, 4)
        result['front_net_bps'] = round(front_net_bps, 2)
        result['back_net_bps'] = round(back_net_bps, 2)
        result['peak_gain_pct'] = round(peak_gain_pct, 4)
        result['retrace_from_high_pct'] = round(retrace_from_high_pct, 4)
        result['pull_up_not_stable'] = pull_up_not_stable
        result['active_buy_fading'] = active_buy_fading

    # 3) 识别诱多 / 诱空 / 对倒
    lure_long_hit = bool(
        result['big_order_bias'] >= _T0_LURE_LONG_BIG_BIAS_MIN
        and result['bid_ask_ratio'] <= _T0_LURE_LONG_BIDASK_MAX
        and result['pull_up_not_stable']
        and result['active_buy_fading']
        and not ask_wall_absorbed
    )
    if lure_long_hit:
        result['lure_long'] = True
        result['tag'] = 'lure_long'
        result['strength_delta'] = -25   # 对买入信号明显减分
    # 诱空：大单卖出占优但买盘承接很强
    elif result['big_order_bias'] < -0.3 and result['bid_ask_ratio'] > 1.5:
        result['lure_short'] = True
        result['tag'] = 'lure_short'
        result['strength_delta'] = +25   # 对买入信号明显加分（更偏向抄底反弹）

    # 对倒：买卖大单笔数接近 + 价格窄幅震荡 + 大单成交占比高
    elif big_buy_cnt >= 3 and big_sell_cnt >= 3:
        cnt_ratio = min(big_buy_cnt, big_sell_cnt) / max(big_buy_cnt, big_sell_cnt)
        if all_prices and total_vol > 0:
            price_range = (max(all_prices) - min(all_prices)) / max(all_prices)
            big_vol_ratio = total_big / total_vol
            if cnt_ratio > 0.7 and price_range < 0.003 and big_vol_ratio > 0.4:
                result['wash_trade'] = True
                result['tag'] = 'wash'
                result['strength_delta'] = -20   # 对买入信号明显降分
    # 识别真实流入/流出（未识别为诱多/诱空/对倒时）
    if result['tag'] == 'normal':
        if result['big_order_bias'] > 0.3 and result['bid_ask_ratio'] >= 1.0:
            result['tag'] = 'real_buy'
            result['strength_delta'] = +15
        elif result['big_order_bias'] < -0.3 and result['bid_ask_ratio'] <= 1.0:
            result['tag'] = 'real_sell'
            result['strength_delta'] = -15
        elif ask_wall_absorbed and not bid_wall_broken:
            result['tag'] = 'real_buy'
            result['strength_delta'] = +18
        elif bid_wall_broken and not ask_wall_absorbed:
            result['tag'] = 'real_sell'
            result['strength_delta'] = -18

    # 超大单偏置作为二级确认：在已识别真实流入/流出后再做强弱修正。
    if result['tag'] == 'real_buy' and result['super_order_bias'] > 0.25:
        result['strength_delta'] = min(30, int(result['strength_delta']) + 5)
    elif result['tag'] == 'real_sell' and result['super_order_bias'] < -0.25:
        result['strength_delta'] = max(-30, int(result['strength_delta']) - 5)

    # 吸收/击穿作为结构确认，进一步修正分值。
    if ask_wall_absorbed and not bid_wall_broken:
        result['strength_delta'] = min(30, int(result['strength_delta']) + 4)
    if bid_wall_broken and not ask_wall_absorbed:
        result['strength_delta'] = max(-30, int(result['strength_delta']) - 4)

    return result


def _detect_v_power_divergence(state: dict, txns: Optional[list] = None) -> tuple[bool, dict]:
    """
    量价背离检测：
    1) 价格新高但最新 vpower 弱于近期均值
    2) 价格新高但主动买流没有同步增强
    """
    info = {
        'price_new_high': False,
        'vpower_last': None,
        'vpower_recent_avg': None,
        'vpower_divergence': False,
        'flow_divergence': False,
        'front_buy_share': None,
        'back_buy_share': None,
        'front_net_bps': None,
        'back_net_bps': None,
    }
    pw = state.get('price_window')
    if not pw or len(pw) < 20:
        info['reason'] = 'insufficient_price_window'
        return False, info
    recent = list(pw)[-20:]
    prices = [p for _, p, _ in recent]
    vpower = [p * dv for _, p, dv in recent]
    last_price = float(prices[-1] or 0)
    prior_high = max(prices[:-1]) if len(prices) > 1 else last_price
    eps = float(_T0_DIV_PRICE_EPS_PCT) / 100.0
    price_new_high = bool(last_price > 0 and prior_high > 0 and last_price >= prior_high * (1 - eps))
    info['price_new_high'] = price_new_high
    if len(vpower) < 5:
        info['reason'] = 'insufficient_vpower'
        return False, info
    vpower_recent_avg = sum(vpower[-5:]) / 5
    info['vpower_last'] = round(float(vpower[-1]), 2)
    info['vpower_recent_avg'] = round(float(vpower_recent_avg), 2)
    vpower_div = bool(price_new_high and vpower[-1] < vpower_recent_avg)
    info['vpower_divergence'] = vpower_div

    flow_div = False
    if txns and len(txns) >= 12:
        tail = list(txns)[-20:]
        half = len(tail) // 2
        front = _txn_flow_stats(tail[:half])
        back = _txn_flow_stats(tail[half:])
        info['front_buy_share'] = round(float(front.get('buy_share', 0.0) or 0.0), 4)
        info['back_buy_share'] = round(float(back.get('buy_share', 0.0) or 0.0), 4)
        info['front_net_bps'] = round(float(front.get('net_bps', 0.0) or 0.0), 2)
        info['back_net_bps'] = round(float(back.get('net_bps', 0.0) or 0.0), 2)
        front_buy_share = float(front.get('buy_share', 0.0) or 0.0)
        back_buy_share = float(back.get('buy_share', 0.0) or 0.0)
        front_net_bps = float(front.get('net_bps', 0.0) or 0.0)
        back_net_bps = float(back.get('net_bps', 0.0) or 0.0)
        flow_div = bool(
            price_new_high
            and front_buy_share > 0
            and back_buy_share < front_buy_share * _T0_DIV_FLOW_RATIO_MAX
            and back_net_bps < front_net_bps - _T0_DIV_NET_BPS_DROP
        )
    info['flow_divergence'] = flow_div
    info['reason'] = 'ok'
    return bool(vpower_div or flow_div), info


def _postprocess_fired_signals(
    ts_code: str,
    tick: dict,
    all_fired: list[dict],
    now: Optional[int] = None,
    *,
    persist: bool = True,
) -> dict:
    """
    Unified signal postprocess for gm/mootdx:
    - dedup by signal type and pool
    - reverse-signal expire/reset
    - state machine update (cached + newly fired merged)
    - in-memory signal cache refresh
    - optional persistence enqueue for newly-fired signals
    """
    if now is None:
        now = int(time.time())
    else:
        now = int(now)

    fired_list = list(all_fired or [])
    cached_entry = _main_signal_cache.get(ts_code)
    cached_signals_by_type: dict[str, dict] = {}
    if cached_entry:
        for cs in cached_entry.get('signals', []):
            sig_type = cs.get('type')
            if sig_type:
                # Carry cached signals forward into state machine; they are not "new" by default.
                cs_copy = dict(cs)
                cs_copy['is_new'] = False
                cached_signals_by_type[str(sig_type)] = cs_copy

    # Merge strategy: start from cached signals, then overlay this-round fired signals.
    merged_by_type: dict[str, dict] = dict(cached_signals_by_type)
    pools = _main_stock_pools.get(ts_code, set())
    for s in fired_list:
        signal_type = str(s.get('type') or '')
        if not signal_type:
            continue
        key = (ts_code, signal_type)
        last_ts = int(_main_signal_last_fire.get(key, 0) or 0)
        dedup_seconds = _dedup_seconds_for_signal(signal_type, pools)

        # If reverse signal appears, expire opposite side immediately.
        reverse_types = REVERSE_SIGNAL_PAIRS.get(signal_type) or ()
        if isinstance(reverse_types, str):
            reverse_types = (reverse_types,)
        for reverse_type in reverse_types:
            reverse_key = (ts_code, reverse_type)
            if reverse_key in _main_signal_last_fire:
                del _main_signal_last_fire[reverse_key]
            merged_by_type.pop(reverse_type, None)
            _expire_reversed_signal_state(ts_code, reverse_type)

        if now - last_ts >= dedup_seconds:
            _main_signal_last_fire[key] = now
            s_copy = dict(s)
            s_copy['is_new'] = True
            merged_by_type[signal_type] = s_copy
            continue

        original = merged_by_type.get(signal_type)
        if original is not None:
            original_copy = dict(original)
            original_copy['is_new'] = False
            merged_by_type[signal_type] = original_copy
            continue
        # Dedup blocked and no cached baseline: skip to avoid phantom re-activation.

    # state/age/current_strength/expire_reason
    next_signals: list[dict] = []
    for sig_type, sig in merged_by_type.items():
        s_in = dict(sig)
        s_in.setdefault('type', sig_type)
        s_in.setdefault('is_new', False)
        s_out = _update_signal_state(ts_code, s_in, now)

        # Remove expired signals from display/cache after state transition closes.
        if s_out.get('state') == 'expired':
            states = _main_signal_state_cache.get(ts_code)
            if isinstance(states, dict):
                states.pop(sig_type, None)
                if not states:
                    _main_signal_state_cache.pop(ts_code, None)
            continue

        next_signals.append(s_out)

    price = tick.get('price', 0)
    pct_chg = tick.get('pct_chg', 0)
    if cached_entry is None:
        _main_signal_cache[ts_code] = {
            'ts_code': ts_code,
            'name': tick.get('name', ts_code),
            'price': price,
            'pct_chg': pct_chg,
            'signals': next_signals,
            'evaluated_at': now,
        }
    else:
        cached_entry['signals'] = next_signals
        cached_entry['evaluated_at'] = now
        cached_entry['price'] = price
        cached_entry['pct_chg'] = pct_chg

    new_count = sum(1 for s in next_signals if s.get('is_new'))
    if new_count > 0:
        stock_pools = _main_stock_pools.get(ts_code, set())
        for s in next_signals:
            if not s.get('is_new'):
                continue
            cur_ids = s.get('_pool_ids')
            if isinstance(cur_ids, list) and len(cur_ids) > 0:
                continue
            s['_pool_ids'] = list(stock_pools)
        if persist and _persist_queue is not None:
            try:
                _persist_queue.put_nowait(('signal', ts_code, tick, next_signals))
            except Exception:
                pass

    return {
        'signals': next_signals,
        'new_count': new_count,
        'new_types': [s.get('type') for s in next_signals if s.get('is_new')],
    }


def _start_consumer_thread():
    """启动消费线程：queue->cache->Layer2 信号计算。"""
    global _consumer_thread_started
    if _consumer_thread_started:
        return
    _consumer_thread_started = True

    def _consume_loop():
        logger.info("tick 消费线程启动（含 Layer2 信号计算）")
        count = 0
        signal_fires = 0
        dropped_invalid_ticks = 0
        while True:
            try:
                item = _main_queue.get(timeout=1.0)
            except _QueueEmpty:
                continue
            except Exception as e:
                logger.error(f"tick 消费异常: {e}")
                continue
            # 批量拉取并按股票合并，只处理每只股票最新一笔，降低高频重算压力。
            raw_batch: list[dict] = [item]
            max_batch = max(1, int(_CONSUMER_BATCH_MAX))
            drain_max = max(max_batch, int(_CONSUMER_DRAIN_MAX))
            backlog_trigger = max(0, int(_CONSUMER_BACKLOG_DRAIN_TRIGGER))
            try:
                backlog_now = int(_main_queue.qsize())
            except Exception:
                backlog_now = 0
            target_batch = max_batch
            if backlog_trigger > 0 and backlog_now >= backlog_trigger:
                # 积压时优先追最新状态，适度扩大单轮拉取上限，减少前端可见延迟。
                target_batch = min(drain_max, max_batch + backlog_now)
            for _ in range(max(0, target_batch - 1)):
                try:
                    raw_batch.append(_main_queue.get_nowait())
                except _QueueEmpty:
                    break
                except Exception:
                    break
            process_batch = raw_batch
            if _CONSUMER_COALESCE_ENABLED and len(raw_batch) >= max(1, int(_CONSUMER_COALESCE_MIN_BATCH)):
                latest_by_code: dict[str, dict] = {}
                for it in raw_batch:
                    if not isinstance(it, dict):
                        continue
                    code = str(it.get('ts_code') or '')
                    if not code:
                        continue
                    latest_by_code[code] = it
                if latest_by_code:
                    process_batch = list(latest_by_code.values())
            _record_consumer_batch_perf(
                len(raw_batch),
                len(process_batch),
                backlog=backlog_now,
                target=target_batch,
            )
            for item in process_batch:
                try:
                    ts_code = item.get('ts_code')
                    if not ts_code:
                        continue
                    try:
                        px = float(item.get('price', 0) or 0)
                    except Exception:
                        px = 0.0
                    if px <= 0:
                        est_px, est_src = _derive_price_from_order_book(item)
                        if est_px > 0:
                            item = dict(item)
                            item['price'] = float(est_px)
                            item['price_source'] = str(item.get('price_source') or f'consumer_{est_src}')
                            pre_close = float(item.get('pre_close', 0) or 0)
                            if pre_close > 0:
                                item['pct_chg'] = (float(est_px) - pre_close) / pre_close * 100
                            px = float(est_px)
                        else:
                            dropped_invalid_ticks += 1
                            if dropped_invalid_ticks <= 3 or dropped_invalid_ticks % 200 == 0:
                                logger.debug(f"[消费线程] 丢弃无效tick {ts_code} price={px} dropped={dropped_invalid_ticks}")
                            continue

                    # 1. 更新 tick cache
                    _main_cache[ts_code] = item
                    count += 1

                    # 1.5 异步写入持久化队列（Layer3）
                    try:
                        if _persist_queue is not None:
                            _persist_queue.put_nowait(('tick', ts_code, item, None))
                    except Exception:
                        pass  # 队列满时丢弃，保证消费线程不阻塞
                    # 2. Layer2：若已加载日线指标，则立即计算信号
                    daily = _main_daily_cache.get(ts_code)
                    if daily:
                        sig_t0 = time.perf_counter()
                        try:
                            all_fired = _compute_signals_for_tick(item, daily, ts_code=ts_code)
                        except Exception as ce:
                            logger.warning(f"信号计算失败 {ts_code}: {ce}")
                            all_fired = []

                        processed = _postprocess_fired_signals(
                            ts_code=ts_code,
                            tick=item,
                            all_fired=all_fired,
                            now=int(time.time()),
                            persist=True,
                        )
                        sig_ms = (time.perf_counter() - sig_t0) * 1000.0
                        _record_signal_perf(sig_ms)
                        if sig_ms >= (_SIGNAL_PERF_SLOW_MS * 2):
                            logger.debug(f"[信号慢路径] {ts_code} signal_ms={sig_ms:.2f} fired={len(all_fired)}")
                        new_fired = processed.get('signals', [])
                        new_count = int(processed.get('new_count', 0) or 0)
                        if new_count > 0:
                            signal_fires += 1
                            if signal_fires <= 10 or signal_fires % 100 == 0:
                                types = processed.get('new_types', [])
                                logger.info(f"[新信号#{signal_fires}] {ts_code} price={item.get('price')} types={types}")

                    if count <= 3 or count % 500 == 0:
                        msg = f"[消费线程 #{count}] {ts_code} price={item.get('price')} daily_loaded={bool(daily)}"
                        if count % 500 == 0:
                            msg = f"{msg} | {_pool1_observe_summary()}"
                        logger.info(msg)
                except Exception as e:
                    logger.warning(f"tick 消费处理失败: {e}")

    t = threading.Thread(target=_consume_loop, daemon=True, name='tick-consumer')
    t.start()


# ============================================================
# GmTickProvider
# ============================================================
class GmTickProvider(TickProvider):
    """
    掘金量化(gm) Tick Provider（subscribe 订阅模式）。
    由 gm.run() 驱动 on_tick 写入缓存，get_tick() 从内存缓存读取。"""

    @property
    def name(self) -> str:
        return 'gm'

    @property
    def display_name(self) -> str:
        return '掘金量化(gm)'

    def get_tick(self, ts_code: str) -> dict:
        """从主进程内存 cache 读取最新 tick，O(1) 访问。"""
        tick = self.get_cached_tick(ts_code)
        if tick is not None:
            return tick

        # Cache-miss fallback to mootdx is throttled; avoid blocking hot paths.
        if _GM_MISS_FALLBACK_ENABLED:
            now = time.time()
            if _GM_MISS_FALLBACK_WHEN_OPEN or (not _is_cn_trading_time_now()):
                last = float(_main_miss_fallback_at.get(ts_code, 0.0) or 0.0)
                if now - last >= max(0.5, float(_GM_MISS_FALLBACK_TTL_SEC)):
                    _main_miss_fallback_at[ts_code] = now
                    try:
                        mt = mootdx_client.get_tick(ts_code)
                        if float(mt.get('price', 0) or 0) > 0:
                            return mt
                    except Exception:
                        pass
        # gm 缓存无数据时返回空结构，不回退 mock
        return {
            'ts_code': ts_code,
            'name': '',
            'price': 0,
            'open': 0,
            'high': 0,
            'low': 0,
            'pre_close': 0,
            'volume': 0,
            'amount': 0,
            'pct_chg': 0,
            'timestamp': int(time.time()),
            'bids': [(0, 0)] * 5,
            'asks': [(0, 0)] * 5,
            'is_mock': False,
        }

    def get_cached_tick(self, ts_code: str) -> Optional[dict]:
        """读取缓存 tick，不触发网络请求。"""
        tick = _main_cache.get(ts_code)
        if tick is None:
            return None
        return _with_pre_close_fallback(ts_code, tick)

    def subscribe_symbols(self, ts_codes: list[str]):
        """
        更新订阅列表，并将结果持久化到文件供 gm 策略脚本读取。
        """
        new_codes = []
        with _tick_lock:
            for code in ts_codes:
                if code not in _subscribed_codes:
                    _subscribed_codes.add(code)
                    new_codes.append(code)

        if new_codes:
            logger.info(f"gm 订阅列表新增 {len(new_codes)} 只: {new_codes}")
            _save_subscribed_codes()

    def unsubscribe_symbols(self, ts_codes: list[str]):
        """取消订阅。"""
        with _tick_lock:
            for code in ts_codes:
                _subscribed_codes.discard(code)

    # ========================================================
    # Layer2 数据更新入口
    # ========================================================
    def update_daily_factors(self, ts_code: str, factors: dict):
        """更新单只股票的日线因子缓存（供消费线程计算信号使用）。"""
        _main_daily_cache[ts_code] = dict(factors)

    def bulk_update_daily_factors(self, factors_map: dict[str, dict]):
        """批量更新日线因子缓存（用于定时刷新）。"""
        _main_daily_cache.update(factors_map)
        logger.info(f"Layer2 日线因子缓存已更新: {len(factors_map)}")

    def bulk_update_chip_features(self, features_map: dict[str, dict]):
        """批量更新筹码特征缓存（Pool1 筹码加分）。"""
        _main_chip_cache.update(features_map)
        logger.info(f"Layer2 筹码特征缓存已更新: {len(features_map)}")

    def update_stock_pools(self, ts_code: str, pool_ids: set):
        """更新单只股票的池归属（set of pool_id）。"""
        _main_stock_pools[ts_code] = set(pool_ids)

    def bulk_update_stock_pools(self, mapping: dict[str, set]):
        """
        批量更新股票->池映射。mapping: {ts_code: {pool_id, ...}}
        """
        _main_stock_pools.clear()
        for k, v in mapping.items():
            _main_stock_pools[k] = set(v)
        logger.info(f"Layer2 股票池映射已更新: {len(mapping)}")

    def update_stock_industry(self, ts_code: str, industry: str):
        """更新单只股票行业标签（供动态阈值分桶使用）。"""
        _main_stock_industry[ts_code] = str(industry or "")

    def bulk_update_stock_industry(self, mapping: dict[str, str]):
        """批量更新股票行业标签。mapping: {ts_code: industry}"""
        _main_stock_industry.clear()
        for k, v in (mapping or {}).items():
            _main_stock_industry[str(k)] = str(v or "")
        logger.info(f"Layer2 股票行业映射已更新: {len(mapping or {})}")

    def update_instrument_profile(self, ts_code: str, profile: dict):
        """更新单只股票的制度画像。"""
        _main_instrument_profile[str(ts_code)] = dict(profile or {})

    def bulk_update_instrument_profiles(self, mapping: dict[str, dict]):
        """批量更新股票制度画像。mapping: {ts_code: {...}}"""
        _main_instrument_profile.clear()
        for k, v in (mapping or {}).items():
            _main_instrument_profile[str(k)] = dict(v or {})
        logger.info(f"Layer2 标的制度画像已更新: {len(mapping or {})}")

    def update_recent_txns(self, ts_code: str, txns: list):
        """更新单只股票的逐笔成交缓存。"""
        if txns:
            _main_recent_txns[ts_code] = list(txns)[-TXN_ANALYZE_COUNT:]

    def bulk_update_recent_txns(self, mapping: dict):
        """批量更新逐笔成交缓存。"""
        for k, v in mapping.items():
            if v:
                _main_recent_txns[k] = list(v)[-TXN_ANALYZE_COUNT:]

    def get_cached_signals(self, ts_code: str) -> Optional[dict]:
        """读取指定 ts_code 的信号缓存（秒级响应）。"""
        return _main_signal_cache.get(ts_code)

    def get_cached_transactions(self, ts_code: str) -> Optional[list]:
        """读取指定 ts_code 的逐笔成交缓存（秒级响应）。"""
        return _main_recent_txns.get(ts_code)

    def get_prev_price(self, ts_code: str) -> Optional[float]:
        """读取上一笔价格（用于慢路径突破跨越确认）。"""
        try:
            v = _main_prev_price.get(ts_code)
            return float(v) if v is not None else None
        except Exception:
            return None

    def get_cached_signals_bulk(self, ts_codes: list[str]) -> list[dict]:
        """批量读取信号缓存，用于盯盘池列表。"""
        result = []
        for code in ts_codes:
            entry = _main_signal_cache.get(code)
            if entry is not None:
                result.append(entry)
        return result

    def get_pool1_observe_stats(self) -> Optional[dict]:
        """读取 Pool1 两阶段统计快照。"""
        return get_pool1_observe_stats()

    def get_pool1_position_state(self, ts_code: str) -> Optional[dict]:
        """读取 Pool1 单票持仓状态快照（observe/holding）。"""
        return get_pool1_position_state(ts_code)

    def get_pool1_position_storage_status(self) -> Optional[dict]:
        """读取 Pool1 持仓状态存储运行态（redis/file 来源）。"""
        return get_pool1_position_storage_status()

    def get_signal_perf_stats(self) -> Optional[dict]:
        """读取 Layer2 信号计算性能快照（毫秒级）。"""
        return get_signal_perf_stats()

    def start(self):
        """启动 gm 后台订阅进程。"""
        if not _GM_AVAILABLE:
            logger.warning("gm 未安装，GmTickProvider 将仅返回空数据")
            return

        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from config import GM_TOKEN

        if not GM_TOKEN:
            logger.warning("GM_TOKEN 未配置，GmTickProvider 将仅返回空数据")
            return

        # 策略脚本存在性检查
        if not os.path.exists(_STRATEGY_FILE):
            logger.error(f"gm strategy script not found: {_STRATEGY_FILE}")
            logger.error("please ensure _gm_strategy.py exists in project root")
            return

        # 1. 启动 BaseManager TCP server（共享 queue + cache）
        _start_manager_server()
        # 2. 启动消费线程（queue -> cache）
        _start_consumer_thread()
        # 3. 启动 gm 后台进程（通过 env 连接 manager）
        process = multiprocessing.Process(target=self._run_gm, daemon=True, name='gm-subscribe')
        process.start()
        logger.info(f"gm subscribe process started (strategy: {_STRATEGY_FILE})")

    def stop(self):
        """停止 gm 后台进程（当前为请求停止）。"""
        logger.info("gm subscribe process stop requested")

    @staticmethod
    def _run_gm():
        """在独立进程中运行 gm.run()。"""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from config import GM_TOKEN, GM_MODE

        try:
            # 切换到项目根目录，确保 gm.run() 能找到策略脚本
            original_dir = os.getcwd()
            os.chdir(_PROJECT_ROOT)
            logger.info(f"switch cwd to {_PROJECT_ROOT}")

            logger.info(f"gm.run() 启动, mode={GM_MODE}, token={GM_TOKEN[:8]}...")
            run(
                strategy_id='quant_tick_subscriber',
                filename='_gm_strategy.py',
                mode=GM_MODE,
                token=GM_TOKEN,
                backtest_start_time='2026-04-17 08:00:00',
                backtest_end_time='2026-04-17 16:00:00',
                backtest_adjust=ADJUST_PREV,
                backtest_initial_cash=10000000,
                backtest_commission_ratio=0.0001,
                backtest_slippage_ratio=0.0001
            )
        except Exception as e:
            logger.error(f"gm 后台进程异常: {e}")
        finally:
            # 恢复原工作目录
            os.chdir(original_dir)


