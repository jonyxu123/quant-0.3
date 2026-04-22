"""
GM provider compatibility layer.

This module keeps the historical public interfaces used by:
- backend.realtime.tick_provider (GmTickProvider)
- backend.realtime.mootdx_tick (shared caches + signal compute entry)
- backend.api.routers.realtime_monitor (indirectly via provider)

Implementation note:
- To keep runtime stable, this compatibility layer polls market data via
  mootdx and computes signals in-process.
"""
from __future__ import annotations

import datetime as _dt
import threading
import time
from collections import deque
from typing import Optional

from loguru import logger

from backend.realtime import signals as sig
from backend.realtime.mootdx_client import client as mootdx_client
from backend.realtime.tick_provider import TickProvider

# ---------------------------
# shared caches (public usage)
# ---------------------------
_tick_lock = threading.Lock()
_subscribed_codes: set[str] = set()

_main_cache: dict[str, dict] = {}
_main_daily_cache: dict[str, dict] = {}
_main_chip_cache: dict[str, dict] = {}
_main_signal_cache: dict[str, dict] = {}
_main_stock_pools: dict[str, set] = {}
_main_recent_txns: dict[str, list] = {}
_main_prev_price: dict[str, float] = {}

TXN_ANALYZE_COUNT = 50
BIG_ORDER_THRESHOLD = 500

# intraday helper state
INTRADAY_WINDOW_SECONDS = 20 * 60
_intraday_state: dict[str, dict] = {}

# pool1 observe counters
_pool1_observe = {
    "trade_date": "",
    "updated_at": 0,
    "screen_total": 0,
    "screen_pass": 0,
    "stage2_triggered": 0,
}


def _pool1_observe_trade_date() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%d")


def _ensure_pool1_observe_day() -> None:
    today = _pool1_observe_trade_date()
    if _pool1_observe.get("trade_date") == today:
        return
    _pool1_observe["trade_date"] = today
    _pool1_observe["updated_at"] = int(time.time())
    _pool1_observe["screen_total"] = 0
    _pool1_observe["screen_pass"] = 0
    _pool1_observe["stage2_triggered"] = 0


def _record_pool1_observe(stage1_pass: bool, stage2_triggered: bool) -> None:
    _ensure_pool1_observe_day()
    _pool1_observe["screen_total"] += 1
    if stage1_pass:
        _pool1_observe["screen_pass"] += 1
        if stage2_triggered:
            _pool1_observe["stage2_triggered"] += 1
    _pool1_observe["updated_at"] = int(time.time())


def _pool1_observe_summary() -> str:
    _ensure_pool1_observe_day()
    total = int(_pool1_observe.get("screen_total", 0) or 0)
    passed = int(_pool1_observe.get("screen_pass", 0) or 0)
    triggered = int(_pool1_observe.get("stage2_triggered", 0) or 0)
    if total <= 0:
        return "pool1无样本"
    pass_rate = (passed / total) * 100
    trigger_rate = (triggered / passed) * 100 if passed > 0 else 0.0
    return (
        f"pool1通过率={pass_rate:.1f}%({passed}/{total}) "
        f"二阶段触发率={trigger_rate:.1f}%({triggered}/{passed})"
    )


def get_pool1_observe_stats() -> dict:
    _ensure_pool1_observe_day()
    total = int(_pool1_observe.get("screen_total", 0) or 0)
    passed = int(_pool1_observe.get("screen_pass", 0) or 0)
    triggered = int(_pool1_observe.get("stage2_triggered", 0) or 0)
    updated_at = int(_pool1_observe.get("updated_at", 0) or 0)
    pass_rate = (passed / total) if total > 0 else 0.0
    trigger_rate = (triggered / passed) if passed > 0 else 0.0
    return {
        "trade_date": _pool1_observe.get("trade_date") or _pool1_observe_trade_date(),
        "updated_at": updated_at,
        "screen_total": total,
        "screen_pass": passed,
        "stage2_triggered": triggered,
        "pass_rate": pass_rate,
        "trigger_rate": trigger_rate,
        "summary": _pool1_observe_summary(),
    }


def _mock_tick(ts_code: str) -> dict:
    base = 10.0
    return {
        "ts_code": ts_code,
        "name": "",
        "price": base,
        "open": base,
        "high": base,
        "low": base,
        "pre_close": base,
        "volume": 0,
        "amount": 0,
        "pct_chg": 0,
        "timestamp": int(time.time()),
        "bids": [(0, 0)] * 5,
        "asks": [(0, 0)] * 5,
        "is_mock": False,
    }


def _compute_bid_ask_ratio(tick: dict) -> Optional[float]:
    bids = tick.get("bids") or []
    asks = tick.get("asks") or []
    bid_vol = sum(int(v or 0) for _, v in bids[:5]) if bids else 0
    ask_vol = sum(int(v or 0) for _, v in asks[:5]) if asks else 0
    if ask_vol <= 0:
        return None
    return bid_vol / ask_vol


def _update_intraday_state(ts_code: str, tick: dict) -> dict:
    today = _dt.datetime.now().strftime("%Y-%m-%d")
    st = _intraday_state.get(ts_code)
    if st is None or st.get("date") != today:
        st = {
            "date": today,
            "cum_volume": 0,
            "cum_amount": 0.0,
            "last_volume": 0,
            "price_window": deque(),
            "gub5_trend_prev": None,
        }
        _intraday_state[ts_code] = st

    now_ts = int(tick.get("timestamp", time.time()) or time.time())
    price = float(tick.get("price", 0) or 0)
    cum_vol = int(tick.get("volume", 0) or 0)
    cum_amt = float(tick.get("amount", 0) or 0)
    delta_v = max(0, cum_vol - int(st.get("last_volume", 0) or 0))

    st["cum_volume"] = max(int(st.get("cum_volume", 0) or 0), cum_vol)
    st["cum_amount"] = max(float(st.get("cum_amount", 0.0) or 0.0), cum_amt)
    st["last_volume"] = cum_vol

    pw: deque = st["price_window"]
    pw.append((now_ts, price, delta_v))
    cutoff = now_ts - INTRADAY_WINDOW_SECONDS
    while pw and pw[0][0] < cutoff:
        pw.popleft()
    return st


def _gub5_trend_from_txns(txns: Optional[list]) -> str:
    if not txns:
        return "flat"
    n = min(len(txns), TXN_ANALYZE_COUNT)
    recent = txns[-n:]
    small_sell = 0
    for t in recent:
        d = int(t.get("direction", 0) or 0)  # mootdx: 1=sell
        v = int(t.get("volume", 0) or 0)
        if d == 1 and v < 50:
            small_sell += 1
    ratio = (small_sell / n) if n > 0 else 0.0
    if ratio > 0.4:
        return "up"
    if ratio > 0.2:
        return "flat"
    return "down"


def _gub5_transition(prev: Optional[str], cur: str) -> str:
    p = prev or cur
    return f"{p}->{cur}"


def _compute_signals_for_tick(tick: dict, daily: dict, ts_code: str = "") -> list[dict]:
    if not ts_code:
        ts_code = str(tick.get("ts_code", "") or "")
    if not ts_code:
        return []
    pools = _main_stock_pools.get(ts_code, set())
    if not pools:
        return []

    st = _update_intraday_state(ts_code, tick)
    price = float(tick.get("price", 0) or 0)
    pre_close = float(tick.get("pre_close", 0) or 0)
    if pre_close > 0:
        tick["pct_chg"] = (price - pre_close) / pre_close * 100

    vwap = None
    cv = int(st.get("cum_volume", 0) or 0)
    ca = float(st.get("cum_amount", 0) or 0.0)
    if cv > 0:
        vwap = ca / cv

    txns = _main_recent_txns.get(ts_code)
    gub5 = _gub5_trend_from_txns(txns)
    prev_gub5 = st.get("gub5_trend_prev")
    trans = _gub5_transition(prev_gub5, gub5)
    st["gub5_trend_prev"] = gub5

    bid_ask_ratio = _compute_bid_ask_ratio(tick)
    intraday_prices = [p for _, p, _ in list(st.get("price_window", []))]

    m = {
        "ts_code": ts_code,
        "name": tick.get("name", ts_code),
        "price": price,
        "pct_chg": float(tick.get("pct_chg", 0) or 0),
        "timestamp": int(tick.get("timestamp", time.time()) or time.time()),
        "pre_close": pre_close,
        "bids": tick.get("bids") or [],
        "asks": tick.get("asks") or [],
        "volume": int(tick.get("volume", 0) or 0),
        "amount": float(tick.get("amount", 0) or 0),
        "up_limit": (pre_close * 1.1) if pre_close > 0 else None,
        "down_limit": (pre_close * 0.9) if pre_close > 0 else None,
        "prev_price": _main_prev_price.get(ts_code),
        "bid_ask_ratio": bid_ask_ratio,
        "vwap": vwap,
        "intraday_prices": intraday_prices,
        "gub5_trend": gub5,
        "gub5_transition": trans,
        "lure_long": False,
        "wash_trade": False,
        "spread_abnormal": False,
        "v_power_divergence": False,
        "ask_wall_building": False,
        "real_buy_fading": False,
        "wash_trade_severe": False,
        "liquidity_drain": False,
    }
    m.update({
        "boll_upper": float(daily.get("boll_upper", 0) or 0),
        "boll_mid": float(daily.get("boll_mid", 0) or 0),
        "boll_lower": float(daily.get("boll_lower", 0) or 0),
        "rsi6": daily.get("rsi6"),
        "ma5": daily.get("ma5"),
        "ma10": daily.get("ma10"),
        "volume_ratio": daily.get("volume_ratio"),
        "vol_20": daily.get("vol_20"),
        "atr_14": daily.get("atr_14"),
        "chip_concentration_pct": daily.get("chip_concentration_pct"),
        "winner_rate": daily.get("winner_rate"),
    })

    fired: list[dict] = []
    if 1 in pools:
        p1 = sig.evaluate_pool(1, [m])
        s1 = p1[0].get("signals", []) if p1 else []
        _record_pool1_observe(stage1_pass=True, stage2_triggered=bool(s1))
        fired.extend(s1)
    if 2 in pools:
        p2 = sig.evaluate_pool(2, [m])
        s2 = p2[0].get("signals", []) if p2 else []
        fired.extend(s2)

    _main_prev_price[ts_code] = price
    return fired


class GmTickProvider(TickProvider):
    """Compatibility provider for historical `gm` mode."""

    def __init__(self):
        self._started = False
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def name(self) -> str:
        return "gm"

    @property
    def display_name(self) -> str:
        return "gm (compat)"

    def get_tick(self, ts_code: str) -> dict:
        return _main_cache.get(ts_code) or _mock_tick(ts_code)

    def subscribe_symbols(self, ts_codes: list[str]):
        with _tick_lock:
            for c in ts_codes:
                _subscribed_codes.add(c)

    def unsubscribe_symbols(self, ts_codes: list[str]):
        with _tick_lock:
            for c in ts_codes:
                _subscribed_codes.discard(c)

    def update_daily_factors(self, ts_code: str, factors: dict):
        _main_daily_cache[ts_code] = dict(factors)

    def bulk_update_daily_factors(self, factors_map: dict[str, dict]):
        _main_daily_cache.update(factors_map)
        logger.info(f"Layer2 daily factor cache updated: {len(factors_map)}")

    def bulk_update_chip_features(self, features_map: dict[str, dict]):
        _main_chip_cache.update(features_map)

    def update_stock_pools(self, ts_code: str, pool_ids: set):
        _main_stock_pools[ts_code] = set(pool_ids)

    def bulk_update_stock_pools(self, mapping: dict[str, set]):
        _main_stock_pools.clear()
        for k, v in mapping.items():
            _main_stock_pools[k] = set(v)
        logger.info(f"Layer2 stock pool mapping updated: {len(mapping)}")

    def update_recent_txns(self, ts_code: str, txns: list):
        if txns:
            _main_recent_txns[ts_code] = list(txns)[-TXN_ANALYZE_COUNT:]

    def bulk_update_recent_txns(self, mapping: dict):
        for k, v in mapping.items():
            if v:
                _main_recent_txns[k] = list(v)[-TXN_ANALYZE_COUNT:]

    def get_cached_signals(self, ts_code: str) -> Optional[dict]:
        return _main_signal_cache.get(ts_code)

    def get_cached_transactions(self, ts_code: str) -> Optional[list]:
        return _main_recent_txns.get(ts_code)

    def get_cached_tick(self, ts_code: str) -> Optional[dict]:
        return _main_cache.get(ts_code)

    def get_pool1_observe_stats(self) -> Optional[dict]:
        return get_pool1_observe_stats()

    def start(self):
        if self._started:
            return
        self._started = True
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name="gm-compat-poller")
        self._thread.start()
        logger.info("gm compat poller started")

    def stop(self):
        self._stop_evt.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
        self._started = False

    def _poll_loop(self):
        while not self._stop_evt.is_set():
            with _tick_lock:
                codes = list(_subscribed_codes)
            if not codes:
                self._stop_evt.wait(0.5)
                continue
            ticks_map = {}
            try:
                ticks_map = mootdx_client.get_ticks(codes)
            except Exception as e:
                logger.debug(f"gm compat get_ticks failed: {e}")
            now = int(time.time())
            for code in codes:
                try:
                    t = ticks_map.get(code) or mootdx_client.get_tick(code) or _mock_tick(code)
                    _main_cache[code] = t
                    daily = _main_daily_cache.get(code, {})
                    signals = _compute_signals_for_tick(t, daily, ts_code=code) if daily else []
                    _main_signal_cache[code] = {
                        "ts_code": code,
                        "name": t.get("name", code),
                        "price": float(t.get("price", 0) or 0),
                        "pct_chg": float(t.get("pct_chg", 0) or 0),
                        "signals": signals,
                        "evaluated_at": now,
                    }
                except Exception as e:
                    logger.debug(f"gm compat poll failed {code}: {e}")
            self._stop_evt.wait(1.0)

