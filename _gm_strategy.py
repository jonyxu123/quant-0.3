# coding=utf-8
"""
GM strategy entry script used by gm.run().

Responsibilities:
1) Read subscribed symbols from .gm_tick_subs.txt at init.
2) Subscribe tick stream.
3) Keep on_tick lightweight: parse -> normalize -> enqueue to main process manager queue.
"""
from __future__ import print_function, absolute_import

import os
import time
from multiprocessing.managers import BaseManager

from gm.api import *  # type: ignore


_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = _current_dir
_SUBS_FILE = os.path.join(_project_root, ".gm_tick_subs.txt")

_subscribed_codes = set()


class _TickSyncManager(BaseManager):
    pass


_TickSyncManager.register("get_queue")
_TickSyncManager.register("get_cache")

_remote_queue = None
_tick_count = 0
_enqueue_skip_same = 0
_pre_close_cache = {}  # symbol -> {'trade_date': 'YYYY-MM-DD', 'pre_close': float}
_pre_close_retry_at = {}  # symbol -> epoch
_last_enqueued_sig = {}  # ts_code -> (signature_tuple, enqueue_ms)
_PRE_CLOSE_RETRY_SEC = float(os.environ.get("GM_PRE_CLOSE_RETRY_SEC", "60") or 60)
_TICK_ENQUEUE_DEDUP_MS = float(os.environ.get("GM_TICK_ENQUEUE_DEDUP_MS", "20") or 20)
_GM_TICK_DEBUG = os.environ.get("GM_TICK_DEBUG", "0").lower() in ("1", "true", "yes", "on")


def _tf(obj, key, default=0):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _connect_manager():
    global _remote_queue
    if _remote_queue is not None:
        return _remote_queue

    host = os.environ.get("GM_TICK_MGR_HOST")
    port = os.environ.get("GM_TICK_MGR_PORT")
    authkey_hex = os.environ.get("GM_TICK_MGR_AUTHKEY")
    if not (host and port and authkey_hex):
        print("[gm strategy] TickManager env not found, cannot connect main process")
        return None
    try:
        mgr = _TickSyncManager(address=(host, int(port)), authkey=bytes.fromhex(authkey_hex))
        mgr.connect()
        _remote_queue = mgr.get_queue()
        print(f"[gm strategy] connected TickManager {host}:{port}")
        return _remote_queue
    except Exception as e:
        print(f"[gm strategy] connect TickManager failed: {e}")
        return None


def _load_state():
    global _subscribed_codes
    try:
        if os.path.exists(_SUBS_FILE):
            with open(_SUBS_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    _subscribed_codes = set(x.strip() for x in content.split(",") if x.strip())
    except Exception:
        pass


def _ts_code_to_gm_symbol(ts_code):
    code, suffix = ts_code.split(".")
    s = suffix.upper()
    if s == "SH":
        return "SHSE." + code
    if s == "SZ":
        return "SZSE." + code
    if s == "BJ":
        return "BJSE." + code
    return s + "." + code


def _gm_symbol_to_ts_code(symbol):
    exchange, code = symbol.split(".")
    if exchange == "SHSE":
        return code + ".SH"
    if exchange == "SZSE":
        return code + ".SZ"
    if exchange == "BJSE":
        return code + ".BJ"
    return code + "." + exchange


def init(context):
    _load_state()
    _connect_manager()

    codes = list(_subscribed_codes)
    if not codes:
        print("gm init: subscription list is empty")
        return

    gm_symbols = [_ts_code_to_gm_symbol(c) for c in codes]
    symbol_str = ",".join(gm_symbols)
    print(f"gm init: subscribe {len(gm_symbols)} symbols tick: {symbol_str}")
    subscribe(symbols=symbol_str, frequency="tick", count=1, unsubscribe_previous=True)


def _resolve_pre_close(context, symbol):
    now = int(time.time())
    today = time.strftime("%Y-%m-%d", time.localtime(now))
    cached = _pre_close_cache.get(symbol)
    if cached and cached.get("trade_date") == today:
        v = float(cached.get("pre_close", 0) or 0)
        if v > 0:
            return v

    pre_close = 0.0
    try:
        from gm.api import current

        snap = current(symbols=symbol, fields="pre_close")
        if snap and len(snap) > 0:
            pre_close = float(_tf(snap[0], "pre_close", 0) or 0)
    except Exception:
        pass

    if pre_close <= 0:
        try:
            data = context.data(symbol=symbol, frequency="1d", count=2, fields="close,pre_close")
            if data is not None and len(data) > 0:
                if len(data) >= 2:
                    pre_close = float(data.iloc[-2].get("close", 0) or 0)
                if pre_close <= 0:
                    pre_close = float(data.iloc[-1].get("pre_close", 0) or data.iloc[-1].get("close", 0) or 0)
        except Exception:
            pass

    if pre_close > 0:
        _pre_close_cache[symbol] = {"trade_date": today, "pre_close": pre_close}
    return pre_close


def _get_cached_pre_close(symbol):
    now = int(time.time())
    today = time.strftime("%Y-%m-%d", time.localtime(now))
    cached = _pre_close_cache.get(symbol)
    if cached and cached.get("trade_date") == today:
        v = float(cached.get("pre_close", 0) or 0)
        if v > 0:
            return v
    return 0.0


def _derive_price_from_quotes(bids, asks):
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
        return (bid1 + ask1) / 2.0, "bidask_mid"
    if bid1 > 0:
        return bid1, "bid1"
    if ask1 > 0:
        return ask1, "ask1"
    return 0.0, "none"


def on_tick(context, tick):
    global _tick_count, _enqueue_skip_same
    _tick_count += 1

    symbol = _tf(tick, "symbol", "") or ""
    if _GM_TICK_DEBUG and (_tick_count <= 3 or _tick_count % 500 == 0):
        print(f"[on_tick #{_tick_count}] symbol={symbol} price={_tf(tick, 'price', 0)}")

    try:
        if not symbol:
            return
        ts_code = _gm_symbol_to_ts_code(symbol)

        bids = []
        asks = []
        quotes = _tf(tick, "quotes", []) or []
        if quotes and len(quotes) > 0:
            for q in quotes[:5]:
                bids.append((float(_tf(q, "bid_p", 0) or 0), int(_tf(q, "bid_v", 0) or 0)))
                asks.append((float(_tf(q, "ask_p", 0) or 0), int(_tf(q, "ask_v", 0) or 0)))
        if not bids:
            bids = [(0, 0)] * 5
            asks = [(0, 0)] * 5

        price = float(_tf(tick, "price", 0) or 0)
        cum_volume = int(_tf(tick, "cum_volume", 0) or 0)
        cum_amount = float(_tf(tick, "cum_amount", 0) or 0)
        open_price = float(_tf(tick, "open", 0) or 0)
        high = float(_tf(tick, "high", 0) or 0)
        low = float(_tf(tick, "low", 0) or 0)

        pre_close = float(_tf(tick, "pre_close", 0) or 0)
        pct_chg = 0.0
        if pre_close > 0:
            now = int(time.time())
            _pre_close_cache[symbol] = {
                "trade_date": time.strftime("%Y-%m-%d", time.localtime(now)),
                "pre_close": pre_close,
            }
        else:
            pre_close = _get_cached_pre_close(symbol)
            if pre_close <= 0:
                now_retry = time.time()
                last_retry = float(_pre_close_retry_at.get(symbol, 0.0) or 0.0)
                if now_retry - last_retry >= max(1.0, _PRE_CLOSE_RETRY_SEC):
                    _pre_close_retry_at[symbol] = now_retry
                    pre_close = _resolve_pre_close(context, symbol)

        price_source = "trade"
        if price <= 0:
            q_price, q_src = _derive_price_from_quotes(bids, asks)
            if q_price > 0:
                price = q_price
                price_source = "auction_" + q_src
            elif open_price > 0:
                price = open_price
                price_source = "open_fallback"
            elif pre_close > 0:
                price = pre_close
                price_source = "pre_close_fallback"

        if price <= 0:
            if _GM_TICK_DEBUG and (_tick_count <= 3 or _tick_count % 500 == 0):
                print(f"[on_tick skip] symbol={symbol} invalid price={price}")
            return

        if pre_close > 0:
            pct_chg = (price - pre_close) / pre_close * 100

        now_ts = time.time()
        now_ms = int(now_ts * 1000)
        bid1 = float((bids[0] or (0, 0))[0] or 0) if bids else 0.0
        ask1 = float((asks[0] or (0, 0))[0] or 0) if asks else 0.0
        sig = (
            round(float(price), 3),
            int(cum_volume),
            round(float(cum_amount), 2),
            round(float(bid1), 3),
            round(float(ask1), 3),
            round(float(pct_chg), 4),
        )
        if _TICK_ENQUEUE_DEDUP_MS > 0:
            last = _last_enqueued_sig.get(ts_code)
            if isinstance(last, tuple) and len(last) == 2:
                last_sig, last_ms = last
                if sig == last_sig and (now_ms - int(last_ms)) < int(_TICK_ENQUEUE_DEDUP_MS):
                    _enqueue_skip_same += 1
                    if _GM_TICK_DEBUG and (_enqueue_skip_same <= 3 or _enqueue_skip_same % 500 == 0):
                        print(
                            f"[on_tick dedup skip #{_enqueue_skip_same}] "
                            f"symbol={symbol} ts_code={ts_code} price={price}"
                        )
                    return
            _last_enqueued_sig[ts_code] = (sig, now_ms)

        tick_dict = {
            "ts_code": ts_code,
            "name": ts_code,
            "price": price,
            "open": open_price,
            "high": high,
            "low": low,
            "pre_close": pre_close,
            "volume": cum_volume,
            "amount": cum_amount,
            "pct_chg": pct_chg,
            "timestamp": int(now_ts),
            "bids": bids,
            "asks": asks,
            "price_source": price_source,
        }

        q = _connect_manager()
        if q is not None:
            try:
                q.put(tick_dict, block=False)
            except Exception as qe:
                if _GM_TICK_DEBUG and _tick_count <= 5:
                    print(f"[gm strategy] queue.put failed (#{_tick_count}): {qe}")

    except Exception as e:
        if _GM_TICK_DEBUG and _tick_count <= 5:
            import traceback

            print(f"gm on_tick failed: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    import sys

    sys.path.insert(0, _project_root)
    try:
        from config import GM_TOKEN, GM_MODE
    except ImportError:
        print("cannot import config, ensure _gm_strategy.py is in project root")
        sys.exit(1)

    run(
        strategy_id="quant_tick_subscriber",
        filename="_gm_strategy.py",
        mode=GM_MODE,
        token=GM_TOKEN,
        backtest_start_time="2026-04-17 08:00:00",
        backtest_end_time="2026-04-17 16:00:00",
        backtest_adjust=ADJUST_PREV,
        backtest_initial_cash=10000000,
        backtest_commission_ratio=0.0001,
        backtest_slippage_ratio=0.0001,
    )
