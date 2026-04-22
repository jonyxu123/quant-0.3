"""Tests for Pool1 60m resonance fast-path cache in gm_tick."""

from __future__ import annotations

from backend.realtime import gm_tick
from backend.realtime import signals


def _bars_1m(n: int = 240) -> list[dict]:
    bars: list[dict] = []
    base = 10.0
    for i in range(n):
        base += 0.002
        bars.append({
            "open": base - 0.01,
            "high": base + 0.02,
            "low": base - 0.02,
            "close": base,
            "volume": 1000 + (i % 20) * 10,
        })
    return bars


def test_pool1_resonance_fast_path_uses_cache(monkeypatch) -> None:
    old_enabled = gm_tick._P1_RESONANCE_ENABLED
    old_refresh = gm_tick._P1_RESONANCE_REFRESH_SEC
    gm_tick._pool1_resonance_cache.clear()

    calls = {"bars": 0}

    def fake_bars(ts_code: str, n: int = 240):
        calls["bars"] += 1
        return _bars_1m(n)

    try:
        monkeypatch.setattr(gm_tick.mootdx_client, "get_minute_bars", fake_bars)
        monkeypatch.setattr(signals, "compute_pool1_resonance_60m", lambda bars, fallback=False: (True, {"reason": "ok"}))
        gm_tick._P1_RESONANCE_ENABLED = True
        gm_tick._P1_RESONANCE_REFRESH_SEC = 120

        v1, info1 = gm_tick._pool1_resonance_60m("000001.SZ", {}, 10.1, 10.0, now_ts=1000)
        v2, info2 = gm_tick._pool1_resonance_60m("000001.SZ", {}, 10.2, 10.1, now_ts=1050)

        assert v1 is True
        assert info1.get("source") == "1m_aggregate"
        assert v2 is True
        assert info2.get("source") == "cache"
        assert calls["bars"] == 1
    finally:
        gm_tick._P1_RESONANCE_ENABLED = old_enabled
        gm_tick._P1_RESONANCE_REFRESH_SEC = old_refresh
        gm_tick._pool1_resonance_cache.clear()


def test_pool1_resonance_fast_path_fallback_proxy(monkeypatch) -> None:
    old_enabled = gm_tick._P1_RESONANCE_ENABLED
    old_refresh = gm_tick._P1_RESONANCE_REFRESH_SEC
    gm_tick._pool1_resonance_cache.clear()

    try:
        monkeypatch.setattr(gm_tick.mootdx_client, "get_minute_bars", lambda ts_code, n=240: (_ for _ in ()).throw(RuntimeError("bars failed")))
        monkeypatch.setattr(gm_tick, "_pool1_resonance_60m_proxy", lambda daily, price, prev_price: False)
        gm_tick._P1_RESONANCE_ENABLED = True
        gm_tick._P1_RESONANCE_REFRESH_SEC = 120

        v, info = gm_tick._pool1_resonance_60m("000001.SZ", {"ma5": 0, "ma10": 0, "boll_mid": 0}, 10.1, 10.0, now_ts=2000)
        assert v is False
        assert info.get("source") == "proxy_fallback"
    finally:
        gm_tick._P1_RESONANCE_ENABLED = old_enabled
        gm_tick._P1_RESONANCE_REFRESH_SEC = old_refresh
        gm_tick._pool1_resonance_cache.clear()
