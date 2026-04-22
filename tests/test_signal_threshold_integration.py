"""Integration tests for thresholds injected into signal detectors."""

from __future__ import annotations

from backend.realtime import signals


def _build_uptrend_1m_bars(n: int = 240) -> list[dict]:
    bars: list[dict] = []
    base = 10.0
    for i in range(n):
        base += 0.003
        bars.append(
            {
                "open": base - 0.01,
                "high": base + 0.02,
                "low": base - 0.03,
                "close": base,
                "volume": 1000 + (i % 30) * 20,
            }
        )
    return bars


def test_left_side_buy_blocked_by_threshold_filter() -> None:
    s = signals.detect_left_side_buy(
        price=9.5,
        boll_upper=10.5,
        boll_mid=10.0,
        boll_lower=9.6,
        rsi6=20,
        pct_chg=-3.2,
        bid_ask_ratio=1.5,
        thresholds={
            "blocked": True,
            "threshold_version": "dyn-test",
            "market_phase": "open",
            "regime": "high_vol",
        },
    )
    assert s["has_signal"] is False
    assert s["details"]["threshold"]["blocked"] is True


def test_right_side_breakout_uses_threshold_eps_override() -> None:
    s = signals.detect_right_side_breakout(
        price=10.015,
        boll_upper=10.20,
        boll_mid=10.0,
        boll_lower=9.8,
        volume_ratio=1.6,
        ma5=10.1,
        ma10=10.0,
        rsi6=60,
        prev_price=9.99,
        bid_ask_ratio=1.5,
        thresholds={
            "eps_mid": 1.001,
            "eps_upper": 1.001,
            "breakout_max_offset_pct": 3.0,
        },
    )
    assert s["has_signal"] is True


def test_positive_t_observer_only_flag_from_threshold_filter() -> None:
    s = signals.detect_positive_t(
        tick_price=9.4,
        boll_lower=9.6,
        vwap=10.0,
        gub5_transition="up->down",
        bid_ask_ratio=1.3,
        lure_short=False,
        wash_trade=False,
        spread_abnormal=False,
        thresholds={
            "observer_only": True,
            "threshold_version": "dyn-test",
            "market_phase": "afternoon",
            "regime": "mid_vol",
        },
    )
    assert s["has_signal"] is True
    assert s["details"]["observe_only"] is True
    assert s["details"]["threshold"]["observer_only"] is True


def test_reverse_t_respects_threshold_zscore_override() -> None:
    s = signals.detect_reverse_t(
        tick_price=10.0,
        boll_upper=20.0,
        intraday_prices=None,
        v_power_divergence=True,
        robust_zscore=2.5,
        thresholds={"z_th": 2.8, "eps_over": 0.002},
    )
    assert s["has_signal"] is False

    s2 = signals.detect_reverse_t(
        tick_price=10.0,
        boll_upper=20.0,
        intraday_prices=None,
        v_power_divergence=True,
        robust_zscore=2.5,
        thresholds={"z_th": 2.0, "eps_over": 0.002},
    )
    assert s2["has_signal"] is True


def test_evaluate_pool_pool1_uses_dynamic_limit_profile_in_threshold_engine() -> None:
    baseline = signals.detect_left_side_buy(
        price=9.05,
        boll_upper=10.8,
        boll_mid=10.0,
        boll_lower=9.2,
        rsi6=22,
        pct_chg=-4.0,
        bid_ask_ratio=1.4,
        lure_long=False,
        wash_trade=False,
        up_limit=11.0,
        down_limit=9.0,
        resonance_60m=True,
        thresholds=None,
    )
    assert baseline["has_signal"] is True

    evaluated = signals.evaluate_pool(
        1,
        [
            {
                "ts_code": "000001.SZ",
                "name": "PingAn",
                "price": 9.05,
                "pct_chg": -4.0,
                "boll_upper": 10.8,
                "boll_mid": 10.0,
                "boll_lower": 9.2,
                "rsi6": 22,
                "bid_ask_ratio": 1.4,
                "lure_long": False,
                "wash_trade": False,
                "up_limit": 11.0,
                "down_limit": 9.0,
                "resonance_60m": True,
            }
        ],
    )
    assert evaluated[0]["signals"] == []


def test_evaluate_pool_pool1_still_blocks_when_too_close_to_limit() -> None:
    evaluated = signals.evaluate_pool(
        1,
        [
            {
                "ts_code": "000001.SZ",
                "name": "PingAn",
                "price": 9.01,
                "pct_chg": -4.3,
                "boll_upper": 10.8,
                "boll_mid": 10.0,
                "boll_lower": 9.2,
                "rsi6": 22,
                "bid_ask_ratio": 1.4,
                "lure_long": False,
                "wash_trade": False,
                "up_limit": 11.0,
                "down_limit": 9.0,
                "resonance_60m": True,
            }
        ],
    )
    assert evaluated[0]["signals"] == []


def test_evaluate_pool_pool1_right_side_is_observer_only_near_limit() -> None:
    evaluated = signals.evaluate_pool(
        1,
        [
            {
                "ts_code": "000001.SZ",
                "name": "PingAn",
                "price": 10.5,
                "pct_chg": 2.3,
                "boll_upper": 10.4,
                "boll_mid": 10.0,
                "boll_lower": 9.3,
                "volume_ratio": 1.6,
                "ma5": 10.2,
                "ma10": 10.0,
                "rsi6": 62,
                "prev_price": 10.3,
                "bid_ask_ratio": 1.3,
                "lure_long": False,
                "wash_trade": False,
                "up_limit": 10.6,
                "down_limit": 8.5,
                "resonance_60m": True,
            }
        ],
    )
    fired = evaluated[0]["signals"]
    right = next(s for s in fired if s["type"] == "right_side_breakout")
    assert right["details"].get("observe_only") is True
    assert right["details"].get("threshold", {}).get("observer_only") is True
    assert "仅观察" in right["message"]


def test_evaluate_pool_pool1_filters_weak_signal_below_observe_score() -> None:
    evaluated = signals.evaluate_pool(
        1,
        [
            {
                "ts_code": "000001.SZ",
                "name": "PingAn",
                "price": 9.24,
                "pct_chg": -0.5,
                "boll_upper": 10.8,
                "boll_mid": 10.0,
                "boll_lower": 9.2,
                "rsi6": 60,
                "bid_ask_ratio": 0.8,
                "lure_long": False,
                "wash_trade": False,
                "up_limit": 11.0,
                "down_limit": 8.0,
                "resonance_60m": True,
            }
        ],
    )
    assert evaluated[0]["signals"] == []


def test_evaluate_pool_pool1_keeps_precomputed_resonance_info() -> None:
    evaluated = signals.evaluate_pool(
        1,
        [
            {
                "ts_code": "000001.SZ",
                "name": "PingAn",
                "price": 9.05,
                "pct_chg": -4.0,
                "boll_upper": 10.8,
                "boll_mid": 10.0,
                "boll_lower": 9.2,
                "rsi6": 22,
                "bid_ask_ratio": 1.4,
                "lure_long": False,
                "wash_trade": False,
                "up_limit": 11.0,
                "down_limit": 8.0,
                "resonance_60m": True,
                "resonance_60m_info": {"source": "cache", "cache_age_sec": 7.5},
            }
        ],
    )
    fired = evaluated[0]["signals"]
    left = next(s for s in fired if s["type"] == "left_side_buy")
    stage = left["details"].get("pool1_stage", {})
    assert stage.get("resonance_60m") is True
    assert stage.get("resonance_60m_info", {}).get("source") == "cache"


def test_evaluate_pool_pool2_injects_threshold_metadata() -> None:
    signals._hysteresis_state.clear()
    evaluated = signals.evaluate_pool(
        2,
        [
            {
                "ts_code": "000001.SZ",
                "name": "PingAn",
                "price": 9.4,
                "pct_chg": -2.1,
                "pre_close": 10.0,
                "boll_lower": 9.7,
                "boll_upper": 10.8,
                "vwap": 10.0,
                "intraday_prices": [10.0, 9.95, 9.9, 9.8, 9.7, 9.6, 9.5, 9.45, 9.42, 9.4],
                "gub5_trend": "up",
                "gub5_transition": "up->down",
                "bid_ask_ratio": 1.3,
                "lure_short": False,
                "wash_trade": False,
                "spread_abnormal": False,
                "v_power_divergence": False,
                "ask_wall_building": False,
                "real_buy_fading": False,
                "wash_trade_severe": False,
                "liquidity_drain": False,
                "up_limit": 11.0,
                "down_limit": 9.0,
            }
        ],
    )

    fired = evaluated[0]["signals"]
    positive = next(s for s in fired if s["type"] == "positive_t")
    threshold_meta = positive["details"].get("threshold", {})
    assert threshold_meta.get("threshold_version") == "dyn-v1"
    assert threshold_meta.get("market_phase") is not None


def test_compute_pool1_resonance_60m_from_1m_bars() -> None:
    ok, info = signals.compute_pool1_resonance_60m(_build_uptrend_1m_bars(), fallback=False)
    assert ok is True
    assert info["bars_60m"] >= 3
    assert info["trend_up"] is True
    assert info["momentum_up"] is True


def test_evaluate_pool_pool1_derives_resonance_from_minute_bars() -> None:
    old_resonance_enabled = signals._P1_RESONANCE_ENABLED
    try:
        signals._P1_RESONANCE_ENABLED = True
        evaluated = signals.evaluate_pool(
            1,
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "PingAn",
                    "price": 10.18,
                    "pct_chg": 1.2,
                    "boll_upper": 10.6,
                    "boll_mid": 10.0,
                    "boll_lower": 9.4,
                    "volume_ratio": 1.5,
                    "ma5": 10.1,
                    "ma10": 9.9,
                    "rsi6": 60,
                    "prev_price": 9.95,
                    "bid_ask_ratio": 0.8,
                    "lure_long": False,
                    "wash_trade": False,
                    "resonance_60m": None,
                    "minute_bars_1m": _build_uptrend_1m_bars(),
                }
            ],
        )
    finally:
        signals._P1_RESONANCE_ENABLED = old_resonance_enabled

    fired = evaluated[0]["signals"]
    breakout = next(s for s in fired if s["type"] == "right_side_breakout")
    stage = breakout["details"].get("pool1_stage", {})
    assert stage.get("resonance_60m") is True
    assert stage.get("resonance_60m_info", {}).get("source") == "1m_aggregate"
