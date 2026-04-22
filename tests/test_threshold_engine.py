"""Unit tests for realtime dynamic threshold engine."""

from __future__ import annotations

import datetime as dt

from backend.realtime.threshold_engine import (
    apply_limit_magnet_filter,
    build_signal_context,
    get_thresholds,
    resolve_market_phase,
    resolve_regime,
)


CST = dt.timezone(dt.timedelta(hours=8))


def _ts(hour: int, minute: int) -> int:
    return int(dt.datetime(2026, 4, 20, hour, minute, tzinfo=CST).timestamp())


def test_resolve_market_phase_boundaries() -> None:
    assert resolve_market_phase(_ts(9, 30)) == "open"
    assert resolve_market_phase(_ts(9, 59)) == "open"
    assert resolve_market_phase(_ts(10, 0)) == "morning"
    assert resolve_market_phase(_ts(11, 29)) == "morning"
    assert resolve_market_phase(_ts(11, 30)) == "off_session"
    assert resolve_market_phase(_ts(13, 0)) == "afternoon"
    assert resolve_market_phase(_ts(14, 29)) == "afternoon"
    assert resolve_market_phase(_ts(14, 30)) == "close"
    assert resolve_market_phase(_ts(15, 0)) == "close"
    assert resolve_market_phase(_ts(15, 1)) == "off_session"


def test_resolve_regime_with_priority_limit_magnet() -> None:
    assert resolve_regime({"limit_magnet": True, "vol_20": 0.99}) == "limit_magnet"
    assert resolve_regime({"limit_magnet": False, "vol_20": 0.35}) == "high_vol"
    assert resolve_regime({"limit_magnet": False, "vol_20": 0.20}) == "mid_vol"
    assert resolve_regime({"limit_magnet": False, "vol_20": 0.05}) == "low_vol"
    assert resolve_regime({"limit_magnet": False, "vol_20": None}) == "mid_vol"
    assert resolve_regime({"limit_magnet": False, "vol_20": "bad"}) == "mid_vol"


def test_build_signal_context_computes_core_fields() -> None:
    tick = {
        "timestamp": _ts(10, 15),
        "price": 9.86,
        "pre_close": 10.00,
        "pct_chg": 0,
        "volume": 123456,
        "amount": 987654.0,
        "bids": [(9.85, 1000), (9.84, 500)],
        "asks": [(9.87, 800), (9.88, 400)],
    }
    daily = {
        "boll_upper": 10.4,
        "boll_mid": 10.0,
        "boll_lower": 9.6,
        "up_limit": 11.0,
        "down_limit": 9.0,
        "vol_20": 0.31,
    }
    intraday = {"vwap": 9.95, "robust_zscore": 2.1, "gub5_ratio": 0.42, "gub5_transition": "up->flat"}

    ctx = build_signal_context("000001.SZ", 2, tick, daily, intraday=intraday, market={"atr_14": 0.22})

    assert ctx["ts_code"] == "000001.SZ"
    assert ctx["pool_id"] == 2
    assert ctx["market_phase"] == "morning"
    assert round(ctx["pct_chg"], 3) == -1.4
    assert round(ctx["bid_ask_ratio"], 3) == 1.25
    assert round(ctx["spread_bps"], 3) == round((9.87 - 9.85) / 9.86 * 10000, 3)
    assert ctx["vol_20"] == 0.31
    assert ctx["regime"] in {"high_vol", "limit_magnet"}
    assert ctx["min_dist_limit_pct"] is not None


def test_apply_limit_magnet_filter_actions() -> None:
    profile = {
        "limit_magnet": {
            "enabled": True,
            "distance_pct": 1.5,
            "actions": {
                "left_side_buy": "blocked",
                "right_side_breakout": "observer_only",
            },
        }
    }
    near_ctx = {"min_dist_limit_pct": 1.2}
    far_ctx = {"min_dist_limit_pct": 2.5}

    blocked = apply_limit_magnet_filter(near_ctx, {"signal_type": "left_side_buy"}, profile)
    observe = apply_limit_magnet_filter(near_ctx, {"signal_type": "right_side_breakout"}, profile)
    none_action = apply_limit_magnet_filter(far_ctx, {"signal_type": "left_side_buy"}, profile)

    assert blocked["limit_magnet"] is True
    assert blocked["blocked"] is True
    assert blocked["action"] == "blocked"

    assert observe["observer_only"] is True
    assert observe["action"] == "observer_only"

    assert none_action["limit_magnet"] is False
    assert none_action["action"] == "none"


def test_apply_limit_magnet_filter_prefers_ctx_distance_override() -> None:
    profile = {
        "limit_magnet": {
            "enabled": True,
            "distance_pct": 1.5,
            "actions": {"left_side_buy": "blocked"},
        }
    }
    # 距离 0.56%，若用全局1.5%会触发 blocked；若用ctx覆盖0.3%则不触发。
    ctx = {"min_dist_limit_pct": 0.56, "limit_magnet_distance_pct": 0.3}
    out = apply_limit_magnet_filter(ctx, {"signal_type": "left_side_buy"}, profile)
    assert out["limit_magnet"] is False
    assert out["blocked"] is False
    assert out["action"] == "none"
    assert out["threshold_pct"] == 0.3


def test_get_thresholds_merge_order_and_metadata() -> None:
    profile = {
        "version": "dyn-test-v1",
        "base": {
            "positive_t": {
                "bias_vwap_th": -1.5,
                "z_th": 2.2,
                "nested": {"x": 1, "y": 2},
            }
        },
        "phase_overrides": {
            "open": {
                "positive_t": {
                    "bias_vwap_th": -1.8,
                    "nested": {"y": 9},
                }
            }
        },
        "regime_overrides": {
            "high_vol": {
                "positive_t": {
                    "bias_vwap_th": -2.0,
                    "nested": {"z": 3},
                }
            }
        },
        "limit_magnet": {
            "enabled": True,
            "distance_pct": 1.5,
            "actions": {"positive_t": "blocked"},
        },
    }
    ctx = {
        "market_phase": "open",
        "regime": "high_vol",
        "min_dist_limit_pct": 1.0,
    }

    th = get_thresholds("positive_t", ctx, profile=profile)

    assert th["bias_vwap_th"] == -2.0
    assert th["z_th"] == 2.2
    assert th["nested"] == {"x": 1, "y": 9, "z": 3}
    assert th["threshold_version"] == "dyn-test-v1"
    assert th["market_phase"] == "open"
    assert th["regime"] == "high_vol"
    assert th["blocked"] is True
    assert th["limit_filter"]["action"] == "blocked"


def test_get_thresholds_defaults_to_mid_vol_when_vol_missing() -> None:
    profile = {
        "version": "dyn-test-v2",
        "base": {"positive_t": {"bias_vwap_th": -1.5}},
        "regime_overrides": {
            "low_vol": {"positive_t": {"bias_vwap_th": -1.2}},
            "mid_vol": {"positive_t": {"bias_vwap_th": -1.6}},
        },
    }
    ctx = {
        "market_phase": "morning",
        "limit_magnet": False,
        "vol_20": None,
    }

    th = get_thresholds("positive_t", ctx, profile=profile)
    assert th["regime"] == "mid_vol"
    assert th["bias_vwap_th"] == -1.6
