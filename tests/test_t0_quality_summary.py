"""Tests for T+0 quality summary aggregation helpers."""

from __future__ import annotations

import pandas as pd

from backend.api.routers.realtime_monitor import _build_t0_quality_payload, _calc_churn_stats


def test_calc_churn_stats_basic() -> None:
    df = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "signal_type": "positive_t", "triggered_at": "2026-04-21 09:35:00"},
            {"ts_code": "000001.SZ", "signal_type": "reverse_t", "triggered_at": "2026-04-21 09:40:00"},
            {"ts_code": "000001.SZ", "signal_type": "reverse_t", "triggered_at": "2026-04-21 09:45:00"},
            {"ts_code": "000002.SZ", "signal_type": "positive_t", "triggered_at": "2026-04-21 09:36:00"},
            {"ts_code": "000002.SZ", "signal_type": "reverse_t", "triggered_at": "2026-04-21 09:42:00"},
        ]
    )

    churn = _calc_churn_stats(df, hours=2)
    assert churn["flip_count"] == 2
    assert churn["sample_count"] == 5
    assert churn["symbols"] == 2
    assert churn["churn_ratio"] == 0.4
    assert churn["flips_per_hour"] == 1.0


def test_build_t0_quality_payload_multihorizon() -> None:
    df = pd.DataFrame(
        [
            # positive_t
            {"signal_type": "positive_t", "direction": "buy", "eval_horizon_sec": 60, "ret_bps": 10, "mfe_bps": 10, "mae_bps": 0, "direction_correct": True, "market_phase": "open"},
            {"signal_type": "positive_t", "direction": "buy", "eval_horizon_sec": 180, "ret_bps": -5, "mfe_bps": 0, "mae_bps": -5, "direction_correct": False, "market_phase": "open"},
            {"signal_type": "positive_t", "direction": "buy", "eval_horizon_sec": 300, "ret_bps": 20, "mfe_bps": 20, "mae_bps": 0, "direction_correct": True, "market_phase": "open"},
            # reverse_t
            {"signal_type": "reverse_t", "direction": "sell", "eval_horizon_sec": 60, "ret_bps": -8, "mfe_bps": 0, "mae_bps": -8, "direction_correct": False, "market_phase": "afternoon"},
            {"signal_type": "reverse_t", "direction": "sell", "eval_horizon_sec": 180, "ret_bps": 15, "mfe_bps": 15, "mae_bps": 0, "direction_correct": True, "market_phase": "afternoon"},
            {"signal_type": "reverse_t", "direction": "sell", "eval_horizon_sec": 300, "ret_bps": 5, "mfe_bps": 5, "mae_bps": 0, "direction_correct": True, "market_phase": "afternoon"},
        ]
    )

    payload = _build_t0_quality_payload(
        df,
        hours=24,
        churn={"flip_count": 1, "sample_count": 4, "symbols": 2, "churn_ratio": 0.25, "flips_per_hour": 0.04},
    )

    assert payload["hours"] == 24
    assert payload["total_signals"] == 2
    assert payload["total_signals_any"] == 2
    assert payload["precision_1m"] == 0.5
    assert payload["precision_3m"] == 0.5
    assert payload["precision_5m"] == 1.0

    assert payload["by_horizon"]["60"]["count"] == 2
    assert payload["by_horizon"]["180"]["count"] == 2
    assert payload["by_horizon"]["300"]["count"] == 2

    assert payload["by_type"]["positive_t"]["precision_1m"] == 1.0
    assert payload["by_type"]["positive_t"]["precision_3m"] == 0.0
    assert payload["by_type"]["positive_t"]["precision_5m"] == 1.0

    assert payload["signal_churn"]["churn_ratio"] == 0.25
