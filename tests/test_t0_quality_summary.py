"""Tests for T+0 quality summary aggregation helpers."""

from __future__ import annotations

import pandas as pd

from backend.realtime_runtime.common import _build_t0_quality_payload, _calc_churn_stats
from backend.realtime_runtime.diagnostics_service import (
    T0ExecutionOverrideApplyRequest,
    _build_t0_execution_override_reset_payload,
    _build_t0_execution_override_update,
)
from backend.realtime_runtime.pool_service import _build_t0_execution_calibration_payload


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
            {
                "signal_type": "positive_t",
                "direction": "buy",
                "eval_horizon_sec": 300,
                "ret_bps": 20,
                "mfe_bps": 20,
                "mae_bps": 0,
                "direction_correct": True,
                "market_phase": "open",
                "action_level_after_downgrade": "execute",
                "exec_status": "execute",
                "shadow_mode": "live_path",
                "hard_block_reason_primary": "",
                "final_qty": 300,
                "execution_score": 82.0,
            },
            # reverse_t
            {"signal_type": "reverse_t", "direction": "sell", "eval_horizon_sec": 60, "ret_bps": -8, "mfe_bps": 0, "mae_bps": -8, "direction_correct": False, "market_phase": "afternoon"},
            {"signal_type": "reverse_t", "direction": "sell", "eval_horizon_sec": 180, "ret_bps": 15, "mfe_bps": 15, "mae_bps": 0, "direction_correct": True, "market_phase": "afternoon"},
            {
                "signal_type": "reverse_t",
                "direction": "sell",
                "eval_horizon_sec": 300,
                "ret_bps": 5,
                "mfe_bps": 5,
                "mae_bps": 0,
                "direction_correct": True,
                "market_phase": "afternoon",
                "action_level_after_downgrade": "observe",
                "exec_status": "hard_block",
                "shadow_mode": "shadow_only",
                "hard_block_reason_primary": "qty_too_small_after_multiplier",
                "final_qty": 0,
                "execution_score": 61.0,
            },
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
    assert payload["by_action_level"]["execute"]["count"] == 1
    assert payload["by_action_level"]["execute"]["avg_final_qty"] == 300.0
    assert payload["by_exec_status"]["hard_block"]["count"] == 1
    assert payload["by_shadow_mode"]["shadow_only"]["count"] == 1
    assert payload["by_hard_block_reason"]["qty_too_small_after_multiplier"]["count"] == 1


def test_build_t0_execution_calibration_payload() -> None:
    payload = _build_t0_execution_calibration_payload(
        action_level_rows=[
            {
                "action_level": "execute",
                "recent_count": 12,
                "recent_precision_5m": 0.41,
                "recent_avg_ret_bps": -8.5,
                "baseline_count": 18,
                "baseline_precision_5m": 0.58,
                "baseline_avg_ret_bps": 7.2,
                "precision_drop": 0.17,
            }
        ],
        exec_status_rows=[
            {
                "exec_status": "hard_block",
                "recent_count": 9,
                "recent_precision_5m": 0.67,
                "recent_avg_ret_bps": 12.0,
                "baseline_count": 9,
                "baseline_precision_5m": 0.52,
                "baseline_avg_ret_bps": 5.0,
                "precision_drop": -0.15,
            }
        ],
        shadow_mode_rows=[
            {
                "shadow_mode": "shadow_only",
                "recent_count": 10,
                "recent_precision_5m": 0.66,
                "recent_avg_ret_bps": 10.0,
                "baseline_count": 10,
                "baseline_precision_5m": 0.56,
                "baseline_avg_ret_bps": 6.0,
                "precision_drop": -0.10,
            },
            {
                "shadow_mode": "live_path",
                "recent_count": 10,
                "recent_precision_5m": 0.48,
                "recent_avg_ret_bps": 2.0,
                "baseline_count": 10,
                "baseline_precision_5m": 0.59,
                "baseline_avg_ret_bps": 7.0,
                "precision_drop": 0.11,
            },
        ],
        hard_block_reason_rows=[
            {
                "hard_block_reason": "qty_too_small_after_multiplier",
                "recent_count": 7,
                "recent_precision_5m": 0.64,
                "recent_avg_ret_bps": 8.0,
                "baseline_count": 7,
                "baseline_precision_5m": 0.55,
                "baseline_avg_ret_bps": 4.0,
                "precision_drop": -0.09,
            }
        ],
    )

    assert payload["enabled"] is True
    assert payload["summary"]["suggestion_count"] >= 4
    titles = [str(item.get("title") or "") for item in payload["items"]]
    assert any("执行档近期胜率回落" in title for title in titles)
    assert any("硬阻断样本事后表现偏强" in title for title in titles)
    assert any("影子路径显著优于实盘" in title for title in titles)
    assert any("手数不足阻断可纳入白名单" in title for title in titles)


def test_build_t0_execution_override_update() -> None:
    current = {
        "action_qty_multiplier": {"execute": 0.25},
        "score_thresholds": {},
        "live_qty_scale": 1.0,
        "applied_items": [],
    }
    req = T0ExecutionOverrideApplyRequest(
        group="action_level",
        bucket="execute",
        action="tighten",
        label="执行档",
        title="执行档近期胜率回落",
    )
    updated = _build_t0_execution_override_update(req, current)
    assert updated["action_qty_multiplier"]["execute"] == 0.22
    assert updated["applied_items"][0]["bucket"] == "execute"

    req2 = T0ExecutionOverrideApplyRequest(
        group="shadow_mode",
        bucket="shadow_only_vs_live_path",
        action="promote_review",
        label="影子优于实盘",
        title="影子路径显著优于实盘",
    )
    updated2 = _build_t0_execution_override_update(req2, updated)
    assert updated2["live_qty_scale"] == 1.1
    assert updated2["applied_items"][0]["operator"] == "manual-ui"

    req3 = T0ExecutionOverrideApplyRequest(
        group="hard_block_reason",
        bucket="qty_too_small_after_multiplier",
        action="relax_qty_floor",
        label="手数不足",
        title="手数不足阻断可纳入白名单",
    )
    updated3 = _build_t0_execution_override_update(req3, updated2)
    assert updated3["live_qty_scale"] == 1.15
    assert updated3["applied_items"][0]["action"] == "relax_qty_floor"

    req4 = T0ExecutionOverrideApplyRequest(
        group="hard_block_reason",
        bucket="action_level_block",
        action="lower_block_threshold",
        label="执行层分数阻断",
        title="分数阻断可纳入白名单",
    )
    updated4 = _build_t0_execution_override_update(req4, updated3)
    assert updated4["score_thresholds"]["block"] == 53.0
    assert any(str(chg.get("field")) == "score_thresholds.block" for chg in updated4["applied_items"][0]["changes"])


def test_build_t0_execution_override_reset_payload() -> None:
    current = {
        "action_qty_multiplier": {"execute": 0.22, "test": 0.08},
        "score_thresholds": {"block": 53.0},
        "live_qty_scale": 1.1,
        "applied_items": [
            {
                "group": "action_level",
                "bucket": "execute",
                "action": "tighten",
                "operator": "ops-a",
                "applied_at": "2026-04-25T10:00:00",
                "changes": [{"field": "action_qty_multiplier.execute", "before": 0.25, "after": 0.22}],
            }
        ],
    }
    payload = _build_t0_execution_override_reset_payload(current, operator="ops-b", source="ui")
    assert payload["action_qty_multiplier"] == {}
    assert payload["score_thresholds"] == {}
    assert payload["live_qty_scale"] == 1.0
    assert payload["applied_items"][0]["event_type"] == "reset"
    assert payload["applied_items"][0]["operator"] == "ops-b"
    assert any(str(item.get("field")) == "live_qty_scale" for item in payload["applied_items"][0]["changes"])
    assert any(str(item.get("field")) == "score_thresholds.block" for item in payload["applied_items"][0]["changes"])