"""Unit tests for Pool2 execution-layer scoring, downgrade, and sizing."""

from __future__ import annotations

from backend.realtime.gm_runtime import position_state, signal_engine


def _base_signal(sig_type: str = "positive_t", *, max_action_qty: int = 1000) -> dict:
    return {
        "has_signal": True,
        "type": sig_type,
        "price": 10.0,
        "strength": 80,
        "message": "test-signal",
        "details": {
            "observe_only": False,
            "observe_reason": "",
            "threshold": {
                "blocked": False,
                "observer_only": False,
                "session_policy": "afternoon",
            },
            "t0_inventory": {
                "state_ready": True,
                "max_action_qty": int(max_action_qty),
                "suggested_action_qty": int(max_action_qty),
                "lot_size": 100,
                "today_t_count": 0,
            },
            "positive_rebuild_quality": {
                "observe_reasons": [],
            },
            "do_not_t_env": {
                "session_policy": "afternoon",
                "reasons": [],
                "theme_accelerating": False,
                "breadth_climax": False,
            },
            "index_context_gate": {
                "state": "neutral",
                "reasons": [],
                "blocked": False,
                "observe_only": False,
            },
            "theme_leadership_guard": {
                "active": False,
                "distribution_confirm_count": 0,
            },
            "trend_guard": {
                "active": False,
                "surge_absorb_guard": False,
                "bearish_confirms": [],
            },
            "bearish_confirms": [],
            "concept_ecology": {"score": 40},
            "industry_ecology": {"score": 30},
            "market_structure": {"strength_delta": 0.0, "tag": ""},
            "anti_churn": {},
        },
    }


def test_score_to_action_level_and_helper_transitions() -> None:
    assert signal_engine.score_to_action_level(54.9) == signal_engine.T0ActionLevel.BLOCK.value
    assert signal_engine.score_to_action_level(55.0) == signal_engine.T0ActionLevel.OBSERVE.value
    assert signal_engine.score_to_action_level(65.0) == signal_engine.T0ActionLevel.TEST.value
    assert signal_engine.score_to_action_level(75.0) == signal_engine.T0ActionLevel.EXECUTE.value
    assert signal_engine.score_to_action_level(88.0) == signal_engine.T0ActionLevel.AGGRESSIVE.value

    assert (
        signal_engine.downgrade_action_level(signal_engine.T0ActionLevel.AGGRESSIVE.value, 2)
        == signal_engine.T0ActionLevel.TEST.value
    )
    assert (
        signal_engine.cap_action_level(
            signal_engine.T0ActionLevel.AGGRESSIVE.value,
            signal_engine.T0ActionLevel.EXECUTE.value,
        )
        == signal_engine.T0ActionLevel.EXECUTE.value
    )


def test_finalize_positive_exec_floors_qty_to_lot(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_engine,
        "_compute_positive_t_action_score",
        lambda signal, details: {
            "score": 80.0,
            "position_score": 70.0,
            "mean_reversion_score": 78.0,
            "orderflow_score": 74.0,
            "inventory_score": 68.0,
            "env_score": 62.0,
            "micro_risk_score": 5.0,
            "net_executable_edge_bps": 28.0,
        },
    )
    sig = _base_signal("positive_t", max_action_qty=1000)

    out = signal_engine._finalize_pool2_execution_signal(sig)
    details = out["details"]

    assert details["action_level_before_downgrade"] == signal_engine.T0ActionLevel.EXECUTE.value
    assert details["action_level_after_downgrade"] == signal_engine.T0ActionLevel.EXECUTE.value
    assert details["qty_multiplier"] == 0.25
    assert details["final_qty"] == 200
    assert details["observe_only"] is False


def test_finalize_qty_below_lot_auto_observe(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_engine,
        "_compute_positive_t_action_score",
        lambda signal, details: {
            "score": 80.0,
            "position_score": 70.0,
            "mean_reversion_score": 78.0,
            "orderflow_score": 74.0,
            "inventory_score": 68.0,
            "env_score": 62.0,
            "micro_risk_score": 5.0,
            "net_executable_edge_bps": 18.0,
        },
    )
    sig = _base_signal("positive_t", max_action_qty=350)

    out = signal_engine._finalize_pool2_execution_signal(sig)
    details = out["details"]

    assert details["final_qty"] == 0
    assert details["observe_only"] is True
    assert "qty_too_small_after_multiplier" in details["hard_block_reasons"]


def test_positive_panic_selloff_still_hard_block(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_engine,
        "_compute_positive_t_action_score",
        lambda signal, details: {
            "score": 92.0,
            "position_score": 82.0,
            "mean_reversion_score": 85.0,
            "orderflow_score": 80.0,
            "inventory_score": 75.0,
            "env_score": 30.0,
            "micro_risk_score": 4.0,
            "net_executable_edge_bps": 35.0,
        },
    )
    sig = _base_signal("positive_t", max_action_qty=1500)
    sig["details"]["index_context_gate"] = {
        "state": "panic_selloff",
        "reasons": ["index_panic_selloff"],
        "blocked": True,
        "observe_only": True,
    }

    out = signal_engine._finalize_pool2_execution_signal(sig)
    details = out["details"]

    assert details["action_level_after_downgrade"] == signal_engine.T0ActionLevel.BLOCK.value
    assert details["final_qty"] == 0
    assert details["observe_only"] is True
    assert "index_panic_selloff" in details["hard_block_reasons"]


def test_reverse_main_rally_guard_downgrades_instead_of_veto(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_engine,
        "_compute_reverse_t_action_score",
        lambda signal, details: {
            "score": 80.0,
            "overextension_score": 82.0,
            "buy_fading_score": 68.0,
            "orderflow_score": 72.0,
            "inventory_score": 70.0,
            "env_score": 60.0,
            "main_rally_penalty": 12.0,
            "net_executable_edge_bps": 26.0,
        },
    )
    sig = _base_signal("reverse_t", max_action_qty=2000)
    sig["details"]["main_rally_guard"] = True
    sig["details"]["trend_guard"] = {
        "active": True,
        "surge_absorb_guard": False,
        "bearish_confirms": ["bid_wall_break"],
    }
    sig["details"]["bearish_confirms"] = ["bid_wall_break"]

    out = signal_engine._finalize_pool2_execution_signal(sig)
    details = out["details"]

    assert details["hard_block_reasons"] == []
    assert "reverse_main_rally_guard" in details["downgrade_reasons"]
    assert details["action_level_before_downgrade"] == signal_engine.T0ActionLevel.EXECUTE.value
    assert details["action_level_after_downgrade"] == signal_engine.T0ActionLevel.TEST.value
    assert details["final_qty"] == 300
    assert details["observe_only"] is False


def test_aggressive_is_shadow_only_in_phase1(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_engine,
        "_compute_positive_t_action_score",
        lambda signal, details: {
            "score": 95.0,
            "position_score": 88.0,
            "mean_reversion_score": 90.0,
            "orderflow_score": 87.0,
            "inventory_score": 80.0,
            "env_score": 72.0,
            "micro_risk_score": 2.0,
            "net_executable_edge_bps": 42.0,
        },
    )
    monkeypatch.setattr(signal_engine, "_T0_EXEC_ENABLE_AGGRESSIVE_LIVE", False)
    monkeypatch.setattr(signal_engine, "_T0_EXEC_AGGRESSIVE_SHADOW_ONLY", True)
    sig = _base_signal("positive_t", max_action_qty=2000)

    out = signal_engine._finalize_pool2_execution_signal(sig)
    details = out["details"]

    assert details["action_level_before_downgrade"] == signal_engine.T0ActionLevel.AGGRESSIVE.value
    assert details["shadow_action_level"] == signal_engine.T0ActionLevel.AGGRESSIVE.value
    assert details["shadow_only"] is True
    assert details["final_qty"] == 0
    assert details["observe_only"] is True
    assert "aggressive_shadow_only" in details["downgrade_reasons"]


def test_pool2_inventory_records_execution_layer_fields(monkeypatch) -> None:
    code = "UT_POOL2_EXEC_FIELDS.SZ"
    monkeypatch.setattr(position_state, "_persist_pool2_t0_inventory_state", lambda **kwargs: None)
    with position_state._pool2_t0_inventory_state_lock:
        position_state._pool2_t0_inventory_state.pop(code, None)

    position_state._pool2_update_t0_inventory_state(
        code,
        {
            "overnight_base_qty": 3000,
            "reserve_qty": 500,
            "tradable_t_qty": 2000,
            "cash_available_for_t": 50000,
            "inventory_anchor_cost": 10.2,
        },
        now_ts=1_713_000_000,
    )
    before, after = position_state._pool2_apply_t0_signal(
        code,
        signal_type="positive_t",
        signal_price=9.8,
        action_qty=300,
        action_level="test",
        shadow_action_level="aggressive",
        shadow_only=False,
        qty_multiplier=0.1,
        soft_qty_multiplier=0.7,
        base_action_qty=2000,
        final_qty=300,
        score=76.5,
        now_ts=1_713_000_001,
    )

    assert before["tradable_t_qty"] == 2000
    assert after["tradable_t_qty"] == 1700
    assert after["last_action_level"] == "test"
    assert after["last_shadow_action_level"] == "aggressive"
    assert after["last_qty_multiplier"] == 0.1
    assert after["last_soft_qty_multiplier"] == 0.7
    assert after["last_base_action_qty"] == 2000
    assert after["last_final_qty"] == 300
    assert after["last_action_score"] == 76.5
    assert after["today_action_log"][-1]["action_level"] == "test"
    assert after["today_action_log"][-1]["final_qty"] == 300
