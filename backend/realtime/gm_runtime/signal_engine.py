"""gm runtime signal engine, state machine, and microstructure analysis."""
from __future__ import annotations

from enum import Enum

import backend.realtime.gm_runtime.common as gm_common
from .common import *  # noqa: F401,F403
from .position_state import *  # noqa: F401,F403
from .observe_service import *  # noqa: F401,F403
try:
    from config import REALTIME_RUNTIME_STATE_CONFIG as _RUNTIME_STATE_CFG
except Exception:
    _RUNTIME_STATE_CFG = {}
try:
    import redis as _redis_mod
except Exception:
    _redis_mod = None


class T0ActionLevel(str, Enum):
    BLOCK = "block"
    OBSERVE = "observe"
    TEST = "test"
    EXECUTE = "execute"
    AGGRESSIVE = "aggressive"


_T0_EXEC_CFG = _T0_CFG.get('execution_layer', {}) if isinstance(_T0_CFG, dict) else {}
_T0_EXEC_ENABLED = bool(_T0_EXEC_CFG.get('enabled', True))
_T0_EXEC_SCORE_CFG = _T0_EXEC_CFG.get('score_thresholds', {}) if isinstance(_T0_EXEC_CFG.get('score_thresholds'), dict) else {}
_T0_EXEC_QTY_CFG = _T0_EXEC_CFG.get('action_qty_multiplier', {}) if isinstance(_T0_EXEC_CFG.get('action_qty_multiplier'), dict) else {}
_T0_EXEC_SOFT_CFG = _T0_EXEC_CFG.get('soft_downgrade', {}) if isinstance(_T0_EXEC_CFG.get('soft_downgrade'), dict) else {}
_T0_EXEC_ENABLE_AGGRESSIVE_LIVE = bool(_T0_EXEC_CFG.get('enable_aggressive_live', False))
_T0_EXEC_AGGRESSIVE_SHADOW_ONLY = bool(_T0_EXEC_CFG.get('aggressive_shadow_only', True))
_T0_EXEC_SCORE_BLOCK_MAX = float(_T0_EXEC_SCORE_CFG.get('block', 55) or 55)
_T0_EXEC_SCORE_OBSERVE_MAX = float(_T0_EXEC_SCORE_CFG.get('observe', 65) or 65)
_T0_EXEC_SCORE_TEST_MAX = float(_T0_EXEC_SCORE_CFG.get('test', 75) or 75)
_T0_EXEC_SCORE_EXECUTE_MAX = float(_T0_EXEC_SCORE_CFG.get('execute', 88) or 88)
ACTION_QTY_MULTIPLIER = {
    T0ActionLevel.BLOCK.value: float(_T0_EXEC_QTY_CFG.get('block', 0.00) or 0.00),
    T0ActionLevel.OBSERVE.value: float(_T0_EXEC_QTY_CFG.get('observe', 0.00) or 0.00),
    T0ActionLevel.TEST.value: float(_T0_EXEC_QTY_CFG.get('test', 0.10) or 0.10),
    T0ActionLevel.EXECUTE.value: float(_T0_EXEC_QTY_CFG.get('execute', 0.25) or 0.25),
    T0ActionLevel.AGGRESSIVE.value: float(_T0_EXEC_QTY_CFG.get('aggressive', 0.40) or 0.40),
}
ACTION_LEVEL_ORDER = {
    T0ActionLevel.BLOCK.value: 0,
    T0ActionLevel.OBSERVE.value: 1,
    T0ActionLevel.TEST.value: 2,
    T0ActionLevel.EXECUTE.value: 3,
    T0ActionLevel.AGGRESSIVE.value: 4,
}
_T0_EXEC_OPEN_STRICT_QTY_MULT = float(_T0_EXEC_SOFT_CFG.get('open_strict_qty_multiplier', 0.70) or 0.70)
_T0_EXEC_OPEN_STRICT_CAP_LEVEL = str(_T0_EXEC_SOFT_CFG.get('open_strict_cap_level', T0ActionLevel.TEST.value) or T0ActionLevel.TEST.value)
_T0_EXEC_RISK_OFF_QTY_MULT = float(_T0_EXEC_SOFT_CFG.get('risk_off_qty_multiplier', 0.70) or 0.70)
_T0_EXEC_RISK_ON_QTY_MULT = float(_T0_EXEC_SOFT_CFG.get('risk_on_qty_multiplier', 0.70) or 0.70)
_T0_EXEC_REV_TREND_QTY_MULT = float(_T0_EXEC_SOFT_CFG.get('reverse_trend_guard_qty_multiplier', 0.60) or 0.60)
_T0_EXEC_REV_THEME_QTY_MULT = float(_T0_EXEC_SOFT_CFG.get('reverse_theme_guard_qty_multiplier', 0.60) or 0.60)
_T0_EXEC_REV_HARD_PROTECT_CAP = str(_T0_EXEC_SOFT_CFG.get('reverse_hard_protect_cap_level', T0ActionLevel.OBSERVE.value) or T0ActionLevel.OBSERVE.value)
_T0_EXEC_THEME_ACCEL_QTY_MULT = float(_T0_EXEC_SOFT_CFG.get('theme_acceleration_qty_multiplier', 0.70) or 0.70)
_T0_EXEC_BREADTH_CLIMAX_QTY_MULT = float(_T0_EXEC_SOFT_CFG.get('breadth_climax_qty_multiplier', 0.70) or 0.70)
_T0_EXEC_RUNTIME_REDIS_CFG = _RUNTIME_STATE_CFG.get('redis', {}) if isinstance(_RUNTIME_STATE_CFG, dict) else {}
_T0_EXEC_RUNTIME_OVERRIDE_ENABLED = str(os.getenv("T0_EXEC_RUNTIME_OVERRIDE_ENABLED", "1") or "1").lower() in ("1", "true", "yes", "on")
_T0_EXEC_RUNTIME_OVERRIDE_REFRESH_SEC = max(3.0, float(os.getenv("T0_EXEC_RUNTIME_OVERRIDE_REFRESH_SEC", "5") or 5.0))
_T0_EXEC_RUNTIME_REDIS_URL = str(
    _T0_EXEC_RUNTIME_REDIS_CFG.get(
        "url",
        os.getenv("RUNTIME_STATE_REDIS_URL", os.getenv("REALTIME_DB_WRITER_REDIS_URL", "redis://127.0.0.1:6379/0")),
    )
)
_T0_EXEC_RUNTIME_REDIS_ENABLED = bool(
    _T0_EXEC_RUNTIME_REDIS_CFG.get("enabled", os.getenv("RUNTIME_STATE_REDIS_ENABLED", "0").lower() in ("1", "true", "yes", "on"))
)
_T0_EXEC_RUNTIME_REDIS_KEY = f"{str(_T0_EXEC_RUNTIME_REDIS_CFG.get('key_prefix', os.getenv('RUNTIME_STATE_REDIS_KEY_PREFIX', 'quant:runtime')))}:t0:execution_override:latest"
_t0_exec_runtime_redis = None
_t0_exec_runtime_next_retry = 0.0
_t0_exec_runtime_cache_at = 0.0
_t0_exec_runtime_cache_payload: dict = {}


def _safe_float_value(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _clamp_score(value: object, low: float = 0.0, high: float = 100.0) -> float:
    try:
        vv = float(value)
    except Exception:
        vv = float(low)
    return max(float(low), min(float(high), vv))


def _normalized_score(value: Optional[float], low: float, high: float, *, invert: bool = False, neutral: float = 50.0) -> float:
    if value is None:
        return float(neutral)
    lo = float(low)
    hi = float(high)
    if hi <= lo:
        return float(neutral)
    ratio = (float(value) - lo) / (hi - lo)
    ratio = max(0.0, min(1.0, ratio))
    if invert:
        ratio = 1.0 - ratio
    return ratio * 100.0


def _get_t0_exec_runtime_redis():
    global _t0_exec_runtime_redis, _t0_exec_runtime_next_retry
    if not (_T0_EXEC_RUNTIME_OVERRIDE_ENABLED and _T0_EXEC_RUNTIME_REDIS_ENABLED) or _redis_mod is None:
        return None
    if _t0_exec_runtime_redis is not None:
        return _t0_exec_runtime_redis
    now = time.time()
    if now < _t0_exec_runtime_next_retry:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_T0_EXEC_RUNTIME_REDIS_URL, decode_responses=True)
        cli.ping()
        _t0_exec_runtime_redis = cli
        return _t0_exec_runtime_redis
    except Exception:
        _t0_exec_runtime_next_retry = now + 5.0
        return None


def _load_t0_exec_runtime_override() -> dict:
    global _t0_exec_runtime_cache_at, _t0_exec_runtime_cache_payload
    now = time.time()
    if _t0_exec_runtime_cache_payload and (now - _t0_exec_runtime_cache_at) < _T0_EXEC_RUNTIME_OVERRIDE_REFRESH_SEC:
        return _t0_exec_runtime_cache_payload
    payload: dict = {}
    cli = _get_t0_exec_runtime_redis()
    if cli is not None:
        try:
            raw = cli.get(_T0_EXEC_RUNTIME_REDIS_KEY)
            if raw:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    payload = parsed
        except Exception:
            payload = {}
    if not payload:
        try:
            import backend.realtime_runtime.runtime_jobs as _runtime_jobs

            local_payload = _runtime_jobs.get_t0_execution_override_snapshot_local()
            if isinstance(local_payload, dict):
                payload = local_payload
        except Exception:
            payload = {}
    _t0_exec_runtime_cache_payload = payload or {}
    _t0_exec_runtime_cache_at = now
    return _t0_exec_runtime_cache_payload


def _get_t0_exec_effective_cfg() -> dict:
    override = _load_t0_exec_runtime_override()
    score_cfg = dict(_T0_EXEC_SCORE_CFG) if isinstance(_T0_EXEC_SCORE_CFG, dict) else {}
    qty_cfg = dict(_T0_EXEC_QTY_CFG) if isinstance(_T0_EXEC_QTY_CFG, dict) else {}
    soft_cfg = dict(_T0_EXEC_SOFT_CFG) if isinstance(_T0_EXEC_SOFT_CFG, dict) else {}
    if isinstance(override.get('score_thresholds'), dict):
        score_cfg.update(override.get('score_thresholds') or {})
    if isinstance(override.get('action_qty_multiplier'), dict):
        qty_cfg.update(override.get('action_qty_multiplier') or {})
    if isinstance(override.get('soft_downgrade'), dict):
        soft_cfg.update(override.get('soft_downgrade') or {})
    return {
        'score_thresholds': score_cfg,
        'action_qty_multiplier': qty_cfg,
        'soft_downgrade': soft_cfg,
        'enable_aggressive_live': bool(override.get('enable_aggressive_live', _T0_EXEC_ENABLE_AGGRESSIVE_LIVE)),
        'aggressive_shadow_only': bool(override.get('aggressive_shadow_only', _T0_EXEC_AGGRESSIVE_SHADOW_ONLY)),
        'live_qty_scale': max(0.0, _safe_float_value(override.get('live_qty_scale', 1.0), 1.0)),
        'override': override if isinstance(override, dict) else {},
    }


def score_to_action_level(score: float) -> str:
    vv = _clamp_score(score)
    cfg = _get_t0_exec_effective_cfg()
    score_cfg = cfg.get('score_thresholds') if isinstance(cfg.get('score_thresholds'), dict) else {}
    block_max = float(score_cfg.get('block', _T0_EXEC_SCORE_BLOCK_MAX) or _T0_EXEC_SCORE_BLOCK_MAX)
    observe_max = float(score_cfg.get('observe', _T0_EXEC_SCORE_OBSERVE_MAX) or _T0_EXEC_SCORE_OBSERVE_MAX)
    test_max = float(score_cfg.get('test', _T0_EXEC_SCORE_TEST_MAX) or _T0_EXEC_SCORE_TEST_MAX)
    execute_max = float(score_cfg.get('execute', _T0_EXEC_SCORE_EXECUTE_MAX) or _T0_EXEC_SCORE_EXECUTE_MAX)
    if vv < block_max:
        return T0ActionLevel.BLOCK.value
    if vv < observe_max:
        return T0ActionLevel.OBSERVE.value
    if vv < test_max:
        return T0ActionLevel.TEST.value
    if vv < execute_max:
        return T0ActionLevel.EXECUTE.value
    return T0ActionLevel.AGGRESSIVE.value


def downgrade_action_level(level: str, steps: int = 1) -> str:
    lvl = str(level or T0ActionLevel.BLOCK.value).strip().lower()
    if lvl not in ACTION_LEVEL_ORDER:
        lvl = T0ActionLevel.BLOCK.value
    rank = max(0, ACTION_LEVEL_ORDER[lvl] - max(0, int(steps or 0)))
    for name, order in ACTION_LEVEL_ORDER.items():
        if order == rank:
            return name
    return T0ActionLevel.BLOCK.value


def cap_action_level(level: str, max_level: str) -> str:
    lvl = str(level or T0ActionLevel.BLOCK.value).strip().lower()
    cap = str(max_level or T0ActionLevel.BLOCK.value).strip().lower()
    if lvl not in ACTION_LEVEL_ORDER:
        lvl = T0ActionLevel.BLOCK.value
    if cap not in ACTION_LEVEL_ORDER:
        cap = T0ActionLevel.BLOCK.value
    if ACTION_LEVEL_ORDER[lvl] <= ACTION_LEVEL_ORDER[cap]:
        return lvl
    return cap


def _t0_floor_to_lot(qty: float, lot_size: int) -> int:
    lot = max(1, int(lot_size or 1))
    try:
        value = int(float(qty))
    except Exception:
        value = 0
    if value <= 0:
        return 0
    return max(0, value // lot * lot)


def _t0_distribution_confirm_count(details: dict) -> int:
    trend_guard = details.get('trend_guard') if isinstance(details.get('trend_guard'), dict) else {}
    theme_guard = details.get('theme_leadership_guard') if isinstance(details.get('theme_leadership_guard'), dict) else {}
    bearish_confirms = details.get('bearish_confirms') if isinstance(details.get('bearish_confirms'), list) else []
    return max(
        len(bearish_confirms),
        int(theme_guard.get('distribution_confirm_count', 0) or 0),
        len(trend_guard.get('bearish_confirms') or []),
    )


def _compute_positive_t_action_score(signal: dict, details: dict) -> dict:
    inventory = details.get('t0_inventory') if isinstance(details.get('t0_inventory'), dict) else {}
    rebuild = details.get('positive_rebuild_quality') if isinstance(details.get('positive_rebuild_quality'), dict) else {}
    anti = details.get('anti_churn') if isinstance(details.get('anti_churn'), dict) else {}
    env = details.get('do_not_t_env') if isinstance(details.get('do_not_t_env'), dict) else {}
    index_gate = details.get('index_context_gate') if isinstance(details.get('index_context_gate'), dict) else {}
    concept = details.get('concept_ecology') if isinstance(details.get('concept_ecology'), dict) else {}
    industry = details.get('industry_ecology') if isinstance(details.get('industry_ecology'), dict) else {}
    ms = details.get('market_structure') if isinstance(details.get('market_structure'), dict) else {}

    tick_price = _safe_float_value(signal.get('price') or details.get('tick_price'), 0.0)
    anchor_cost = _safe_float_value(inventory.get('inventory_anchor_cost'), 0.0)
    expected_edge_bps = rebuild.get('expected_edge_bps')
    if expected_edge_bps is None:
        expected_edge_bps = anti.get('expected_edge_bps')
    net_edge_bps = rebuild.get('net_executable_edge_bps')
    bias_vwap = details.get('bias_vwap')
    bid_ask_ratio = details.get('bid_ask_ratio')
    super_order_bias = details.get('super_order_bias')
    super_net_flow_bps = details.get('super_net_flow_bps')
    recovery_score = rebuild.get('recover_after_rebuild_score')

    position_score = 50.0
    if tick_price > 0 and anchor_cost > 0:
        improve_bps = (anchor_cost - tick_price) / anchor_cost * 10000.0
        position_score = _normalized_score(improve_bps, -80.0, 180.0, neutral=50.0)

    edge_score = _normalized_score(
        float(expected_edge_bps) if expected_edge_bps is not None else None,
        0.0,
        80.0,
        neutral=50.0,
    )
    bias_score = _normalized_score(
        abs(float(bias_vwap)) if bias_vwap is not None and float(bias_vwap) < 0 else 0.0,
        0.0,
        3.0,
        neutral=50.0,
    )
    mean_reversion_score = _clamp_score(
        (
            _clamp_score(recovery_score if recovery_score is not None else 50.0) * 0.50
            + edge_score * 0.30
            + bias_score * 0.20
        )
        + (6.0 if bool(details.get('boll_break')) else 0.0)
    )

    orderflow_score = 45.0
    orderflow_score += (_normalized_score(float(bid_ask_ratio), 0.9, 1.6, neutral=45.0) - 50.0) * 0.30 if bid_ask_ratio is not None else 0.0
    orderflow_score += (_normalized_score(float(super_order_bias), -0.2, 0.45, neutral=50.0) - 50.0) * 0.20 if super_order_bias is not None else 0.0
    orderflow_score += (_normalized_score(float(super_net_flow_bps), -120.0, 220.0, neutral=50.0) - 50.0) * 0.25 if super_net_flow_bps is not None else 0.0
    if bool(details.get('ask_wall_absorb')) or bool(ms.get('ask_wall_absorbed')):
        orderflow_score += 10.0
    if bool(details.get('lure_short')):
        orderflow_score += 8.0
    if str(ms.get('tag') or '') == 'real_buy':
        orderflow_score += 8.0
    if str(ms.get('tag') or '') == 'lure_short':
        orderflow_score += 10.0
    if str(ms.get('tag') or '') in {'wash', 'lure_long'}:
        orderflow_score -= 10.0
    orderflow_score = _clamp_score(orderflow_score)

    max_action_qty = max(0, int(inventory.get('max_action_qty', 0) or 0))
    cash_room_qty = max(0, int(inventory.get('cash_room_qty', 0) or 0))
    today_t_count = max(0, int(inventory.get('today_t_count', 0) or 0))
    reverse_room_ratio_after = rebuild.get('reverse_room_ratio_after')
    inventory_score = 40.0
    if bool(inventory.get('state_ready')):
        inventory_score += 20.0
    if max_action_qty > 0:
        inventory_score += 18.0
    if cash_room_qty > 0:
        inventory_score += 12.0
    if reverse_room_ratio_after is not None:
        inventory_score += (_normalized_score(float(reverse_room_ratio_after), 0.15, 0.60, neutral=50.0) - 50.0) * 0.25
    inventory_score -= max(0.0, float(today_t_count)) * 4.0
    inventory_score = _clamp_score(inventory_score)

    env_score = 60.0
    env_score += (_normalized_score(float(concept.get('score', 0.0) or 0.0), 10.0, 60.0, neutral=50.0) - 50.0) * 0.20 if concept else 0.0
    env_score += (_normalized_score(float(industry.get('score', 0.0) or 0.0), 5.0, 50.0, neutral=50.0) - 50.0) * 0.15 if industry else 0.0
    if str(index_gate.get('state') or '') == 'risk_off':
        env_score -= 18.0
    elif str(index_gate.get('state') or '') == 'panic_selloff':
        env_score -= 35.0
    elif str(index_gate.get('state') or '') == 'risk_on':
        env_score += 6.0
    if str(env.get('session_policy') or '') == 'open_strict':
        env_score -= 8.0
    if bool(env.get('theme_accelerating')):
        env_score -= 10.0
    if bool(env.get('breadth_climax')):
        env_score -= 8.0
    env_score = _clamp_score(env_score)

    micro_risk_score = 0.0
    if bool(details.get('wash_trade')):
        micro_risk_score += 10.0
    if bool(details.get('spoofing_suspected')):
        micro_risk_score += 25.0
    if str(ms.get('tag') or '') == 'wash':
        micro_risk_score += 8.0
    if str(ms.get('tag') or '') == 'lure_long':
        micro_risk_score += 10.0
    if _safe_float_value(ms.get('strength_delta'), 0.0) < 0:
        micro_risk_score += min(12.0, abs(_safe_float_value(ms.get('strength_delta'), 0.0)) * 0.35)
    micro_risk_score = _clamp_score(micro_risk_score, 0.0, 45.0)

    score = (
        position_score * 0.30
        + mean_reversion_score * 0.25
        + orderflow_score * 0.25
        + inventory_score * 0.10
        + env_score * 0.10
        - micro_risk_score
    )
    return {
        'score': _clamp_score(score),
        'position_score': _clamp_score(position_score),
        'mean_reversion_score': _clamp_score(mean_reversion_score),
        'orderflow_score': _clamp_score(orderflow_score),
        'inventory_score': _clamp_score(inventory_score),
        'env_score': _clamp_score(env_score),
        'micro_risk_score': _clamp_score(micro_risk_score),
        'net_executable_edge_bps': _safe_float_value(net_edge_bps, _safe_float_value(expected_edge_bps, 0.0)),
    }


def _compute_reverse_t_action_score(signal: dict, details: dict) -> dict:
    inventory = details.get('t0_inventory') if isinstance(details.get('t0_inventory'), dict) else {}
    anti = details.get('anti_churn') if isinstance(details.get('anti_churn'), dict) else {}
    env = details.get('do_not_t_env') if isinstance(details.get('do_not_t_env'), dict) else {}
    index_gate = details.get('index_context_gate') if isinstance(details.get('index_context_gate'), dict) else {}
    theme_guard = details.get('theme_leadership_guard') if isinstance(details.get('theme_leadership_guard'), dict) else {}
    trend_guard = details.get('trend_guard') if isinstance(details.get('trend_guard'), dict) else {}
    concept = details.get('concept_ecology') if isinstance(details.get('concept_ecology'), dict) else {}
    industry = details.get('industry_ecology') if isinstance(details.get('industry_ecology'), dict) else {}
    ms = details.get('market_structure') if isinstance(details.get('market_structure'), dict) else {}

    robust_zscore = details.get('robust_zscore')
    expected_edge_bps = anti.get('expected_edge_bps')
    bid_ask_ratio = details.get('bid_ask_ratio')
    super_order_bias = details.get('super_order_bias')
    super_net_flow_bps = details.get('super_net_flow_bps')
    pct_chg = details.get('pct_chg')

    overextension_score = 45.0
    overextension_score += (_normalized_score(float(robust_zscore), 1.8, 4.0, neutral=50.0) - 50.0) * 0.40 if robust_zscore is not None else 0.0
    overextension_score += (_normalized_score(float(expected_edge_bps), 0.0, 100.0, neutral=50.0) - 50.0) * 0.30 if expected_edge_bps is not None else 0.0
    overextension_score += (_normalized_score(float(pct_chg), 0.5, 6.0, neutral=50.0) - 50.0) * 0.20 if pct_chg is not None else 0.0
    if bool(details.get('boll_over')):
        overextension_score += 8.0
    overextension_score = _clamp_score(overextension_score)

    bearish_confirms = details.get('bearish_confirms') if isinstance(details.get('bearish_confirms'), list) else []
    buy_fading_score = 35.0 + min(30.0, len(bearish_confirms) * 8.0)
    if bool(details.get('v_power_divergence')):
        buy_fading_score += 12.0
    if bool(details.get('real_buy_fading')):
        buy_fading_score += 14.0
    if bool(details.get('ask_wall_building')):
        buy_fading_score += 10.0
    if bool(details.get('bid_wall_break')):
        buy_fading_score += 12.0
    buy_fading_score = _clamp_score(buy_fading_score)

    orderflow_sell_score = 45.0
    orderflow_sell_score += (_normalized_score(-float(super_order_bias), 0.0, 0.45, neutral=50.0) - 50.0) * 0.25 if super_order_bias is not None else 0.0
    orderflow_sell_score += (_normalized_score(-float(super_net_flow_bps), 0.0, 220.0, neutral=50.0) - 50.0) * 0.30 if super_net_flow_bps is not None else 0.0
    orderflow_sell_score += (_normalized_score(1.4 - float(bid_ask_ratio), -0.3, 0.7, neutral=50.0) - 50.0) * 0.20 if bid_ask_ratio is not None else 0.0
    if str(ms.get('tag') or '') == 'real_sell':
        orderflow_sell_score += 10.0
    if bool(details.get('bid_wall_break')) or bool(ms.get('bid_wall_broken')):
        orderflow_sell_score += 10.0
    if str(ms.get('tag') or '') == 'real_buy':
        orderflow_sell_score -= 10.0
    orderflow_sell_score = _clamp_score(orderflow_sell_score)

    max_action_qty = max(0, int(inventory.get('max_action_qty', 0) or 0))
    tradable_qty = max(0, int(inventory.get('tradable_t_qty', 0) or 0))
    today_t_count = max(0, int(inventory.get('today_t_count', 0) or 0))
    inventory_score = 40.0
    if bool(inventory.get('state_ready')):
        inventory_score += 20.0
    if max_action_qty > 0:
        inventory_score += 18.0
    if tradable_qty > 0:
        inventory_score += 10.0
    inventory_score -= max(0.0, float(today_t_count)) * 4.0
    inventory_score = _clamp_score(inventory_score)

    env_score = 60.0
    env_score += (_normalized_score(float(concept.get('score', 0.0) or 0.0), 10.0, 60.0, neutral=50.0) - 50.0) * 0.10 if concept else 0.0
    env_score += (_normalized_score(float(industry.get('score', 0.0) or 0.0), 5.0, 50.0, neutral=50.0) - 50.0) * 0.08 if industry else 0.0
    if str(index_gate.get('state') or '') == 'risk_on':
        env_score -= 15.0
    elif str(index_gate.get('state') or '') == 'risk_off':
        env_score += 6.0
    if str(env.get('session_policy') or '') == 'open_strict':
        env_score -= 8.0
    if bool(env.get('theme_accelerating')) or bool(env.get('breadth_climax')):
        env_score -= 8.0
    env_score = _clamp_score(env_score)

    main_rally_penalty = 0.0
    distribution_count = _t0_distribution_confirm_count(details)
    if bool(details.get('main_rally_guard')) or bool(trend_guard.get('active')):
        main_rally_penalty += 12.0
    if bool(trend_guard.get('surge_absorb_guard')):
        main_rally_penalty += 18.0
    if bool(theme_guard.get('active')):
        main_rally_penalty += 10.0
    if distribution_count >= 3:
        main_rally_penalty = max(0.0, main_rally_penalty - 18.0)
    main_rally_penalty = _clamp_score(main_rally_penalty, 0.0, 45.0)

    score = (
        overextension_score * 0.35
        + buy_fading_score * 0.25
        + orderflow_sell_score * 0.20
        + inventory_score * 0.10
        + env_score * 0.10
        - main_rally_penalty
    )
    return {
        'score': _clamp_score(score),
        'overextension_score': _clamp_score(overextension_score),
        'buy_fading_score': _clamp_score(buy_fading_score),
        'orderflow_score': _clamp_score(orderflow_sell_score),
        'inventory_score': _clamp_score(inventory_score),
        'env_score': _clamp_score(env_score),
        'main_rally_penalty': _clamp_score(main_rally_penalty),
        'net_executable_edge_bps': _safe_float_value(expected_edge_bps, 0.0),
    }


def _collect_t0_hard_block_reasons(signal_type: str, details: dict) -> list[str]:
    reasons: list[str] = []
    threshold = details.get('threshold') if isinstance(details.get('threshold'), dict) else {}
    inventory = details.get('t0_inventory') if isinstance(details.get('t0_inventory'), dict) else {}
    rebuild = details.get('positive_rebuild_quality') if isinstance(details.get('positive_rebuild_quality'), dict) else {}
    env = details.get('do_not_t_env') if isinstance(details.get('do_not_t_env'), dict) else {}
    index_gate = details.get('index_context_gate') if isinstance(details.get('index_context_gate'), dict) else {}

    if bool(threshold.get('blocked')):
        reasons.append(str(threshold.get('blocked_reason') or 'threshold_blocked'))

    observe_reason = str(inventory.get('observe_reason') or inventory.get('blocked_reason') or '')
    if observe_reason in {
        't0_inventory_missing',
        'no_tradable_t_inventory',
        'no_cash_available_for_positive_t',
        't_count_exhausted',
        'action_qty_below_lot',
    }:
        reasons.append(observe_reason)

    for rr in rebuild.get('observe_reasons') or []:
        if str(rr) in {'net_executable_edge_low'}:
            reasons.append(str(rr))

    for rr in env.get('reasons') or []:
        reason = str(rr or '')
        if reason.startswith('session_policy:'):
            reasons.append(reason)
        elif reason in {
            'limit_magnet',
            'board_seal_env',
            'risk_warning',
            'volume_drought_low_liquidity',
        }:
            reasons.append(reason)
        elif reason.startswith('listing_stage:'):
            reasons.append(reason)

    if signal_type == 'positive_t' and bool(index_gate.get('blocked')):
        reasons.extend(str(x) for x in (index_gate.get('reasons') or []) if str(x))

    if bool(details.get('spoofing_suspected')):
        reasons.append('spoofing_suspected')
    if bool(details.get('wash_trade')) and bool(details.get('spread_abnormal')):
        reasons.append('wash_trade_spread_abnormal')

    deduped: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        rr = str(reason or '').strip()
        if not rr or rr in seen:
            continue
        seen.add(rr)
        deduped.append(rr)
    return deduped


def _apply_soft_downgrades(signal_type: str, details: dict, action_level: str, qty_multiplier: float) -> tuple[str, float, list[str]]:
    level = str(action_level or T0ActionLevel.BLOCK.value)
    soft_qty_multiplier = max(0.0, float(qty_multiplier or 0.0))
    downgrade_reasons: list[str] = []
    cfg = _get_t0_exec_effective_cfg()
    soft_cfg = cfg.get('soft_downgrade') if isinstance(cfg.get('soft_downgrade'), dict) else {}
    open_strict_qty_mult = float(soft_cfg.get('open_strict_qty_multiplier', _T0_EXEC_OPEN_STRICT_QTY_MULT) or _T0_EXEC_OPEN_STRICT_QTY_MULT)
    open_strict_cap_level = str(soft_cfg.get('open_strict_cap_level', _T0_EXEC_OPEN_STRICT_CAP_LEVEL) or _T0_EXEC_OPEN_STRICT_CAP_LEVEL)
    risk_off_qty_mult = float(soft_cfg.get('risk_off_qty_multiplier', _T0_EXEC_RISK_OFF_QTY_MULT) or _T0_EXEC_RISK_OFF_QTY_MULT)
    risk_on_qty_mult = float(soft_cfg.get('risk_on_qty_multiplier', _T0_EXEC_RISK_ON_QTY_MULT) or _T0_EXEC_RISK_ON_QTY_MULT)
    reverse_trend_qty_mult = float(soft_cfg.get('reverse_trend_guard_qty_multiplier', _T0_EXEC_REV_TREND_QTY_MULT) or _T0_EXEC_REV_TREND_QTY_MULT)
    reverse_theme_qty_mult = float(soft_cfg.get('reverse_theme_guard_qty_multiplier', _T0_EXEC_REV_THEME_QTY_MULT) or _T0_EXEC_REV_THEME_QTY_MULT)
    reverse_hard_cap = str(soft_cfg.get('reverse_hard_protect_cap_level', _T0_EXEC_REV_HARD_PROTECT_CAP) or _T0_EXEC_REV_HARD_PROTECT_CAP)
    theme_accel_qty_mult = float(soft_cfg.get('theme_acceleration_qty_multiplier', _T0_EXEC_THEME_ACCEL_QTY_MULT) or _T0_EXEC_THEME_ACCEL_QTY_MULT)
    breadth_climax_qty_mult = float(soft_cfg.get('breadth_climax_qty_multiplier', _T0_EXEC_BREADTH_CLIMAX_QTY_MULT) or _T0_EXEC_BREADTH_CLIMAX_QTY_MULT)
    env = details.get('do_not_t_env') if isinstance(details.get('do_not_t_env'), dict) else {}
    index_gate = details.get('index_context_gate') if isinstance(details.get('index_context_gate'), dict) else {}
    trend_guard = details.get('trend_guard') if isinstance(details.get('trend_guard'), dict) else {}
    theme_guard = details.get('theme_leadership_guard') if isinstance(details.get('theme_leadership_guard'), dict) else {}
    threshold = details.get('threshold') if isinstance(details.get('threshold'), dict) else {}

    if str(threshold.get('session_policy') or env.get('session_policy') or '') == 'open_strict':
        capped = cap_action_level(level, open_strict_cap_level)
        if capped != level:
            level = capped
            downgrade_reasons.append('open_strict_cap')
        soft_qty_multiplier *= open_strict_qty_mult

    state = str(index_gate.get('state') or '').strip()
    if signal_type == 'positive_t' and state == 'risk_off':
        level = downgrade_action_level(level, 1)
        soft_qty_multiplier *= risk_off_qty_mult
        downgrade_reasons.append('index_risk_off')
    if signal_type == 'reverse_t' and state == 'risk_on':
        distribution_count = _t0_distribution_confirm_count(details)
        if distribution_count < 3:
            level = downgrade_action_level(level, 1)
            soft_qty_multiplier *= risk_on_qty_mult
            downgrade_reasons.append('index_risk_on')

    if signal_type == 'reverse_t':
        distribution_count = _t0_distribution_confirm_count(details)
        if bool(trend_guard.get('surge_absorb_guard')) and distribution_count < 3:
            capped = cap_action_level(level, reverse_hard_cap)
            if capped != level:
                level = capped
            downgrade_reasons.append('reverse_hard_protect')
        elif (bool(details.get('main_rally_guard')) or bool(trend_guard.get('active'))) and distribution_count < 3:
            level = downgrade_action_level(level, 1)
            soft_qty_multiplier *= reverse_trend_qty_mult
            downgrade_reasons.append('reverse_main_rally_guard')
        if bool(theme_guard.get('active')) and distribution_count < 3:
            level = downgrade_action_level(level, 1)
            soft_qty_multiplier *= reverse_theme_qty_mult
            downgrade_reasons.append('reverse_theme_guard')

    if bool(env.get('theme_accelerating')):
        level = downgrade_action_level(level, 1)
        soft_qty_multiplier *= theme_accel_qty_mult
        downgrade_reasons.append('theme_accelerating')
    if bool(env.get('breadth_climax')):
        level = downgrade_action_level(level, 1)
        soft_qty_multiplier *= breadth_climax_qty_mult
        downgrade_reasons.append('breadth_climax')

    return level, max(0.0, soft_qty_multiplier), downgrade_reasons


def _rewrite_t0_execution_message(message: str, *, action_level: str, final_qty: int, shadow_action_level: str, shadow_only: bool, hard_block_reasons: list[str], downgrade_reasons: list[str]) -> str:
    msg = str(message or '')
    for suffix in ('·仅观察', '·可执行'):
        msg = msg.replace(suffix, '')
    level_label_map = {
        T0ActionLevel.BLOCK.value: '阻断',
        T0ActionLevel.OBSERVE.value: '观察',
        T0ActionLevel.TEST.value: '试单',
        T0ActionLevel.EXECUTE.value: '执行',
        T0ActionLevel.AGGRESSIVE.value: '激进',
    }
    action_label = level_label_map.get(str(action_level or ''), str(action_level or '--'))
    parts = [msg] if msg else []
    if shadow_only and shadow_action_level:
        parts.append(f" | 影子档:{level_label_map.get(shadow_action_level, shadow_action_level)}")
    else:
        parts.append(f" | 执行层:{action_label}")
    if final_qty > 0:
        parts.append(f" | 数量:{int(final_qty)}股")
    if hard_block_reasons:
        parts.append(f" | 硬阻断:{'|'.join(hard_block_reasons[:2])}")
    elif downgrade_reasons:
        parts.append(f" | 降档:{'|'.join(downgrade_reasons[:2])}")
    return ''.join(parts)

def _finalize_pool2_execution_signal(signal: dict) -> dict:
    if not _T0_EXEC_ENABLED or not isinstance(signal, dict) or not signal.get('has_signal'):
        return signal
    sig_type = str(signal.get('type') or '')
    if sig_type not in {'positive_t', 'reverse_t'}:
        return signal

    out = dict(signal)
    details = out.get('details') if isinstance(out.get('details'), dict) else {}
    out['details'] = details
    legacy_observe_only = bool(details.get('observe_only'))
    legacy_observe_reason = str(details.get('observe_reason') or '')

    score_info = _compute_positive_t_action_score(out, details) if sig_type == 'positive_t' else _compute_reverse_t_action_score(out, details)
    score = _clamp_score(score_info.get('score', out.get('strength', 0)))
    exec_cfg = _get_t0_exec_effective_cfg()
    qty_cfg = exec_cfg.get('action_qty_multiplier') if isinstance(exec_cfg.get('action_qty_multiplier'), dict) else {}
    live_qty_scale = max(0.0, _safe_float_value(exec_cfg.get('live_qty_scale', 1.0), 1.0))
    action_level_before = score_to_action_level(score)
    action_level_after = str(action_level_before)
    qty_multiplier = float(qty_cfg.get(action_level_before, ACTION_QTY_MULTIPLIER.get(action_level_before, 0.0)) or ACTION_QTY_MULTIPLIER.get(action_level_before, 0.0))
    soft_qty_multiplier = 1.0
    downgrade_reasons: list[str] = []
    hard_block_reasons = _collect_t0_hard_block_reasons(sig_type, details)
    inventory = details.get('t0_inventory') if isinstance(details.get('t0_inventory'), dict) else {}
    base_action_qty = max(0, int(inventory.get('max_action_qty', 0) or 0))
    if base_action_qty <= 0:
        base_action_qty = max(0, int(inventory.get('suggested_action_qty', 0) or 0))
    lot_size = max(1, int(inventory.get('lot_size', _T0_INV_LOT_SIZE) or _T0_INV_LOT_SIZE))

    if hard_block_reasons:
        action_level_after = T0ActionLevel.BLOCK.value
        soft_qty_multiplier = 0.0
    else:
        action_level_after, soft_qty_multiplier, downgrade_reasons = _apply_soft_downgrades(
            sig_type,
            details,
            action_level_after,
            1.0,
        )

    shadow_action_level = ''
    shadow_only = False
    if not hard_block_reasons and action_level_before == T0ActionLevel.AGGRESSIVE.value:
        shadow_action_level = T0ActionLevel.AGGRESSIVE.value
        if bool(exec_cfg.get('aggressive_shadow_only', _T0_EXEC_AGGRESSIVE_SHADOW_ONLY)) and not bool(exec_cfg.get('enable_aggressive_live', _T0_EXEC_ENABLE_AGGRESSIVE_LIVE)):
            shadow_only = True
            action_level_after = T0ActionLevel.OBSERVE.value
            downgrade_reasons.append('aggressive_shadow_only')
        elif not bool(exec_cfg.get('enable_aggressive_live', _T0_EXEC_ENABLE_AGGRESSIVE_LIVE)):
            capped = cap_action_level(action_level_after, T0ActionLevel.EXECUTE.value)
            if capped != action_level_after:
                action_level_after = capped
                downgrade_reasons.append('aggressive_capped_execute')

    final_qty = 0
    if not hard_block_reasons and not shadow_only and action_level_after not in {T0ActionLevel.BLOCK.value, T0ActionLevel.OBSERVE.value}:
        final_qty = _t0_floor_to_lot(base_action_qty * qty_multiplier * soft_qty_multiplier * live_qty_scale, lot_size)
        if final_qty < lot_size:
            hard_block_reasons.append('qty_too_small_after_multiplier')
            final_qty = 0
            action_level_after = T0ActionLevel.OBSERVE.value

    execution_observe_only = bool(final_qty <= 0)
    if execution_observe_only and not hard_block_reasons and action_level_after == T0ActionLevel.BLOCK.value:
        hard_block_reasons.append('action_level_block')

    details['signal_observe_only_legacy'] = bool(legacy_observe_only)
    details['signal_observe_reason_legacy'] = legacy_observe_reason
    details['score'] = round(float(score), 2)
    details['base_action_qty'] = int(base_action_qty)
    details['final_qty'] = int(final_qty)
    details['qty_multiplier'] = round(float(qty_multiplier), 4)
    details['soft_qty_multiplier'] = round(float(soft_qty_multiplier), 4)
    details['live_qty_scale'] = round(float(live_qty_scale), 4)
    details['action_level_before_downgrade'] = str(action_level_before)
    details['action_level_after_downgrade'] = str(action_level_after)
    details['hard_block_reasons'] = list(hard_block_reasons)
    details['downgrade_reasons'] = list(dict.fromkeys(downgrade_reasons))
    details['shadow_action_level'] = str(shadow_action_level or '')
    details['shadow_only'] = bool(shadow_only)
    details['observe_only'] = bool(execution_observe_only)
    details['observe_reason'] = (
        hard_block_reasons[0]
        if hard_block_reasons
        else (details['downgrade_reasons'][0] if details['downgrade_reasons'] else (legacy_observe_reason if execution_observe_only else ''))
    )
    details['net_executable_edge_bps'] = round(float(score_info.get('net_executable_edge_bps', 0.0) or 0.0), 2)
    for key, value in score_info.items():
        if key == 'score':
            continue
        details[key] = round(float(value), 2)
    details['t0_execution'] = {
        'score': details['score'],
        'base_action_qty': int(base_action_qty),
        'final_qty': int(final_qty),
        'action_level_before_downgrade': str(action_level_before),
        'action_level_after_downgrade': str(action_level_after),
        'qty_multiplier': details['qty_multiplier'],
        'soft_qty_multiplier': details['soft_qty_multiplier'],
        'live_qty_scale': details['live_qty_scale'],
        'hard_block_reasons': list(hard_block_reasons),
        'downgrade_reasons': list(details['downgrade_reasons']),
        'shadow_action_level': str(shadow_action_level or ''),
        'shadow_only': bool(shadow_only),
        'runtime_override_updated_at': str((exec_cfg.get('override') or {}).get('updated_at') or ''),
    }
    out['message'] = _rewrite_t0_execution_message(
        str(out.get('message') or ''),
        action_level=str(action_level_after),
        final_qty=int(final_qty),
        shadow_action_level=str(shadow_action_level or ''),
        shadow_only=bool(shadow_only),
        hard_block_reasons=list(hard_block_reasons),
        downgrade_reasons=list(details['downgrade_reasons']),
    )
    return out


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

    # 按股票所属池分流
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
    index_context = load_t0_index_context(instrument_profile=instrument_profile, ts_code=ts_code)
    industry_name = str(_main_stock_industry.get(ts_code, '') or '')
    industry_code = str(_main_stock_industry_code.get(ts_code, '') or '').strip().upper()
    industry_ecology = dict(_main_industry_snapshot.get(industry_code, {}) or {}) if industry_code else {}
    if not industry_ecology and industry_name:
        industry_ecology = dict(_main_industry_snapshot.get(industry_name, {}) or {})
    concept_boards = list(_main_stock_concepts.get(ts_code, []) or [])
    concept_codes = list(_main_stock_concept_codes.get(ts_code, []) or [])
    core_concept_board = str(_main_stock_core_concept.get(ts_code, '') or '')
    core_concept_code = str(_main_stock_core_concept_code.get(ts_code, '') or '').strip().upper()
    concept_ecology = dict(_main_concept_snapshot.get(core_concept_code, {}) or {}) if core_concept_code else {}
    if not concept_ecology:
        concept_ecology = dict(_main_concept_snapshot.get(core_concept_board, {}) or {})
    concept_ecology_multi: list[dict] = []
    _concept_multi_seen: set[str] = set()
    for idx, concept_name in enumerate(concept_boards):
        board_code = str(concept_codes[idx] if idx < len(concept_codes) else '' or '').strip().upper()
        snapshot = dict(_main_concept_snapshot.get(board_code, {}) or {}) if board_code else {}
        if not snapshot:
            snapshot = dict(_main_concept_snapshot.get(str(concept_name or ''), {}) or {})
        if not snapshot:
            continue
        if concept_name and not snapshot.get('concept_name'):
            snapshot['concept_name'] = str(concept_name)
        if board_code and not snapshot.get('board_code'):
            snapshot['board_code'] = board_code
        multi_key = f"{str(snapshot.get('board_code') or board_code)}|{str(snapshot.get('concept_name') or concept_name)}"
        if multi_key in _concept_multi_seen:
            continue
        _concept_multi_seen.add(multi_key)
        concept_ecology_multi.append(snapshot)
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
                    'industry': industry_name,
                    'industry_code': industry_code,
                    'industry_ecology': industry_ecology,
                    'name': tick.get('name'),
                    'market_name': instrument_profile.get('market_name'),
                    'list_date': instrument_profile.get('list_date'),
                    'concept_boards': concept_boards,
                    'concept_codes': concept_codes,
                    'core_concept_board': core_concept_board,
                    'core_concept_code': core_concept_code,
                    'concept_ecology': concept_ecology,
                    'concept_ecology_multi': concept_ecology_multi,
                    'instrument_profile': instrument_profile,
                    'index_context': index_context,
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

    intraday_state = _update_intraday_state(ts_code, tick) if (1 in pools or 2 in pools) else None
    pool1_vwap = _compute_vwap(intraday_state) if isinstance(intraday_state, dict) else None
    pool1_short_zscore = _compute_price_zscore(intraday_state) if isinstance(intraday_state, dict) else None
    pool1_intraday_prices = None
    pool1_bias_vwap = None
    pool1_minute_bars_1m = None
    if isinstance(intraday_state, dict):
        try:
            pw = list(intraday_state.get('price_window') or [])
            prices = [float(p) for _, p, _ in pw if float(p or 0) > 0]
            pool1_intraday_prices = prices or None
            if pw:
                pool1_minute_bars_1m = [
                    {
                        'timestamp': int(ts),
                        'close': float(px),
                        'high': float(px),
                        'low': float(px),
                        'open': float(px),
                        'volume': int(max(0, dv)),
                        'amount': float(px) * float(max(0, dv)),
                    }
                    for ts, px, dv in pw
                    if float(px or 0) > 0
                ]
        except Exception:
            pool1_intraday_prices = None
            pool1_minute_bars_1m = None
    if pool1_vwap and pool1_vwap > 0 and price > 0:
        try:
            pool1_bias_vwap = (price - pool1_vwap) / pool1_vwap * 100.0
        except Exception:
            pool1_bias_vwap = None

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
        p1_industry_code = str(_main_stock_industry_code.get(ts_code, '') or '').strip().upper()
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
            rebuild_cfg = dict((_P1_CFG or {}).get('rebuild') or {})
            rebuild_enabled = bool(rebuild_cfg.get('enabled', False))
            rebuild_require_stage1 = bool(rebuild_cfg.get('require_stage1_pass', True))
            rebuild_allow_left = bool(rebuild_cfg.get('allow_left_side_buy', True))
            rebuild_allow_right = bool(rebuild_cfg.get('allow_right_side_breakout', True))
            rebuild_add_ratio = max(0.0, min(1.0, float(rebuild_cfg.get('add_ratio', 0.5) or 0.5)))
            rebuild_observe_tier_add_ratio_bias = float(
                rebuild_cfg.get('observe_tier_add_ratio_bias', 0.15) or 0.15
            )
            rebuild_full_tier_add_ratio_bias = float(
                rebuild_cfg.get('full_tier_add_ratio_bias', -0.15) or -0.15
            )
            rebuild_soft_source_add_ratio_bias = float(
                rebuild_cfg.get('soft_source_add_ratio_bias', 0.10) or 0.10
            )
            rebuild_core_source_add_ratio_bias = float(
                rebuild_cfg.get('core_source_add_ratio_bias', -0.15) or -0.15
            )
            rebuild_min_gap = max(0.0, min(1.0, float(rebuild_cfg.get('min_position_gap', 0.12) or 0.12)))
            rebuild_min_strength = float(rebuild_cfg.get('min_signal_strength', 78.0) or 78.0)
            rebuild_min_delay_sec = max(0.0, float(rebuild_cfg.get('min_minutes_after_reduce', 20.0) or 20.0) * 60.0)
            rebuild_full_tier_extra_delay_sec = max(
                0.0, float(rebuild_cfg.get('full_tier_extra_delay_minutes', 20.0) or 20.0) * 60.0
            )
            rebuild_core_source_extra_delay_sec = max(
                0.0, float(rebuild_cfg.get('core_source_extra_delay_minutes', 15.0) or 15.0) * 60.0
            )
            position_ratio_before = max(0.0, min(1.0, float(pos_state_before.get('position_ratio', 0.0) or 0.0)))
            position_gap = max(0.0, 1.0 - position_ratio_before)
            last_reduce_at = int(pos_state_before.get('last_reduce_at', 0) or 0)
            last_reduce_source = str(
                pos_state_before.get('last_reduce_source')
                or pos_state_before.get('last_exit_reduce_source')
                or ''
            ).strip()
            last_reduce_avwap_tier = str(
                pos_state_before.get('last_reduce_avwap_tier')
                or pos_state_before.get('last_exit_reduce_avwap_tier')
                or ''
            ).strip().lower()
            last_reduce_avwap_reason = str(
                pos_state_before.get('last_reduce_avwap_reason')
                or pos_state_before.get('last_exit_reduce_avwap_reason')
                or ''
            ).strip()
            rebuild_add_ratio_bias = 0.0
            reduce_source_tokens = {x for x in last_reduce_source.split('+') if x}
            if last_reduce_avwap_tier == 'observe':
                rebuild_add_ratio_bias += rebuild_observe_tier_add_ratio_bias
            elif last_reduce_avwap_tier == 'full':
                rebuild_add_ratio_bias += rebuild_full_tier_add_ratio_bias
            if {'session_avwap', 'entry_cost_anchor', 'avwap_soft_weak'} & reduce_source_tokens:
                rebuild_add_ratio_bias += rebuild_soft_source_add_ratio_bias
            if {'entry_day_avwap', 'avwap_flow_shift'} & reduce_source_tokens:
                rebuild_add_ratio_bias += rebuild_core_source_add_ratio_bias
            rebuild_add_ratio_effective = max(0.15, min(1.0, rebuild_add_ratio + rebuild_add_ratio_bias))
            rebuild_delay_bias_sec = 0.0
            if last_reduce_avwap_tier == 'full':
                rebuild_delay_bias_sec += rebuild_full_tier_extra_delay_sec
            if {'entry_day_avwap', 'avwap_flow_shift'} & reduce_source_tokens:
                rebuild_delay_bias_sec += rebuild_core_source_extra_delay_sec
            rebuild_min_delay_effective_sec = max(0.0, rebuild_min_delay_sec + rebuild_delay_bias_sec)
            resonance_60m, resonance_60m_info = _pool1_resonance_60m(ts_code, daily, price, prev_price)
            anchor_bars_1m = list(pool1_minute_bars_1m or []) or None
            chip_feat = dict(_main_chip_cache.get(ts_code, {}))
            chip_feat.update({
                'chip_concentration_pct': daily.get('chip_concentration_pct'),
                'winner_rate': daily.get('winner_rate'),
            })
            chip_bonus, chip_info = sig.compute_pool1_chip_bonus(chip_feat)
            clear_sig = sig.detect_timing_clear(
                price=price,
                boll_mid=boll_mid,
                boll_upper=boll_upper,
                ma5=daily.get('ma5'),
                ma10=daily.get('ma10'),
                ma20=daily.get('ma20'),
                atr_14=daily.get('atr_14'),
                rsi6=rsi6,
                pct_chg=pct_chg,
                volume_ratio=volume_ratio,
                bid_ask_ratio=bid_ask_ratio,
                resonance_60m=resonance_60m,
                vwap=pool1_vwap,
                big_order_bias=ms_pool1.get('big_order_bias'),
                super_order_bias=ms_pool1.get('super_order_bias'),
                big_net_flow_bps=ms_pool1.get('big_net_flow_bps'),
                super_net_flow_bps=ms_pool1.get('super_net_flow_bps'),
                volume_pace_ratio=p1_vol_pace.get('ratio'),
                volume_pace_state=p1_vol_pace.get('state'),
                last_buy_price=pos_state_before.get('last_buy_price'),
                last_buy_at=pos_state_before.get('last_buy_at'),
                minute_bars_1m=anchor_bars_1m,
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
            clear_sig_actionable = bool(
                clear_sig.get('has_signal')
                and not bool(((clear_sig.get('details') if isinstance(clear_sig.get('details'), dict) else {}) or {}).get('observe_only', False))
            )
            rebuild_delay_ok = last_reduce_at > 0 and float(now_ts - last_reduce_at) >= rebuild_min_delay_effective_sec
            rebuild_allowed = (
                rebuild_enabled
                and not clear_sig_actionable
                and position_ratio_before > 0
                and position_gap >= rebuild_min_gap
                and rebuild_delay_ok
                and (stage1_pass or not rebuild_require_stage1)
            )
            if rebuild_allowed:
                p1_left_th = _get_th('left_side_buy', p1_ctx)
                p1_right_th = _get_th('right_side_breakout', p1_ctx)
                if isinstance(p1_left_th, dict):
                    p1_regime = str(p1_left_th.get('regime') or p1_regime or '')
                if not p1_regime and isinstance(p1_right_th, dict):
                    p1_regime = str(p1_right_th.get('regime') or '')

                def _finalize_rebuild_signal(base_sig: dict, thresholds: Optional[dict]) -> Optional[dict]:
                    if not isinstance(base_sig, dict) or not base_sig.get('has_signal'):
                        return None
                    base_sig['strength'] = min(100, int(base_sig.get('strength', 0)) + stage1_bonus + chip_bonus)
                    base_sig = sig.finalize_pool1_signal(base_sig, thresholds=thresholds)
                    if not base_sig.get('has_signal'):
                        return None
                    base_sig = _attach_signal_channel_meta(base_sig, pool_id=1)
                    details = base_sig.setdefault('details', {})
                    details['rebuild_mode'] = True
                    details['rebuild_add_ratio'] = round(float(rebuild_add_ratio_effective), 4)
                    details['rebuild_add_ratio_base'] = round(float(rebuild_add_ratio), 4)
                    details['rebuild_add_ratio_bias'] = round(float(rebuild_add_ratio_bias), 4)
                    details['rebuild_min_delay_base_min'] = round(float(rebuild_min_delay_sec / 60.0), 2)
                    details['rebuild_min_delay_effective_min'] = round(float(rebuild_min_delay_effective_sec / 60.0), 2)
                    details['rebuild_delay_bias_min'] = round(float(rebuild_delay_bias_sec / 60.0), 2)
                    details['rebuild_position_gap'] = round(float(position_gap), 4)
                    details['rebuild_min_strength'] = round(float(rebuild_min_strength), 2)
                    details['rebuild_linked_partial_source'] = last_reduce_source or None
                    details['rebuild_linked_partial_tier'] = last_reduce_avwap_tier or None
                    details['rebuild_linked_partial_reason'] = last_reduce_avwap_reason or None
                    details['pool1_stage'] = {
                        'daily_screening': stage1_info,
                        'stage2_triggered': True,
                        'chip': chip_info,
                        'resonance_60m': bool(resonance_60m),
                        'resonance_60m_info': resonance_60m_info,
                        'position_mode': 'holding_rebuild',
                    }
                    try:
                        sig_strength = float(base_sig.get('current_strength', base_sig.get('strength', 0)) or 0.0)
                    except Exception:
                        sig_strength = 0.0
                    if sig_strength < rebuild_min_strength:
                        details['observe_only'] = True
                        details.setdefault('observe_reason', 'rebuild_strength_insufficient')
                        details['score_status'] = 'observe'
                        msg = str(base_sig.get('message') or '').replace('·可执行', '').replace('可执行', '').strip('·')
                        if '回补观察' not in msg:
                            base_sig['message'] = f'{msg}·回补观察' if msg else '回补观察'
                    else:
                        details['score_status'] = 'observe' if bool(details.get('observe_only', False)) else 'executable'
                        suffix = '回补观察' if bool(details.get('observe_only', False)) else '回补'
                        msg = str(base_sig.get('message') or '')
                        if suffix not in msg:
                            base_sig['message'] = f'{msg}·{suffix}' if msg else suffix
                    return base_sig

                if rebuild_allow_left:
                    left_rebuild = sig.detect_left_side_buy(
                        price=price, boll_upper=boll_upper, boll_mid=boll_mid, boll_lower=boll_lower,
                        rsi6=rsi6, pct_chg=pct_chg,
                        vwap=pool1_vwap,
                        bias_vwap=pool1_bias_vwap,
                        intraday_prices=pool1_intraday_prices,
                        short_zscore=pool1_short_zscore,
                        bid_ask_ratio=bid_ask_ratio,
                        lure_long=ms_pool1.get('lure_long', False),
                        wash_trade=ms_pool1.get('wash_trade', False),
                        up_limit=up_limit, down_limit=down_limit,
                        resonance_60m=resonance_60m,
                        volume_pace_ratio=p1_vol_pace.get('ratio'),
                        volume_pace_state=p1_vol_pace.get('state'),
                        thresholds=p1_left_th,
                        prev_price=prev_price,
                        ask_wall_absorb_ratio=ms_pool1.get('ask_wall_absorb_ratio'),
                        super_net_flow_bps=ms_pool1.get('super_net_flow_bps'),
                        minute_bars_1m=anchor_bars_1m,
                        industry=p1_industry,
                        industry_ecology=industry_ecology,
                        core_concept_board=core_concept_board,
                        concept_boards=concept_boards,
                        concept_codes=concept_codes,
                        core_concept_code=core_concept_code,
                        concept_ecology=concept_ecology,
                        concept_ecology_multi=concept_ecology_multi,
                        position_state=pos_state_before,
                    )
                    left_rebuild = _finalize_rebuild_signal(left_rebuild, p1_left_th)
                    if left_rebuild:
                        fired_pool1.append(left_rebuild)

                if rebuild_allow_right:
                    right_rebuild = sig.detect_right_side_breakout(
                        price=price, boll_upper=boll_upper, boll_mid=boll_mid, boll_lower=boll_lower,
                        volume_ratio=volume_ratio,
                        ma5=daily.get('ma5'), ma10=daily.get('ma10'), rsi6=rsi6,
                        vwap=pool1_vwap,
                        prev_price=prev_price,
                        bid_ask_ratio=bid_ask_ratio,
                        lure_long=ms_pool1.get('lure_long', False),
                        wash_trade=ms_pool1.get('wash_trade', False),
                        up_limit=up_limit, down_limit=down_limit,
                        resonance_60m=resonance_60m,
                        volume_pace_ratio=p1_vol_pace.get('ratio'),
                        volume_pace_state=p1_vol_pace.get('state'),
                        thresholds=p1_right_th,
                        minute_bars_1m=anchor_bars_1m,
                        industry=p1_industry,
                        industry_code=p1_industry_code,
                        industry_ecology=industry_ecology,
                        core_concept_board=core_concept_board,
                        concept_boards=concept_boards,
                        concept_codes=concept_codes,
                        core_concept_code=core_concept_code,
                        concept_ecology=concept_ecology,
                        concept_ecology_multi=concept_ecology_multi,
                    )
                    right_rebuild = _finalize_rebuild_signal(right_rebuild, p1_right_th)
                    if right_rebuild:
                        fired_pool1.append(right_rebuild)
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
                    vwap=pool1_vwap,
                    bias_vwap=pool1_bias_vwap,
                    intraday_prices=pool1_intraday_prices,
                    short_zscore=pool1_short_zscore,
                    bid_ask_ratio=bid_ask_ratio,
                    lure_long=ms_pool1.get('lure_long', False),
                    wash_trade=ms_pool1.get('wash_trade', False),
                    up_limit=up_limit, down_limit=down_limit,
                    resonance_60m=resonance_60m,
                    volume_pace_ratio=p1_vol_pace.get('ratio'),
                    volume_pace_state=p1_vol_pace.get('state'),
                    thresholds=p1_left_th,
                    prev_price=prev_price,
                    ask_wall_absorb_ratio=ms_pool1.get('ask_wall_absorb_ratio'),
                    super_net_flow_bps=ms_pool1.get('super_net_flow_bps'),
                    industry=p1_industry,
                    industry_code=p1_industry_code,
                    industry_ecology=industry_ecology,
                    core_concept_board=core_concept_board,
                    concept_boards=concept_boards,
                    concept_codes=concept_codes,
                    core_concept_code=core_concept_code,
                    concept_ecology=concept_ecology,
                    concept_ecology_multi=concept_ecology_multi,
                    position_state=pos_state_before,
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
                    vwap=pool1_vwap,
                    prev_price=prev_price,
                    bid_ask_ratio=bid_ask_ratio,
                    lure_long=ms_pool1.get('lure_long', False),
                    wash_trade=ms_pool1.get('wash_trade', False),
                    up_limit=up_limit, down_limit=down_limit,
                    resonance_60m=resonance_60m,
                    volume_pace_ratio=p1_vol_pace.get('ratio'),
                    volume_pace_state=p1_vol_pace.get('state'),
                    thresholds=p1_right_th,
                    minute_bars_1m=pool1_minute_bars_1m,
                    industry=p1_industry,
                    industry_code=p1_industry_code,
                    industry_ecology=industry_ecology,
                    core_concept_board=core_concept_board,
                    concept_boards=concept_boards,
                    concept_codes=concept_codes,
                    core_concept_code=core_concept_code,
                    concept_ecology=concept_ecology,
                    concept_ecology_multi=concept_ecology_multi,
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
                board_segment=str(instrument_profile.get('board_segment') or ''),
                security_type=str(instrument_profile.get('security_type') or ''),
                listing_stage=str(instrument_profile.get('listing_stage') or ''),
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
                best_sell_details = best_sell.get('details') if isinstance(best_sell.get('details'), dict) else {}
                clear_level = str(best_sell_details.get('clear_level') or best_sell_details.get('clear_level_hint') or '').strip().lower()
                try:
                    reduce_ratio = float(best_sell_details.get('suggest_reduce_ratio', 0.0) or 0.0)
                except Exception:
                    reduce_ratio = 0.0
                reduce_meta = {
                    'source': str(best_sell_details.get('reduce_ratio_source') or '').strip(),
                    'avwap_tier': str(best_sell_details.get('avwap_exit_tier') or '').strip().lower(),
                    'avwap_reason': str(best_sell_details.get('avwap_exit_reason') or '').strip(),
                }
                pos_state_after = _pool1_set_position_state(
                    ts_code,
                    'holding' if clear_level == 'partial' else 'observe',
                    now_ts=now_ts,
                    signal_type=str(best_sell.get('type') or ''),
                    signal_price=float(best_sell.get('price', price) or price),
                    clear_level=clear_level,
                    reduce_ratio=reduce_ratio,
                    reduce_meta=reduce_meta,
                )
                after_status = str(pos_state_after.get('status') or 'observe')
                if clear_level == 'partial' and after_status == 'holding':
                    reduce_streak_after = int(pos_state_after.get('reduce_streak', 0) or 0)
                    transition = 'holding->holding(partial_chain)' if reduce_streak_after >= 2 else 'holding->holding(partial)'
                elif clear_level == 'partial' and after_status == 'observe':
                    transition = 'holding->observe(after_partial_exhausted)'
                elif bool(pos_state_after.get('last_exit_after_partial', False)):
                    transition = 'holding->observe(after_partial)'
                else:
                    transition = 'holding->observe'
            else:
                rebuild_buys = [
                    s for s in fired_pool1
                    if str(s.get('type') or '') in _POOL1_BUY_SIGNAL_TYPES
                    and _pool1_actionable(s)
                    and bool(((s.get('details') if isinstance(s.get('details'), dict) else {}) or {}).get('rebuild_mode', False))
                ]
                if rebuild_buys:
                    best_rebuild = max(rebuild_buys, key=lambda x: float(x.get('strength', 0) or 0))
                    best_rebuild_details = best_rebuild.get('details') if isinstance(best_rebuild.get('details'), dict) else {}
                    try:
                        rebuild_ratio = float(best_rebuild_details.get('rebuild_add_ratio', 0.0) or 0.0)
                    except Exception:
                        rebuild_ratio = 0.0
                    pos_state_after = _pool1_set_position_state(
                        ts_code,
                        'holding',
                        now_ts=now_ts,
                        signal_type=str(best_rebuild.get('type') or ''),
                        signal_price=float(best_rebuild.get('price', price) or price),
                        rebuild_ratio=rebuild_ratio,
                        rebuild_meta={
                            'base_add_ratio': round(float(rebuild_add_ratio), 4),
                            'add_ratio_bias': round(float(rebuild_add_ratio_bias), 4),
                        },
                    )
                    if str(pos_state_after.get('last_rebuild_transition') or '') == 'holding->holding(rebuild)':
                        transition = 'holding->holding(rebuild)'
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
                if int(pos_state_after.get('last_rebuild_at', 0) or 0) == now_ts and int(pos_state_after.get('last_rebuild_from_partial_count', 0) or 0) > 0:
                    transition = 'observe->holding(rebuild_after_partial)'
                else:
                    transition = 'observe->holding'

        for s in fired_pool1:
            d = s.setdefault('details', {})
            d['pool1_position'] = {
                'status_before': pos_state_before.get('status', 'observe'),
                'status_after': pos_state_after.get('status', pos_state_before.get('status', 'observe')),
                'position_ratio_before': round(float(pos_state_before.get('position_ratio', 0.0) or 0.0), 4),
                'position_ratio_after': round(float(pos_state_after.get('position_ratio', pos_state_before.get('position_ratio', 0.0)) or 0.0), 4),
                'holding_days_before': round(holding_days_before, 4),
                'holding_days_after': round(float(pos_state_after.get('holding_days', holding_days_before) or 0.0), 4),
                'reduce_streak_before': int(pos_state_before.get('reduce_streak', 0) or 0),
                'reduce_streak_after': int(pos_state_after.get('reduce_streak', pos_state_before.get('reduce_streak', 0)) or 0),
                'reduce_ratio_cum_before': round(float(pos_state_before.get('reduce_ratio_cum', 0.0) or 0.0), 4),
                'reduce_ratio_cum_after': round(float(pos_state_after.get('reduce_ratio_cum', pos_state_before.get('reduce_ratio_cum', 0.0)) or 0.0), 4),
                'last_reduce_source': str(pos_state_after.get('last_reduce_source', '') or ''),
                'last_reduce_avwap_tier': str(pos_state_after.get('last_reduce_avwap_tier', '') or ''),
                'last_rebuild_at': int(pos_state_after.get('last_rebuild_at', 0) or 0),
                'last_rebuild_transition': str(pos_state_after.get('last_rebuild_transition', '') or ''),
                'last_rebuild_add_ratio': round(float(pos_state_after.get('last_rebuild_add_ratio', 0.0) or 0.0), 4),
                'last_rebuild_add_ratio_base': round(float(pos_state_after.get('last_rebuild_add_ratio_base', 0.0) or 0.0), 4),
                'last_rebuild_add_ratio_bias': round(float(pos_state_after.get('last_rebuild_add_ratio_bias', 0.0) or 0.0), 4),
                'last_rebuild_from_partial_count': int(pos_state_after.get('last_rebuild_from_partial_count', 0) or 0),
                'last_rebuild_from_partial_ratio': round(float(pos_state_after.get('last_rebuild_from_partial_ratio', 0.0) or 0.0), 4),
                'last_rebuild_from_partial_source': str(pos_state_after.get('last_rebuild_from_partial_source', '') or ''),
                'last_rebuild_from_partial_avwap_tier': str(pos_state_after.get('last_rebuild_from_partial_avwap_tier', '') or ''),
                'last_exit_after_partial': bool(pos_state_after.get('last_exit_after_partial', False)),
                'last_exit_partial_count': int(pos_state_after.get('last_exit_partial_count', 0) or 0),
                'last_exit_reduce_ratio_cum': round(float(pos_state_after.get('last_exit_reduce_ratio_cum', 0.0) or 0.0), 4),
                'last_exit_reduce_source': str(pos_state_after.get('last_exit_reduce_source', '') or ''),
                'last_exit_reduce_avwap_tier': str(pos_state_after.get('last_exit_reduce_avwap_tier', '') or ''),
                'transition': transition,
            }

    # ===== Pool2：T+0 日内 =====
    if 2 in pools and ts_code:
        now_ts = int(tick.get('timestamp') or time.time())
        pool2_inventory_before = _pool2_get_t0_inventory_state(ts_code, now_ts=now_ts)
        state = intraday_state or _update_intraday_state(ts_code, tick)
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
                    liquidity_drain=feat.get('liquidity_drain', False),
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
                    position_state=pool2_inventory_before,
                    industry_ecology=industry_ecology,
                    concept_ecology=concept_ecology,
                    concept_ecology_multi=concept_ecology_multi,
                    index_context=index_context,
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
                    industry_ecology=industry_ecology,
                    concept_ecology=concept_ecology,
                    concept_ecology_multi=concept_ecology_multi,
                    index_context=index_context,
                    ts_code=ts_code,
                    thresholds=t0_rev_th,
                    position_state=pool2_inventory_before,
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

    if fired_pool2:
        fired_pool2 = [_finalize_pool2_execution_signal(s) for s in fired_pool2]

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

    for s in next_signals:
        sig_type = str(s.get('type') or '')
        if sig_type not in {'positive_t', 'reverse_t'} or not s.get('is_new'):
            continue
        details = s.get('details') if isinstance(s.get('details'), dict) else {}
        final_qty = int(details.get('final_qty', 0) or 0)
        shadow_only = bool(details.get('shadow_only', False))
        if bool(details.get('observe_only', False)) and final_qty <= 0:
            continue
        if shadow_only or final_qty <= 0:
            continue
        inventory_info = details.get('t0_inventory') if isinstance(details.get('t0_inventory'), dict) else {}
        action_qty = final_qty if final_qty > 0 else int(inventory_info.get('suggested_action_qty', 0) or 0)
        if action_qty <= 0:
            continue
        before_inventory, after_inventory = _pool2_apply_t0_signal(
            ts_code,
            signal_type=sig_type,
            signal_price=float(s.get('price', tick.get('price', 0)) or 0.0),
            action_qty=action_qty,
            action_level=str(details.get('action_level_after_downgrade') or ''),
            shadow_action_level=str(details.get('shadow_action_level') or ''),
            shadow_only=bool(details.get('shadow_only', False)),
            qty_multiplier=details.get('qty_multiplier'),
            soft_qty_multiplier=details.get('soft_qty_multiplier'),
            base_action_qty=details.get('base_action_qty'),
            final_qty=details.get('final_qty'),
            score=details.get('score'),
            now_ts=now,
        )
        inventory_info = dict(inventory_info)
        inventory_info['applied_action_qty'] = int(action_qty)
        inventory_info['executed'] = True
        inventory_info['state_before'] = before_inventory
        inventory_info['state_after'] = after_inventory
        details['t0_inventory'] = inventory_info
        details['t0_action'] = {
            'mode': str(inventory_info.get('action_path') or ''),
            'role': str(inventory_info.get('action_role') or ''),
            'family': 'base_rebalance',
            'applied_action_qty': int(action_qty),
            'action_level': str(details.get('action_level_after_downgrade') or ''),
            'inventory_unchanged_expected': True,
        }
        s['details'] = details

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
        if persist and gm_common._persist_queue is not None:
            try:
                gm_common._persist_queue.put_nowait(('signal', ts_code, tick, next_signals))
            except Exception:
                pass

    return {
        'signals': next_signals,
        'new_count': new_count,
        'new_types': [s.get('type') for s in next_signals if s.get('is_new')],
    }


def _state_refresh_tick_snapshot(ts_code: str, cached_entry: Optional[dict] = None) -> dict:
    """构造状态机刷新所需的最小 tick 快照，不触发网络请求。"""
    tick = _main_cache.get(ts_code)
    if isinstance(tick, dict):
        return _with_pre_close_fallback(ts_code, dict(tick))
    entry = cached_entry if isinstance(cached_entry, dict) else (_main_signal_cache.get(ts_code) or {})
    price = float(entry.get('price', 0) or 0)
    pct_chg = float(entry.get('pct_chg', 0) or 0)
    return {
        'ts_code': ts_code,
        'name': str(entry.get('name') or ts_code),
        'price': price,
        'open': price,
        'high': price,
        'low': price,
        'pre_close': 0,
        'volume': 0,
        'amount': 0,
        'pct_chg': pct_chg,
        'timestamp': int(time.time()),
        'bids': [(0, 0)] * 5,
        'asks': [(0, 0)] * 5,
        'is_mock': False,
        'price_source': 'state_refresh_cache',
    }


def _start_signal_state_refresh_thread():
    """午休/尾盘无新成交时，按时间推进信号状态机。"""
    global _signal_state_refresh_started
    if _signal_state_refresh_started or not _SIGNAL_STATE_REFRESH_ENABLED:
        return
    _signal_state_refresh_started = True

    def _loop():
        logger.info(
            f"signal state refresh thread started interval={_SIGNAL_STATE_REFRESH_INTERVAL_SEC:.1f}s "
            f"policies={sorted(_SIGNAL_STATE_REFRESH_POLICIES)}"
        )
        while True:
            try:
                now_ts = int(time.time())
                policy = _get_session_policy(now_ts)
                if policy not in _SIGNAL_STATE_REFRESH_POLICIES:
                    time.sleep(max(2.0, _SIGNAL_STATE_REFRESH_INTERVAL_SEC))
                    continue
                snapshot = list(_main_signal_cache.items())
                refreshed = 0
                for ts_code, entry in snapshot:
                    if not isinstance(entry, dict):
                        continue
                    signals = entry.get('signals')
                    if not isinstance(signals, list) or len(signals) <= 0:
                        continue
                    tick = _state_refresh_tick_snapshot(ts_code, entry)
                    _postprocess_fired_signals(
                        ts_code,
                        tick,
                        [],
                        now=now_ts,
                        persist=False,
                    )
                    refreshed += 1
                if refreshed > 0:
                    logger.debug(f"[state-refresh] policy={policy} refreshed={refreshed}")
            except Exception as e:
                logger.warning(f"signal state refresh failed: {e}")
            time.sleep(max(2.0, _SIGNAL_STATE_REFRESH_INTERVAL_SEC))

    t = threading.Thread(target=_loop, daemon=True, name='signal-state-refresh')
    t.start()


__all__ = [name for name in globals() if not name.startswith("__")]
