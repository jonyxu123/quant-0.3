"""
盘口信号计算框架

两个监控池：
  1号：择时监控池（趋势策略建仓）
  - 左侧买入信号：价格贴近/跌破布林下轨 + RSI 超卖 + 确认项
  - 右侧突破信号：价格突破中轨/上轨 + 量能/结构确认
  - T+0 正T：低吸高抛，结合 VWAP 偏离、GUB5 转向与盘口确认
  - T+0 反T：高抛低吸，结合 Z-Score、上轨突破与风险门控

本文提供信号计算框架，实际计算需结合实时与日线数据。
- 数据源：日线因子来自 DuckDB；实时 tick/分钟线/逐笔来自 mootdx。
每个信号函数返回：
    {
        'has_signal': bool,
        'type': str,         # 信号类型
        'strength': float,   # 0-100 强度分数
        'message': str,      # 可读描述
        'triggered_at': int, # 触发时间戳；'details': dict 计算细节
    }
"""
from __future__ import annotations

from typing import Optional
import time
import statistics
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
try:
    from config import T0_SIGNAL_CONFIG as _T0_CFG
except Exception:
    _T0_CFG = {}

try:
    from config import POOL1_STAGE2_CONFIG as _P1_STAGE2_CFG
except Exception:
    _P1_STAGE2_CFG = {}

try:
    from config import POOL1_SIGNAL_CONFIG as _P1_CFG
except Exception:
    _P1_CFG = {}

try:
    from backend.realtime.threshold_engine import (
        build_signal_context as _build_signal_context,
        get_thresholds as _get_thresholds,
    )
except Exception:
    _build_signal_context = None
    _get_thresholds = None

_ENABLED_V2       = _T0_CFG.get('enabled_v2', True)

_TRIGGER_CFG      = _T0_CFG.get('trigger', {})
_BIAS_VWAP_PCT    = _TRIGGER_CFG.get('bias_vwap_pct', -1.5)
_ROBUST_ZSCORE_TH = _TRIGGER_CFG.get('robust_zscore', 2.2)
_EPS_BREAK        = _TRIGGER_CFG.get('eps_break', 0.003)   # 下轨穿透系数
_EPS_OVER         = _TRIGGER_CFG.get('eps_over', 0.002)    # 上轨突破系数

_CONFIRM_CFG      = _T0_CFG.get('confirm', {})
_BID_ASK_TH       = _CONFIRM_CFG.get('bid_ask_ratio', 1.15)
_ASK_ABSORB_TH    = _CONFIRM_CFG.get('ask_wall_absorb_ratio', 0.6)
_BID_WALL_BREAK_TH = float(_CONFIRM_CFG.get('bid_wall_break_ratio', 0.75) or 0.75)
_GUB5_TRANSITIONS = set(_CONFIRM_CFG.get('gub5_transitions', ('up->flat', 'up->down')))
_SUPER_NET_INFLOW_BUY_BPS = float(_CONFIRM_CFG.get('super_net_inflow_buy_bps', 120) or 120)
_SUPER_NET_INFLOW_SELL_BPS = float(_CONFIRM_CFG.get('super_net_inflow_sell_bps', 120) or 120)
_SUPER_ORDER_BIAS_BUY_TH = float(_CONFIRM_CFG.get('super_order_bias_buy_th', 0.20) or 0.20)
_SUPER_ORDER_BIAS_SELL_TH = float(_CONFIRM_CFG.get('super_order_bias_sell_th', 0.20) or 0.20)

_VETO_CFG         = _T0_CFG.get('veto', {})
_SPOOFING_RATIO   = _VETO_CFG.get('spoofing_vol_ratio', 3.0)
_WASH_BIAS_FLOOR  = _VETO_CFG.get('wash_trade_bias_floor', 0.05)

_SCORING_CFG      = _T0_CFG.get('scoring', {})
_SCORE_EXECUTABLE = _SCORING_CFG.get('score_executable', 70)
_SCORE_OBSERVE    = _SCORING_CFG.get('score_observe', 55)
_MICRO_GATE_FLOOR = _SCORING_CFG.get('micro_gate_floor', 0.1)
_RISK_GATE_FLOOR  = _SCORING_CFG.get('risk_gate_floor', 0.3)

_HYSTERESIS_CFG   = _T0_CFG.get('hysteresis', {})
_HYST_POS_PCT     = _HYSTERESIS_CFG.get('positive_t_pct', 0.5) / 100.0
_HYST_REV_PCT     = _HYSTERESIS_CFG.get('reverse_t_pct', 0.5) / 100.0

# T+0 反复横跳抑制与费用覆盖约束
_ANTI_CHURN_CFG = _T0_CFG.get('anti_churn', {}) if isinstance(_T0_CFG, dict) else {}
_T0_ANTI_CHURN_ENABLED = bool(_ANTI_CHURN_CFG.get('enabled', True))
_T0_MIN_FLIP_SEC = int(_ANTI_CHURN_CFG.get('min_flip_sec', 120) or 120)
_T0_MIN_FLIP_SPREAD_PCT = float(_ANTI_CHURN_CFG.get('min_flip_spread_pct', 0.35) or 0.35)
_T0_ROUNDTRIP_FEE_BPS = float(_ANTI_CHURN_CFG.get('roundtrip_fee_bps', 16.0) or 16.0)
_T0_MIN_NET_EDGE_BPS = float(_ANTI_CHURN_CFG.get('min_net_edge_bps', 6.0) or 6.0)
_T0_MIN_EXPECTED_EDGE_BPS = float(_ANTI_CHURN_CFG.get('min_expected_edge_bps', 25.0) or 25.0)
_T0_REVERSE_BIDASK_BLOCK_TH = float(_ANTI_CHURN_CFG.get('reverse_bidask_block_th', 1.22) or 1.22)
_T0_REVERSE_VWAP_PREMIUM_BPS = float(_ANTI_CHURN_CFG.get('reverse_vwap_premium_bps', 18.0) or 18.0)
_T0_VOL_PACE_CFG = _T0_CFG.get('volume_pace', {}) if isinstance(_T0_CFG, dict) else {}
_T0_VOL_PACE_ENABLED = bool(_T0_VOL_PACE_CFG.get('enabled', True))
_T0_VOL_PACE_MIN_PROGRESS = float(_T0_VOL_PACE_CFG.get('min_progress', 0.12) or 0.12)
_T0_VOL_PACE_SHRINK_TH = float(_T0_VOL_PACE_CFG.get('shrink_th', 0.70) or 0.70)
_T0_VOL_PACE_EXPAND_TH = float(_T0_VOL_PACE_CFG.get('expand_th', 1.30) or 1.30)
_T0_VOL_PACE_SURGE_TH = float(_T0_VOL_PACE_CFG.get('surge_th', 2.00) or 2.00)
_T0_VOL_POS_SHRINK_PENALTY = int(_T0_VOL_PACE_CFG.get('positive_shrink_penalty', 8) or 8)
_T0_VOL_POS_EXPAND_BONUS = int(_T0_VOL_PACE_CFG.get('positive_expand_bonus', 4) or 4)
_T0_VOL_POS_SURGE_BONUS = int(_T0_VOL_PACE_CFG.get('positive_surge_bonus', 6) or 6)
_T0_VOL_REV_SHRINK_BONUS = int(_T0_VOL_PACE_CFG.get('reverse_shrink_bonus', 4) or 4)
_T0_VOL_REV_EXPAND_PENALTY = int(_T0_VOL_PACE_CFG.get('reverse_expand_penalty', 6) or 6)
_T0_VOL_REV_SURGE_PENALTY = int(_T0_VOL_PACE_CFG.get('reverse_surge_penalty', 10) or 10)
_T0_VOL_REV_SURGE_BIDASK_BLOCK_TH = float(
    _T0_VOL_PACE_CFG.get('reverse_surge_bidask_block_th', 1.10) or 1.10
)
_T0_TREND_GUARD_CFG = _T0_CFG.get('trend_guard', {}) if isinstance(_T0_CFG, dict) else {}
_T0_TREND_GUARD_ENABLED = bool(_T0_TREND_GUARD_CFG.get('enabled', True))
_T0_TREND_GUARD_VWAP_BPS_MIN = float(_T0_TREND_GUARD_CFG.get('price_above_vwap_bps_min', 15) or 15)
_T0_TREND_GUARD_PCT_CHG_MIN = float(_T0_TREND_GUARD_CFG.get('pct_chg_min', 1.2) or 1.2)
_T0_TREND_GUARD_BIDASK_MIN = float(_T0_TREND_GUARD_CFG.get('bid_ask_ratio_min', 1.08) or 1.08)
_T0_TREND_GUARD_BIG_BIAS_MIN = float(_T0_TREND_GUARD_CFG.get('big_order_bias_min', 0.12) or 0.12)
_T0_TREND_GUARD_SUPER_BIAS_MIN = float(_T0_TREND_GUARD_CFG.get('super_order_bias_min', 0.10) or 0.10)
_T0_TREND_GUARD_SUPER_INFLOW_BPS_MIN = float(_T0_TREND_GUARD_CFG.get('super_net_inflow_bps_min', 80) or 80)
_T0_TREND_GUARD_MIN_SCORE = int(_T0_TREND_GUARD_CFG.get('min_guard_score', 3) or 3)
_T0_TREND_GUARD_MIN_BEARISH = int(_T0_TREND_GUARD_CFG.get('min_bearish_confirms', 2) or 2)
_T0_TREND_GUARD_SURGE_MIN_BEARISH = int(
    _T0_TREND_GUARD_CFG.get('surge_min_bearish_confirms', 3) or 3
)
_T0_TREND_GUARD_SURGE_BLOCK_ENABLED = bool(_T0_TREND_GUARD_CFG.get('surge_absorb_block_enabled', True))
_T0_TREND_GUARD_SURGE_PCT_CHG_MIN = float(_T0_TREND_GUARD_CFG.get('surge_absorb_pct_chg_min', 1.8) or 1.8)
_T0_TREND_GUARD_SURGE_BIDASK_MIN = float(_T0_TREND_GUARD_CFG.get('surge_absorb_bidask_min', 1.10) or 1.10)
_T0_TREND_GUARD_SURGE_BIG_BIAS_MIN = float(_T0_TREND_GUARD_CFG.get('surge_absorb_big_bias_min', 0.10) or 0.10)
_T0_TREND_GUARD_SURGE_SUPER_INFLOW_BPS_MIN = float(
    _T0_TREND_GUARD_CFG.get('surge_absorb_super_inflow_bps_min', 100) or 100
)
_T0_TREND_GUARD_SURGE_MIN_BEARISH_HARD = int(
    _T0_TREND_GUARD_CFG.get('surge_absorb_min_bearish_confirms', 4) or 4
)

# T+0 滞回状态：{ts_code: {'positive_t': last_trigger_price, 'reverse_t': last_trigger_price}}
_hysteresis_state: dict[str, dict[str, float]] = {}
_t0_last_signal_state: dict[str, dict] = {}

_P1_STAGE2_ENABLED = bool(_P1_STAGE2_CFG.get('enabled', False))
_P1_CHIP_CFG = _P1_STAGE2_CFG.get('chip', {}) if isinstance(_P1_STAGE2_CFG, dict) else {}
_P1_CHIP_ENABLED = bool(_P1_CHIP_CFG.get('enabled', False))
_P1_CHIP_WEIGHT = int(_P1_CHIP_CFG.get('score_weight', 12) or 0)
_P1_CHIP_WINNER_MIN = float(_P1_CHIP_CFG.get('winner_rate_min', 0.45) or 0.45)
_P1_CHIP_BAND_MAX = float(_P1_CHIP_CFG.get('concentration_band_max_pct', 35) or 35)

_P1_ENABLED_V2 = bool(_P1_CFG.get('enabled_v2', True))
_P1_CONFIRM_CFG = _P1_CFG.get('confirm', {}) if isinstance(_P1_CFG, dict) else {}
_P1_BID_ASK_TH = float(_P1_CONFIRM_CFG.get('bid_ask_ratio', 1.12) or 1.12)
_P1_VETO_CFG = _P1_CFG.get('veto', {}) if isinstance(_P1_CFG, dict) else {}
_P1_VETO_LURE_LONG = bool(_P1_VETO_CFG.get('veto_lure_long', True))
_P1_VETO_WASH_TRADE = bool(_P1_VETO_CFG.get('veto_wash_trade', True))
_P1_DYNAMIC_CFG = _P1_CFG.get('dynamic', {}) if isinstance(_P1_CFG, dict) else {}
_P1_DYNAMIC_ENABLED = bool(_P1_DYNAMIC_CFG.get('enabled', True))
_P1_LIMIT_CFG = _P1_CFG.get('limit_magnet', {}) if isinstance(_P1_CFG, dict) else {}
_P1_LIMIT_ENABLED = bool(_P1_LIMIT_CFG.get('enabled', True))
_P1_LIMIT_TH_PCT = float(_P1_LIMIT_CFG.get('threshold_pct', 0.3) or 0.3)
_P1_RESONANCE_CFG = _P1_CFG.get('resonance_60m', {}) if isinstance(_P1_CFG, dict) else {}
_P1_RESONANCE_ENABLED = bool(_P1_RESONANCE_CFG.get('enabled', False))
_P1_SCORING_CFG = _P1_CFG.get('scoring', {}) if isinstance(_P1_CFG, dict) else {}
_P1_SCORE_EXECUTABLE = float(_P1_SCORING_CFG.get('score_executable', 70) or 70)
_P1_SCORE_OBSERVE = float(_P1_SCORING_CFG.get('score_observe', 55) or 55)
_P1_EXIT_CFG = _P1_CFG.get('exit', {}) if isinstance(_P1_CFG, dict) else {}
_P1_EXIT_ENABLED = bool(_P1_EXIT_CFG.get('enabled', True))
_P1_EXIT_MIN_HOLD_DAYS = float(_P1_EXIT_CFG.get('min_hold_days', 14.0) or 14.0)
_P1_EXIT_LONG_HOLD_DAYS = float(_P1_EXIT_CFG.get('long_hold_days', 14.0) or 14.0)
_P1_EXIT_LONG_HOLD_MIN_CONFIRM = int(_P1_EXIT_CFG.get('long_hold_min_confirm', 3) or 3)
_P1_EXIT_ALLOW_EARLY_RISK = bool(_P1_EXIT_CFG.get('allow_early_risk_exit', True))
_P1_EXIT_HARD_DROP_PCT = float(_P1_EXIT_CFG.get('hard_drop_pct', -5.0) or -5.0)
_P1_EXIT_RSI_WEAK_TH = float(_P1_EXIT_CFG.get('rsi_weak_th', 42) or 42)
_P1_EXIT_BIDASK_WEAK_RATIO = float(_P1_EXIT_CFG.get('bid_ask_weak_ratio', 0.95) or 0.95)
_P1_EXIT_BIG_BIAS_SELL_TH = float(_P1_EXIT_CFG.get('big_order_bias_sell_th', -0.25) or -0.25)
_P1_EXIT_SUPER_BIAS_SELL_TH = float(_P1_EXIT_CFG.get('super_order_bias_sell_th', -0.20) or -0.20)
_P1_EXIT_BIG_NET_OUT_BPS_TH = float(_P1_EXIT_CFG.get('big_net_outflow_bps_th', -80) or -80)
_P1_EXIT_SUPER_NET_OUT_BPS_TH = float(_P1_EXIT_CFG.get('super_net_outflow_bps_th', -120) or -120)
_P1_VOL_PACE_CFG = _P1_CFG.get('volume_pace', {}) if isinstance(_P1_CFG, dict) else {}
_P1_VOL_PACE_ENABLED = bool(_P1_VOL_PACE_CFG.get('enabled', True))
_P1_VOL_PACE_MIN_PROGRESS = float(_P1_VOL_PACE_CFG.get('min_progress', 0.15) or 0.15)
_P1_VOL_PACE_SHRINK_TH = float(_P1_VOL_PACE_CFG.get('shrink_th', 0.70) or 0.70)
_P1_VOL_PACE_EXPAND_TH = float(_P1_VOL_PACE_CFG.get('expand_th', 1.30) or 1.30)
_P1_VOL_PACE_SURGE_TH = float(_P1_VOL_PACE_CFG.get('surge_th', 2.00) or 2.00)
_P1_VOL_LEFT_SHRINK_PENALTY = int(_P1_VOL_PACE_CFG.get('left_shrink_penalty', 10) or 10)
_P1_VOL_LEFT_EXPAND_BONUS = int(_P1_VOL_PACE_CFG.get('left_expand_bonus', 4) or 4)
_P1_VOL_LEFT_SURGE_BONUS = int(_P1_VOL_PACE_CFG.get('left_surge_bonus', 6) or 6)
_P1_VOL_RIGHT_MIN_PACE = float(_P1_VOL_PACE_CFG.get('right_min_pace', 0.75) or 0.75)
_P1_VOL_RIGHT_EXPAND_BONUS = int(_P1_VOL_PACE_CFG.get('right_expand_bonus', 6) or 6)
_P1_VOL_RIGHT_SURGE_BONUS = int(_P1_VOL_PACE_CFG.get('right_surge_bonus', 8) or 8)


def _threshold_value(thresholds: Optional[dict], key: str, default: float) -> float:
    """Read a numeric threshold override safely."""
    if not isinstance(thresholds, dict):
        return default
    try:
        return float(thresholds.get(key, default))
    except Exception:
        return default


def _threshold_meta(thresholds: Optional[dict]) -> dict:
    """Extract compact threshold metadata for details/audit."""
    if not isinstance(thresholds, dict):
        return {}
    return {
        'threshold_version': thresholds.get('threshold_version'),
        'market_phase': thresholds.get('market_phase'),
        'regime': thresholds.get('regime'),
        'board_segment': thresholds.get('board_segment'),
        'security_type': thresholds.get('security_type'),
        'listing_stage': thresholds.get('listing_stage'),
        'price_limit_pct': thresholds.get('price_limit_pct'),
        'risk_warning': thresholds.get('risk_warning'),
        'volume_drought': thresholds.get('volume_drought'),
        'board_seal_env': thresholds.get('board_seal_env'),
        'seal_side': thresholds.get('seal_side'),
        'instrument_profile': thresholds.get('instrument_profile'),
        'micro_environment': thresholds.get('micro_environment'),
        'profile_override': thresholds.get('profile_override'),
        'limit_filter': thresholds.get('limit_filter'),
        'observer_only': bool(thresholds.get('observer_only', False)),
        'blocked': bool(thresholds.get('blocked', False)),
    }


def _pool1_reject(result: dict, reason: str, extra: Optional[dict] = None) -> dict:
    """Return a standardized no-signal payload with reject reason."""
    out = dict(result or _empty('pool1'))
    details = dict(out.get('details') or {})
    details['reject_reason'] = str(reason or 'unknown')
    if isinstance(extra, dict):
        details.update(extra)
    out['details'] = details
    return out


def _resolve_volume_pace_state(
    pace_ratio: Optional[float],
    given_state: Optional[str],
    *,
    shrink_th: float,
    expand_th: float,
    surge_th: float,
) -> str:
    """统一量能进度状态：unknown / shrink / normal / expand / surge。"""
    if isinstance(given_state, str) and given_state.strip():
        s = given_state.strip().lower()
        if s in {'unknown', 'shrink', 'normal', 'expand', 'surge'}:
            return s
    if pace_ratio is None:
        return 'unknown'
    try:
        p = float(pace_ratio)
    except Exception:
        return 'unknown'
    if p <= 0:
        return 'unknown'
    if p >= float(surge_th):
        return 'surge'
    if p >= float(expand_th):
        return 'expand'
    if p < float(shrink_th):
        return 'shrink'
    return 'normal'


def _compute_t0_main_rally_guard(
    *,
    bias_vwap: Optional[float],
    pct_chg: Optional[float],
    trend_up: bool,
    bid_ask_ratio: Optional[float],
    ask_wall_absorb: bool,
    bid_wall_break: bool,
    big_order_bias: Optional[float],
    super_order_bias: Optional[float],
    super_net_flow_bps: Optional[float],
    volume_pace_ratio: Optional[float],
    volume_pace_state: Optional[str],
    v_power_divergence: bool,
    real_buy_fading: bool,
) -> tuple[bool, dict]:
    """识别主升浪延续态，用于反T门控。"""
    pace_state = _resolve_volume_pace_state(
        volume_pace_ratio,
        volume_pace_state,
        shrink_th=_T0_VOL_PACE_SHRINK_TH,
        expand_th=_T0_VOL_PACE_EXPAND_TH,
        surge_th=_T0_VOL_PACE_SURGE_TH,
    )
    info = {
        'enabled': bool(_T0_TREND_GUARD_ENABLED),
        'trend_up': bool(trend_up),
        'pace_state': pace_state,
        'bias_vwap_bps': None,
        'pct_chg': None,
        'guard_score': 0,
        'guard_reasons': [],
        'structure_reasons': [],
        'negative_flags': [],
    }
    if not _T0_TREND_GUARD_ENABLED:
        info['reason'] = 'disabled'
        return False, info

    try:
        bias_vwap_bps = float(bias_vwap) * 100.0 if bias_vwap is not None else None
    except Exception:
        bias_vwap_bps = None
    info['bias_vwap_bps'] = round(bias_vwap_bps, 2) if bias_vwap_bps is not None else None
    try:
        pct_chg_val = float(pct_chg) if pct_chg is not None else None
    except Exception:
        pct_chg_val = None
    info['pct_chg'] = round(pct_chg_val, 3) if pct_chg_val is not None else None

    reasons: list[str] = []
    structure_reasons: list[str] = []
    score = 0

    if trend_up:
        score += 1
        reasons.append('daily_trend_up')
    if bias_vwap_bps is not None and bias_vwap_bps >= _T0_TREND_GUARD_VWAP_BPS_MIN:
        score += 1
        reasons.append('price_above_vwap')
    if pct_chg_val is not None and pct_chg_val >= _T0_TREND_GUARD_PCT_CHG_MIN:
        score += 1
        reasons.append('intraday_gain')
    if pace_state in {'expand', 'surge'}:
        score += 1
        reasons.append(f'volume_pace_{pace_state}')

    structure_strong = False
    if ask_wall_absorb:
        structure_strong = True
        structure_reasons.append('ask_wall_absorb')
    if bid_ask_ratio is not None and bid_ask_ratio >= _T0_TREND_GUARD_BIDASK_MIN:
        structure_strong = True
        structure_reasons.append('bid_ask_support')
    if big_order_bias is not None and big_order_bias >= _T0_TREND_GUARD_BIG_BIAS_MIN:
        structure_strong = True
        structure_reasons.append('big_order_inflow')
    if super_order_bias is not None and super_order_bias >= _T0_TREND_GUARD_SUPER_BIAS_MIN:
        structure_strong = True
        structure_reasons.append('super_order_inflow')
    if super_net_flow_bps is not None and super_net_flow_bps >= _T0_TREND_GUARD_SUPER_INFLOW_BPS_MIN:
        structure_strong = True
        structure_reasons.append('super_net_inflow')
    if structure_strong:
        score += 1

    negative_flags: list[str] = []
    if v_power_divergence:
        negative_flags.append('v_power_divergence')
    if real_buy_fading:
        negative_flags.append('real_buy_fading')
    if bid_wall_break:
        negative_flags.append('bid_wall_break')

    info['guard_score'] = int(score)
    info['guard_reasons'] = reasons
    info['structure_reasons'] = structure_reasons
    info['negative_flags'] = negative_flags

    guard = bool(
        score >= _T0_TREND_GUARD_MIN_SCORE
        and (trend_up or pace_state in {'expand', 'surge'})
        and not negative_flags
    )
    info['guard'] = guard
    info['reason'] = 'guard_on' if guard else 'guard_off'
    return guard, info


def compute_pool1_resonance_60m(minute_bars_1m: Optional[list[dict]], fallback: bool = False) -> tuple[bool, dict]:
    """
    Pool1 的 60 分钟共振确认：由 1 分钟 bars 聚合为 60m bars 后计算。
    规则要点：
      1) 趋势向上：close > MA3 且 MA3 >= MA4
      2) 动量/结构确认：收盘上行 + 区间突破或量能支持
    """
    info = {
        'source': '1m_aggregate',
        'fallback': bool(fallback),
        'bars_1m': 0,
        'bars_60m': 0,
        'trend_up': False,
        'momentum_up': False,
        'range_break': False,
        'volume_support': False,
    }
    if not minute_bars_1m:
        info['reason'] = 'no_minute_bars'
        return bool(fallback), info

    valid_1m: list[dict] = []
    for b in minute_bars_1m:
        try:
            c = float(b.get('close', 0) or 0)
            h = float(b.get('high', c) or c)
            l = float(b.get('low', c) or c)
            o = float(b.get('open', c) or c)
            v = float(b.get('volume', 0) or 0)
            if c <= 0:
                continue
            valid_1m.append({'open': o, 'high': h, 'low': l, 'close': c, 'volume': v})
        except Exception:
            continue

    info['bars_1m'] = len(valid_1m)
    if len(valid_1m) < 120:
        info['reason'] = 'insufficient_1m_bars'
        return bool(fallback), info

    agg_60m: list[dict] = []
    for i in range(0, len(valid_1m), 60):
        chunk = valid_1m[i:i + 60]
        if len(chunk) < 20:
            continue
        agg_60m.append({
            'open': chunk[0]['open'],
            'high': max(x['high'] for x in chunk),
            'low': min(x['low'] for x in chunk),
            'close': chunk[-1]['close'],
            'volume': sum(x['volume'] for x in chunk),
        })

    info['bars_60m'] = len(agg_60m)
    if len(agg_60m) < 3:
        info['reason'] = 'insufficient_60m_bars'
        return bool(fallback), info

    closes = [x['close'] for x in agg_60m]
    highs = [x['high'] for x in agg_60m]
    vols = [x['volume'] for x in agg_60m]

    ma3 = statistics.mean(closes[-3:])
    ma4 = statistics.mean(closes[-4:]) if len(closes) >= 4 else ma3
    trend_up = closes[-1] > ma3 and ma3 >= ma4
    momentum_up = closes[-1] > closes[-2]
    range_break = highs[-1] >= max(highs[-3:-1]) if len(highs) >= 3 else False
    vol_base = statistics.mean(vols[-3:])
    volume_support = vols[-1] >= vol_base * 0.8 if vol_base > 0 else True

    info.update({
        'ma3': round(ma3, 4),
        'ma4': round(ma4, 4),
        'trend_up': bool(trend_up),
        'momentum_up': bool(momentum_up),
        'range_break': bool(range_break),
        'volume_support': bool(volume_support),
    })

    resonance = bool(trend_up and momentum_up and (range_break or volume_support))
    info['reason'] = 'ok' if resonance else 'not_resonant'
    return resonance, info


def compute_pool1_chip_bonus(chip: Optional[dict]) -> tuple[int, dict]:
    """
    Pool1 二阶段筹码加分：在满足前置条件时给出 [0, weight] 加分。
    若筹码条件不满足（如 winner_rate 过低或集中度过弱），返回 0。
    """
    info = {
        'enabled': bool(_P1_STAGE2_ENABLED and _P1_CHIP_ENABLED),
        'applied': False,
        'bonus': 0,
        'reason': '',
        'weight': _P1_CHIP_WEIGHT,
        'winner_rate_min': _P1_CHIP_WINNER_MIN,
        'band_max_pct': _P1_CHIP_BAND_MAX,
    }
    if not (_P1_STAGE2_ENABLED and _P1_CHIP_ENABLED):
        info['reason'] = 'stage2_or_chip_disabled'
        return 0, info
    if not chip:
        info['reason'] = 'no_chip_features'
        return 0, info

    band = chip.get('chip_concentration_pct')
    winner = chip.get('winner_rate')
    try:
        band_val = float(band) if band is not None else None
    except Exception:
        band_val = None
    try:
        winner_val = float(winner) if winner is not None else None
    except Exception:
        winner_val = None

    info['chip_concentration_pct'] = band_val
    info['winner_rate'] = winner_val

    if band_val is None:
        info['reason'] = 'missing_chip_concentration'
        return 0, info
    if winner_val is not None and winner_val < _P1_CHIP_WINNER_MIN:
        info['reason'] = 'winner_rate_too_low'
        return 0, info
    if band_val > _P1_CHIP_BAND_MAX:
        info['reason'] = 'concentration_too_weak'
        return 0, info

    # band 越小代表筹码越集中，加分越高（0 到 weight）
    ratio = max(0.0, min(1.0, (_P1_CHIP_BAND_MAX - band_val) / max(_P1_CHIP_BAND_MAX, 1e-6)))
    bonus = int(round(_P1_CHIP_WEIGHT * ratio))
    info['applied'] = bonus > 0
    info['bonus'] = max(0, bonus)
    info['reason'] = 'ok' if bonus > 0 else 'weak_bonus'
    return max(0, bonus), info


# ============================================================
# P0-2 / 5.1  T+0 多因子特征构建（含微观结构偏置）
# ============================================================
def build_t0_features(
    tick_price: float,
    boll_lower: float,
    boll_upper: float,
    vwap: Optional[float],
    intraday_prices: Optional[list[float]],
    pct_chg: Optional[float],
    trend_up: bool,
    gub5_trend: Optional[str],
    gub5_transition: Optional[str],
    bid_ask_ratio: Optional[float],
    # 微观结构风险特征
    lure_short: bool = False,
    wash_trade: bool = False,
    spread_abnormal: bool = False,
    v_power_divergence: bool = False,
    ask_wall_building: bool = False,
    real_buy_fading: bool = False,
    wash_trade_severe: bool = False,
    liquidity_drain: bool = False,
    # 高阶微观结构特征（可选）
    ask_wall_absorb_ratio: Optional[float] = None,
    bid_wall_break_ratio: Optional[float] = None,
    spoofing_vol_ratio: Optional[float] = None,
    big_order_bias: Optional[float] = None,
    big_net_flow_bps: Optional[float] = None,
    super_order_bias: Optional[float] = None,
    super_net_flow_bps: Optional[float] = None,
    volume_pace_ratio: Optional[float] = None,
    volume_pace_state: Optional[str] = None,
) -> dict:
    """
    构建 T+0 多因子输入特征，输出标准化字典供打分模型使用。
    其中包含衍生特征：robust_zscore / bias_vwap / boll_break / boll_over 等。
    """
    # 1) 价格相对 VWAP 偏离
    bias_vwap = None
    if vwap and vwap > 0 and tick_price > 0:
        bias_vwap = (tick_price - vwap) / vwap * 100

    # 2) robust Z-score（基于 MAD）
    robust_z = _robust_zscore(intraday_prices) if intraday_prices else None

    # 3) eps 穿越判定（配置驱动）
    boll_break = (boll_lower > 0 and tick_price < boll_lower * (1 - _EPS_BREAK))
    boll_over  = (boll_upper > 0 and tick_price > boll_upper * (1 + _EPS_OVER))

    # 4) 欺骗挂单判定（3.2）
    spoofing_suspected = (
        spoofing_vol_ratio is not None and spoofing_vol_ratio > _SPOOFING_RATIO
    )

    # 5) 吃单吸收判定（2.2）
    ask_wall_absorb = (
        ask_wall_absorb_ratio is not None and ask_wall_absorb_ratio > _ASK_ABSORB_TH
    )
    bid_wall_break = (
        bid_wall_break_ratio is not None and bid_wall_break_ratio > _BID_WALL_BREAK_TH
    )
    pace_state = _resolve_volume_pace_state(
        volume_pace_ratio,
        volume_pace_state,
        shrink_th=_T0_VOL_PACE_SHRINK_TH,
        expand_th=_T0_VOL_PACE_EXPAND_TH,
        surge_th=_T0_VOL_PACE_SURGE_TH,
    )
    try:
        pace_ratio_val = float(volume_pace_ratio) if volume_pace_ratio is not None else None
    except Exception:
        pace_ratio_val = None
    main_rally_guard, main_rally_info = _compute_t0_main_rally_guard(
        bias_vwap=bias_vwap,
        pct_chg=pct_chg,
        trend_up=bool(trend_up),
        bid_ask_ratio=bid_ask_ratio,
        ask_wall_absorb=ask_wall_absorb,
        bid_wall_break=bid_wall_break,
        big_order_bias=big_order_bias,
        super_order_bias=super_order_bias,
        super_net_flow_bps=super_net_flow_bps,
        volume_pace_ratio=pace_ratio_val,
        volume_pace_state=pace_state,
        v_power_divergence=v_power_divergence,
        real_buy_fading=real_buy_fading,
    )

    return {
        # 原始输入特征
        'tick_price': tick_price,
        'boll_lower': boll_lower,
        'boll_upper': boll_upper,
        'vwap': vwap,
        'pct_chg': pct_chg,
        'trend_up': bool(trend_up),
        'gub5_trend': gub5_trend,
        'gub5_transition': gub5_transition,
        'bid_ask_ratio': bid_ask_ratio,
        # 微观结构风险特征
        'lure_short': lure_short,
        'wash_trade': wash_trade,
        'spread_abnormal': spread_abnormal,
        'v_power_divergence': v_power_divergence,
        'ask_wall_building': ask_wall_building,
        'real_buy_fading': real_buy_fading,
        'wash_trade_severe': wash_trade_severe,
        'liquidity_drain': liquidity_drain,
        'big_order_bias': big_order_bias,
        'big_net_flow_bps': big_net_flow_bps,
        'super_order_bias': super_order_bias,
        'super_net_flow_bps': super_net_flow_bps,
        # 衍生特征（供打分与风控）
        'bias_vwap': round(bias_vwap, 3) if bias_vwap is not None else None,
        'robust_zscore': round(robust_z, 3) if robust_z is not None else None,
        'boll_break': boll_break,
        'boll_over': boll_over,
        'ask_wall_absorb': ask_wall_absorb,
        'ask_wall_absorb_ratio': ask_wall_absorb_ratio,
        'bid_wall_break': bid_wall_break,
        'bid_wall_break_ratio': bid_wall_break_ratio,
        'spoofing_suspected': spoofing_suspected,
        'volume_pace_ratio': round(pace_ratio_val, 4) if pace_ratio_val is not None else None,
        'volume_pace_state': pace_state,
        'main_rally_guard': bool(main_rally_guard),
        'main_rally_info': main_rally_info,
    }


# ============================================================
# 4.5  滞回区间状态机
def check_hysteresis(ts_code: str, sig_type: str, current_price: float) -> bool:
    """
    检查是否满足“重触发条件”：价格需回到允许再次触发的区间。
    返回 True=允许重触发；False=仍处于滞回区间（跳过）。
    """
    state = _hysteresis_state.get(ts_code, {})
    last_price = state.get(sig_type)
    if last_price is None:
        return True  # 首次触发，允许
    if sig_type == 'positive_t':
        # 正T：价格需从上次触发点回落 hyst_pct 才允许再次触发
        return current_price < last_price * (1 - _HYST_POS_PCT)
    else:  # reverse_t
        # 反T：价格需从上次触发点回升 hyst_pct 才允许再次触发
        return current_price > last_price * (1 + _HYST_REV_PCT)


def update_hysteresis(ts_code: str, sig_type: str, trigger_price: float):
    """信号触发时更新滞回状态。"""
    if ts_code not in _hysteresis_state:
        _hysteresis_state[ts_code] = {}
    _hysteresis_state[ts_code][sig_type] = trigger_price


def _estimate_reversion_edge_bps(
    sig_type: str,
    tick_price: float,
    vwap: Optional[float] = None,
    bias_vwap: Optional[float] = None,
) -> Optional[float]:
    """
    估算当前信号的“均值回归空间”(bps)：
      - 正T: (VWAP - 现价)/现价
      - 反T: (现价 - VWAP)/现价
    若缺少 VWAP，则回退使用 bias_vwap(%)。
    """
    if tick_price <= 0:
        return None
    if vwap is not None and float(vwap or 0) > 0:
        vv = float(vwap)
        if sig_type == 'positive_t':
            return (vv - tick_price) / tick_price * 10000.0
        return (tick_price - vv) / tick_price * 10000.0
    if bias_vwap is not None:
        b = float(bias_vwap)
        if sig_type == 'positive_t':
            return (-b) * 100.0
        return b * 100.0
    return None


def _update_t0_last_signal_state(ts_code: str, sig_type: str, trigger_price: float, now_ts: int) -> None:
    if not ts_code or trigger_price <= 0:
        return
    _t0_last_signal_state[ts_code] = {
        'type': str(sig_type or ''),
        'price': float(trigger_price),
        'ts': int(now_ts),
    }


def _check_t0_anti_churn(
    ts_code: Optional[str],
    sig_type: str,
    tick_price: float,
    now_ts: int,
    vwap: Optional[float] = None,
    bias_vwap: Optional[float] = None,
    bid_ask_ratio: Optional[float] = None,
) -> tuple[bool, dict]:
    """
    T+0 防抖+费用门槛检查：
      1) 最小预期边际（防止无效小波动）
      2) 反向切换最小时间与最小价差
      3) 反T在买盘过强时拦截（防追涨反T）
    """
    detail = {
        'enabled': bool(_T0_ANTI_CHURN_ENABLED),
        'min_flip_sec': int(_T0_MIN_FLIP_SEC),
        'min_flip_spread_pct': float(_T0_MIN_FLIP_SPREAD_PCT),
        'roundtrip_fee_bps': float(_T0_ROUNDTRIP_FEE_BPS),
        'min_net_edge_bps': float(_T0_MIN_NET_EDGE_BPS),
        'min_expected_edge_bps': float(_T0_MIN_EXPECTED_EDGE_BPS),
    }
    if (not _T0_ANTI_CHURN_ENABLED) or (not ts_code) or tick_price <= 0:
        return True, detail

    edge_bps = _estimate_reversion_edge_bps(sig_type, tick_price, vwap=vwap, bias_vwap=bias_vwap)
    detail['expected_edge_bps'] = round(edge_bps, 2) if edge_bps is not None else None
    if edge_bps is not None and edge_bps < _T0_MIN_EXPECTED_EDGE_BPS:
        detail['reject_reason'] = 'expected_edge_too_small'
        return False, detail

    if sig_type == 'reverse_t':
        detail['reverse_bid_ask_block_th'] = float(_T0_REVERSE_BIDASK_BLOCK_TH)
        detail['reverse_vwap_premium_bps'] = float(_T0_REVERSE_VWAP_PREMIUM_BPS)
        detail['bid_ask_ratio'] = round(float(bid_ask_ratio), 3) if bid_ask_ratio is not None else None
        if bid_ask_ratio is not None and float(bid_ask_ratio) >= _T0_REVERSE_BIDASK_BLOCK_TH:
            detail['reject_reason'] = 'reverse_bid_strength_too_high'
            return False, detail
        if edge_bps is not None and edge_bps < _T0_REVERSE_VWAP_PREMIUM_BPS:
            detail['reject_reason'] = 'reverse_vwap_premium_insufficient'
            return False, detail

    last = _t0_last_signal_state.get(str(ts_code))
    if not isinstance(last, dict):
        return True, detail

    last_type = str(last.get('type') or '')
    last_price = float(last.get('price', 0) or 0)
    last_ts = int(last.get('ts', 0) or 0)
    dt = max(0, int(now_ts) - last_ts)
    detail['last_type'] = last_type
    detail['last_price'] = round(last_price, 3) if last_price > 0 else None
    detail['seconds_since_last'] = dt

    if last_type == sig_type or last_price <= 0:
        return True, detail

    if dt < _T0_MIN_FLIP_SEC:
        detail['reject_reason'] = 'flip_too_fast'
        return False, detail

    spread_pct = abs(tick_price - last_price) / last_price * 100.0
    realized_bps = spread_pct * 100.0
    fee_floor_bps = float(_T0_ROUNDTRIP_FEE_BPS + _T0_MIN_NET_EDGE_BPS)
    detail['flip_spread_pct'] = round(spread_pct, 3)
    detail['flip_realized_bps'] = round(realized_bps, 2)
    detail['flip_fee_floor_bps'] = round(fee_floor_bps, 2)

    if spread_pct < _T0_MIN_FLIP_SPREAD_PCT:
        detail['reject_reason'] = 'flip_spread_too_small'
        return False, detail

    if realized_bps < fee_floor_bps:
        detail['reject_reason'] = 'fee_not_covered'
        return False, detail

    return True, detail


# ============================================================
# 11.7.1  T+0 门控与最终分数
# ============================================================
def _compute_t0_gates(
    sig_type: str,
    trigger_items: list[str],
    confirm_items: list[str],
    wash_trade: bool = False,
    liquidity_drain: bool = False,
    spoofing_suspected: bool = False,
    big_order_bias: Optional[float] = None,
) -> tuple[float, float, str]:
    """
    计算 T+0 门控评分，返回 (raw_score, gate_multiplier, gate_reason)。
    raw_score = 40 + 触发项得分 + 确认项得分；gate_multiplier 取值 [0,1]
    """
    raw_score = 40 + min(20, 10 * len(trigger_items)) + min(30, 15 * len(confirm_items))

    gate = 1.0
    reasons: list[str] = []

    if sig_type == 'positive_t':
        if wash_trade:
            gate = min(gate, 0.6)
            reasons.append('wash_trade')
        if spoofing_suspected:
            gate = min(gate, _MICRO_GATE_FLOOR)
            reasons.append('spoofing')
    else:  # reverse_t
        if liquidity_drain:
            gate = min(gate, _RISK_GATE_FLOOR)
            reasons.append('liquidity_drain')
        if wash_trade:
            gate = min(gate, 0.5)
            reasons.append('wash_trade')
        if big_order_bias is not None and big_order_bias > 0.3:
            gate = min(gate, 0.7)   # 微观结构恶化时压低门控
            reasons.append(f'big_buy_bias={big_order_bias:.2f}')

    return raw_score, gate, ';'.join(reasons)


def finalize_pool1_signal(signal: dict, thresholds: Optional[dict] = None) -> dict:
    """
    Pool1 评分结果分级：<observe 为过滤，observe~executable 为观察，>=executable 为可执行。
    """
    if not isinstance(signal, dict) or not signal.get('has_signal'):
        return signal

    sig_type = str(signal.get('type', ''))
    try:
        strength = float(signal.get('strength', 0) or 0)
    except Exception:
        strength = 0.0

    if strength < _P1_SCORE_OBSERVE:
        return _empty(sig_type)

    out = dict(signal)
    details = out.get('details')
    if not isinstance(details, dict):
        details = {}
        out['details'] = details

    th_meta = details.get('threshold')
    if not isinstance(th_meta, dict):
        th_meta = _threshold_meta(thresholds)
        if th_meta:
            details['threshold'] = th_meta

    observe_only = (strength < _P1_SCORE_EXECUTABLE) or bool(
        th_meta.get('observer_only') if isinstance(th_meta, dict) else False
    )
    details['observe_only'] = bool(observe_only)
    details['score_exec_th'] = _P1_SCORE_EXECUTABLE
    details['score_observe_th'] = _P1_SCORE_OBSERVE

    msg = str(out.get('message') or '')
    if observe_only:
        if '仅观察' not in msg:
            out['message'] = f'{msg}·仅观察' if msg else '仅观察'
    else:
        if '可执行' not in msg:
            out['message'] = f'{msg}·可执行' if msg else '可执行'
    out['strength'] = int(round(strength))
    return out


# ============================================================
# 1. 择时监控池策略
# ============================================================
def detect_left_side_buy(
    price: float,
    boll_upper: float,
    boll_mid: float,
    boll_lower: float,
    rsi6: Optional[float] = None,
    pct_chg: Optional[float] = None,
    bid_ask_ratio: Optional[float] = None,
    lure_long: bool = False,
    wash_trade: bool = False,
    up_limit: Optional[float] = None,
    down_limit: Optional[float] = None,
    resonance_60m: bool = False,
    volume_pace_ratio: Optional[float] = None,
    volume_pace_state: Optional[str] = None,
    thresholds: Optional[dict] = None,
) -> dict:
    """
    左侧买入信号：价格贴近/跌破下轨，配合 RSI 与确认项。
    - 日内超跌时（跌幅 > 3%）提供额外强度加分
    """
    result = _empty('left_side_buy')
    if price <= 0 or boll_lower <= 0:
        return _pool1_reject(result, 'invalid_input')

    th_meta = _threshold_meta(thresholds)
    if th_meta.get('blocked'):
        reason = 'limit_magnet_block' if th_meta.get('limit_filter') else 'threshold_blocked'
        return _pool1_reject(result, reason, {'threshold': th_meta})

    # 动态阈值已提供 limit_filter 时，以动态引擎结果为准

    # 仅在无阈值输入时使用本地兜底过滤

    if _P1_LIMIT_ENABLED and not isinstance(thresholds, dict):
        if up_limit and up_limit > 0:
            dist_up = abs(up_limit - price) / up_limit * 100
            if dist_up <= _P1_LIMIT_TH_PCT:
                return _pool1_reject(result, 'limit_magnet_block')
        if down_limit and down_limit > 0:
            dist_dn = abs(price - down_limit) / down_limit * 100
            if dist_dn <= _P1_LIMIT_TH_PCT:
                return _pool1_reject(result, 'limit_magnet_block')

    if _P1_ENABLED_V2:
        if _P1_VETO_LURE_LONG and lure_long:
            return _pool1_reject(result, 'veto_lure_long')
        if _P1_VETO_WASH_TRADE and wash_trade:
            return _pool1_reject(result, 'veto_wash_trade')

    dist_to_lower = (price - boll_lower) / boll_lower * 100  # 正值表示在下轨上方
    band_width_pct = 0.0
    if boll_mid > 0:
        band_width_pct = (boll_upper - boll_lower) / boll_mid * 100
    if isinstance(thresholds, dict):
        near_lower_th = _threshold_value(thresholds, 'near_lower_th', 0.5)
    elif _P1_DYNAMIC_ENABLED:
        near_lower_th = 0.8 if band_width_pct >= 6 else 0.5
    else:
        near_lower_th = 0.5
    near_lower = dist_to_lower <= near_lower_th
    below_lower = price < boll_lower
    if isinstance(thresholds, dict):
        rsi_oversold_th = _threshold_value(thresholds, 'rsi_oversold', 30)
    else:
        rsi_oversold_th = 32 if (pct_chg is not None and pct_chg <= -1.5) else 30

    if _P1_ENABLED_V2 and not (near_lower or below_lower):
        return _pool1_reject(result, 'trigger_missing')

    conditions = []
    if near_lower:
        conditions.append(f'贴近下轨({dist_to_lower:+.2f}%<= {near_lower_th:.1f}%)')
    if below_lower:
        conditions.append('跌破下轨')
    if rsi6 is not None and rsi6 <= rsi_oversold_th:
        conditions.append(f'RSI6超卖({rsi6:.1f}<={rsi_oversold_th:.0f})')
    if pct_chg is not None and pct_chg < -3:
        conditions.append(f'跌幅{pct_chg:.2f}%')

    if _P1_ENABLED_V2:
        if _P1_RESONANCE_ENABLED and not resonance_60m:
            return _pool1_reject(result, 'resonance_missing')
        confirm_items = []
        if bid_ask_ratio is not None and bid_ask_ratio >= _P1_BID_ASK_TH:
            confirm_items.append(f'承接{bid_ask_ratio:.2f}')
        if _P1_RESONANCE_ENABLED and resonance_60m:
            confirm_items.append('60m共振')
        if not confirm_items:
            return _pool1_reject(result, 'confirm_missing')
        conditions.append('确认:' + '+'.join(confirm_items))

    if not conditions:
        return _pool1_reject(result, 'condition_empty')

    pace_state = _resolve_volume_pace_state(
        volume_pace_ratio,
        volume_pace_state,
        shrink_th=_P1_VOL_PACE_SHRINK_TH,
        expand_th=_P1_VOL_PACE_EXPAND_TH,
        surge_th=_P1_VOL_PACE_SURGE_TH,
    )
    try:
        pace_ratio_val = float(volume_pace_ratio) if volume_pace_ratio is not None else None
    except Exception:
        pace_ratio_val = None
    pace_adj = 0
    if _P1_VOL_PACE_ENABLED:
        if pace_state == 'shrink':
            pace_adj -= _P1_VOL_LEFT_SHRINK_PENALTY
        elif pace_state == 'expand':
            pace_adj += _P1_VOL_LEFT_EXPAND_BONUS
        elif pace_state == 'surge':
            pace_adj += _P1_VOL_LEFT_SURGE_BONUS

    strength = 0
    if near_lower:
        strength += 30
    if below_lower:
        strength += 25
    if rsi6 is not None and rsi6 <= rsi_oversold_th:
        strength += 25
    if pct_chg is not None and pct_chg < -3:
        strength += 20
    if below_lower and rsi6 is not None and rsi6 <= rsi_oversold_th:
        strength += 10
    strength = max(0, min(100, strength + pace_adj))
    return {
        'has_signal': True,
        'type': 'left_side_buy',
        'direction': 'buy',
        'price': round(price, 3),
        'strength': strength,
        'message': f"左侧买入信号@{price:.2f} | " + ' + '.join(conditions),
        'triggered_at': int(time.time()),
        'details': {
            'price': price, 'boll_lower': boll_lower,
            'dist_to_lower_pct': round(dist_to_lower, 2),
            'near_lower_th_pct': near_lower_th,
            'band_width_pct': round(band_width_pct, 2),
            'rsi6': rsi6, 'pct_chg': pct_chg,
            'resonance_60m': bool(resonance_60m),
            'volume_pace_ratio': round(pace_ratio_val, 4) if pace_ratio_val is not None else None,
            'volume_pace_state': pace_state,
            'volume_pace_adj': int(pace_adj),
            'threshold': th_meta,
        },
    }


def detect_right_side_breakout(
    price: float,
    boll_upper: float,
    boll_mid: float,
    boll_lower: float,
    volume_ratio: Optional[float] = None,  # 瑜扮亯妫╁繗蹇旂槷
    ma5: Optional[float] = None,
    ma10: Optional[float] = None,
    rsi6: Optional[float] = None,
    prev_price: Optional[float] = None,
    bid_ask_ratio: Optional[float] = None,
    lure_long: bool = False,
    wash_trade: bool = False,
    up_limit: Optional[float] = None,
    down_limit: Optional[float] = None,
    resonance_60m: bool = False,
    volume_pace_ratio: Optional[float] = None,
    volume_pace_state: Optional[str] = None,
    thresholds: Optional[dict] = None,
) -> dict:
    """
    右侧突破信号（顺势突破）
    - 价格有效突破布林中轨/上轨（正向穿越，过滤贴线噪音）
    - 量比 >= 1.3（放量）
    - MA5 上穿 MA10（金叉辅助）
    - RSI6 保持在可持续区间（45~85）
    """
    result = _empty('right_side_breakout')
    if price <= 0 or boll_mid <= 0:
        return _pool1_reject(result, 'invalid_input')

    th_meta = _threshold_meta(thresholds)
    if th_meta.get('blocked'):
        reason = 'limit_magnet_block' if th_meta.get('limit_filter') else 'threshold_blocked'
        return _pool1_reject(result, reason, {'threshold': th_meta})

    if _P1_ENABLED_V2 and (prev_price is None or prev_price <= 0):
        return _pool1_reject(result, 'missing_prev_price')

    # 动态阈值已提供 limit_filter 时，以动态引擎结果为准

    # 仅在无阈值输入时使用本地兜底过滤

    if _P1_LIMIT_ENABLED and not isinstance(thresholds, dict):
        if up_limit and up_limit > 0:
            dist_up = abs(up_limit - price) / up_limit * 100
            if dist_up <= _P1_LIMIT_TH_PCT:
                return _pool1_reject(result, 'limit_magnet_block')
        if down_limit and down_limit > 0:
            dist_dn = abs(price - down_limit) / down_limit * 100
            if dist_dn <= _P1_LIMIT_TH_PCT:
                return _pool1_reject(result, 'limit_magnet_block')

    if _P1_ENABLED_V2:
        if _P1_VETO_LURE_LONG and lure_long:
            return _pool1_reject(result, 'veto_lure_long')
        if _P1_VETO_WASH_TRADE and wash_trade:
            return _pool1_reject(result, 'veto_wash_trade')

    if isinstance(thresholds, dict):
        eps_mid = _threshold_value(thresholds, 'eps_mid', 1.002)
        eps_upper = _threshold_value(thresholds, 'eps_upper', 1.001)
        breakout_max_offset_pct = _threshold_value(thresholds, 'breakout_max_offset_pct', 3.0) / 100.0
    else:
        eps_mid = 1.002
        eps_upper = 1.001
        breakout_max_offset_pct = 0.03
    if (not isinstance(thresholds, dict)) and _P1_DYNAMIC_ENABLED:
        band_width_pct = ((boll_upper - boll_lower) / boll_mid * 100) if boll_mid > 0 else 0
        if band_width_pct >= 6:
            eps_mid = 1.001
            eps_upper = 1.0005
    break_mid = price > boll_mid * eps_mid and (price - boll_mid) / boll_mid <= breakout_max_offset_pct
    break_upper = boll_upper > 0 and price > boll_upper * eps_upper
    cross_mid = False
    cross_upper = False
    if prev_price is not None and prev_price > 0:
        cross_mid = prev_price <= boll_mid and price > boll_mid * eps_mid
        if boll_upper > 0:
            cross_upper = prev_price <= boll_upper and price > boll_upper * eps_upper
    if _P1_ENABLED_V2 and not (cross_mid or cross_upper):
        return _pool1_reject(result, 'breakout_not_cross')

    rsi_ok = (rsi6 is None) or (45 <= rsi6 <= 85)
    vol_break_th = 1.3

    conditions = []
    if break_mid:
        conditions.append('突破中轨')
    if break_upper:
        conditions.append('突破上轨')
    if volume_ratio is not None and volume_ratio >= vol_break_th:
        conditions.append(f'量比{volume_ratio:.2f}')
    if ma5 and ma10 and ma5 > ma10:
        conditions.append('MA5>MA10')
    if rsi6 is not None and rsi_ok:
        conditions.append(f'RSI6={rsi6:.1f}')

    if _P1_ENABLED_V2:
        if _P1_RESONANCE_ENABLED and not resonance_60m:
            return _pool1_reject(result, 'resonance_missing')
        confirm_items = []
        if bid_ask_ratio is not None and bid_ask_ratio >= _P1_BID_ASK_TH:
            confirm_items.append(f'承接{bid_ask_ratio:.2f}')
        if _P1_RESONANCE_ENABLED and resonance_60m:
            confirm_items.append('60m共振')
        if not confirm_items:
            return _pool1_reject(result, 'confirm_missing')
        conditions.append('确认:' + '+'.join(confirm_items))

    if not conditions or not (break_mid or break_upper):
        return _pool1_reject(result, 'trigger_missing')
    if not rsi_ok:
        return _pool1_reject(result, 'rsi_not_ok')

    pace_state = _resolve_volume_pace_state(
        volume_pace_ratio,
        volume_pace_state,
        shrink_th=_P1_VOL_PACE_SHRINK_TH,
        expand_th=_P1_VOL_PACE_EXPAND_TH,
        surge_th=_P1_VOL_PACE_SURGE_TH,
    )
    try:
        pace_ratio_val = float(volume_pace_ratio) if volume_pace_ratio is not None else None
    except Exception:
        pace_ratio_val = None
    pace_adj = 0
    if _P1_VOL_PACE_ENABLED and pace_state == 'shrink':
        if pace_ratio_val is None or pace_ratio_val < _P1_VOL_RIGHT_MIN_PACE:
            return _pool1_reject(
                result,
                'volume_pace_too_low',
                {
                    'volume_pace_ratio': round(pace_ratio_val, 4) if pace_ratio_val is not None else None,
                    'volume_pace_state': pace_state,
                    'volume_pace_min': _P1_VOL_RIGHT_MIN_PACE,
                    'threshold': th_meta,
                },
            )
    if _P1_VOL_PACE_ENABLED:
        if pace_state == 'expand':
            pace_adj += _P1_VOL_RIGHT_EXPAND_BONUS
        elif pace_state == 'surge':
            pace_adj += _P1_VOL_RIGHT_SURGE_BONUS

    strength = 0
    if break_mid:
        strength += 35
    if break_upper:
        strength += 35
    if volume_ratio is not None and volume_ratio >= vol_break_th:
        strength += 15
    if ma5 and ma10 and ma5 > ma10:
        strength += 10
    if rsi6 is not None and rsi_ok:
        strength += 10
    strength = max(0, min(100, strength + pace_adj))
    return {
        'has_signal': True,
        'type': 'right_side_breakout',
        'direction': 'buy',
        'price': round(price, 3),
        'strength': strength,
        'message': f"右侧突破信号@{price:.2f} | " + ' + '.join(conditions),
        'triggered_at': int(time.time()),
        'details': {
            'price': price, 'boll_mid': boll_mid, 'boll_upper': boll_upper,
            'volume_ratio': volume_ratio, 'ma5': ma5, 'ma10': ma10,
            'rsi6': rsi6, 'rsi_ok': rsi_ok,
            'prev_price': prev_price, 'cross_mid': cross_mid, 'cross_upper': cross_upper,
            'resonance_60m': bool(resonance_60m),
            'volume_pace_ratio': round(pace_ratio_val, 4) if pace_ratio_val is not None else None,
            'volume_pace_state': pace_state,
            'volume_pace_adj': int(pace_adj),
            'threshold': th_meta,
        },
    }


# ============================================================
# 1.1 择时监控池：清仓信号（持仓态）
# ============================================================
def detect_timing_clear(
    price: float,
    boll_mid: float,
    ma5: Optional[float] = None,
    ma10: Optional[float] = None,
    ma20: Optional[float] = None,
    rsi6: Optional[float] = None,
    pct_chg: Optional[float] = None,
    volume_ratio: Optional[float] = None,
    bid_ask_ratio: Optional[float] = None,
    resonance_60m: Optional[bool] = None,
    big_order_bias: Optional[float] = None,
    super_order_bias: Optional[float] = None,
    big_net_flow_bps: Optional[float] = None,
    super_net_flow_bps: Optional[float] = None,
    volume_pace_ratio: Optional[float] = None,
    volume_pace_state: Optional[str] = None,
    in_holding: bool = False,
    holding_days: float = 0.0,
    thresholds: Optional[dict] = None,
) -> dict:
    """
    择时池确认清仓信号（中持仓周期）。
    设计目标：
    - 仅在持仓态评估，不在观望态误触发
    - 卖出门槛高于买入，降低中长期持仓的噪声换手
    - 支持“强风险提前清仓”兜底
    """
    result = _empty('timing_clear')
    if not _P1_EXIT_ENABLED:
        return _pool1_reject(result, 'exit_disabled')
    if price <= 0:
        return _pool1_reject(result, 'invalid_input')
    if not in_holding:
        return _pool1_reject(result, 'not_holding')

    th_meta = _threshold_meta(thresholds)
    if th_meta.get('blocked'):
        return _pool1_reject(result, 'threshold_blocked', {'threshold': th_meta})

    try:
        hold_days = max(0.0, float(holding_days or 0.0))
    except Exception:
        hold_days = 0.0

    break_mid = bool(boll_mid and boll_mid > 0 and price < boll_mid * 0.998)
    break_ma10 = bool(ma10 and ma10 > 0 and price < ma10 * 0.998)
    break_ma20 = bool(ma20 and ma20 > 0 and price < ma20 * 0.995)
    trend_reversal = bool(
        ma5 and ma10 and ma5 > 0 and ma10 > 0 and (ma5 < ma10 * 0.998)
    )
    hard_drop = bool(pct_chg is not None and pct_chg <= _P1_EXIT_HARD_DROP_PCT)
    vol_down = bool(
        volume_ratio is not None and pct_chg is not None and volume_ratio >= 1.4 and pct_chg < 0
    )
    early_risk_exit = bool(hard_drop or (break_ma20 and vol_down))

    if hold_days < _P1_EXIT_MIN_HOLD_DAYS and not (_P1_EXIT_ALLOW_EARLY_RISK and early_risk_exit):
        return _pool1_reject(
            result,
            'hold_not_enough',
            {
                'holding_days': round(hold_days, 2),
                'min_hold_days': _P1_EXIT_MIN_HOLD_DAYS,
                'threshold': th_meta,
            },
        )

    trigger_items: list[str] = []
    if break_ma10:
        trigger_items.append('跌破MA10')
    if break_ma20:
        trigger_items.append('跌破MA20')
    if break_mid:
        trigger_items.append('失守布林中轨')
    if trend_reversal:
        trigger_items.append('短均线走弱')
    if rsi6 is not None and rsi6 <= _P1_EXIT_RSI_WEAK_TH:
        trigger_items.append(f'RSI6转弱({rsi6:.1f})')
    if pct_chg is not None and pct_chg <= -2.0:
        trigger_items.append(f'日内走弱({pct_chg:.2f}%)')
    if vol_down:
        trigger_items.append('放量下跌')

    major_break = bool(break_ma20 or (break_ma10 and break_mid) or trend_reversal)
    if not major_break and not early_risk_exit:
        return _pool1_reject(result, 'trigger_missing', {'threshold': th_meta})

    confirm_items: list[str] = []
    if bid_ask_ratio is not None and bid_ask_ratio <= _P1_EXIT_BIDASK_WEAK_RATIO:
        confirm_items.append(f'承接转弱({bid_ask_ratio:.2f})')
    if _P1_RESONANCE_ENABLED and resonance_60m is False:
        confirm_items.append('60m失共振')
    if big_order_bias is not None and big_order_bias <= _P1_EXIT_BIG_BIAS_SELL_TH:
        confirm_items.append(f'大单偏空({big_order_bias:.2f})')
    if super_order_bias is not None and super_order_bias <= _P1_EXIT_SUPER_BIAS_SELL_TH:
        confirm_items.append(f'超大单偏空({super_order_bias:.2f})')
    if big_net_flow_bps is not None and big_net_flow_bps <= _P1_EXIT_BIG_NET_OUT_BPS_TH:
        confirm_items.append(f'大单净流出({big_net_flow_bps:.0f}bps)')
    if super_net_flow_bps is not None and super_net_flow_bps <= _P1_EXIT_SUPER_NET_OUT_BPS_TH:
        confirm_items.append(f'超大单净流出({super_net_flow_bps:.0f}bps)')

    min_confirm = 1 if early_risk_exit else 2
    long_hold = hold_days >= max(_P1_EXIT_MIN_HOLD_DAYS, _P1_EXIT_LONG_HOLD_DAYS)
    if long_hold and not early_risk_exit:
        min_confirm = max(2, _P1_EXIT_LONG_HOLD_MIN_CONFIRM)
        if not (hard_drop or break_ma20 or trend_reversal):
            return _pool1_reject(
                result,
                'long_hold_weak_exit',
                {
                    'holding_days': round(hold_days, 2),
                    'min_hold_days': _P1_EXIT_MIN_HOLD_DAYS,
                    'long_hold_days': _P1_EXIT_LONG_HOLD_DAYS,
                    'threshold': th_meta,
                },
            )
    if len(confirm_items) < min_confirm:
        return _pool1_reject(
            result,
            'confirm_missing',
            {
                'holding_days': round(hold_days, 2),
                'min_confirm': int(min_confirm),
                'long_hold': bool(long_hold),
                'threshold': th_meta,
            },
        )

    strength = 0
    if break_ma10:
        strength += 20
    if break_mid:
        strength += 20
    if break_ma20:
        strength += 30
    if trend_reversal:
        strength += 15
    if vol_down:
        strength += 10
    if hard_drop:
        strength += 15
    strength += min(20, len(confirm_items) * 6)
    if hold_days >= 10:
        strength += 5
    strength = min(100, int(round(strength)))

    msg = (
        f"择时清仓信号@{price:.2f} | "
        + ' + '.join(trigger_items[:4])
        + " | 确认:"
        + ' + '.join(confirm_items[:4])
    )
    return {
        'has_signal': True,
        'type': 'timing_clear',
        'direction': 'sell',
        'price': round(price, 3),
        'strength': strength,
        'message': msg,
        'triggered_at': int(time.time()),
        'details': {
            'holding_days': round(hold_days, 2),
            'long_hold': bool(long_hold),
            'in_holding': bool(in_holding),
            'break_mid': break_mid,
            'break_ma10': break_ma10,
            'break_ma20': break_ma20,
            'trend_reversal': trend_reversal,
            'early_risk_exit': early_risk_exit,
            'hard_drop': hard_drop,
            'vol_down': vol_down,
            'rsi6': rsi6,
            'pct_chg': pct_chg,
            'volume_ratio': volume_ratio,
            'bid_ask_ratio': bid_ask_ratio,
            'resonance_60m': resonance_60m,
            'big_order_bias': big_order_bias,
            'super_order_bias': super_order_bias,
            'big_net_flow_bps': big_net_flow_bps,
            'super_net_flow_bps': super_net_flow_bps,
            'volume_pace_ratio': volume_pace_ratio,
            'volume_pace_state': volume_pace_state,
            'threshold': th_meta,
        },
    }


# ============================================================
# Pool2 / T+0 统计工具：robust_zscore（MAD）
# ============================================================
def _robust_zscore(prices: list[float]) -> Optional[float]:
    """MAD-based robust Z-Score."""
    if len(prices) < 10:
        return None
    try:
        median = statistics.median(prices[:-1])
        deviations = [abs(p - median) for p in prices[:-1]]
        mad = statistics.median(deviations)
        if mad == 0:
            return None
        return (prices[-1] - median) / (mad * 1.4826)
    except Exception:
        return None


def detect_positive_t(
    tick_price: float,
    boll_lower: float,
    vwap: Optional[float] = None,
    gub5_trend: Optional[str] = None,
    gub5_transition: Optional[str] = None,
    bid_ask_ratio: Optional[float] = None,
    lure_short: bool = False,
    wash_trade: bool = False,
    spread_abnormal: bool = False,
    boll_break: Optional[bool] = None,
    ask_wall_absorb: bool = False,
    ask_wall_absorb_ratio: Optional[float] = None,
    spoofing_suspected: bool = False,
    bias_vwap: Optional[float] = None,
    super_order_bias: Optional[float] = None,
    super_net_flow_bps: Optional[float] = None,
    volume_pace_ratio: Optional[float] = None,
    volume_pace_state: Optional[str] = None,
    ts_code: Optional[str] = None,
    thresholds: Optional[dict] = None,
) -> dict:
    """T+0 正T（低吸高抛）信号。"""
    result = _empty('positive_t')
    now_ts = int(time.time())
    if tick_price <= 0:
        return result

    th_meta = _threshold_meta(thresholds)
    if th_meta.get('blocked'):
        result['details'] = {'threshold': th_meta}
        return result

    bias_vwap_th = _threshold_value(thresholds, 'bias_vwap_th', _BIAS_VWAP_PCT)
    eps_break_th = _threshold_value(thresholds, 'eps_break', _EPS_BREAK)

    if not _ENABLED_V2:
        triggered = (boll_lower > 0 and tick_price < boll_lower) or (
            vwap and vwap > 0 and (tick_price - vwap) / vwap * 100 < bias_vwap_th
        )
        if not triggered:
            return result
        return {
            'has_signal': True,
            'type': 'positive_t',
            'direction': 'buy',
            'price': round(tick_price, 3),
            'strength': 60,
            'message': f"正T买入{tick_price:.2f}·V1简化触发",
            'triggered_at': int(time.time()),
            'details': {'v1_mode': True, 'tick_price': tick_price},
        }

    if ts_code and not check_hysteresis(ts_code, 'positive_t', tick_price):
        return result

    trigger_items = []
    if boll_break is None:
        boll_break = boll_lower > 0 and tick_price < boll_lower * (1 - eps_break_th)
    if boll_break:
        trigger_items.append('穿透下轨')

    if bias_vwap is None and vwap and vwap > 0:
        bias_vwap = (tick_price - vwap) / vwap * 100
    if bias_vwap is not None and bias_vwap < bias_vwap_th:
        trigger_items.append(f'VWAP偏离{bias_vwap:.2f}%')

    if not trigger_items:
        return result

    if (wash_trade and spread_abnormal) or spoofing_suspected:
        reason = 'spoofing' if spoofing_suspected else 'wash+spread'
        return {**result, 'details': {'veto': reason, 'tick_price': tick_price}}

    confirm_items = []
    if gub5_transition in _GUB5_TRANSITIONS:
        confirm_items.append(f'GUB5转向({gub5_transition})')
    if bid_ask_ratio is not None and bid_ask_ratio > _BID_ASK_TH:
        confirm_items.append(f'买盘承接({bid_ask_ratio:.2f})')
    if lure_short:
        confirm_items.append('诱空信号')
    if ask_wall_absorb:
        confirm_items.append('卖墙被吃')
    if super_order_bias is not None and super_order_bias >= _SUPER_ORDER_BIAS_BUY_TH:
        confirm_items.append(f'超大单偏置{super_order_bias:.2f}')
    if super_net_flow_bps is not None and super_net_flow_bps >= _SUPER_NET_INFLOW_BUY_BPS:
        confirm_items.append(f'超大单净流入{super_net_flow_bps:.0f}bps')

    if not confirm_items:
        return result

    raw_score, micro_gate, gate_reason = _compute_t0_gates(
        'positive_t',
        trigger_items,
        confirm_items,
        wash_trade=wash_trade,
        spoofing_suspected=spoofing_suspected,
    )
    vol_pace_state = _resolve_volume_pace_state(
        volume_pace_ratio,
        volume_pace_state,
        shrink_th=_T0_VOL_PACE_SHRINK_TH,
        expand_th=_T0_VOL_PACE_EXPAND_TH,
        surge_th=_T0_VOL_PACE_SURGE_TH,
    )
    try:
        vol_pace_ratio_val = float(volume_pace_ratio) if volume_pace_ratio is not None else None
    except Exception:
        vol_pace_ratio_val = None
    vol_pace_adj = 0
    if _T0_VOL_PACE_ENABLED:
        if vol_pace_state == 'shrink':
            vol_pace_adj -= _T0_VOL_POS_SHRINK_PENALTY
        elif vol_pace_state == 'expand':
            vol_pace_adj += _T0_VOL_POS_EXPAND_BONUS
        elif vol_pace_state == 'surge':
            vol_pace_adj += _T0_VOL_POS_SURGE_BONUS
    raw_score_base = raw_score
    raw_score = max(0, raw_score + vol_pace_adj)
    final_score = min(100, round(raw_score * micro_gate))

    if final_score < _SCORE_OBSERVE:
        return result

    anti_ok, anti_detail = _check_t0_anti_churn(
        ts_code=ts_code,
        sig_type='positive_t',
        tick_price=tick_price,
        now_ts=now_ts,
        vwap=vwap,
        bias_vwap=bias_vwap,
        bid_ask_ratio=bid_ask_ratio,
    )
    if not anti_ok:
        return {
            **result,
            'details': {
                'veto': 'anti_churn',
                'tick_price': tick_price,
                'anti_churn': anti_detail,
            },
        }

    if ts_code:
        update_hysteresis(ts_code, 'positive_t', tick_price)

    observe_only = (final_score < _SCORE_EXECUTABLE) or bool(th_meta.get('observer_only'))
    if ts_code and not observe_only:
        _update_t0_last_signal_state(ts_code, 'positive_t', tick_price, now_ts)
    msg_parts = ' + '.join(trigger_items) + ' | 确认:' + ' + '.join(confirm_items)
    msg_suffix = '·仅观察' if observe_only else '·可执行'

    return {
        'has_signal': True,
        'type': 'positive_t',
        'direction': 'buy',
        'price': round(tick_price, 3),
        'strength': final_score,
        'message': f"正T买入{tick_price:.2f}{msg_parts}{msg_suffix}",
        'triggered_at': now_ts,
        'details': {
            'tick_price': tick_price,
            'boll_lower': boll_lower,
            'vwap': vwap,
            'bias_vwap': round(bias_vwap, 3) if bias_vwap is not None else None,
            'gub5_trend': gub5_trend,
            'gub5_transition': gub5_transition,
            'bid_ask_ratio': bid_ask_ratio,
            'lure_short': lure_short,
            'boll_break': boll_break,
            'ask_wall_absorb': ask_wall_absorb,
            'ask_wall_absorb_ratio': ask_wall_absorb_ratio,
            'super_order_bias': super_order_bias,
            'super_net_flow_bps': super_net_flow_bps,
            'spoofing_suspected': spoofing_suspected,
            'volume_pace_ratio': round(vol_pace_ratio_val, 4) if vol_pace_ratio_val is not None else None,
            'volume_pace_state': vol_pace_state,
            'volume_pace_adj': int(vol_pace_adj),
            'raw_score_base': raw_score_base,
            'raw_score': raw_score,
            'micro_gate': micro_gate,
            'gate_reason': gate_reason,
            'anti_churn': anti_detail,
            'observe_only': observe_only,
            'score_exec_th': _SCORE_EXECUTABLE,
            'score_observe_th': _SCORE_OBSERVE,
            'score_status': 'observe' if observe_only else 'executable',
            'threshold': th_meta,
        },
    }


def detect_reverse_t(
    tick_price: float,
    boll_upper: float,
    vwap: Optional[float] = None,
    intraday_prices: Optional[list[float]] = None,
    pct_chg: Optional[float] = None,
    trend_up: bool = False,
    bid_ask_ratio: Optional[float] = None,
    v_power_divergence: bool = False,
    ask_wall_building: bool = False,
    real_buy_fading: bool = False,
    wash_trade_severe: bool = False,
    liquidity_drain: bool = False,
    ask_wall_absorb: bool = False,
    ask_wall_absorb_ratio: Optional[float] = None,
    bid_wall_break: bool = False,
    bid_wall_break_ratio: Optional[float] = None,
    boll_over: Optional[bool] = None,
    spoofing_suspected: bool = False,
    big_order_bias: Optional[float] = None,
    super_order_bias: Optional[float] = None,
    super_net_flow_bps: Optional[float] = None,
    robust_zscore: Optional[float] = None,
    volume_pace_ratio: Optional[float] = None,
    volume_pace_state: Optional[str] = None,
    main_rally_guard: bool = False,
    main_rally_info: Optional[dict] = None,
    ts_code: Optional[str] = None,
    thresholds: Optional[dict] = None,
) -> dict:
    """T+0 反T（高抛低吸）信号。"""
    result = _empty('reverse_t')
    now_ts = int(time.time())
    if tick_price <= 0:
        return result

    th_meta = _threshold_meta(thresholds)
    if th_meta.get('blocked'):
        result['details'] = {'threshold': th_meta}
        return result

    z_th = _threshold_value(thresholds, 'z_th', _ROBUST_ZSCORE_TH)
    eps_over_th = _threshold_value(thresholds, 'eps_over', _EPS_OVER)

    if not _ENABLED_V2:
        robust_z_v1 = _robust_zscore(intraday_prices) if intraday_prices else None
        triggered = (boll_upper > 0 and tick_price >= boll_upper) or (
            robust_z_v1 is not None and robust_z_v1 > z_th
        )
        if not triggered:
            return result
        return {
            'has_signal': True,
            'type': 'reverse_t',
            'direction': 'sell',
            'price': round(tick_price, 3),
            'strength': 60,
            'message': f"反T卖出{tick_price:.2f}·V1简化触发",
            'triggered_at': int(time.time()),
            'details': {'v1_mode': True, 'tick_price': tick_price},
        }

    if ts_code and not check_hysteresis(ts_code, 'reverse_t', tick_price):
        return result

    trigger_items = []
    robust_z = robust_zscore
    if robust_z is None and intraday_prices and len(intraday_prices) >= 10:
        robust_z = _robust_zscore(intraday_prices)

    if boll_over is None:
        boll_over = boll_upper > 0 and tick_price > boll_upper * (1 + eps_over_th)
    if boll_over:
        trigger_items.append('突破上轨')
    if robust_z is not None and robust_z > z_th:
        trigger_items.append(f'RobustZ={robust_z:.2f}')

    if not trigger_items:
        return result

    if wash_trade_severe or spoofing_suspected:
        reason = 'spoofing' if spoofing_suspected else 'wash_trade_severe'
        return {**result, 'details': {'veto': reason, 'tick_price': tick_price}}

    confirm_items = []
    if v_power_divergence:
        confirm_items.append('量价背离')
    if ask_wall_building:
        confirm_items.append('卖墙堆积')
    if bid_wall_break:
        if bid_wall_break_ratio is not None:
            confirm_items.append(f'托盘击穿({bid_wall_break_ratio:.2f})')
        else:
            confirm_items.append('托盘击穿')
    if real_buy_fading:
        confirm_items.append('买入力衰减')
    if super_order_bias is not None and super_order_bias <= -_SUPER_ORDER_BIAS_SELL_TH:
        confirm_items.append(f'超大单偏空{super_order_bias:.2f}')
    if super_net_flow_bps is not None and super_net_flow_bps <= -_SUPER_NET_INFLOW_SELL_BPS:
        confirm_items.append(f'超大单净流出{super_net_flow_bps:.0f}bps')

    if not confirm_items:
        return result

    vol_pace_state = _resolve_volume_pace_state(
        volume_pace_ratio,
        volume_pace_state,
        shrink_th=_T0_VOL_PACE_SHRINK_TH,
        expand_th=_T0_VOL_PACE_EXPAND_TH,
        surge_th=_T0_VOL_PACE_SURGE_TH,
    )
    try:
        vol_pace_ratio_val = float(volume_pace_ratio) if volume_pace_ratio is not None else None
    except Exception:
        vol_pace_ratio_val = None
    bearish_confirms: list[str] = []
    if v_power_divergence:
        bearish_confirms.append('v_power_divergence')
    if ask_wall_building:
        bearish_confirms.append('ask_wall_building')
    if bid_wall_break:
        bearish_confirms.append('bid_wall_break')
    if real_buy_fading:
        bearish_confirms.append('real_buy_fading')
    if super_order_bias is not None and super_order_bias <= -_SUPER_ORDER_BIAS_SELL_TH:
        bearish_confirms.append('super_order_bias')
    if super_net_flow_bps is not None and super_net_flow_bps <= -_SUPER_NET_INFLOW_SELL_BPS:
        bearish_confirms.append('super_net_flow_bps')

    if (
        _T0_VOL_PACE_ENABLED
        and vol_pace_state == 'surge'
        and bid_ask_ratio is not None
        and bid_ask_ratio >= _T0_VOL_REV_SURGE_BIDASK_BLOCK_TH
    ):
        return {
            **result,
            'details': {
                'veto': 'volume_pace_surge_block',
                'tick_price': tick_price,
                'volume_pace_ratio': round(vol_pace_ratio_val, 4) if vol_pace_ratio_val is not None else None,
                'volume_pace_state': vol_pace_state,
                'bid_ask_ratio': bid_ask_ratio,
                'reverse_surge_bidask_block_th': _T0_VOL_REV_SURGE_BIDASK_BLOCK_TH,
            },
        }
    trend_guard_detail = dict(main_rally_info or {})
    trend_guard_detail['trend_up'] = bool(trend_up)
    trend_guard_detail['pct_chg'] = pct_chg
    trend_guard_detail['bearish_confirms'] = bearish_confirms
    trend_guard_detail['active'] = bool(main_rally_guard)
    trend_guard_detail['ask_wall_absorb'] = bool(ask_wall_absorb)
    trend_guard_detail['ask_wall_absorb_ratio'] = (
        round(float(ask_wall_absorb_ratio), 4) if ask_wall_absorb_ratio is not None else None
    )
    try:
        pct_chg_val = float(pct_chg) if pct_chg is not None else None
    except Exception:
        pct_chg_val = None
    try:
        bid_ask_ratio_val = float(bid_ask_ratio) if bid_ask_ratio is not None else None
    except Exception:
        bid_ask_ratio_val = None
    try:
        big_order_bias_val = float(big_order_bias) if big_order_bias is not None else None
    except Exception:
        big_order_bias_val = None
    try:
        super_net_flow_bps_val = float(super_net_flow_bps) if super_net_flow_bps is not None else None
    except Exception:
        super_net_flow_bps_val = None
    required_bearish_confirms = (
        _T0_TREND_GUARD_SURGE_MIN_BEARISH
        if vol_pace_state == 'surge'
        else _T0_TREND_GUARD_MIN_BEARISH
    )
    trend_guard_detail['required_bearish_confirms'] = int(required_bearish_confirms)
    surge_absorb_guard = bool(
        _T0_TREND_GUARD_SURGE_BLOCK_ENABLED
        and vol_pace_state == 'surge'
        and bool(ask_wall_absorb)
        and not bool(bid_wall_break)
        and not bool(real_buy_fading)
        and (pct_chg_val is not None and pct_chg_val >= _T0_TREND_GUARD_SURGE_PCT_CHG_MIN)
        and (bid_ask_ratio_val is not None and bid_ask_ratio_val >= _T0_TREND_GUARD_SURGE_BIDASK_MIN)
        and (big_order_bias_val is not None and big_order_bias_val >= _T0_TREND_GUARD_SURGE_BIG_BIAS_MIN)
        and (super_net_flow_bps_val is not None and super_net_flow_bps_val >= _T0_TREND_GUARD_SURGE_SUPER_INFLOW_BPS_MIN)
        and bool(trend_up or main_rally_guard)
    )
    trend_guard_detail['surge_absorb_guard'] = bool(surge_absorb_guard)
    trend_guard_detail['surge_absorb_required_bearish_confirms'] = int(_T0_TREND_GUARD_SURGE_MIN_BEARISH_HARD)
    if surge_absorb_guard and len(bearish_confirms) < _T0_TREND_GUARD_SURGE_MIN_BEARISH_HARD:
        return {
            **result,
            'details': {
                'veto': 'trend_surge_absorb_guard',
                'tick_price': tick_price,
                'pct_chg': pct_chg,
                'trend_up': bool(trend_up),
                'volume_pace_ratio': round(vol_pace_ratio_val, 4) if vol_pace_ratio_val is not None else None,
                'volume_pace_state': vol_pace_state,
                'trend_guard': trend_guard_detail,
            },
        }
    if main_rally_guard and len(bearish_confirms) < required_bearish_confirms:
        return {
            **result,
            'details': {
                'veto': 'trend_extension_guard',
                'tick_price': tick_price,
                'pct_chg': pct_chg,
                'trend_up': bool(trend_up),
                'volume_pace_ratio': round(vol_pace_ratio_val, 4) if vol_pace_ratio_val is not None else None,
                'volume_pace_state': vol_pace_state,
                'trend_guard': trend_guard_detail,
            },
        }

    raw_score, risk_gate, gate_reason = _compute_t0_gates(
        'reverse_t',
        trigger_items,
        confirm_items,
        wash_trade=wash_trade_severe,
        liquidity_drain=liquidity_drain,
        spoofing_suspected=spoofing_suspected,
        big_order_bias=big_order_bias,
    )
    vol_pace_adj = 0
    if _T0_VOL_PACE_ENABLED:
        if vol_pace_state == 'shrink':
            vol_pace_adj += _T0_VOL_REV_SHRINK_BONUS
        elif vol_pace_state == 'expand':
            vol_pace_adj -= _T0_VOL_REV_EXPAND_PENALTY
        elif vol_pace_state == 'surge':
            vol_pace_adj -= _T0_VOL_REV_SURGE_PENALTY
    raw_score_base = raw_score
    raw_score = max(0, raw_score + vol_pace_adj)
    final_score = min(100, round(raw_score * risk_gate))

    if final_score < _SCORE_OBSERVE:
        return result

    anti_ok, anti_detail = _check_t0_anti_churn(
        ts_code=ts_code,
        sig_type='reverse_t',
        tick_price=tick_price,
        now_ts=now_ts,
        vwap=vwap,
        bias_vwap=None,
        bid_ask_ratio=bid_ask_ratio,
    )
    if not anti_ok:
        return {
            **result,
            'details': {
                'veto': 'anti_churn',
                'tick_price': tick_price,
                'anti_churn': anti_detail,
            },
        }

    if ts_code:
        update_hysteresis(ts_code, 'reverse_t', tick_price)

    observe_only = (final_score < _SCORE_EXECUTABLE) or bool(th_meta.get('observer_only'))
    if ts_code and not observe_only:
        _update_t0_last_signal_state(ts_code, 'reverse_t', tick_price, now_ts)
    msg_parts = ' + '.join(trigger_items) + ' | 确认:' + ' + '.join(confirm_items)
    msg_suffix = '·仅观察' if observe_only else '·可执行'

    return {
        'has_signal': True,
        'type': 'reverse_t',
        'direction': 'sell',
        'price': round(tick_price, 3),
        'strength': final_score,
        'message': f"反T卖出{tick_price:.2f}{msg_parts}{msg_suffix}",
        'triggered_at': now_ts,
        'details': {
            'tick_price': tick_price,
            'trigger_items': list(trigger_items),
            'confirm_items': list(confirm_items),
            'bearish_confirms': list(bearish_confirms),
            'boll_upper': boll_upper,
            'vwap': vwap,
            'bid_ask_ratio': bid_ask_ratio,
            'robust_zscore': round(robust_z, 3) if robust_z is not None else None,
            'boll_over': boll_over,
            'spoofing_suspected': spoofing_suspected,
            'big_order_bias': big_order_bias,
            'super_order_bias': super_order_bias,
            'super_net_flow_bps': super_net_flow_bps,
            'v_power_divergence': v_power_divergence,
            'ask_wall_building': ask_wall_building,
            'ask_wall_absorb': ask_wall_absorb,
            'ask_wall_absorb_ratio': ask_wall_absorb_ratio,
            'bid_wall_break': bid_wall_break,
            'bid_wall_break_ratio': bid_wall_break_ratio,
            'real_buy_fading': real_buy_fading,
            'volume_pace_ratio': round(vol_pace_ratio_val, 4) if vol_pace_ratio_val is not None else None,
            'volume_pace_state': vol_pace_state,
            'volume_pace_adj': int(vol_pace_adj),
            'raw_score_base': raw_score_base,
            'raw_score': raw_score,
            'risk_gate': risk_gate,
            'gate_reason': gate_reason,
            'pct_chg': pct_chg,
            'trend_up': bool(trend_up),
            'main_rally_guard': bool(main_rally_guard),
            'trend_guard': trend_guard_detail,
            'trend_guard_override': bool(main_rally_guard and len(bearish_confirms) >= required_bearish_confirms),
            'anti_churn': anti_detail,
            'observe_only': observe_only,
            'score_exec_th': _SCORE_EXECUTABLE,
            'score_observe_th': _SCORE_OBSERVE,
            'score_status': 'observe' if observe_only else 'executable',
            'threshold': th_meta,
        },
    }

def evaluate_pool(pool_id: int, members_data: list[dict]) -> list[dict]:
    """
    对监控池成员批量运行对应信号。
    Args:
        pool_id: 1=择时池, 2=T+0池
        members_data:
            [{ts_code, name, price, boll_upper, boll_mid, boll_lower,
              rsi6?, pct_chg?, volume_ratio?, ma5?, ma10?, vwap?, gub5_trend?, intraday_prices?}]

    Returns:
        [{ts_code, name, price, pct_chg, signals: [...]}]
    """
    def _build_ctx_for_member(m: dict, intraday: Optional[dict] = None) -> Optional[dict]:
        if _build_signal_context is None:
            return None
        try:
            tick_ctx = {
                'timestamp': m.get('timestamp', int(time.time())),
                'price': m.get('price', 0),
                'pre_close': m.get('pre_close', 0),
                'pct_chg': m.get('pct_chg', 0),
                'volume': m.get('volume', 0),
                'amount': m.get('amount', 0),
                'bids': m.get('bids') or [],
                'asks': m.get('asks') or [],
            }
            daily_ctx = {
                'boll_upper': m.get('boll_upper', 0),
                'boll_mid': m.get('boll_mid', 0),
                'boll_lower': m.get('boll_lower', 0),
                'up_limit': m.get('up_limit'),
                'down_limit': m.get('down_limit'),
                'vol_20': m.get('vol_20'),
                'atr_14': m.get('atr_14'),
            }
            ctx = _build_signal_context(
                ts_code=str(m.get('ts_code', '')),
                pool_id=int(pool_id),
                tick=tick_ctx,
                daily=daily_ctx,
                intraday={
                    **(intraday or {}),
                    'volume_pace_ratio': m.get('volume_pace_ratio'),
                    'volume_pace_state': m.get('volume_pace_state'),
                    'progress_ratio': m.get('volume_pace_progress'),
                },
                market={
                    'vol_20': m.get('vol_20'),
                    'atr_14': m.get('atr_14'),
                    'industry': m.get('industry'),
                    'regime_hint': m.get('regime_hint'),
                    'name': m.get('name'),
                    'market_name': m.get('market_name'),
                    'list_date': m.get('list_date'),
                    'instrument_profile': m.get('instrument_profile'),
                },
            )
            # Pool1 limit-magnet distance now follows dynamic threshold engine config.
            return ctx
        except Exception:
            return None

    def _resolve_threshold(signal_type: str, ctx: Optional[dict]) -> Optional[dict]:
        if _get_thresholds is None or ctx is None:
            return None
        try:
            return _get_thresholds(signal_type, ctx)
        except Exception:
            return None

    def _attach_channel(signal: dict, pool_id: int) -> dict:
        if not isinstance(signal, dict):
            return signal
        channel = 'pool1_timing' if int(pool_id) == 1 else 'pool2_t0'
        source = 'evaluate_pool'
        signal['channel'] = channel
        signal['signal_source'] = source
        signal['_pool_ids'] = [int(pool_id)]
        details = signal.setdefault('details', {})
        details.setdefault('channel', channel)
        details.setdefault('signal_source', source)
        details.setdefault('pool_id', int(pool_id))
        return signal

    results = []
    for m in members_data:
        signals = []
        if pool_id == 1:
            resonance_60m_val = m.get('resonance_60m')
            resonance_60m_info = m.get('resonance_60m_info')
            if resonance_60m_val is None:
                resonance_60m_val, resonance_60m_info = compute_pool1_resonance_60m(
                    m.get('minute_bars_1m'),
                    fallback=False,
                )
            if not isinstance(resonance_60m_info, dict):
                resonance_60m_info = {}
            resonance_60m_info = dict(resonance_60m_info)
            resonance_60m_info.setdefault('source', 'precomputed')
            resonance_60m_info.setdefault('enabled', bool(_P1_RESONANCE_ENABLED))

            def _attach_pool1_resonance_detail(signal: dict) -> None:
                details = signal.setdefault('details', {})
                stage = details.get('pool1_stage')
                if not isinstance(stage, dict):
                    stage = {}
                stage['resonance_60m'] = bool(resonance_60m_val)
                stage['resonance_60m_info'] = dict(resonance_60m_info)
                details['pool1_stage'] = stage

            p1_ctx = _build_ctx_for_member(m)
            p1_left_th = _resolve_threshold('left_side_buy', p1_ctx)
            p1_right_th = _resolve_threshold('right_side_breakout', p1_ctx)
            p1_clear_th = _resolve_threshold('timing_clear', p1_ctx)
            in_holding = bool(m.get('pool1_in_holding', False))
            if not in_holding:
                s1 = detect_left_side_buy(
                    m.get('price', 0), m.get('boll_upper', 0),
                    m.get('boll_mid', 0), m.get('boll_lower', 0),
                    m.get('rsi6'), m.get('pct_chg'),
                    m.get('bid_ask_ratio'), m.get('lure_long', False), m.get('wash_trade', False),
                    m.get('up_limit'), m.get('down_limit'), bool(resonance_60m_val),
                    m.get('volume_pace_ratio'), m.get('volume_pace_state'),
                    thresholds=p1_left_th,
                )
                if s1['has_signal']:
                    s1 = finalize_pool1_signal(s1, thresholds=p1_left_th)
                if s1['has_signal']:
                    _attach_pool1_resonance_detail(s1)
                    s1 = _attach_channel(s1, pool_id=1)
                    signals.append(s1)
                s2 = detect_right_side_breakout(
                    m.get('price', 0), m.get('boll_upper', 0),
                    m.get('boll_mid', 0), m.get('boll_lower', 0),
                    m.get('volume_ratio'), m.get('ma5'), m.get('ma10'), m.get('rsi6'),
                    m.get('prev_price'), m.get('bid_ask_ratio'), m.get('lure_long', False), m.get('wash_trade', False),
                    m.get('up_limit'), m.get('down_limit'), bool(resonance_60m_val),
                    m.get('volume_pace_ratio'), m.get('volume_pace_state'),
                    thresholds=p1_right_th,
                )
                if s2['has_signal']:
                    s2 = finalize_pool1_signal(s2, thresholds=p1_right_th)
                if s2['has_signal']:
                    _attach_pool1_resonance_detail(s2)
                    s2 = _attach_channel(s2, pool_id=1)
                    signals.append(s2)
            s3 = detect_timing_clear(
                price=m.get('price', 0),
                boll_mid=m.get('boll_mid', 0),
                ma5=m.get('ma5'),
                ma10=m.get('ma10'),
                ma20=m.get('ma20'),
                rsi6=m.get('rsi6'),
                pct_chg=m.get('pct_chg'),
                volume_ratio=m.get('volume_ratio'),
                bid_ask_ratio=m.get('bid_ask_ratio'),
                resonance_60m=bool(resonance_60m_val),
                big_order_bias=m.get('big_order_bias'),
                super_order_bias=m.get('super_order_bias'),
                big_net_flow_bps=m.get('big_net_flow_bps'),
                super_net_flow_bps=m.get('super_net_flow_bps'),
                volume_pace_ratio=m.get('volume_pace_ratio'),
                volume_pace_state=m.get('volume_pace_state'),
                in_holding=in_holding,
                holding_days=float(m.get('pool1_holding_days', 0) or 0),
                thresholds=p1_clear_th,
            )
            if s3['has_signal']:
                s3 = finalize_pool1_signal(s3, thresholds=p1_clear_th)
            if s3['has_signal']:
                _attach_pool1_resonance_detail(s3)
                s3 = _attach_channel(s3, pool_id=1)
                signals.append(s3)
        elif pool_id == 2:
            ts_code = m.get('ts_code')
            # 统一特征构建（P0-2 / 5.1）
            feat = build_t0_features(
                tick_price=m.get('price', 0),
                boll_lower=m.get('boll_lower', 0),
                boll_upper=m.get('boll_upper', 0),
                vwap=m.get('vwap'),
                intraday_prices=m.get('intraday_prices'),
                pct_chg=m.get('pct_chg'),
                trend_up=bool(
                    (float(m.get('ma5', 0) or 0) > float(m.get('ma10', 0) or 0) > float(m.get('ma20', 0) or 0))
                    and float(m.get('ma20', 0) or 0) > 0
                ),
                gub5_trend=m.get('gub5_trend'),
                gub5_transition=m.get('gub5_transition'),
                bid_ask_ratio=m.get('bid_ask_ratio'),
                lure_short=bool(m.get('lure_short', False)),
                wash_trade=bool(m.get('wash_trade', False)),
                spread_abnormal=bool(m.get('spread_abnormal', False)),
                v_power_divergence=bool(m.get('v_power_divergence', False)),
                ask_wall_building=bool(m.get('ask_wall_building', False)),
                real_buy_fading=bool(m.get('real_buy_fading', False)),
                wash_trade_severe=bool(m.get('wash_trade_severe', False)),
                liquidity_drain=bool(m.get('liquidity_drain', False)),
                ask_wall_absorb_ratio=m.get('ask_wall_absorb_ratio'),
                bid_wall_break_ratio=m.get('bid_wall_break_ratio'),
                spoofing_vol_ratio=m.get('spoofing_vol_ratio'),
                big_order_bias=m.get('big_order_bias'),
                big_net_flow_bps=m.get('big_net_flow_bps'),
                super_order_bias=m.get('super_order_bias'),
                super_net_flow_bps=m.get('super_net_flow_bps'),
                volume_pace_ratio=m.get('volume_pace_ratio'),
                volume_pace_state=m.get('volume_pace_state'),
            )
            t0_ctx = _build_ctx_for_member(
                m,
                intraday={
                    'vwap': feat.get('vwap'),
                    'bias_vwap': feat.get('bias_vwap'),
                    'robust_zscore': feat.get('robust_zscore'),
                    'gub5_ratio': m.get('gub5_ratio'),
                    'gub5_transition': feat.get('gub5_transition'),
                },
            )
            t0_pos_th = _resolve_threshold('positive_t', t0_ctx)
            t0_rev_th = _resolve_threshold('reverse_t', t0_ctx)
            s1 = detect_positive_t(
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
            if s1['has_signal']:
                s1 = _attach_channel(s1, pool_id=2)
                signals.append(s1)
            s2 = detect_reverse_t(
                tick_price=feat['tick_price'],
                boll_upper=feat['boll_upper'],
                vwap=feat['vwap'],
                intraday_prices=m.get('intraday_prices'),
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
            if s2['has_signal']:
                s2 = _attach_channel(s2, pool_id=2)
                signals.append(s2)

        results.append({
            'ts_code': m.get('ts_code'),
            'name': m.get('name'),
            'price': m.get('price'),
            'pct_chg': m.get('pct_chg'),
            'signals': signals,
        })
    return results


# ============================================================
# 输出映射
# ============================================================
# 信号类型 -> 买卖方向
SIGNAL_DIRECTION = {
    'left_side_buy': 'buy',           # 左侧买入
    'right_side_breakout': 'buy',     # 右侧突破
    'timing_clear': 'sell',           # 择时清仓
    'positive_t': 'buy',              # 正T 买入
    'reverse_t': 'sell',              # 反T 卖出
}


def _empty(signal_type: str) -> dict:
    return {
        'has_signal': False, 'type': signal_type,
        'direction': SIGNAL_DIRECTION.get(signal_type, 'buy'),
        'price': 0,
        'strength': 0,
        'message': '', 'triggered_at': 0, 'details': {},
    }








