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
import datetime as _dt
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
_P1_LEFT_RECLAIM_CFG = _P1_CFG.get('left_reclaim', {}) if isinstance(_P1_CFG, dict) else {}
_P1_LEFT_RECLAIM_ENABLED = bool(_P1_LEFT_RECLAIM_CFG.get('enabled', True))
_P1_LEFT_RECLAIM_BELOW_MIN_CONFIRMS = int(_P1_LEFT_RECLAIM_CFG.get('below_lower_min_confirms', 2) or 2)
_P1_LEFT_RECLAIM_NEAR_MIN_CONFIRMS = int(_P1_LEFT_RECLAIM_CFG.get('near_lower_min_confirms', 1) or 1)
_P1_LEFT_RECLAIM_BIAS_VWAP_DEEP_THRESHOLD_PCT = float(_P1_LEFT_RECLAIM_CFG.get('bias_vwap_deep_threshold_pct', -1.20) or -1.20)
_P1_LEFT_RECLAIM_LEFT_TAIL_ZSCORE_THRESHOLD = float(_P1_LEFT_RECLAIM_CFG.get('left_tail_zscore_threshold', -1.60) or -1.60)
_P1_LEFT_RECLAIM_BIDASK_MIN = float(_P1_LEFT_RECLAIM_CFG.get('bid_ask_ratio_min', 1.05) or 1.05)
_P1_LEFT_RECLAIM_ASK_ABSORB_MIN = float(_P1_LEFT_RECLAIM_CFG.get('ask_wall_absorb_ratio_min', 0.55) or 0.55)
_P1_LEFT_RECLAIM_SUPER_INFLOW_BPS_MIN = float(_P1_LEFT_RECLAIM_CFG.get('super_net_inflow_bps_min', 20) or 20)
_P1_LEFT_RECLAIM_HIGH_POSITION_DROP_PCT = float(_P1_LEFT_RECLAIM_CFG.get('high_position_drop_pct', -2.50) or -2.50)
_P1_LEFT_RECLAIM_HIGH_POSITION_MIDLINE_BUFFER_PCT = float(_P1_LEFT_RECLAIM_CFG.get('high_position_midline_buffer_pct', 0.20) or 0.20) / 100.0
_P1_LEFT_RECLAIM_DISTRIBUTION_BID_ASK_MAX = float(_P1_LEFT_RECLAIM_CFG.get('distribution_bid_ask_max', 0.95) or 0.95)
_P1_LEFT_RECLAIM_DISTRIBUTION_ASK_ABSORB_MAX = float(_P1_LEFT_RECLAIM_CFG.get('distribution_ask_absorb_max', 0.35) or 0.35)
_P1_LEFT_RECLAIM_DISTRIBUTION_SUPER_OUT_BPS = float(_P1_LEFT_RECLAIM_CFG.get('distribution_super_out_bps', -30) or -30)
_P1_LEFT_RECLAIM_PRICE_BUFFER_PCT = float(_P1_LEFT_RECLAIM_CFG.get('price_reclaim_buffer_pct', 0.10) or 0.10) / 100.0
_P1_LEFT_RECLAIM_INTRADAY_AVWAP_RECLAIM_BUFFER_PCT = float(
    _P1_LEFT_RECLAIM_CFG.get('intraday_avwap_reclaim_buffer_pct', 0.05) or 0.05
) / 100.0
_P1_LEFT_RECLAIM_LOOKBACK_BARS = int(_P1_LEFT_RECLAIM_CFG.get('recent_break_lookback_bars', 5) or 5)
_P1_LEFT_RECLAIM_BREAK_EPS_PCT = float(_P1_LEFT_RECLAIM_CFG.get('recent_break_eps_pct', 0.20) or 0.20) / 100.0
_P1_LEFT_RECLAIM_HIGHER_LOW_RISE_PCT = float(_P1_LEFT_RECLAIM_CFG.get('higher_low_min_rise_pct', 0.05) or 0.05) / 100.0
_P1_LEFT_RECLAIM_HIGHER_LOW_3M_RISE_PCT = float(
    _P1_LEFT_RECLAIM_CFG.get('higher_low_3m_min_rise_pct', 0.08) or 0.08
) / 100.0
_P1_LEFT_RECLAIM_SCORE_BONUS = int(_P1_LEFT_RECLAIM_CFG.get('score_bonus_per_confirm', 4) or 4)
_P1_LEFT_RECLAIM_MAX_BONUS = int(_P1_LEFT_RECLAIM_CFG.get('max_confirm_bonus', 12) or 12)
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
_P1_EXIT_RSI_HOT_TH = float(_P1_EXIT_CFG.get('rsi_hot_th', 72) or 72)
_P1_EXIT_TAKE_PROFIT_DAY_GAIN_PCT = float(_P1_EXIT_CFG.get('take_profit_day_gain_pct', 3.0) or 3.0)
_P1_EXIT_TAKE_PROFIT_NEAR_UPPER_PCT = float(_P1_EXIT_CFG.get('take_profit_near_upper_pct', 0.20) or 0.20)
_P1_EXIT_PARTIAL_REDUCE_RATIO = float(_P1_EXIT_CFG.get('partial_reduce_ratio', 0.50) or 0.50)
_P1_EXIT_BETA_REDUCE_RATIO = float(_P1_EXIT_CFG.get('beta_reduce_ratio', 0.33) or 0.33)
_P1_EXIT_OBSERVE_REDUCE_RATIO = float(_P1_EXIT_CFG.get('observe_reduce_ratio', 0.25) or 0.25)
_P1_EXIT_ATR_LOW_PCT = float(_P1_EXIT_CFG.get('atr_low_pct', 0.020) or 0.020)
_P1_EXIT_ATR_HIGH_PCT = float(_P1_EXIT_CFG.get('atr_high_pct', 0.045) or 0.045)
_P1_EXIT_ATR_LOW_EXTRA_CONFIRM = int(_P1_EXIT_CFG.get('atr_low_extra_confirm', 1) or 1)
_P1_EXIT_ATR_HIGH_RELAX_CONFIRM = int(_P1_EXIT_CFG.get('atr_high_relax_confirm', 1) or 1)
_P1_EXIT_BIDASK_WEAK_RATIO = float(_P1_EXIT_CFG.get('bid_ask_weak_ratio', 0.95) or 0.95)
_P1_EXIT_BIG_BIAS_SELL_TH = float(_P1_EXIT_CFG.get('big_order_bias_sell_th', -0.25) or -0.25)
_P1_EXIT_SUPER_BIAS_SELL_TH = float(_P1_EXIT_CFG.get('super_order_bias_sell_th', -0.20) or -0.20)
_P1_EXIT_BIG_NET_OUT_BPS_TH = float(_P1_EXIT_CFG.get('big_net_outflow_bps_th', -80) or -80)
_P1_EXIT_SUPER_NET_OUT_BPS_TH = float(_P1_EXIT_CFG.get('super_net_outflow_bps_th', -120) or -120)
_P1_EXIT_ANCHOR_CFG = _P1_CFG.get('exit_anchor', {}) if isinstance(_P1_CFG, dict) else {}
_P1_EXIT_ANCHOR_ENABLED = bool(_P1_EXIT_ANCHOR_CFG.get('enabled', True))
_P1_EXIT_ENTRY_COST_BREAK_PCT = float(_P1_EXIT_ANCHOR_CFG.get('entry_cost_break_pct', 1.20) or 1.20)
_P1_EXIT_ENTRY_AVWAP_BREAK_PCT = float(_P1_EXIT_ANCHOR_CFG.get('entry_avwap_break_pct', 0.35) or 0.35)
_P1_EXIT_ENTRY_AVWAP_REDUCE_RATIO = float(_P1_EXIT_ANCHOR_CFG.get('entry_avwap_reduce_ratio', 1.00) or 1.00)
_P1_EXIT_BREAKOUT_AVWAP_REDUCE_RATIO = float(_P1_EXIT_ANCHOR_CFG.get('breakout_avwap_reduce_ratio', 0.50) or 0.50)
_P1_EXIT_EVENT_AVWAP_REDUCE_RATIO = float(_P1_EXIT_ANCHOR_CFG.get('event_avwap_reduce_ratio', 0.40) or 0.40)
_P1_EXIT_SESSION_AVWAP_REDUCE_RATIO = float(_P1_EXIT_ANCHOR_CFG.get('session_avwap_reduce_ratio', 0.20) or 0.20)
_P1_EXIT_ENTRY_COST_REDUCE_RATIO = float(_P1_EXIT_ANCHOR_CFG.get('entry_cost_reduce_ratio', 0.25) or 0.25)
_P1_EXIT_AVWAP_SOFT_WEAK_REDUCE_RATIO = float(_P1_EXIT_ANCHOR_CFG.get('avwap_soft_weak_reduce_ratio', 0.15) or 0.15)
_P1_EXIT_TAKE_PROFIT_COST_PREMIUM_PCT = float(
    _P1_EXIT_ANCHOR_CFG.get('take_profit_cost_premium_pct', 6.0) or 6.0
)
_P1_EXIT_BETA_REDUCE_COST_PREMIUM_PCT = float(
    _P1_EXIT_ANCHOR_CFG.get('beta_reduce_cost_premium_pct', 3.5) or 3.5
)
_P1_EXIT_INTRADAY_AVWAP_MIN_BARS = int(_P1_EXIT_ANCHOR_CFG.get('intraday_avwap_min_bars', 8) or 8)
_P1_EXIT_SESSION_AVWAP_ENABLED = bool(_P1_EXIT_ANCHOR_CFG.get('session_avwap_enabled', True))
_P1_EXIT_TIMING_CLEAR_AVWAP_FLOW_REQUIRED = bool(_P1_EXIT_ANCHOR_CFG.get('timing_clear_avwap_flow_required', True))
_P1_BREAKOUT_ANCHOR_CFG = _P1_CFG.get('breakout_anchor', {}) if isinstance(_P1_CFG, dict) else {}
_P1_BREAKOUT_ANCHOR_ENABLED = bool(_P1_BREAKOUT_ANCHOR_CFG.get('enabled', True))
_P1_BREAKOUT_ANCHOR_LOOKBACK_BARS = int(_P1_BREAKOUT_ANCHOR_CFG.get('lookback_bars', 30) or 30)
_P1_BREAKOUT_ANCHOR_HIGH_LOOKBACK_BARS = int(_P1_BREAKOUT_ANCHOR_CFG.get('high_lookback_bars', 5) or 5)
_P1_BREAKOUT_ANCHOR_VOLUME_EXPAND_RATIO = float(
    _P1_BREAKOUT_ANCHOR_CFG.get('volume_expand_ratio', 1.35) or 1.35
)
_P1_BREAKOUT_ANCHOR_MIN_BARS = int(_P1_BREAKOUT_ANCHOR_CFG.get('min_anchor_bars', 5) or 5)
_P1_BREAKOUT_ANCHOR_AVWAP_BREAK_PCT = float(
    _P1_BREAKOUT_ANCHOR_CFG.get('avwap_break_pct', 0.30) or 0.30
)
_P1_BREAKOUT_ANCHOR_BETA_REDUCE_PREMIUM_PCT = float(
    _P1_BREAKOUT_ANCHOR_CFG.get('beta_reduce_premium_pct', 2.50) or 2.50
)
_P1_BREAKOUT_ANCHOR_REQUIRE_AVWAP_CONTROL = bool(_P1_BREAKOUT_ANCHOR_CFG.get('require_avwap_control', True))
_P1_BREAKOUT_ANCHOR_MIN_CONTROL_COUNT = int(_P1_BREAKOUT_ANCHOR_CFG.get('min_control_count', 1) or 1)
_P1_BREAKOUT_ANCHOR_ALLOW_MISSING_STACK = bool(_P1_BREAKOUT_ANCHOR_CFG.get('allow_missing_stack', True))
_P1_BREAKOUT_ANCHOR_ALLOW_SESSION_AVWAP_CONTROL = bool(
    _P1_BREAKOUT_ANCHOR_CFG.get('allow_session_avwap_control', True)
)
_P1_EVENT_ANCHOR_CFG = _P1_CFG.get('event_anchor', {}) if isinstance(_P1_CFG, dict) else {}
_P1_EVENT_ANCHOR_ENABLED = bool(_P1_EVENT_ANCHOR_CFG.get('enabled', True))
_P1_EVENT_ANCHOR_LOOKBACK_BARS = int(_P1_EVENT_ANCHOR_CFG.get('lookback_bars', 60) or 60)
_P1_EVENT_ANCHOR_HIGH_LOOKBACK_BARS = int(_P1_EVENT_ANCHOR_CFG.get('high_lookback_bars', 3) or 3)
_P1_EVENT_ANCHOR_VOLUME_EXPAND_RATIO = float(
    _P1_EVENT_ANCHOR_CFG.get('volume_expand_ratio', 1.50) or 1.50
)
_P1_EVENT_ANCHOR_IMPULSE_PCT = float(_P1_EVENT_ANCHOR_CFG.get('impulse_pct', 1.00) or 1.00)
_P1_EVENT_ANCHOR_MIN_BARS = int(_P1_EVENT_ANCHOR_CFG.get('min_anchor_bars', 4) or 4)
_P1_EVENT_ANCHOR_AVWAP_BREAK_PCT = float(_P1_EVENT_ANCHOR_CFG.get('avwap_break_pct', 0.25) or 0.25)
_P1_EVENT_ANCHOR_BETA_REDUCE_PREMIUM_PCT = float(
    _P1_EVENT_ANCHOR_CFG.get('beta_reduce_premium_pct', 2.00) or 2.00
)
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
_P1_CONCEPT_ECOLOGY_CFG = _P1_CFG.get('concept_ecology', {}) if isinstance(_P1_CFG, dict) else {}
_P1_CONCEPT_ECOLOGY_ENABLED = bool(_P1_CONCEPT_ECOLOGY_CFG.get('enabled', False))
_P1_CONCEPT_ECOLOGY_HARD_GATE_ENABLED = bool(_P1_CONCEPT_ECOLOGY_CFG.get('hard_gate_enabled', True))
_P1_CONCEPT_ECOLOGY_BLOCK_ON_RETREAT = bool(_P1_CONCEPT_ECOLOGY_CFG.get('block_on_retreat', True))
_P1_CONCEPT_ECOLOGY_OBSERVE_ON_HARD_WEAK = bool(_P1_CONCEPT_ECOLOGY_CFG.get('observe_on_hard_weak', True))
_P1_CONCEPT_ECOLOGY_HARD_RETREAT_THRESHOLD = float(
    _P1_CONCEPT_ECOLOGY_CFG.get('hard_retreat_threshold', _P1_CONCEPT_ECOLOGY_CFG.get('retreat_score_max', -20.0)) or -20.0
)
_P1_CONCEPT_ECOLOGY_HARD_WEAK_THRESHOLD = float(
    _P1_CONCEPT_ECOLOGY_CFG.get('hard_weak_threshold', _P1_CONCEPT_ECOLOGY_CFG.get('weak_score_max', 5.0)) or 5.0
)
_P1_CONCEPT_ECOLOGY_HARD_STRONG_THRESHOLD = float(
    _P1_CONCEPT_ECOLOGY_CFG.get('hard_strong_threshold', _P1_CONCEPT_ECOLOGY_CFG.get('strong_score_min', 30.0)) or 30.0
)
_P1_CONCEPT_ECOLOGY_SCORE_BOARD_WEIGHT = float(_P1_CONCEPT_ECOLOGY_CFG.get('score_board_weight', 30.0) or 30.0)
_P1_CONCEPT_ECOLOGY_SCORE_LEADER_WEIGHT = float(_P1_CONCEPT_ECOLOGY_CFG.get('score_leader_weight', 25.0) or 25.0)
_P1_CONCEPT_ECOLOGY_SCORE_BREADTH_WEIGHT = float(_P1_CONCEPT_ECOLOGY_CFG.get('score_breadth_weight', 20.0) or 20.0)
_P1_CONCEPT_ECOLOGY_SCORE_FUND_FLOW_WEIGHT = float(_P1_CONCEPT_ECOLOGY_CFG.get('score_fund_flow_weight', 15.0) or 15.0)
_P1_CONCEPT_ECOLOGY_SCORE_CORE_WEIGHT = float(_P1_CONCEPT_ECOLOGY_CFG.get('score_core_weight', 10.0) or 10.0)
_P1_CONCEPT_ECOLOGY_OBSERVE_ON_RETREAT = bool(_P1_CONCEPT_ECOLOGY_CFG.get('observe_on_retreat', True))
_P1_CONCEPT_ECOLOGY_OBSERVE_ON_WEAK = bool(_P1_CONCEPT_ECOLOGY_CFG.get('observe_on_weak', False))
_P1_CONCEPT_ECOLOGY_OBSERVE_ON_HEAT_CLIFF = bool(_P1_CONCEPT_ECOLOGY_CFG.get('observe_on_heat_cliff', True))
_P1_CONCEPT_ECOLOGY_RETREAT_SCORE_MAX = float(_P1_CONCEPT_ECOLOGY_CFG.get('retreat_score_max', -20.0) or -20.0)
_P1_CONCEPT_ECOLOGY_WEAK_SCORE_MAX = float(_P1_CONCEPT_ECOLOGY_CFG.get('weak_score_max', 5.0) or 5.0)
_P1_CONCEPT_ECOLOGY_HEAT_CLIFF_SCORE_MAX = float(_P1_CONCEPT_ECOLOGY_CFG.get('heat_cliff_score_max', -8.0) or -8.0)
_P1_CONCEPT_ECOLOGY_HEAT_CLIFF_BREADTH_MAX = float(_P1_CONCEPT_ECOLOGY_CFG.get('heat_cliff_breadth_max', 0.38) or 0.38)
_P1_CONCEPT_ECOLOGY_HEAT_CLIFF_LEADER_PCT_MAX = float(_P1_CONCEPT_ECOLOGY_CFG.get('heat_cliff_leader_pct_max', 1.5) or 1.5)
_P1_CONCEPT_ECOLOGY_STRONG_SCORE_MIN = float(_P1_CONCEPT_ECOLOGY_CFG.get('strong_score_min', 30.0) or 30.0)
_P1_CONCEPT_ECOLOGY_EXPAND_SCORE_MIN = float(_P1_CONCEPT_ECOLOGY_CFG.get('expand_score_min', 45.0) or 45.0)
_P1_CONCEPT_ECOLOGY_STRONG_BONUS = int(_P1_CONCEPT_ECOLOGY_CFG.get('strong_bonus', 4) or 4)
_P1_CONCEPT_ECOLOGY_EXPAND_BONUS = int(_P1_CONCEPT_ECOLOGY_CFG.get('expand_bonus', 6) or 6)
_P1_CONCEPT_ECOLOGY_WEAK_PENALTY = int(_P1_CONCEPT_ECOLOGY_CFG.get('weak_penalty', 5) or 5)
_P1_CONCEPT_ECOLOGY_RETREAT_PENALTY = int(_P1_CONCEPT_ECOLOGY_CFG.get('retreat_penalty', 10) or 10)
_P1_CONCEPT_ECOLOGY_JOINT_ENABLED = bool(_P1_CONCEPT_ECOLOGY_CFG.get('joint_enabled', True))
_P1_CONCEPT_ECOLOGY_INDUSTRY_CONTEXT_ENABLED = bool(_P1_CONCEPT_ECOLOGY_CFG.get('industry_context_enabled', True))
_P1_CONCEPT_ECOLOGY_OBSERVE_ON_JOINT_RETREAT = bool(_P1_CONCEPT_ECOLOGY_CFG.get('observe_on_joint_retreat', True))
_P1_CONCEPT_ECOLOGY_OBSERVE_ON_WEAK_NO_SUPPORT = bool(_P1_CONCEPT_ECOLOGY_CFG.get('observe_on_weak_no_support', True))
_P1_CONCEPT_ECOLOGY_JOINT_RETREAT_MIN_COUNT = int(_P1_CONCEPT_ECOLOGY_CFG.get('joint_retreat_min_count', 2) or 2)
_P1_CONCEPT_ECOLOGY_SECONDARY_SUPPORT_MIN_COUNT = int(_P1_CONCEPT_ECOLOGY_CFG.get('secondary_support_min_count', 1) or 1)
_P1_CONCEPT_ECOLOGY_SECONDARY_SUPPORT_BONUS = int(_P1_CONCEPT_ECOLOGY_CFG.get('secondary_support_bonus', 3) or 3)
_P1_CONCEPT_ECOLOGY_JOINT_RETREAT_PENALTY = int(_P1_CONCEPT_ECOLOGY_CFG.get('joint_retreat_penalty', 6) or 6)
_P1_CONCEPT_ECOLOGY_WEAK_NO_SUPPORT_PENALTY = int(_P1_CONCEPT_ECOLOGY_CFG.get('weak_no_support_penalty', 4) or 4)
_P1_CONCEPT_ECOLOGY_INDUSTRY_SUPPORT_BONUS = int(_P1_CONCEPT_ECOLOGY_CFG.get('industry_support_bonus', 3) or 3)
_P1_CONCEPT_ECOLOGY_INDUSTRY_JOINT_WEAK_PENALTY = int(_P1_CONCEPT_ECOLOGY_CFG.get('industry_joint_weak_penalty', 5) or 5)
_P1_CONCEPT_ECOLOGY_OBSERVE_ON_INDUSTRY_JOINT_WEAK = bool(_P1_CONCEPT_ECOLOGY_CFG.get('observe_on_industry_joint_weak', True))
_P1_LEFT_STREAK_CFG = _P1_CFG.get('left_streak_guard', {}) if isinstance(_P1_CFG, dict) else {}
_P1_LEFT_STREAK_ENABLED = bool(_P1_LEFT_STREAK_CFG.get('enabled', True))
_P1_LEFT_STREAK_ONLY_LEFT = bool(_P1_LEFT_STREAK_CFG.get('only_left_side_buy', True))
_P1_LEFT_STREAK_COOLDOWN_HOURS = float(_P1_LEFT_STREAK_CFG.get('cooldown_hours', 72.0) or 72.0)
_P1_LEFT_STREAK_SAME_DAY_OBS_HOURS = float(_P1_LEFT_STREAK_CFG.get('same_day_observe_hours', 18.0) or 18.0)
_P1_LEFT_STREAK_QUICK_CLEAR_MAX_DAYS = float(_P1_LEFT_STREAK_CFG.get('quick_clear_max_hold_days', 3.0) or 3.0)
_P1_LEFT_STREAK_REPEAT_THRESHOLD = int(_P1_LEFT_STREAK_CFG.get('repeat_streak_threshold', 2) or 2)
_P1_LEFT_STREAK_RECENT_CLEAR_PENALTY = int(_P1_LEFT_STREAK_CFG.get('recent_clear_penalty', 4) or 4)
_P1_LEFT_STREAK_WEAK_PENALTY = int(_P1_LEFT_STREAK_CFG.get('weak_penalty', 4) or 4)
_P1_LEFT_STREAK_RETREAT_PENALTY = int(_P1_LEFT_STREAK_CFG.get('retreat_penalty', 8) or 8)
_P1_LEFT_STREAK_REPEAT_WEAK_PENALTY = int(_P1_LEFT_STREAK_CFG.get('repeat_weak_penalty', 6) or 6)
_P1_LEFT_STREAK_REPEAT_RETREAT_PENALTY = int(_P1_LEFT_STREAK_CFG.get('repeat_retreat_penalty', 12) or 12)
_P1_LEFT_STREAK_OBS_RECENT_CLEAR = bool(_P1_LEFT_STREAK_CFG.get('observe_on_recent_clear', False))
_P1_LEFT_STREAK_OBS_WEAK = bool(_P1_LEFT_STREAK_CFG.get('observe_on_weak', False))
_P1_LEFT_STREAK_OBS_RETREAT = bool(_P1_LEFT_STREAK_CFG.get('observe_on_retreat', True))
_P1_LEFT_STREAK_OBS_REPEAT_WEAK = bool(_P1_LEFT_STREAK_CFG.get('observe_on_repeat_weak', False))
_P1_LEFT_STREAK_OBS_REPEAT_RETREAT = bool(_P1_LEFT_STREAK_CFG.get('observe_on_repeat_retreat', True))


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
        'market_phase_detail': thresholds.get('market_phase_detail'),
        'session_policy': thresholds.get('session_policy'),
        'regime': thresholds.get('regime'),
        'board_segment': thresholds.get('board_segment'),
        'security_type': thresholds.get('security_type'),
        'listing_stage': thresholds.get('listing_stage'),
        'price_limit_pct': thresholds.get('price_limit_pct'),
        'concept_boards': thresholds.get('concept_boards'),
        'core_concept_board': thresholds.get('core_concept_board'),
        'concept_ecology': thresholds.get('concept_ecology'),
        'risk_warning': thresholds.get('risk_warning'),
        'volume_drought': thresholds.get('volume_drought'),
        'board_seal_env': thresholds.get('board_seal_env'),
        'seal_side': thresholds.get('seal_side'),
        'instrument_profile': thresholds.get('instrument_profile'),
        'micro_environment': thresholds.get('micro_environment'),
        'profile_override': thresholds.get('profile_override'),
        'session_override': thresholds.get('session_override'),
        'limit_filter': thresholds.get('limit_filter'),
        'observer_only': bool(thresholds.get('observer_only', False)),
        'observer_reason': thresholds.get('observer_reason'),
        'blocked': bool(thresholds.get('blocked', False)),
        'blocked_reason': thresholds.get('blocked_reason'),
    }


def _threshold_block_reason(th_meta: Optional[dict], default: str = 'threshold_blocked') -> str:
    if not isinstance(th_meta, dict):
        return default
    limit_filter = th_meta.get('limit_filter')
    if isinstance(limit_filter, dict) and bool(limit_filter.get('blocked')):
        return 'limit_magnet_block'
    reason = str(th_meta.get('blocked_reason') or '').strip()
    if reason:
        return reason
    policy = str(th_meta.get('session_policy') or '').strip()
    if policy:
        return policy
    return default


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


def _parse_minute_bar_ts(bar: dict) -> Optional[int]:
    if not isinstance(bar, dict):
        return None
    for key in ('timestamp', 'ts'):
        try:
            raw = bar.get(key)
            if raw is None:
                continue
            return int(float(raw))
        except Exception:
            continue
    raw_time = str(bar.get('time') or bar.get('datetime') or '').strip()
    if not raw_time:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M:%S', '%Y/%m/%d %H:%M'):
        try:
            return int(_dt.datetime.strptime(raw_time, fmt).timestamp())
        except Exception:
            pass
    for fmt in ('%H:%M:%S', '%H:%M'):
        try:
            parsed = _dt.datetime.strptime(raw_time, fmt)
            now = _dt.datetime.now()
            return int(
                now.replace(
                    hour=parsed.hour,
                    minute=parsed.minute,
                    second=getattr(parsed, 'second', 0),
                    microsecond=0,
                ).timestamp()
            )
        except Exception:
            pass
    return None


def _compute_pool1_entry_anchor(
    price: float,
    last_buy_price: Optional[float] = None,
    last_buy_at: Optional[int] = None,
    minute_bars_1m: Optional[list[dict]] = None,
    session_avwap: Optional[float] = None,
) -> dict:
    info = {
        'enabled': bool(_P1_EXIT_ANCHOR_ENABLED),
        'reason': 'disabled',
        'last_buy_at': int(last_buy_at or 0),
        'last_buy_price': None,
        'has_entry_cost_anchor': False,
        'entry_cost_anchor': None,
        'entry_cost_break_line': None,
        'entry_cost_lost': False,
        'entry_premium_pct': None,
        'has_entry_avwap': False,
        'entry_avwap': None,
        'entry_avwap_break_line': None,
        'entry_avwap_lost': False,
        'entry_avwap_premium_pct': None,
        'entry_avwap_bars': 0,
        'has_session_avwap': False,
        'session_avwap': None,
        'session_avwap_premium_pct': None,
        'session_avwap_bars': 0,
        'current_above_session_avwap': False,
        'has_breakout_anchor': False,
        'breakout_anchor_reason': 'disabled',
        'breakout_anchor_index': None,
        'breakout_anchor_ts': None,
        'breakout_anchor_price': None,
        'breakout_anchor_avwap': None,
        'breakout_anchor_break_line': None,
        'breakout_anchor_lost': False,
        'breakout_anchor_premium_pct': None,
        'breakout_anchor_bars': 0,
        'has_event_anchor': False,
        'event_anchor_reason': 'disabled',
        'event_anchor_index': None,
        'event_anchor_ts': None,
        'event_anchor_price': None,
        'event_anchor_avwap': None,
        'event_anchor_break_line': None,
        'event_anchor_lost': False,
        'event_anchor_premium_pct': None,
        'event_anchor_bars': 0,
        'available_anchor_count': 0,
        'lost_anchor_count': 0,
        'lost_anchor_labels': [],
        'support_band_low': None,
        'support_band_high': None,
        'support_anchor_type': '',
        'support_anchor_line': None,
        'support_anchor_lost': False,
        'current_above_entry_avwap': False,
        'current_above_breakout_avwap': False,
        'current_above_event_avwap': False,
        'avwap_stack_ready': False,
        'avwap_control_count': 0,
        'avwap_control_labels': [],
        'avwap_loss_count': 0,
        'avwap_loss_labels': [],
        'holding_avwap_control_count': 0,
        'holding_avwap_control_labels': [],
    }
    if not _P1_EXIT_ANCHOR_ENABLED:
        return info

    try:
        px = float(price or 0.0)
    except Exception:
        px = 0.0
    if px <= 0:
        info['reason'] = 'invalid_price'
        return info

    try:
        buy_price = float(last_buy_price or 0.0)
    except Exception:
        buy_price = 0.0
    try:
        buy_at = int(last_buy_at or 0)
    except Exception:
        buy_at = 0

    bars_norm: list[dict] = []
    if isinstance(minute_bars_1m, list):
        for bar in minute_bars_1m:
            ts = _parse_minute_bar_ts(bar)
            if ts is None:
                continue
            try:
                close = float(bar.get('close', 0) or 0)
            except Exception:
                close = 0.0
            if close <= 0:
                continue
            try:
                high = float(bar.get('high', close) or close)
            except Exception:
                high = close
            try:
                volume = float(bar.get('volume', 0) or 0)
            except Exception:
                volume = 0.0
            try:
                amount = float(bar.get('amount', 0) or 0)
            except Exception:
                amount = 0.0
            if amount <= 0 and volume > 0:
                amount = close * volume
            bars_norm.append(
                {
                    'timestamp': ts,
                    'close': close,
                    'high': high if high > 0 else close,
                    'volume': max(0.0, volume),
                    'amount': max(0.0, amount),
                }
            )
    bars_norm.sort(key=lambda x: int(x.get('timestamp', 0) or 0))

    session_avwap_val = None
    try:
        session_avwap_val = float(session_avwap) if session_avwap is not None else None
    except Exception:
        session_avwap_val = None
    if (session_avwap_val is None or session_avwap_val <= 0) and _P1_EXIT_SESSION_AVWAP_ENABLED and bars_norm:
        info['session_avwap_bars'] = len(bars_norm)
        if len(bars_norm) >= _P1_EXIT_INTRADAY_AVWAP_MIN_BARS:
            total_volume = sum(float(x.get('volume', 0) or 0) for x in bars_norm)
            total_amount = sum(float(x.get('amount', 0) or 0) for x in bars_norm)
            if total_volume > 0 and total_amount > 0:
                session_avwap_val = total_amount / total_volume
    elif bars_norm:
        info['session_avwap_bars'] = len(bars_norm)
    if session_avwap_val is not None and session_avwap_val > 0:
        session_premium_pct = (px - session_avwap_val) / session_avwap_val * 100.0
        info.update(
            {
                'has_session_avwap': True,
                'session_avwap': round(session_avwap_val, 4),
                'session_avwap_premium_pct': round(session_premium_pct, 4),
                'current_above_session_avwap': bool(px > session_avwap_val),
            }
        )

    if buy_price > 0:
        cost_break_line = buy_price * (1 - _P1_EXIT_ENTRY_COST_BREAK_PCT / 100.0)
        premium_pct = (px - buy_price) / buy_price * 100.0
        info.update(
            {
                'reason': 'entry_cost_only',
                'last_buy_price': round(buy_price, 4),
                'has_entry_cost_anchor': True,
                'entry_cost_anchor': round(buy_price, 4),
                'entry_cost_break_line': round(cost_break_line, 4),
                'entry_cost_lost': bool(px <= cost_break_line),
                'entry_premium_pct': round(premium_pct, 4),
            }
        )
    else:
        info['reason'] = 'missing_entry_cost'

    if buy_at > 0 and isinstance(minute_bars_1m, list) and minute_bars_1m:
        try:
            buy_day = _dt.datetime.fromtimestamp(buy_at).date()
        except Exception:
            buy_day = None
        if buy_day == _dt.datetime.now().date():
            start_ts = max(0, buy_at - 60)
            valid_bars: list[tuple[float, float]] = []
            for bar in minute_bars_1m:
                ts = _parse_minute_bar_ts(bar)
                if ts is None or ts < start_ts:
                    continue
                try:
                    if _dt.datetime.fromtimestamp(ts).date() != buy_day:
                        continue
                except Exception:
                    continue
                try:
                    volume = float(bar.get('volume', 0) or 0)
                except Exception:
                    volume = 0.0
                if volume <= 0:
                    continue
                try:
                    amount = float(bar.get('amount', 0) or 0)
                except Exception:
                    amount = 0.0
                if amount <= 0:
                    try:
                        close = float(bar.get('close', 0) or 0)
                    except Exception:
                        close = 0.0
                    if close > 0:
                        amount = close * volume
                if amount <= 0:
                    continue
                valid_bars.append((volume, amount))

            info['entry_avwap_bars'] = len(valid_bars)
            if len(valid_bars) >= _P1_EXIT_INTRADAY_AVWAP_MIN_BARS:
                total_volume = sum(v for v, _ in valid_bars)
                total_amount = sum(a for _, a in valid_bars)
                if total_volume > 0 and total_amount > 0:
                    entry_avwap = total_amount / total_volume
                    avwap_break_line = entry_avwap * (1 - _P1_EXIT_ENTRY_AVWAP_BREAK_PCT / 100.0)
                    avwap_premium_pct = (px - entry_avwap) / entry_avwap * 100.0 if entry_avwap > 0 else None
                    info.update(
                        {
                            'reason': 'entry_avwap_ready',
                            'has_entry_avwap': True,
                            'entry_avwap': round(entry_avwap, 4),
                            'entry_avwap_break_line': round(avwap_break_line, 4),
                            'entry_avwap_lost': bool(px <= avwap_break_line),
                            'entry_avwap_premium_pct': round(avwap_premium_pct, 4) if avwap_premium_pct is not None else None,
                            'current_above_entry_avwap': bool(px > entry_avwap),
                        }
                    )
        else:
            info['reason'] = 'entry_day_not_current_session'

    bars_breakout = list(bars_norm)
    if _P1_BREAKOUT_ANCHOR_LOOKBACK_BARS > 0 and len(bars_breakout) > _P1_BREAKOUT_ANCHOR_LOOKBACK_BARS:
        bars_breakout = bars_breakout[-_P1_BREAKOUT_ANCHOR_LOOKBACK_BARS:]
    if _P1_BREAKOUT_ANCHOR_ENABLED and len(bars_breakout) >= max(_P1_BREAKOUT_ANCHOR_MIN_BARS, _P1_BREAKOUT_ANCHOR_HIGH_LOOKBACK_BARS + 1):
        breakout_idx: Optional[int] = None
        for i in range(_P1_BREAKOUT_ANCHOR_HIGH_LOOKBACK_BARS, len(bars_breakout)):
            hist = bars_breakout[i - _P1_BREAKOUT_ANCHOR_HIGH_LOOKBACK_BARS:i]
            prev_high = max(float(x.get('high', x.get('close', 0)) or 0) for x in hist)
            vols = [float(x.get('volume', 0) or 0) for x in hist]
            avg_vol = statistics.mean(vols) if vols else 0.0
            cur_close = float(bars_breakout[i].get('close', 0) or 0)
            cur_vol = float(bars_breakout[i].get('volume', 0) or 0)
            if cur_close <= 0 or prev_high <= 0 or avg_vol <= 0:
                continue
            if cur_close >= prev_high and cur_vol >= avg_vol * _P1_BREAKOUT_ANCHOR_VOLUME_EXPAND_RATIO:
                breakout_idx = i

        if breakout_idx is None:
            info['breakout_anchor_reason'] = 'no_recent_breakout'
        else:
            anchor_bars = bars_breakout[breakout_idx:]
            info['breakout_anchor_bars'] = len(anchor_bars)
            if len(anchor_bars) < _P1_BREAKOUT_ANCHOR_MIN_BARS:
                info['breakout_anchor_reason'] = 'anchor_window_too_short'
            else:
                total_volume = sum(float(x.get('volume', 0) or 0) for x in anchor_bars)
                total_amount = sum(float(x.get('amount', 0) or 0) for x in anchor_bars)
                if total_volume <= 0 or total_amount <= 0:
                    info['breakout_anchor_reason'] = 'invalid_anchor_volume'
                else:
                    breakout_avwap = total_amount / total_volume
                    breakout_break_line = breakout_avwap * (1 - _P1_BREAKOUT_ANCHOR_AVWAP_BREAK_PCT / 100.0)
                    breakout_premium_pct = (px - breakout_avwap) / breakout_avwap * 100.0 if breakout_avwap > 0 else None
                    info.update(
                        {
                            'has_breakout_anchor': True,
                            'breakout_anchor_reason': 'recent_breakout_ready',
                            'breakout_anchor_index': int(breakout_idx),
                            'breakout_anchor_ts': int(anchor_bars[0].get('timestamp', 0) or 0),
                            'breakout_anchor_price': round(float(anchor_bars[0].get('close', 0) or 0), 4),
                            'breakout_anchor_avwap': round(breakout_avwap, 4),
                            'breakout_anchor_break_line': round(breakout_break_line, 4),
                            'breakout_anchor_lost': bool(px <= breakout_break_line),
                            'breakout_anchor_premium_pct': round(breakout_premium_pct, 4) if breakout_premium_pct is not None else None,
                            'breakout_anchor_bars': len(anchor_bars),
                            'current_above_breakout_avwap': bool(px > breakout_avwap),
                        }
                    )
    elif _P1_BREAKOUT_ANCHOR_ENABLED:
        info['breakout_anchor_reason'] = 'insufficient_bars'

    bars_event = list(bars_norm)
    if buy_at > 0:
        bars_event = [x for x in bars_event if int(x.get('timestamp', 0) or 0) >= max(0, buy_at - 60)]
    if _P1_EVENT_ANCHOR_LOOKBACK_BARS > 0 and len(bars_event) > _P1_EVENT_ANCHOR_LOOKBACK_BARS:
        bars_event = bars_event[-_P1_EVENT_ANCHOR_LOOKBACK_BARS:]
    if _P1_EVENT_ANCHOR_ENABLED and len(bars_event) >= max(_P1_EVENT_ANCHOR_MIN_BARS, _P1_EVENT_ANCHOR_HIGH_LOOKBACK_BARS + 2):
        event_idx: Optional[int] = None
        for i in range(_P1_EVENT_ANCHOR_HIGH_LOOKBACK_BARS + 1, len(bars_event)):
            hist = bars_event[i - _P1_EVENT_ANCHOR_HIGH_LOOKBACK_BARS:i]
            prev_bar = bars_event[i - 1]
            prev_close = float(prev_bar.get('close', 0) or 0)
            prev_high = max(float(x.get('high', x.get('close', 0)) or 0) for x in hist)
            vols = [float(x.get('volume', 0) or 0) for x in hist]
            avg_vol = statistics.mean(vols) if vols else 0.0
            cur_close = float(bars_event[i].get('close', 0) or 0)
            cur_vol = float(bars_event[i].get('volume', 0) or 0)
            if cur_close <= 0 or prev_close <= 0 or avg_vol <= 0:
                continue
            impulse_pct = (cur_close - prev_close) / prev_close * 100.0
            if (
                impulse_pct >= _P1_EVENT_ANCHOR_IMPULSE_PCT
                and cur_close >= prev_high
                and cur_vol >= avg_vol * _P1_EVENT_ANCHOR_VOLUME_EXPAND_RATIO
            ):
                event_idx = i

        if event_idx is None:
            info['event_anchor_reason'] = 'no_recent_event'
        else:
            anchor_bars = bars_event[event_idx:]
            info['event_anchor_bars'] = len(anchor_bars)
            if len(anchor_bars) < _P1_EVENT_ANCHOR_MIN_BARS:
                info['event_anchor_reason'] = 'anchor_window_too_short'
            else:
                total_volume = sum(float(x.get('volume', 0) or 0) for x in anchor_bars)
                total_amount = sum(float(x.get('amount', 0) or 0) for x in anchor_bars)
                if total_volume <= 0 or total_amount <= 0:
                    info['event_anchor_reason'] = 'invalid_anchor_volume'
                else:
                    event_avwap = total_amount / total_volume
                    event_break_line = event_avwap * (1 - _P1_EVENT_ANCHOR_AVWAP_BREAK_PCT / 100.0)
                    event_premium_pct = (px - event_avwap) / event_avwap * 100.0 if event_avwap > 0 else None
                    info.update(
                        {
                            'has_event_anchor': True,
                            'event_anchor_reason': 'recent_event_ready',
                            'event_anchor_index': int(event_idx),
                            'event_anchor_ts': int(anchor_bars[0].get('timestamp', 0) or 0),
                            'event_anchor_price': round(float(anchor_bars[0].get('close', 0) or 0), 4),
                            'event_anchor_avwap': round(event_avwap, 4),
                            'event_anchor_break_line': round(event_break_line, 4),
                            'event_anchor_lost': bool(px <= event_break_line),
                            'event_anchor_premium_pct': round(event_premium_pct, 4) if event_premium_pct is not None else None,
                            'event_anchor_bars': len(anchor_bars),
                            'current_above_event_avwap': bool(px > event_avwap),
                        }
                    )
    elif _P1_EVENT_ANCHOR_ENABLED:
        info['event_anchor_reason'] = 'insufficient_bars'

    anchors: list[tuple[str, float, bool]] = []
    if info.get('entry_cost_break_line') is not None:
        anchors.append(('entry_cost', float(info['entry_cost_break_line']), bool(info.get('entry_cost_lost', False))))
    if info.get('entry_avwap_break_line') is not None:
        anchors.append(('entry_avwap', float(info['entry_avwap_break_line']), bool(info.get('entry_avwap_lost', False))))
    if info.get('breakout_anchor_break_line') is not None:
        anchors.append(('breakout_avwap', float(info['breakout_anchor_break_line']), bool(info.get('breakout_anchor_lost', False))))
    if info.get('event_anchor_break_line') is not None:
        anchors.append(('event_avwap', float(info['event_anchor_break_line']), bool(info.get('event_anchor_lost', False))))

    if anchors:
        sorted_anchors = sorted(anchors, key=lambda x: x[1])
        dominant_anchor = max(anchors, key=lambda x: x[1])
        lost_labels = [label for label, _, lost in anchors if lost]
        info.update(
            {
                'available_anchor_count': len(anchors),
                'lost_anchor_count': len(lost_labels),
                'lost_anchor_labels': lost_labels,
                'support_band_low': round(sorted_anchors[0][1], 4),
                'support_band_high': round(sorted_anchors[-1][1], 4),
                'support_anchor_type': dominant_anchor[0],
                'support_anchor_line': round(dominant_anchor[1], 4),
                'support_anchor_lost': bool(dominant_anchor[2]),
            }
        )
    avwap_checks = []
    holding_checks = []
    if info.get('has_entry_avwap'):
        holding_checks.append(('entry_day_avwap', bool(info.get('current_above_entry_avwap'))))
    if info.get('has_breakout_anchor'):
        avwap_checks.append(('breakout_day_avwap', bool(info.get('current_above_breakout_avwap'))))
        holding_checks.append(('breakout_day_avwap', bool(info.get('current_above_breakout_avwap'))))
    if info.get('has_event_anchor'):
        avwap_checks.append(('event_day_avwap', bool(info.get('current_above_event_avwap'))))
        holding_checks.append(('event_day_avwap', bool(info.get('current_above_event_avwap'))))
    if _P1_BREAKOUT_ANCHOR_ALLOW_SESSION_AVWAP_CONTROL and info.get('has_session_avwap'):
        avwap_checks.append(('session_avwap', bool(info.get('current_above_session_avwap'))))
    if avwap_checks:
        info['avwap_stack_ready'] = True
        info['avwap_control_count'] = int(sum(1 for _, passed in avwap_checks if passed))
        info['avwap_control_labels'] = [label for label, passed in avwap_checks if passed]
        info['avwap_loss_count'] = int(sum(1 for _, passed in avwap_checks if not passed))
        info['avwap_loss_labels'] = [label for label, passed in avwap_checks if not passed]
    if holding_checks:
        info['holding_avwap_control_count'] = int(sum(1 for _, passed in holding_checks if passed))
        info['holding_avwap_control_labels'] = [label for label, passed in holding_checks if passed]
    if info.get('avwap_stack_ready'):
        info['reason'] = 'avwap_stack_ready'
    return info


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


def _compute_pool1_left_reclaim(
    *,
    price: float,
    boll_lower: float,
    near_lower: bool,
    below_lower: bool,
    rsi6: Optional[float],
    rsi_oversold_th: float,
    pct_chg: Optional[float],
    vwap: Optional[float] = None,
    bias_vwap: Optional[float] = None,
    intraday_prices: Optional[list[float]] = None,
    short_zscore: Optional[float] = None,
    prev_price: Optional[float] = None,
    bid_ask_ratio: Optional[float] = None,
    ask_wall_absorb_ratio: Optional[float] = None,
    super_net_flow_bps: Optional[float] = None,
    minute_bars_1m: Optional[list[dict]] = None,
) -> tuple[bool, dict]:
    info = {
        'enabled': bool(_P1_LEFT_RECLAIM_ENABLED),
        'candidate': False,
        'oversold_pressure': False,
        'extreme_items': [],
        'current_above_lower': False,
        'recent_break_below_lower': False,
        'reclaimed_lower': False,
        'has_intraday_avwap': False,
        'current_above_intraday_avwap': False,
        'reclaimed_intraday_avwap': False,
        'higher_low_1m': False,
        'higher_low_3m': False,
        'vwap': None,
        'bias_vwap': None,
        'short_zscore': None,
        'confirm_items': [],
        'required_confirms': 0,
        'confirm_count': 0,
        'score_bonus': 0,
        'reason': '',
        'stage_a': {
            'name': 'extreme_deviation',
            'passed': False,
            'items': [],
            'reason': '',
        },
        'stage_b': {
            'name': 'reclaim_confirmation',
            'passed': False,
            'items': [],
            'required_confirms': 0,
            'confirm_count': 0,
            'reason': '',
        },
    }
    if not _P1_LEFT_RECLAIM_ENABLED or price <= 0 or boll_lower <= 0:
        info['reason'] = 'disabled_or_invalid'
        info['stage_a']['reason'] = 'disabled_or_invalid'
        info['stage_b']['reason'] = 'disabled_or_invalid'
        return True, info

    current_above_lower = bool(price >= boll_lower * (1 - _P1_LEFT_RECLAIM_PRICE_BUFFER_PCT))
    info['current_above_lower'] = current_above_lower

    recent_break = bool(below_lower)
    higher_low = False
    higher_low_3m = False
    bars = minute_bars_1m if isinstance(minute_bars_1m, list) else []
    valid_lows: list[float] = []
    if bars:
        for b in bars[-max(2, _P1_LEFT_RECLAIM_LOOKBACK_BARS):]:
            try:
                low = float(b.get('low', 0) or 0)
            except Exception:
                low = 0.0
            if low > 0:
                valid_lows.append(low)
        if valid_lows:
            recent_low = min(valid_lows)
            recent_break = bool(recent_break or recent_low <= boll_lower * (1 - _P1_LEFT_RECLAIM_BREAK_EPS_PCT))
        if len(valid_lows) >= 3:
            recent_lows = valid_lows[-3:]
            higher_low = bool(
                recent_lows[-2] <= recent_lows[-3]
                and recent_lows[-1] >= recent_lows[-2] * (1 + _P1_LEFT_RECLAIM_HIGHER_LOW_RISE_PCT)
            )
        if len(valid_lows) >= 9:
            grouped_lows: list[float] = []
            recent_valid_lows = valid_lows[-9:]
            for idx in range(0, len(recent_valid_lows), 3):
                chunk = [x for x in recent_valid_lows[idx: idx + 3] if x > 0]
                if chunk:
                    grouped_lows.append(min(chunk))
            if len(grouped_lows) >= 3:
                recent_grouped_lows = grouped_lows[-3:]
                higher_low_3m = bool(
                    recent_grouped_lows[-2] <= recent_grouped_lows[-3]
                    and recent_grouped_lows[-1] >= recent_grouped_lows[-2] * (1 + _P1_LEFT_RECLAIM_HIGHER_LOW_3M_RISE_PCT)
                )
    info['recent_break_below_lower'] = recent_break
    info['higher_low_1m'] = higher_low
    info['higher_low_3m'] = higher_low_3m

    try:
        rsi_val = float(rsi6) if rsi6 is not None else None
    except Exception:
        rsi_val = None
    try:
        pct_val = float(pct_chg) if pct_chg is not None else None
    except Exception:
        pct_val = None
    try:
        vwap_val = float(vwap) if vwap is not None else None
    except Exception:
        vwap_val = None
    try:
        bias_vwap_val = float(bias_vwap) if bias_vwap is not None else None
    except Exception:
        bias_vwap_val = None
    if bias_vwap_val is None and vwap_val is not None and vwap_val > 0 and price > 0:
        try:
            bias_vwap_val = (float(price) - vwap_val) / vwap_val * 100.0
        except Exception:
            bias_vwap_val = None
    try:
        short_zscore_val = float(short_zscore) if short_zscore is not None else None
    except Exception:
        short_zscore_val = None
    if short_zscore_val is None and isinstance(intraday_prices, list):
        valid_prices = []
        for item in intraday_prices:
            try:
                pv = float(item)
            except Exception:
                pv = 0.0
            if pv > 0:
                valid_prices.append(pv)
        if len(valid_prices) >= 10:
            short_zscore_val = _robust_zscore(valid_prices)
    info['vwap'] = round(vwap_val, 4) if vwap_val is not None and vwap_val > 0 else None
    info['bias_vwap'] = round(bias_vwap_val, 4) if bias_vwap_val is not None else None
    info['short_zscore'] = round(short_zscore_val, 4) if short_zscore_val is not None else None
    info['has_intraday_avwap'] = bool(vwap_val is not None and vwap_val > 0)
    oversold_pressure = bool(
        (rsi_val is not None and rsi_val <= float(rsi_oversold_th))
        or (pct_val is not None and pct_val <= -3.0)
    )
    info['oversold_pressure'] = oversold_pressure
    extreme_items: list[str] = []
    if below_lower:
        extreme_items.append('跌破下轨')
    if recent_break:
        extreme_items.append('近期有效跌破')
    if near_lower and oversold_pressure:
        if rsi_val is not None and rsi_val <= float(rsi_oversold_th):
            extreme_items.append(f'贴近下轨+RSI超卖({rsi_val:.1f})')
        elif pct_val is not None and pct_val <= -3.0:
            extreme_items.append(f'贴近下轨+日内大跌({pct_val:.2f}%)')
        else:
            extreme_items.append('贴近下轨+超跌')
    if bias_vwap_val is not None and bias_vwap_val <= _P1_LEFT_RECLAIM_BIAS_VWAP_DEEP_THRESHOLD_PCT:
        extreme_items.append(f'VWAP深偏离({bias_vwap_val:.2f}%)')
    if short_zscore_val is not None and short_zscore_val <= _P1_LEFT_RECLAIM_LEFT_TAIL_ZSCORE_THRESHOLD:
        extreme_items.append(f'短周期左尾Z({short_zscore_val:.2f})')
    candidate = bool(extreme_items)
    info['candidate'] = candidate
    info['extreme_items'] = list(extreme_items)
    info['stage_a']['passed'] = candidate
    info['stage_a']['items'] = list(extreme_items)
    if not candidate:
        info['reason'] = 'reclaim_candidate_missing'
        info['stage_a']['reason'] = 'reclaim_candidate_missing'
        info['stage_b']['reason'] = 'waiting_stage_a'
        return False, info
    info['stage_a']['reason'] = 'ok'

    reclaimed_lower = bool(
        current_above_lower
        and (
            recent_break
            or (prev_price is not None and float(prev_price) > 0 and float(prev_price) < boll_lower)
        )
    )
    info['reclaimed_lower'] = reclaimed_lower
    current_above_intraday_avwap = bool(
        vwap_val is not None
        and vwap_val > 0
        and price >= vwap_val * (1 + _P1_LEFT_RECLAIM_INTRADAY_AVWAP_RECLAIM_BUFFER_PCT)
    )
    intraday_avwap_recent_below = False
    if vwap_val is not None and vwap_val > 0:
        if prev_price is not None:
            try:
                intraday_avwap_recent_below = float(prev_price) < vwap_val
            except Exception:
                intraday_avwap_recent_below = False
        if not intraday_avwap_recent_below and bars:
            break_line = vwap_val * (1 - _P1_LEFT_RECLAIM_INTRADAY_AVWAP_RECLAIM_BUFFER_PCT)
            for b in bars[-max(3, _P1_LEFT_RECLAIM_LOOKBACK_BARS):]:
                try:
                    low = float(b.get('low', 0) or 0)
                except Exception:
                    low = 0.0
                if low > 0 and low <= break_line:
                    intraday_avwap_recent_below = True
                    break
    reclaimed_intraday_avwap = bool(current_above_intraday_avwap and intraday_avwap_recent_below)
    info['current_above_intraday_avwap'] = current_above_intraday_avwap
    info['reclaimed_intraday_avwap'] = reclaimed_intraday_avwap

    confirm_items: list[str] = []
    if reclaimed_lower:
        confirm_items.append('下轨回收')
    if reclaimed_intraday_avwap:
        confirm_items.append('日内AVWAP回收')
    if bid_ask_ratio is not None and float(bid_ask_ratio) >= _P1_LEFT_RECLAIM_BIDASK_MIN:
        confirm_items.append(f'承接回暖({float(bid_ask_ratio):.2f})')
    if ask_wall_absorb_ratio is not None and float(ask_wall_absorb_ratio) >= _P1_LEFT_RECLAIM_ASK_ABSORB_MIN:
        confirm_items.append(f'压盘吸收({float(ask_wall_absorb_ratio):.2f})')
    if super_net_flow_bps is not None and float(super_net_flow_bps) >= _P1_LEFT_RECLAIM_SUPER_INFLOW_BPS_MIN:
        confirm_items.append(f'超大单回流({float(super_net_flow_bps):+.0f}bps)')
    if higher_low:
        confirm_items.append('1m低点抬高')
    if higher_low_3m:
        confirm_items.append('3m低点抬高')

    required_confirms = (
        _P1_LEFT_RECLAIM_BELOW_MIN_CONFIRMS if below_lower else _P1_LEFT_RECLAIM_NEAR_MIN_CONFIRMS
    )
    info['confirm_items'] = confirm_items
    info['required_confirms'] = int(required_confirms)
    info['confirm_count'] = int(len(confirm_items))
    info['score_bonus'] = min(_P1_LEFT_RECLAIM_MAX_BONUS, len(confirm_items) * _P1_LEFT_RECLAIM_SCORE_BONUS)
    info['stage_b']['items'] = list(confirm_items)
    info['stage_b']['required_confirms'] = int(required_confirms)
    info['stage_b']['confirm_count'] = int(len(confirm_items))

    if len(confirm_items) < required_confirms:
        info['reason'] = 'reclaim_confirm_missing'
        info['stage_b']['reason'] = 'reclaim_confirm_missing'
        return False, info

    info['reason'] = 'ok'
    info['stage_b']['passed'] = True
    info['stage_b']['reason'] = 'ok'
    return True, info


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _bounded_component(value: float, weight: float) -> float:
    return max(-abs(float(weight)), min(abs(float(weight)), float(value)))


def _fallback_concept_ecology_components(ecology: dict, state: str) -> dict:
    try:
        pct = float(ecology.get('pct_chg', 0.0) or 0.0)
    except Exception:
        pct = 0.0
    try:
        breadth = float(ecology.get('breadth_ratio', 0.0) or 0.0)
    except Exception:
        breadth = 0.0
    try:
        leader_pct = float(ecology.get('leader_pct', 0.0) or 0.0)
    except Exception:
        leader_pct = 0.0
    try:
        fund_flow = float(ecology.get('main_net_inflow') or ecology.get('fund_flow') or 0.0)
    except Exception:
        fund_flow = 0.0

    board_score = _bounded_component((pct / 3.0) * _P1_CONCEPT_ECOLOGY_SCORE_BOARD_WEIGHT, _P1_CONCEPT_ECOLOGY_SCORE_BOARD_WEIGHT)
    breadth_score = _bounded_component(breadth * _P1_CONCEPT_ECOLOGY_SCORE_BREADTH_WEIGHT, _P1_CONCEPT_ECOLOGY_SCORE_BREADTH_WEIGHT)
    leader_score = _bounded_component((leader_pct / 8.0) * _P1_CONCEPT_ECOLOGY_SCORE_LEADER_WEIGHT, _P1_CONCEPT_ECOLOGY_SCORE_LEADER_WEIGHT)
    if fund_flow > 0:
        fund_score = _P1_CONCEPT_ECOLOGY_SCORE_FUND_FLOW_WEIGHT
    elif fund_flow < 0:
        fund_score = -_P1_CONCEPT_ECOLOGY_SCORE_FUND_FLOW_WEIGHT
    else:
        fund_score = 0.0
    if state in ('expand', 'strong'):
        core_score = _P1_CONCEPT_ECOLOGY_SCORE_CORE_WEIGHT
    elif state == 'retreat':
        core_score = -_P1_CONCEPT_ECOLOGY_SCORE_CORE_WEIGHT
    else:
        core_score = 0.0
    return {
        'board_strength': round(board_score, 2),
        'leader_status': round(leader_score, 2),
        'strong_breadth': round(breadth_score, 2),
        'fund_flow': round(fund_score, 2),
        'core_degree': round(core_score, 2),
    }


def _compute_pool1_concept_hard_score(ecology: dict, state: str) -> tuple[float, dict]:
    raw_components = ecology.get('score_components')
    if not isinstance(raw_components, dict):
        raw_components = ecology.get('ecology_components')
    components = dict(raw_components) if isinstance(raw_components, dict) else _fallback_concept_ecology_components(ecology, state)
    normalized = {
        'board_strength': _safe_float(components.get('board_strength'), 0.0),
        'leader_status': _safe_float(components.get('leader_status'), 0.0),
        'strong_breadth': _safe_float(components.get('strong_breadth'), 0.0),
        'fund_flow': _safe_float(components.get('fund_flow'), 0.0),
        'core_degree': _safe_float(components.get('core_degree'), 0.0),
    }
    try:
        structured_score = float(ecology.get('structured_score', ecology.get('ecology_score')) or 0.0)
    except Exception:
        structured_score = 0.0
    if structured_score == 0.0 and any(abs(v) > 0 for v in normalized.values()):
        structured_score = sum(normalized.values())
    if structured_score == 0.0:
        try:
            structured_score = float(ecology.get('score', 0.0) or 0.0)
        except Exception:
            structured_score = 0.0
    return round(float(structured_score), 2), {k: round(float(v), 2) for k, v in normalized.items()}


def _compute_pool1_concept_ecology_gate(
    *,
    core_concept_board: Optional[str],
    concept_boards: Optional[list[str]],
    concept_ecology: Optional[dict],
    concept_ecology_multi: Optional[list[dict]] = None,
    industry: Optional[str] = None,
    industry_ecology: Optional[dict] = None,
) -> dict:
    info = {
        'enabled': bool(_P1_CONCEPT_ECOLOGY_ENABLED),
        'core_concept_board': str(core_concept_board or '').strip(),
        'concept_boards': list(concept_boards or []),
        'industry': str(industry or '').strip(),
        'state': 'unknown',
        'score': 0.0,
        'hard_score': 0.0,
        'hard_gate_enabled': bool(_P1_CONCEPT_ECOLOGY_HARD_GATE_ENABLED),
        'hard_thresholds': {
            'retreat': _P1_CONCEPT_ECOLOGY_HARD_RETREAT_THRESHOLD,
            'weak': _P1_CONCEPT_ECOLOGY_HARD_WEAK_THRESHOLD,
            'strong': _P1_CONCEPT_ECOLOGY_HARD_STRONG_THRESHOLD,
        },
        'gate_level': 'unknown',
        'blocked': False,
        'blocked_reason': '',
        'score_components': {},
        'core_degree_method': '',
        'score_weights': {
            'board_strength': _P1_CONCEPT_ECOLOGY_SCORE_BOARD_WEIGHT,
            'leader_status': _P1_CONCEPT_ECOLOGY_SCORE_LEADER_WEIGHT,
            'strong_breadth': _P1_CONCEPT_ECOLOGY_SCORE_BREADTH_WEIGHT,
            'fund_flow': _P1_CONCEPT_ECOLOGY_SCORE_FUND_FLOW_WEIGHT,
            'core_degree': _P1_CONCEPT_ECOLOGY_SCORE_CORE_WEIGHT,
        },
        'source': '',
        'updated_at': None,
        'updated_at_iso': '',
        'observe_only': False,
        'observe_reason': '',
        'score_adj': 0,
        'reason': '',
        'concept_ecology_multi': [],
        'concept_ecology_count': 0,
        'secondary_support_count': 0,
        'secondary_weak_count': 0,
        'retreat_like_count': 0,
        'joint_retreat': False,
        'secondary_support': False,
        'weak_no_secondary_support': False,
        'industry_state': 'unknown',
        'industry_score': 0.0,
        'industry_source': '',
        'industry_updated_at': None,
        'industry_updated_at_iso': '',
        'industry_support': False,
        'industry_weak': False,
        'industry_retreat': False,
        'industry_joint_weak': False,
        'supporting_concepts': [],
        'retreat_concepts': [],
        'industry_ecology': {},
    }
    if not _P1_CONCEPT_ECOLOGY_ENABLED:
        info['reason'] = 'disabled'
        info['gate_level'] = 'disabled'
        return info

    ecology = dict(concept_ecology or {})
    if not info['core_concept_board'] and info['concept_boards']:
        info['core_concept_board'] = str(info['concept_boards'][0] or '').strip()
    if not info['core_concept_board']:
        info['reason'] = 'concept_missing'
        info['gate_level'] = 'missing'
        return info
    if not ecology:
        info['reason'] = 'concept_snapshot_missing'
        info['gate_level'] = 'snapshot_missing'
        return info

    state = str(ecology.get('state') or 'unknown').strip().lower()
    try:
        score = float(ecology.get('score', 0.0) or 0.0)
    except Exception:
        score = 0.0
    info['state'] = state or 'unknown'
    info['score'] = round(score, 2)
    hard_score, score_components = _compute_pool1_concept_hard_score(ecology, info['state'])
    core_component = float(score_components.get('core_degree', 0.0) or 0.0)
    if abs(core_component) < 0.001 and info['core_concept_board']:
        primary_board = str((info['concept_boards'] or [''])[0] or '').strip()
        if state in ('expand', 'strong'):
            core_component = _P1_CONCEPT_ECOLOGY_SCORE_CORE_WEIGHT
        elif state == 'retreat':
            core_component = -_P1_CONCEPT_ECOLOGY_SCORE_CORE_WEIGHT
        elif primary_board and primary_board == info['core_concept_board']:
            core_component = round(_P1_CONCEPT_ECOLOGY_SCORE_CORE_WEIGHT * 0.4, 2)
        if abs(core_component) >= 0.001:
            score_components['core_degree'] = round(core_component, 2)
            hard_score = round(float(hard_score) + core_component, 2)
            info['core_degree_method'] = 'core_concept_position'
    info['hard_score'] = round(hard_score, 2)
    info['score_components'] = score_components
    for key in ('pct_chg', 'breadth_ratio', 'leader_pct', 'turnover', 'main_net_inflow', 'main_net_inflow_ratio'):
        if key in ecology:
            info[key] = ecology.get(key)
    info['source'] = str(ecology.get('source') or '').strip()
    info['updated_at'] = ecology.get('updated_at')
    info['updated_at_iso'] = str(ecology.get('updated_at_iso') or '').strip()
    industry_info = dict(industry_ecology or {})
    info['industry_ecology'] = dict(industry_info)
    industry_state = str(industry_info.get('state') or 'unknown').strip().lower()
    try:
        industry_score = float(industry_info.get('score', 0.0) or 0.0)
    except Exception:
        industry_score = 0.0
    info['industry_state'] = industry_state or 'unknown'
    info['industry_score'] = round(industry_score, 2)
    info['industry_source'] = str(industry_info.get('source') or '').strip()
    info['industry_updated_at'] = industry_info.get('updated_at')
    info['industry_updated_at_iso'] = str(industry_info.get('updated_at_iso') or '').strip()

    ecology_multi_items: list[dict] = []
    seen_multi: set[str] = set()
    for raw in concept_ecology_multi or []:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        name = str(item.get('concept_name') or item.get('core_concept_board') or '').strip()
        code = str(item.get('board_code') or '').strip().upper()
        key = f'{code}|{name}'
        if not name and not code:
            continue
        if key in seen_multi:
            continue
        seen_multi.add(key)
        if name:
            item['concept_name'] = name
        if code:
            item['board_code'] = code
        ecology_multi_items.append(item)
    if not ecology_multi_items and ecology:
        fallback_item = dict(ecology)
        if info['core_concept_board'] and not fallback_item.get('concept_name'):
            fallback_item['concept_name'] = info['core_concept_board']
        ecology_multi_items.append(fallback_item)
    info['concept_ecology_multi'] = ecology_multi_items
    info['concept_ecology_count'] = len(ecology_multi_items)

    if _P1_CONCEPT_ECOLOGY_HARD_GATE_ENABLED:
        hard_score = float(info.get('hard_score', 0.0) or 0.0)
        retreat_hit = bool(state == 'retreat' or hard_score < _P1_CONCEPT_ECOLOGY_HARD_RETREAT_THRESHOLD)
        weak_hit = bool(state == 'weak' or hard_score < _P1_CONCEPT_ECOLOGY_HARD_WEAK_THRESHOLD)
        strong_hit = bool(state in ('expand', 'strong') or hard_score >= _P1_CONCEPT_ECOLOGY_HARD_STRONG_THRESHOLD)
        if retreat_hit and _P1_CONCEPT_ECOLOGY_BLOCK_ON_RETREAT:
            info['blocked'] = True
            info['blocked_reason'] = 'concept_ecology_retreat_block'
            info['gate_level'] = 'blocked'
            info['score_adj'] = -abs(_P1_CONCEPT_ECOLOGY_RETREAT_PENALTY)
            info['reason'] = 'hard_retreat_block'
            return info
        if weak_hit:
            info['gate_level'] = 'observe'
            info['score_adj'] -= abs(_P1_CONCEPT_ECOLOGY_WEAK_PENALTY)
            if _P1_CONCEPT_ECOLOGY_OBSERVE_ON_HARD_WEAK:
                info['observe_only'] = True
                info['observe_reason'] = 'concept_ecology_weak_observe'
            info['reason'] = 'hard_weak_observe'
            return info
        info['gate_level'] = 'strong' if strong_hit else 'allow'
    else:
        info['gate_level'] = 'legacy'

    if ecology_multi_items and _P1_CONCEPT_ECOLOGY_JOINT_ENABLED:
        retreat_like_count = 0
        secondary_support_count = 0
        secondary_weak_count = 0
        supporting_concepts: list[str] = []
        retreat_concepts: list[str] = []
        core_name = str(info['core_concept_board'] or '').strip()
        for item in ecology_multi_items:
            item_name = str(item.get('concept_name') or '').strip()
            item_state = str(item.get('state') or '').strip().lower()
            try:
                item_score = float(item.get('score', 0.0) or 0.0)
            except Exception:
                item_score = 0.0
            is_support = item_state in ('expand', 'strong') or item_score >= _P1_CONCEPT_ECOLOGY_STRONG_SCORE_MIN
            is_retreat_like = item_state == 'retreat' or item_score <= _P1_CONCEPT_ECOLOGY_RETREAT_SCORE_MAX
            is_weak_like = item_state == 'weak' or item_score <= _P1_CONCEPT_ECOLOGY_WEAK_SCORE_MAX
            if is_retreat_like:
                retreat_like_count += 1
                if item_name:
                    retreat_concepts.append(item_name)
            if item_name and item_name != core_name:
                if is_support:
                    secondary_support_count += 1
                    supporting_concepts.append(item_name)
                elif is_weak_like:
                    secondary_weak_count += 1
        info['retreat_like_count'] = retreat_like_count
        info['secondary_support_count'] = secondary_support_count
        info['secondary_weak_count'] = secondary_weak_count
        info['supporting_concepts'] = supporting_concepts
        info['retreat_concepts'] = retreat_concepts

        joint_retreat = retreat_like_count >= max(1, _P1_CONCEPT_ECOLOGY_JOINT_RETREAT_MIN_COUNT)
        secondary_support = secondary_support_count >= max(1, _P1_CONCEPT_ECOLOGY_SECONDARY_SUPPORT_MIN_COUNT)
        weak_no_secondary_support = (state == 'weak' or score <= _P1_CONCEPT_ECOLOGY_WEAK_SCORE_MAX) and not secondary_support
        info['joint_retreat'] = bool(joint_retreat)
        info['secondary_support'] = bool(secondary_support)
        info['weak_no_secondary_support'] = bool(weak_no_secondary_support)

        if joint_retreat:
            info['score_adj'] -= abs(_P1_CONCEPT_ECOLOGY_JOINT_RETREAT_PENALTY)
            if _P1_CONCEPT_ECOLOGY_OBSERVE_ON_JOINT_RETREAT:
                info['observe_only'] = True
                info['observe_reason'] = 'concept_joint_retreat'
            info['reason'] = 'joint_retreat'
            return info

        if secondary_support:
            info['score_adj'] += abs(_P1_CONCEPT_ECOLOGY_SECONDARY_SUPPORT_BONUS)

        if weak_no_secondary_support:
            info['score_adj'] -= abs(_P1_CONCEPT_ECOLOGY_WEAK_NO_SUPPORT_PENALTY)
            if _P1_CONCEPT_ECOLOGY_OBSERVE_ON_WEAK_NO_SUPPORT:
                info['observe_only'] = True
                info['observe_reason'] = 'concept_multi_no_support'
                info['reason'] = 'weak_no_secondary_support'
                return info

    industry_support = industry_state in ('expand', 'strong') or industry_score >= _P1_CONCEPT_ECOLOGY_STRONG_SCORE_MIN
    industry_retreat = industry_state == 'retreat' or industry_score <= _P1_CONCEPT_ECOLOGY_RETREAT_SCORE_MAX
    industry_weak = industry_state == 'weak' or industry_score <= _P1_CONCEPT_ECOLOGY_WEAK_SCORE_MAX
    industry_joint_weak = bool(
        industry_info
        and (state in ('weak', 'retreat') or score <= _P1_CONCEPT_ECOLOGY_WEAK_SCORE_MAX or bool(info.get('joint_retreat')))
        and (industry_retreat or industry_weak)
    )
    info['industry_support'] = bool(industry_support)
    info['industry_retreat'] = bool(industry_retreat)
    info['industry_weak'] = bool(industry_weak)
    info['industry_joint_weak'] = bool(industry_joint_weak)

    if industry_joint_weak:
        info['score_adj'] -= abs(_P1_CONCEPT_ECOLOGY_INDUSTRY_JOINT_WEAK_PENALTY)
        if _P1_CONCEPT_ECOLOGY_OBSERVE_ON_INDUSTRY_JOINT_WEAK:
            info['observe_only'] = True
            info['observe_reason'] = 'concept_industry_joint_weak'
        info['reason'] = 'industry_joint_weak'
        return info

    if industry_support and (state == 'weak' or score <= _P1_CONCEPT_ECOLOGY_WEAK_SCORE_MAX):
        info['score_adj'] += abs(_P1_CONCEPT_ECOLOGY_INDUSTRY_SUPPORT_BONUS)

    if score <= _P1_CONCEPT_ECOLOGY_RETREAT_SCORE_MAX or state == 'retreat':
        info['score_adj'] = -abs(_P1_CONCEPT_ECOLOGY_RETREAT_PENALTY)
        if _P1_CONCEPT_ECOLOGY_OBSERVE_ON_RETREAT:
            info['observe_only'] = True
            info['observe_reason'] = 'concept_retreat'
        info['reason'] = 'retreat'
        return info

    if score <= _P1_CONCEPT_ECOLOGY_WEAK_SCORE_MAX or state == 'weak':
        info['score_adj'] += -abs(_P1_CONCEPT_ECOLOGY_WEAK_PENALTY)
        if _P1_CONCEPT_ECOLOGY_OBSERVE_ON_WEAK and not bool(info.get('secondary_support')) and not industry_support:
            info['observe_only'] = True
            info['observe_reason'] = 'concept_weak'
        if industry_support:
            info['reason'] = 'weak_with_industry_support'
        else:
            info['reason'] = 'weak_with_secondary_support' if bool(info.get('secondary_support')) else 'weak'
        return info

    hard_score = float(info.get('hard_score', score) or score)
    if score >= _P1_CONCEPT_ECOLOGY_EXPAND_SCORE_MIN or hard_score >= _P1_CONCEPT_ECOLOGY_EXPAND_SCORE_MIN or state == 'expand':
        info['score_adj'] = abs(_P1_CONCEPT_ECOLOGY_EXPAND_BONUS)
        if info.get('gate_level') in ('unknown', 'allow'):
            info['gate_level'] = 'strong'
        info['reason'] = 'expand'
        return info

    if score >= _P1_CONCEPT_ECOLOGY_STRONG_SCORE_MIN or hard_score >= _P1_CONCEPT_ECOLOGY_HARD_STRONG_THRESHOLD or state == 'strong':
        info['score_adj'] = abs(_P1_CONCEPT_ECOLOGY_STRONG_BONUS)
        if info.get('gate_level') in ('unknown', 'allow'):
            info['gate_level'] = 'strong'
        info['reason'] = 'strong'
        return info

    if info.get('gate_level') in ('unknown', 'legacy'):
        info['gate_level'] = 'allow'
    info['reason'] = 'neutral_with_secondary_support' if bool(info.get('secondary_support')) else 'neutral'
    return info


def _compute_pool1_left_streak_guard(
    *,
    position_state: Optional[dict],
    concept_gate: Optional[dict],
    now_ts: Optional[int] = None,
) -> dict:
    info = {
        'enabled': bool(_P1_LEFT_STREAK_ENABLED),
        'active': False,
        'observe_only': False,
        'observe_reason': '',
        'score_adj': 0,
        'reason': '',
        'last_buy_type': '',
        'last_sell_type': '',
        'hold_days_before_clear': None,
        'hours_since_clear': None,
        'cooldown_hours': float(_P1_LEFT_STREAK_COOLDOWN_HOURS),
        'remaining_cooldown_hours': None,
        'quick_clear_streak': 0,
        'concept_state': str((concept_gate or {}).get('state') or 'unknown'),
    }
    if not _P1_LEFT_STREAK_ENABLED:
        info['reason'] = 'disabled'
        return info
    if not isinstance(position_state, dict):
        info['reason'] = 'position_state_missing'
        return info

    ts_now = int(now_ts or time.time())
    last_buy_at = int(position_state.get('last_buy_at', 0) or 0)
    last_sell_at = int(position_state.get('last_sell_at', 0) or 0)
    last_buy_type = str(position_state.get('last_buy_type') or '').strip()
    last_sell_type = str(position_state.get('last_sell_type') or '').strip()
    quick_clear_streak = int(position_state.get('left_quick_clear_streak', 0) or 0)
    info['last_buy_type'] = last_buy_type
    info['last_sell_type'] = last_sell_type
    info['quick_clear_streak'] = quick_clear_streak
    if last_buy_at <= 0 or last_sell_at <= 0 or last_sell_at < last_buy_at:
        info['reason'] = 'no_recent_roundtrip'
        return info
    if _P1_LEFT_STREAK_ONLY_LEFT and last_buy_type != 'left_side_buy':
        info['reason'] = 'last_buy_not_left_side'
        return info

    hold_days = max(0.0, float(last_sell_at - last_buy_at) / 86400.0)
    hours_since_clear = max(0.0, float(ts_now - last_sell_at) / 3600.0)
    info['hold_days_before_clear'] = round(hold_days, 4)
    info['hours_since_clear'] = round(hours_since_clear, 2)
    info['remaining_cooldown_hours'] = round(max(0.0, _P1_LEFT_STREAK_COOLDOWN_HOURS - hours_since_clear), 2)
    if hold_days > _P1_LEFT_STREAK_QUICK_CLEAR_MAX_DAYS:
        info['reason'] = 'clear_not_quick'
        return info
    if hours_since_clear > _P1_LEFT_STREAK_COOLDOWN_HOURS:
        info['reason'] = 'cooldown_expired'
        return info

    info['active'] = True
    info['score_adj'] -= abs(_P1_LEFT_STREAK_RECENT_CLEAR_PENALTY)
    info['reason'] = 'recent_quick_clear'

    concept_state = str((concept_gate or {}).get('state') or 'unknown').strip().lower()
    if concept_state == 'retreat':
        info['score_adj'] -= abs(_P1_LEFT_STREAK_RETREAT_PENALTY)
        info['reason'] = 'retreat_after_quick_clear'
        if _P1_LEFT_STREAK_OBS_RETREAT:
            info['observe_only'] = True
            info['observe_reason'] = 'left_side_streak_retreat'
    elif concept_state == 'weak':
        info['score_adj'] -= abs(_P1_LEFT_STREAK_WEAK_PENALTY)
        info['reason'] = 'weak_after_quick_clear'
        if _P1_LEFT_STREAK_OBS_WEAK:
            info['observe_only'] = True
            info['observe_reason'] = 'left_side_streak_weak'

    if quick_clear_streak >= _P1_LEFT_STREAK_REPEAT_THRESHOLD:
        if concept_state == 'retreat':
            info['score_adj'] -= abs(_P1_LEFT_STREAK_REPEAT_RETREAT_PENALTY)
            info['reason'] = 'repeat_retreat_after_quick_clear'
            if _P1_LEFT_STREAK_OBS_REPEAT_RETREAT:
                info['observe_only'] = True
                info['observe_reason'] = 'left_side_repeat_retreat'
        elif concept_state == 'weak':
            info['score_adj'] -= abs(_P1_LEFT_STREAK_REPEAT_WEAK_PENALTY)
            info['reason'] = 'repeat_weak_after_quick_clear'
            if _P1_LEFT_STREAK_OBS_REPEAT_WEAK:
                info['observe_only'] = True
                info['observe_reason'] = 'left_side_repeat_weak'

    if (
        hours_since_clear <= _P1_LEFT_STREAK_SAME_DAY_OBS_HOURS
        and not info['observe_only']
        and _P1_LEFT_STREAK_OBS_RECENT_CLEAR
    ):
        info['observe_only'] = True
        info['observe_reason'] = 'left_side_recent_clear'

    if not info['observe_only']:
        info['observe_reason'] = ''
    return info


def _compute_pool1_left_stage_c_gate(
    *,
    price: float,
    boll_mid: float,
    pct_chg: Optional[float],
    bid_ask_ratio: Optional[float] = None,
    ask_wall_absorb_ratio: Optional[float] = None,
    super_net_flow_bps: Optional[float] = None,
    lure_long: bool = False,
    wash_trade: bool = False,
    concept_gate: Optional[dict] = None,
    left_streak_guard: Optional[dict] = None,
) -> dict:
    info = {
        'name': 'ecology_confirmation',
        'passed': True,
        'items': [],
        'reason': 'ok',
        'observe_only': False,
        'observe_reason': '',
        'high_position_catchdown': False,
        'distribution_structure': False,
        'concept_heat_cliff': False,
        'concept_blocked': False,
        'concept_gate_level': '',
        'joint_retreat': False,
        'weak_no_secondary_support': False,
        'secondary_support_count': 0,
        'industry_context': '',
        'industry_joint_weak': False,
        'industry_support': False,
        'industry_retreat': False,
        'industry_weak': False,
    }

    items: list[str] = []
    concept_info = dict(concept_gate or {})
    streak_info = dict(left_streak_guard or {})
    concept_state = str(concept_info.get('state') or '').strip().lower()
    concept_core = str(concept_info.get('core_concept_board') or '').strip()
    try:
        concept_score = float(concept_info.get('score', 0.0) or 0.0)
    except Exception:
        concept_score = 0.0
    try:
        concept_hard_score = float(concept_info.get('hard_score', concept_score) or concept_score)
    except Exception:
        concept_hard_score = concept_score
    try:
        breadth_ratio = float(concept_info.get('breadth_ratio', 0.0) or 0.0)
    except Exception:
        breadth_ratio = 0.0
    try:
        leader_pct = float(concept_info.get('leader_pct', 0.0) or 0.0)
    except Exception:
        leader_pct = 0.0
    joint_retreat = bool(concept_info.get('joint_retreat'))
    concept_blocked = bool(concept_info.get('blocked'))
    concept_gate_level = str(concept_info.get('gate_level') or '').strip()
    weak_no_secondary_support = bool(concept_info.get('weak_no_secondary_support'))
    secondary_support_count = int(concept_info.get('secondary_support_count', 0) or 0)
    industry_context = str(concept_info.get('industry') or '').strip()
    industry_state = str(concept_info.get('industry_state') or '').strip().lower()
    industry_joint_weak = bool(concept_info.get('industry_joint_weak'))
    industry_support = bool(concept_info.get('industry_support'))
    industry_retreat = bool(concept_info.get('industry_retreat'))
    industry_weak = bool(concept_info.get('industry_weak'))
    if concept_core:
        if concept_state:
            items.append(f'概念生态:{concept_core}:{concept_state}')
        else:
            items.append(f'概念生态:{concept_core}')
    if concept_state == 'retreat':
        items.append('概念退潮主跌段')
    elif concept_state == 'weak':
        items.append('概念承接转弱')
    concept_heat_cliff = bool(
        concept_core
        and concept_state not in ('expand', 'strong')
        and concept_hard_score <= _P1_CONCEPT_ECOLOGY_HEAT_CLIFF_SCORE_MAX
        and breadth_ratio <= _P1_CONCEPT_ECOLOGY_HEAT_CLIFF_BREADTH_MAX
        and leader_pct <= _P1_CONCEPT_ECOLOGY_HEAT_CLIFF_LEADER_PCT_MAX
    )
    if concept_heat_cliff:
        items.append('概念热度断崖')
    if joint_retreat:
        items.append('多概念共弱')
    elif weak_no_secondary_support:
        items.append('次概念未共振')
    elif secondary_support_count > 0:
        items.append(f'次概念共振({secondary_support_count})')
    if industry_joint_weak:
        items.append('行业+概念共弱')
    elif industry_retreat:
        items.append('行业退潮')
    elif industry_weak:
        items.append('行业承接转弱')
    elif industry_support:
        items.append('行业支撑')
    if industry_context and _P1_CONCEPT_ECOLOGY_INDUSTRY_CONTEXT_ENABLED:
        items.append(f'行业语义:{industry_context}:{industry_state or "unknown"}')

    try:
        pct_val = float(pct_chg) if pct_chg is not None else None
    except Exception:
        pct_val = None
    try:
        bidask_val = float(bid_ask_ratio) if bid_ask_ratio is not None else None
    except Exception:
        bidask_val = None
    try:
        absorb_val = float(ask_wall_absorb_ratio) if ask_wall_absorb_ratio is not None else None
    except Exception:
        absorb_val = None
    try:
        super_flow_val = float(super_net_flow_bps) if super_net_flow_bps is not None else None
    except Exception:
        super_flow_val = None

    high_position_catchdown = bool(
        price > 0
        and boll_mid > 0
        and price >= boll_mid * (1 + _P1_LEFT_RECLAIM_HIGH_POSITION_MIDLINE_BUFFER_PCT)
        and pct_val is not None
        and pct_val <= _P1_LEFT_RECLAIM_HIGH_POSITION_DROP_PCT
    )
    if high_position_catchdown:
        items.append('高位补跌风险')

    distribution_structure = bool(
        lure_long
        or wash_trade
        or (
            (absorb_val is None or absorb_val <= _P1_LEFT_RECLAIM_DISTRIBUTION_ASK_ABSORB_MAX)
            and super_flow_val is not None
            and super_flow_val <= _P1_LEFT_RECLAIM_DISTRIBUTION_SUPER_OUT_BPS
            and (
                (bidask_val is not None and bidask_val <= _P1_LEFT_RECLAIM_DISTRIBUTION_BID_ASK_MAX)
                or (pct_val is not None and pct_val <= _P1_LEFT_RECLAIM_HIGH_POSITION_DROP_PCT)
            )
        )
    )
    if distribution_structure:
        items.append('出货结构风险')

    info['high_position_catchdown'] = high_position_catchdown
    info['distribution_structure'] = distribution_structure
    info['concept_heat_cliff'] = concept_heat_cliff
    info['concept_blocked'] = concept_blocked
    info['concept_gate_level'] = concept_gate_level
    info['joint_retreat'] = joint_retreat
    info['weak_no_secondary_support'] = weak_no_secondary_support
    info['secondary_support_count'] = secondary_support_count
    info['industry_context'] = industry_context
    info['industry_joint_weak'] = industry_joint_weak
    info['industry_support'] = industry_support
    info['industry_retreat'] = industry_retreat
    info['industry_weak'] = industry_weak

    if concept_blocked:
        info['passed'] = False
        info['observe_only'] = False
        info['observe_reason'] = ''
        info['reason'] = str(concept_info.get('blocked_reason') or 'concept_ecology_blocked')
    elif bool(concept_info.get('observe_only')):
        info['passed'] = False
        info['observe_only'] = True
        info['observe_reason'] = str(concept_info.get('observe_reason') or 'concept_ecology_gate')
        info['reason'] = info['observe_reason']
    elif concept_heat_cliff and _P1_CONCEPT_ECOLOGY_OBSERVE_ON_HEAT_CLIFF:
        info['passed'] = False
        info['observe_only'] = True
        info['observe_reason'] = 'left_concept_heat_cliff'
        info['reason'] = 'left_concept_heat_cliff'
    elif bool(streak_info.get('observe_only')):
        info['passed'] = False
        info['observe_only'] = True
        info['observe_reason'] = str(streak_info.get('observe_reason') or 'left_streak_guard')
        info['reason'] = info['observe_reason']
    elif high_position_catchdown:
        info['passed'] = False
        info['observe_only'] = True
        info['observe_reason'] = 'left_high_position_catchdown'
        info['reason'] = 'left_high_position_catchdown'
    elif distribution_structure:
        info['passed'] = False
        info['observe_only'] = True
        info['observe_reason'] = 'left_distribution_structure'
        info['reason'] = 'left_distribution_structure'

    if not items:
        items.append('生态门未见异常')
    info['items'] = items
    return info


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

    detail_observe_only = bool(details.get('observe_only', False))
    observe_only = (strength < _P1_SCORE_EXECUTABLE) or bool(
        th_meta.get('observer_only') if isinstance(th_meta, dict) else False
    ) or detail_observe_only
    details['observe_only'] = bool(observe_only)
    details['score_exec_th'] = _P1_SCORE_EXECUTABLE
    details['score_observe_th'] = _P1_SCORE_OBSERVE
    if sig_type == 'timing_clear':
        clear_level = str(details.get('clear_level_hint') or 'full').strip().lower()
        clear_family = str(details.get('clear_family') or 'defense').strip().lower()
        if observe_only:
            clear_level = 'observe'
            if not details.get('suggest_reduce_ratio'):
                details['suggest_reduce_ratio'] = round(float(_P1_EXIT_OBSERVE_REDUCE_RATIO), 2)
        elif clear_level not in ('full', 'partial'):
            clear_level = 'full'
        details['clear_level'] = clear_level
        details['clear_family'] = clear_family
        if clear_level == 'partial' and not details.get('suggest_reduce_ratio'):
            details['suggest_reduce_ratio'] = round(float(_P1_EXIT_PARTIAL_REDUCE_RATIO), 2)
        if clear_level == 'full':
            details['clear_label'] = '清仓执行'
        elif clear_level == 'partial':
            details['clear_label'] = '分批减仓'
        else:
            details['clear_label'] = '观察减仓'

    msg = str(out.get('message') or '')
    if sig_type == 'timing_clear':
        clear_label = str(details.get('clear_label') or ('观察减仓' if observe_only else '清仓执行'))
        if clear_label and clear_label not in msg:
            out['message'] = f'{msg}·{clear_label}' if msg else clear_label
    else:
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
    vwap: Optional[float] = None,
    bias_vwap: Optional[float] = None,
    intraday_prices: Optional[list[float]] = None,
    short_zscore: Optional[float] = None,
    bid_ask_ratio: Optional[float] = None,
    lure_long: bool = False,
    wash_trade: bool = False,
    up_limit: Optional[float] = None,
    down_limit: Optional[float] = None,
    resonance_60m: bool = False,
    volume_pace_ratio: Optional[float] = None,
    volume_pace_state: Optional[str] = None,
    thresholds: Optional[dict] = None,
    prev_price: Optional[float] = None,
    ask_wall_absorb_ratio: Optional[float] = None,
    super_net_flow_bps: Optional[float] = None,
    minute_bars_1m: Optional[list[dict]] = None,
    industry: Optional[str] = None,
    industry_code: Optional[str] = None,
    industry_ecology: Optional[dict] = None,
    core_concept_board: Optional[str] = None,
    concept_boards: Optional[list[str]] = None,
    concept_codes: Optional[list[str]] = None,
    core_concept_code: Optional[str] = None,
    concept_ecology: Optional[dict] = None,
    concept_ecology_multi: Optional[list[dict]] = None,
    position_state: Optional[dict] = None,
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
        reason = _threshold_block_reason(th_meta)
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

    try:
        bias_vwap_seed = float(bias_vwap) if bias_vwap is not None else None
    except Exception:
        bias_vwap_seed = None
    if bias_vwap_seed is None:
        try:
            vwap_seed = float(vwap) if vwap is not None else None
        except Exception:
            vwap_seed = None
        if vwap_seed is not None and vwap_seed > 0 and price > 0:
            try:
                bias_vwap_seed = (float(price) - vwap_seed) / vwap_seed * 100.0
            except Exception:
                bias_vwap_seed = None
    try:
        short_zscore_seed = float(short_zscore) if short_zscore is not None else None
    except Exception:
        short_zscore_seed = None
    if short_zscore_seed is None and isinstance(intraday_prices, list):
        try:
            valid_intraday_prices = [float(p) for p in intraday_prices if float(p or 0) > 0]
        except Exception:
            valid_intraday_prices = []
        if len(valid_intraday_prices) >= 10:
            short_zscore_seed = _robust_zscore(valid_intraday_prices)
    stage_a_seed = bool(
        near_lower
        or below_lower
        or (bias_vwap_seed is not None and bias_vwap_seed <= _P1_LEFT_RECLAIM_BIAS_VWAP_DEEP_THRESHOLD_PCT)
        or (short_zscore_seed is not None and short_zscore_seed <= _P1_LEFT_RECLAIM_LEFT_TAIL_ZSCORE_THRESHOLD)
    )

    if _P1_ENABLED_V2 and not stage_a_seed:
        return _pool1_reject(result, 'trigger_missing')

    reclaim_ok, reclaim_info = _compute_pool1_left_reclaim(
        price=price,
        boll_lower=boll_lower,
        near_lower=near_lower,
        below_lower=below_lower,
        rsi6=rsi6,
        rsi_oversold_th=rsi_oversold_th,
        pct_chg=pct_chg,
        vwap=vwap,
        bias_vwap=bias_vwap,
        intraday_prices=intraday_prices,
        short_zscore=short_zscore,
        prev_price=prev_price,
        bid_ask_ratio=bid_ask_ratio,
        ask_wall_absorb_ratio=ask_wall_absorb_ratio,
        super_net_flow_bps=super_net_flow_bps,
        minute_bars_1m=minute_bars_1m,
    )
    reclaim_candidate = bool(reclaim_info.get('candidate'))
    reclaim_reason = str(reclaim_info.get('reason') or 'reclaim_missing')
    reclaim_pending = bool(reclaim_candidate and not reclaim_ok)
    if not reclaim_candidate:
        return _pool1_reject(result, str(reclaim_info.get('reason') or 'reclaim_missing'), {
            'left_reclaim': reclaim_info,
            'threshold': th_meta,
        })

    concept_gate = _compute_pool1_concept_ecology_gate(
        core_concept_board=core_concept_board,
        concept_boards=concept_boards,
        concept_ecology=concept_ecology,
        concept_ecology_multi=concept_ecology_multi,
        industry=industry,
        industry_ecology=industry_ecology,
    )
    if concept_gate.get('blocked'):
        return _pool1_reject(result, str(concept_gate.get('blocked_reason') or 'concept_ecology_blocked'), {
            'left_reclaim': reclaim_info,
            'concept_ecology': concept_gate,
            'threshold': th_meta,
        })
    left_streak_guard = _compute_pool1_left_streak_guard(
        position_state=position_state,
        concept_gate=concept_gate,
    )
    stage_c_gate = _compute_pool1_left_stage_c_gate(
        price=price,
        boll_mid=boll_mid,
        pct_chg=pct_chg,
        bid_ask_ratio=bid_ask_ratio,
        ask_wall_absorb_ratio=ask_wall_absorb_ratio,
        super_net_flow_bps=super_net_flow_bps,
        lure_long=lure_long,
        wash_trade=wash_trade,
        concept_gate=concept_gate,
        left_streak_guard=left_streak_guard,
    )

    conditions = []
    if near_lower:
        conditions.append(f'贴近下轨({dist_to_lower:+.2f}%<= {near_lower_th:.1f}%)')
    if below_lower:
        conditions.append('跌破下轨')
    if rsi6 is not None and rsi6 <= rsi_oversold_th:
        conditions.append(f'RSI6超卖({rsi6:.1f}<={rsi_oversold_th:.0f})')
    if pct_chg is not None and pct_chg < -3:
        conditions.append(f'跌幅{pct_chg:.2f}%')
    reclaim_items = list(reclaim_info.get('confirm_items') or [])
    if reclaim_items:
        conditions.append('回收确认:' + '+'.join(reclaim_items))
    elif reclaim_pending:
        extreme_items = list(reclaim_info.get('extreme_items') or [])
        pending_suffix = '+'.join(extreme_items) if extreme_items else reclaim_reason
        conditions.append('回收待确认:' + pending_suffix)
    concept_summary = ''
    if concept_gate.get('enabled') and concept_gate.get('core_concept_board'):
        concept_summary = (
            f"{concept_gate.get('core_concept_board')}:{concept_gate.get('gate_level') or concept_gate.get('state')}"
            f"({float(concept_gate.get('hard_score', concept_gate.get('score', 0.0)) or 0.0):+.0f})"
        )
        conditions.append('概念生态:' + concept_summary)
    if left_streak_guard.get('active'):
        streak_reason = str(left_streak_guard.get('reason') or 'recent_quick_clear')
        hours_since_clear = left_streak_guard.get('hours_since_clear')
        if hours_since_clear is not None:
            conditions.append(f'左侧抑制:{streak_reason}({float(hours_since_clear):.1f}h)')
        else:
            conditions.append(f'左侧抑制:{streak_reason}')
    if bool(stage_c_gate.get('high_position_catchdown')):
        conditions.append('生态门:高位补跌')
    if bool(stage_c_gate.get('distribution_structure')):
        conditions.append('生态门:出货结构')
    if bool(stage_c_gate.get('concept_heat_cliff')):
        conditions.append('生态门:概念热度断崖')
    if bool(stage_c_gate.get('joint_retreat')):
        conditions.append('生态门:多概念共弱')
    elif bool(stage_c_gate.get('weak_no_secondary_support')):
        conditions.append('生态门:次概念未共振')
    elif int(stage_c_gate.get('secondary_support_count', 0) or 0) > 0:
        conditions.append(f"生态门:次概念共振({int(stage_c_gate.get('secondary_support_count', 0) or 0)})")

    if _P1_ENABLED_V2:
        confirm_items = []
        if _P1_RESONANCE_ENABLED and not resonance_60m:
            if reclaim_ok:
                return _pool1_reject(result, 'resonance_missing')
        if bid_ask_ratio is not None and bid_ask_ratio >= _P1_BID_ASK_TH:
            confirm_items.append(f'承接{bid_ask_ratio:.2f}')
        if _P1_RESONANCE_ENABLED and resonance_60m:
            confirm_items.append('60m共振')
        if reclaim_ok and not confirm_items:
            return _pool1_reject(result, 'confirm_missing')
        if confirm_items:
            conditions.append(('确认:' if reclaim_ok else '附加确认:') + '+'.join(confirm_items))

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
    reclaim_bonus = int(reclaim_info.get('score_bonus', 0) or 0)
    concept_adj = int(concept_gate.get('score_adj', 0) or 0)
    left_streak_adj = int(left_streak_guard.get('score_adj', 0) or 0)

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
    strength = max(0, min(100, strength + pace_adj + reclaim_bonus + concept_adj + left_streak_adj))
    observe_only_gate = reclaim_pending or bool(stage_c_gate.get('observe_only'))
    observe_reason = (
        (reclaim_reason if reclaim_pending else '')
        or str(stage_c_gate.get('observe_reason') or '')
    )
    left_state_machine = {
        'name': 'left_side_core_state_machine',
        'stage_a': dict(reclaim_info.get('stage_a') or {}),
        'stage_b': dict(reclaim_info.get('stage_b') or {}),
        'stage_c': dict(stage_c_gate or {}),
        'reclaim_as_execution_gate': True,
        'candidate_ready': reclaim_candidate,
        'reclaim_ready': bool(reclaim_ok),
        'executable_ready': bool(reclaim_ok) and not observe_only_gate,
        'observe_only': bool(observe_only_gate),
        'observe_reason': observe_reason if observe_only_gate else None,
    }
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
            'vwap': round(float(vwap), 4) if vwap is not None else None,
            'bias_vwap': round(float(bias_vwap), 4) if bias_vwap is not None else reclaim_info.get('bias_vwap'),
            'short_zscore': round(float(short_zscore), 4) if short_zscore is not None else reclaim_info.get('short_zscore'),
            'resonance_60m': bool(resonance_60m),
            'volume_pace_ratio': round(pace_ratio_val, 4) if pace_ratio_val is not None else None,
            'volume_pace_state': pace_state,
            'volume_pace_adj': int(pace_adj),
            'left_reclaim': reclaim_info,
            'left_state_machine': left_state_machine,
            'left_reclaim_bonus': int(reclaim_bonus),
            'concept_board': str(core_concept_board or ''),
            'concept_boards': list(concept_boards or []),
            'concept_codes': list(concept_codes or []),
            'core_concept_code': str(core_concept_code or ''),
            'industry': str(industry or ''),
            'industry_ecology': dict(industry_ecology or {}),
            'concept_ecology': concept_gate,
            'concept_ecology_multi': list(concept_ecology_multi or []),
            'concept_ecology_adj': int(concept_adj),
            'left_streak_guard': left_streak_guard,
            'left_stage_c_gate': stage_c_gate,
            'left_streak_adj': int(left_streak_adj),
            'prev_price': round(float(prev_price), 3) if prev_price is not None else None,
            'ask_wall_absorb_ratio': round(float(ask_wall_absorb_ratio), 3) if ask_wall_absorb_ratio is not None else None,
            'super_net_flow_bps': round(float(super_net_flow_bps), 2) if super_net_flow_bps is not None else None,
            'observe_only': observe_only_gate,
            'observe_reason': observe_reason if observe_only_gate else None,
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
    vwap: Optional[float] = None,
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
    minute_bars_1m: Optional[list[dict]] = None,
    industry: Optional[str] = None,
    industry_code: Optional[str] = None,
    industry_ecology: Optional[dict] = None,
    core_concept_board: Optional[str] = None,
    concept_boards: Optional[list[str]] = None,
    concept_codes: Optional[list[str]] = None,
    core_concept_code: Optional[str] = None,
    concept_ecology: Optional[dict] = None,
    concept_ecology_multi: Optional[list[dict]] = None,
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
        reason = _threshold_block_reason(th_meta)
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

    concept_gate = _compute_pool1_concept_ecology_gate(
        core_concept_board=core_concept_board,
        concept_boards=concept_boards,
        concept_ecology=concept_ecology,
        concept_ecology_multi=concept_ecology_multi,
        industry=industry,
        industry_ecology=industry_ecology,
    )
    if concept_gate.get('blocked'):
        return _pool1_reject(result, str(concept_gate.get('blocked_reason') or 'concept_ecology_blocked'), {
            'concept_ecology': concept_gate,
            'threshold': th_meta,
        })
    concept_summary = ''
    if concept_gate.get('enabled') and concept_gate.get('core_concept_board'):
        concept_summary = (
            f"{concept_gate.get('core_concept_board')}:{concept_gate.get('gate_level') or concept_gate.get('state')}"
            f"({float(concept_gate.get('hard_score', concept_gate.get('score', 0.0)) or 0.0):+.0f})"
        )
        conditions.append('概念生态:' + concept_summary)

    avwap_stack = _compute_pool1_entry_anchor(
        price=price,
        minute_bars_1m=minute_bars_1m,
        session_avwap=vwap,
    )
    avwap_stack_ready = bool(avwap_stack.get('avwap_stack_ready'))
    avwap_control_count = int(avwap_stack.get('avwap_control_count', 0) or 0)
    avwap_control_labels = list(avwap_stack.get('avwap_control_labels') or [])
    if avwap_control_labels:
        conditions.append('AVWAP维持:' + '+'.join(avwap_control_labels))
    elif avwap_stack_ready:
        return _pool1_reject(
            result,
            'avwap_stack_not_in_control',
            {
                'avwap_stack': avwap_stack,
                'threshold': th_meta,
            },
        )
    elif not _P1_BREAKOUT_ANCHOR_ALLOW_MISSING_STACK and _P1_BREAKOUT_ANCHOR_REQUIRE_AVWAP_CONTROL:
        return _pool1_reject(
            result,
            'avwap_stack_missing',
            {
                'avwap_stack': avwap_stack,
                'threshold': th_meta,
            },
        )
    elif not avwap_stack_ready:
        conditions.append('AVWAP栈待就绪')
    if (
        _P1_BREAKOUT_ANCHOR_REQUIRE_AVWAP_CONTROL
        and avwap_stack_ready
        and avwap_control_count < max(1, _P1_BREAKOUT_ANCHOR_MIN_CONTROL_COUNT)
    ):
        return _pool1_reject(
            result,
            'avwap_control_not_enough',
            {
                'avwap_stack': avwap_stack,
                'threshold': th_meta,
            },
        )

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
    concept_adj = int(concept_gate.get('score_adj', 0) or 0)
    avwap_adj = min(12, max(0, avwap_control_count) * 4) if avwap_stack_ready else 0
    observe_only_gate = bool(concept_gate.get('observe_only'))
    observe_reason = str(concept_gate.get('observe_reason') or '') if observe_only_gate else ''

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
    strength = max(0, min(100, strength + pace_adj + concept_adj + avwap_adj))
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
            'vwap': round(float(vwap), 4) if vwap is not None else None,
            'avwap_stack': avwap_stack,
            'avwap_control_count': int(avwap_control_count),
            'avwap_control_labels': avwap_control_labels,
            'avwap_adj': int(avwap_adj),
            'concept_board': str(core_concept_board or ''),
            'concept_boards': list(concept_boards or []),
            'concept_codes': list(concept_codes or []),
            'core_concept_code': str(core_concept_code or ''),
            'industry': str(industry or ''),
            'industry_code': str(industry_code or ''),
            'industry_ecology': dict(industry_ecology or {}),
            'concept_ecology': concept_gate,
            'concept_ecology_multi': list(concept_ecology_multi or []),
            'concept_ecology_adj': int(concept_adj),
            'observe_only': observe_only_gate,
            'observe_reason': observe_reason if observe_only_gate else None,
            'threshold': th_meta,
        },
    }


# ============================================================
# 1.1 择时监控池：清仓信号（持仓态）
# ============================================================
def detect_timing_clear(
    price: float,
    boll_mid: float,
    boll_upper: Optional[float] = None,
    ma5: Optional[float] = None,
    ma10: Optional[float] = None,
    ma20: Optional[float] = None,
    atr_14: Optional[float] = None,
    rsi6: Optional[float] = None,
    pct_chg: Optional[float] = None,
    volume_ratio: Optional[float] = None,
    bid_ask_ratio: Optional[float] = None,
    resonance_60m: Optional[bool] = None,
    vwap: Optional[float] = None,
    big_order_bias: Optional[float] = None,
    super_order_bias: Optional[float] = None,
    big_net_flow_bps: Optional[float] = None,
    super_net_flow_bps: Optional[float] = None,
    volume_pace_ratio: Optional[float] = None,
    volume_pace_state: Optional[str] = None,
    last_buy_price: Optional[float] = None,
    last_buy_at: Optional[int] = None,
    minute_bars_1m: Optional[list[dict]] = None,
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
    - 按 full / partial / observe 输出更细粒度的退出语义
    - 引入 ATR 波动率分桶，对低波/高波持仓使用不同退出模板
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
        return _pool1_reject(result, _threshold_block_reason(th_meta), {'threshold': th_meta})

    try:
        hold_days = max(0.0, float(holding_days or 0.0))
    except Exception:
        hold_days = 0.0
    entry_anchor = _compute_pool1_entry_anchor(
        price=price,
        last_buy_price=last_buy_price,
        last_buy_at=last_buy_at,
        minute_bars_1m=minute_bars_1m,
        session_avwap=vwap,
    )
    try:
        atr_val = float(atr_14) if atr_14 is not None else None
    except Exception:
        atr_val = None
    atr_pct = (atr_val / price) if (atr_val is not None and atr_val > 0 and price > 0) else None
    if atr_pct is None:
        atr_bucket = 'unknown'
    elif atr_pct <= _P1_EXIT_ATR_LOW_PCT:
        atr_bucket = 'low'
    elif atr_pct >= _P1_EXIT_ATR_HIGH_PCT:
        atr_bucket = 'high'
    else:
        atr_bucket = 'mid'
    entry_cost_lost = bool(entry_anchor.get('entry_cost_lost', False))
    entry_avwap_lost = bool(entry_anchor.get('entry_avwap_lost', False))
    breakout_anchor_lost = bool(entry_anchor.get('breakout_anchor_lost', False))
    event_anchor_lost = bool(entry_anchor.get('event_anchor_lost', False))
    lost_anchor_count = int(entry_anchor.get('lost_anchor_count', 0) or 0)
    support_anchor_lost = bool(entry_anchor.get('support_anchor_lost', False))
    entry_premium_pct = entry_anchor.get('entry_premium_pct')
    try:
        entry_premium_val = float(entry_premium_pct) if entry_premium_pct is not None else None
    except Exception:
        entry_premium_val = None
    breakout_anchor_premium_pct = entry_anchor.get('breakout_anchor_premium_pct')
    try:
        breakout_anchor_premium_val = (
            float(breakout_anchor_premium_pct) if breakout_anchor_premium_pct is not None else None
        )
    except Exception:
        breakout_anchor_premium_val = None
    event_anchor_premium_pct = entry_anchor.get('event_anchor_premium_pct')
    try:
        event_anchor_premium_val = (
            float(event_anchor_premium_pct) if event_anchor_premium_pct is not None else None
        )
    except Exception:
        event_anchor_premium_val = None
    anchor_take_profit_ready = bool(
        entry_premium_val is not None and entry_premium_val >= _P1_EXIT_TAKE_PROFIT_COST_PREMIUM_PCT
    )
    anchor_beta_reduce_ready = bool(
        entry_premium_val is not None and entry_premium_val >= _P1_EXIT_BETA_REDUCE_COST_PREMIUM_PCT
    )
    breakout_beta_reduce_ready = bool(
        breakout_anchor_premium_val is not None
        and breakout_anchor_premium_val >= _P1_BREAKOUT_ANCHOR_BETA_REDUCE_PREMIUM_PCT
    )
    event_beta_reduce_ready = bool(
        event_anchor_premium_val is not None
        and event_anchor_premium_val >= _P1_EVENT_ANCHOR_BETA_REDUCE_PREMIUM_PCT
    )
    avwap_stack_ready = bool(entry_anchor.get('avwap_stack_ready'))
    avwap_control_count = int(entry_anchor.get('avwap_control_count', 0) or 0)
    avwap_control_labels = list(entry_anchor.get('avwap_control_labels') or [])
    avwap_loss_labels = list(entry_anchor.get('avwap_loss_labels') or [])
    holding_anchor_lost_labels: list[str] = []
    if entry_avwap_lost:
        holding_anchor_lost_labels.append('entry_day_avwap')
    if breakout_anchor_lost:
        holding_anchor_lost_labels.append('breakout_day_avwap')
    if event_anchor_lost:
        holding_anchor_lost_labels.append('event_day_avwap')
    holding_anchor_loss_count = len(holding_anchor_lost_labels)
    current_above_session_avwap = bool(entry_anchor.get('current_above_session_avwap', False))
    has_session_avwap = bool(entry_anchor.get('has_session_avwap', False))
    session_avwap_lost = bool(has_session_avwap and (not current_above_session_avwap))

    break_mid = bool(boll_mid and boll_mid > 0 and price < boll_mid * 0.998)
    break_ma10 = bool(ma10 and ma10 > 0 and price < ma10 * 0.998)
    break_ma20 = bool(ma20 and ma20 > 0 and price < ma20 * 0.995)
    trend_reversal = bool(
        ma5 and ma10 and ma5 > 0 and ma10 > 0 and (ma5 < ma10 * 0.998)
    )
    near_upper = bool(
        boll_upper and boll_upper > 0 and price >= boll_upper * (1 - _P1_EXIT_TAKE_PROFIT_NEAR_UPPER_PCT / 100.0)
    )
    rsi_hot = bool(rsi6 is not None and rsi6 >= _P1_EXIT_RSI_HOT_TH)
    gain_hot = bool(pct_chg is not None and pct_chg >= _P1_EXIT_TAKE_PROFIT_DAY_GAIN_PCT)
    pace_hot = str(volume_pace_state or '').strip().lower() in ('expand', 'surge')
    hard_drop = bool(pct_chg is not None and pct_chg <= _P1_EXIT_HARD_DROP_PCT)
    vol_down = bool(
        volume_ratio is not None and pct_chg is not None and volume_ratio >= 1.4 and pct_chg < 0
    )
    mild_bid_ask_weak = bool(bid_ask_ratio is not None and bid_ask_ratio <= _P1_EXIT_BIDASK_WEAK_RATIO)
    severe_bid_ask_weak = bool(bid_ask_ratio is not None and bid_ask_ratio <= _P1_EXIT_BIDASK_WEAK_RATIO * 0.92)
    mild_big_bias_sell = bool(big_order_bias is not None and big_order_bias <= (_P1_EXIT_BIG_BIAS_SELL_TH * 0.60))
    severe_big_bias_sell = bool(big_order_bias is not None and big_order_bias <= (_P1_EXIT_BIG_BIAS_SELL_TH * 1.25))
    mild_super_bias_sell = bool(super_order_bias is not None and super_order_bias <= (_P1_EXIT_SUPER_BIAS_SELL_TH * 0.60))
    severe_super_bias_sell = bool(super_order_bias is not None and super_order_bias <= (_P1_EXIT_SUPER_BIAS_SELL_TH * 1.25))
    mild_big_out = bool(big_net_flow_bps is not None and big_net_flow_bps <= (_P1_EXIT_BIG_NET_OUT_BPS_TH * 0.60))
    severe_big_out = bool(big_net_flow_bps is not None and big_net_flow_bps <= (_P1_EXIT_BIG_NET_OUT_BPS_TH * 1.25))
    mild_super_out = bool(super_net_flow_bps is not None and super_net_flow_bps <= (_P1_EXIT_SUPER_NET_OUT_BPS_TH * 0.60))
    severe_super_out = bool(super_net_flow_bps is not None and super_net_flow_bps <= (_P1_EXIT_SUPER_NET_OUT_BPS_TH * 1.25))
    severe_flow = bool(
        severe_bid_ask_weak or severe_big_out or severe_super_out or severe_big_bias_sell or severe_super_bias_sell
    )
    mild_flow = bool(
        mild_bid_ask_weak or mild_big_out or mild_super_out or mild_big_bias_sell or mild_super_bias_sell
    )
    avwap_flow_shift = bool(
        (
            entry_avwap_lost
            or breakout_anchor_lost
            or event_anchor_lost
            or (avwap_stack_ready and avwap_control_count <= 0)
        )
        and (
            (not _P1_EXIT_TIMING_CLEAR_AVWAP_FLOW_REQUIRED)
            or mild_flow
            or severe_flow
            or resonance_60m is False
            or session_avwap_lost
        )
    )
    avwap_stack_soft_weak = bool(
        avwap_stack_ready
        and avwap_control_count <= 0
        and holding_anchor_loss_count <= 0
        and not session_avwap_lost
    )
    early_risk_exit = bool(
        hard_drop
        or (break_ma20 and vol_down)
        or (atr_bucket == 'high' and break_ma10 and vol_down and mild_flow)
    )
    weak_tape = bool(
        mild_flow
        or severe_flow
        or resonance_60m is False
        or break_mid
        or break_ma10
    )
    strong_tape_break = bool(
        hard_drop
        or break_ma20
        or trend_reversal
        or severe_flow
        or support_anchor_lost
    )
    avwap_exit_tier = ''
    avwap_exit_reason = ''
    if (
        (entry_avwap_lost and (strong_tape_break or breakout_anchor_lost or event_anchor_lost or session_avwap_lost or avwap_flow_shift))
        or (holding_anchor_loss_count >= 2 and (weak_tape or session_avwap_lost or lost_anchor_count >= 2))
    ):
        avwap_exit_tier = 'full'
        avwap_exit_reason = 'entry_or_multi_anchor_lost'
    elif entry_avwap_lost or breakout_anchor_lost or event_anchor_lost or avwap_flow_shift:
        avwap_exit_tier = 'partial'
        avwap_exit_reason = 'secondary_or_flow_shift'
    elif entry_cost_lost or session_avwap_lost or avwap_stack_soft_weak:
        avwap_exit_tier = 'observe'
        avwap_exit_reason = 'soft_avwap_warning'
    reduce_ratio_components: list[dict] = []
    if entry_avwap_lost:
        reduce_ratio_components.append(
            {'source': 'entry_day_avwap', 'ratio': round(float(_P1_EXIT_ENTRY_AVWAP_REDUCE_RATIO), 4)}
        )
    if breakout_anchor_lost:
        reduce_ratio_components.append(
            {'source': 'breakout_day_avwap', 'ratio': round(float(_P1_EXIT_BREAKOUT_AVWAP_REDUCE_RATIO), 4)}
        )
    if event_anchor_lost:
        reduce_ratio_components.append(
            {'source': 'event_day_avwap', 'ratio': round(float(_P1_EXIT_EVENT_AVWAP_REDUCE_RATIO), 4)}
        )
    if session_avwap_lost:
        reduce_ratio_components.append(
            {'source': 'session_avwap', 'ratio': round(float(_P1_EXIT_SESSION_AVWAP_REDUCE_RATIO), 4)}
        )
    if entry_cost_lost:
        reduce_ratio_components.append(
            {'source': 'entry_cost_anchor', 'ratio': round(float(_P1_EXIT_ENTRY_COST_REDUCE_RATIO), 4)}
        )
    if avwap_stack_soft_weak:
        reduce_ratio_components.append(
            {'source': 'avwap_soft_weak', 'ratio': round(float(_P1_EXIT_AVWAP_SOFT_WEAK_REDUCE_RATIO), 4)}
        )
    anchor_ratio_sources = {
        'entry_day_avwap',
        'breakout_day_avwap',
        'event_day_avwap',
        'session_avwap',
    }
    if avwap_flow_shift and not any(str(x.get('source') or '') in anchor_ratio_sources for x in reduce_ratio_components):
        reduce_ratio_components.append(
            {'source': 'avwap_flow_shift', 'ratio': round(float(_P1_EXIT_PARTIAL_REDUCE_RATIO), 4)}
        )
    reduce_ratio_component_sum = float(
        sum(float(x.get('ratio', 0.0) or 0.0) for x in reduce_ratio_components)
    )
    reduce_ratio_source = '+'.join(str(x.get('source') or '') for x in reduce_ratio_components if x.get('source')) or ''

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
    if near_upper:
        trigger_items.append('贴近上轨')
    if rsi_hot:
        trigger_items.append(f'RSI过热({rsi6:.1f})')
    if gain_hot:
        trigger_items.append(f'涨幅过热({pct_chg:.2f}%)')
    if pct_chg is not None and pct_chg <= -2.0:
        trigger_items.append(f'日内走弱({pct_chg:.2f}%)')
    if vol_down:
        trigger_items.append('放量下跌')
    if pace_hot and pct_chg is not None and pct_chg > 0:
        trigger_items.append(f'加速放量({str(volume_pace_state or "").strip().lower()})')
    if entry_cost_lost:
        trigger_items.append('失守建仓成本')
    if entry_avwap_lost:
        trigger_items.append('失守建仓AVWAP')
    if breakout_anchor_lost:
        trigger_items.append('失守突破AVWAP')
    if event_anchor_lost:
        trigger_items.append('失守事件AVWAP')
    if session_avwap_lost:
        trigger_items.append('失守日内AVWAP')
    if avwap_stack_ready and avwap_control_count > 0:
        trigger_items.append('AVWAP维持:' + '+'.join(avwap_control_labels[:3]))
    if avwap_stack_soft_weak:
        trigger_items.append('AVWAP控盘转弱')
    if avwap_flow_shift:
        trigger_items.append('AVWAP主导权转移')
    if lost_anchor_count >= 2:
        trigger_items.append(f'多锚失守({lost_anchor_count})')
    if anchor_take_profit_ready:
        trigger_items.append(f'脱离建仓成本({entry_premium_val:.2f}%)')
    if breakout_beta_reduce_ready:
        trigger_items.append(f'脱离突破锚({breakout_anchor_premium_val:.2f}%)')
    if event_beta_reduce_ready:
        trigger_items.append(f'脱离事件锚({event_anchor_premium_val:.2f}%)')

    major_break = bool(break_ma20 or (break_ma10 and break_mid) or trend_reversal)
    defense_candidate = bool(
        early_risk_exit
        or avwap_flow_shift
        or entry_cost_lost
        or entry_avwap_lost
        or session_avwap_lost
        or avwap_stack_soft_weak
        or (breakout_anchor_lost and (break_mid or mild_flow or resonance_60m is False))
        or (event_anchor_lost and (break_mid or mild_flow or resonance_60m is False))
        or (lost_anchor_count >= 2 and (break_mid or mild_flow or support_anchor_lost))
        or break_ma20
        or (trend_reversal and (break_mid or severe_flow))
        or (break_ma10 and break_mid and severe_flow)
    )
    take_profit_candidate = bool(
        hold_days >= _P1_EXIT_MIN_HOLD_DAYS
        and not hard_drop
        and not break_ma20
        and (
            (near_upper and (rsi_hot or gain_hot or pace_hot))
            or (rsi_hot and gain_hot)
            or (anchor_take_profit_ready and (rsi_hot or gain_hot or mild_flow or resonance_60m is False))
        )
    )
    beta_reduce_candidate = bool(
        hold_days >= _P1_EXIT_MIN_HOLD_DAYS
        and not defense_candidate
        and not hard_drop
        and not break_ma20
        and (
            resonance_60m is False
            or mild_flow
            or (break_ma10 and mild_bid_ask_weak)
            or (anchor_beta_reduce_ready and (mild_flow or break_mid or resonance_60m is False))
            or (breakout_beta_reduce_ready and (mild_flow or break_mid or resonance_60m is False))
            or (event_beta_reduce_ready and (mild_flow or break_mid or resonance_60m is False))
        )
    )
    if not defense_candidate and not take_profit_candidate and not beta_reduce_candidate and not major_break and not early_risk_exit:
        return _pool1_reject(result, 'trigger_missing', {'threshold': th_meta})

    confirm_items: list[str] = []
    if mild_bid_ask_weak:
        confirm_items.append(f'承接转弱({bid_ask_ratio:.2f})')
    if _P1_RESONANCE_ENABLED and resonance_60m is False:
        confirm_items.append('60m失共振')
    if mild_big_bias_sell:
        confirm_items.append(f'大单偏空({big_order_bias:.2f})')
    if mild_super_bias_sell:
        confirm_items.append(f'超大单偏空({super_order_bias:.2f})')
    if mild_big_out:
        confirm_items.append(f'大单净流出({big_net_flow_bps:.0f}bps)')
    if mild_super_out:
        confirm_items.append(f'超大单净流出({super_net_flow_bps:.0f}bps)')
    if entry_cost_lost:
        confirm_items.append('成本锚失守')
    if entry_avwap_lost:
        confirm_items.append('AVWAP失守')
    if breakout_anchor_lost:
        confirm_items.append('突破锚失守')
    if event_anchor_lost:
        confirm_items.append('事件锚失守')
    if session_avwap_lost:
        confirm_items.append('日内AVWAP失守')
    if avwap_stack_soft_weak:
        confirm_items.append('AVWAP控盘转弱')
    if avwap_flow_shift:
        confirm_items.append('AVWAP+资金转弱')
    elif avwap_stack_ready and avwap_control_count <= 0:
        confirm_items.append('AVWAP控制丢失')
    if lost_anchor_count >= 2:
        confirm_items.append(f'多锚失守({lost_anchor_count})')
    if near_upper:
        confirm_items.append('临近压力位')
    if rsi_hot:
        confirm_items.append(f'热度过高({rsi6:.1f})')
    if gain_hot:
        confirm_items.append(f'涨幅已大({pct_chg:.2f}%)')
    if pace_hot and pct_chg is not None and pct_chg > 0:
        confirm_items.append(f'加速放量({str(volume_pace_state or "").strip().lower()})')

    long_hold = hold_days >= max(_P1_EXIT_MIN_HOLD_DAYS, _P1_EXIT_LONG_HOLD_DAYS)
    clear_family = 'defense'
    clear_level_hint = 'full'
    suggest_reduce_ratio = 1.0
    if defense_candidate:
        clear_family = 'defense'
        if avwap_exit_tier == 'observe':
            clear_level_hint = 'observe'
        elif avwap_exit_tier == 'partial':
            clear_level_hint = 'partial'
        else:
            clear_level_hint = 'full'
        if clear_level_hint == 'full':
            suggest_reduce_ratio = 1.0
        elif clear_level_hint == 'partial':
            suggest_reduce_ratio = reduce_ratio_component_sum if reduce_ratio_component_sum > 0 else _P1_EXIT_PARTIAL_REDUCE_RATIO
            suggest_reduce_ratio = min(0.95, max(0.10, float(suggest_reduce_ratio)))
        else:
            suggest_reduce_ratio = reduce_ratio_component_sum if reduce_ratio_component_sum > 0 else _P1_EXIT_OBSERVE_REDUCE_RATIO
            suggest_reduce_ratio = min(float(_P1_EXIT_PARTIAL_REDUCE_RATIO), max(0.05, float(suggest_reduce_ratio)))
    elif take_profit_candidate:
        clear_family = 'take_profit'
        clear_level_hint = 'partial'
        suggest_reduce_ratio = _P1_EXIT_PARTIAL_REDUCE_RATIO
    elif beta_reduce_candidate:
        clear_family = 'beta_reduce'
        clear_level_hint = 'partial'
        suggest_reduce_ratio = _P1_EXIT_BETA_REDUCE_RATIO

    if clear_family == 'defense':
        if clear_level_hint == 'observe':
            min_confirm = 1
        elif clear_level_hint == 'partial':
            min_confirm = 1 if (early_risk_exit or avwap_flow_shift or entry_avwap_lost) else 2
        else:
            min_confirm = 1 if (early_risk_exit or avwap_flow_shift) else 2
        if long_hold and not early_risk_exit:
            min_confirm = max(2, _P1_EXIT_LONG_HOLD_MIN_CONFIRM)
            if not (hard_drop or break_ma20 or trend_reversal or bool(avwap_exit_tier)):
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
        if atr_bucket == 'low' and not early_risk_exit:
            min_confirm += _P1_EXIT_ATR_LOW_EXTRA_CONFIRM
        elif atr_bucket == 'high':
            min_confirm = max(1, min_confirm - _P1_EXIT_ATR_HIGH_RELAX_CONFIRM)
    elif clear_family == 'take_profit':
        min_confirm = 2 if atr_bucket == 'low' else 1
    else:
        min_confirm = 2 if atr_bucket == 'low' else 1

    observe_only_hint = False
    if len(confirm_items) < min_confirm:
        if clear_family == 'defense' and clear_level_hint in ('observe', 'partial') and len(confirm_items) >= 1:
            clear_level_hint = 'observe'
            observe_only_hint = True
            if reduce_ratio_component_sum > 0:
                suggest_reduce_ratio = min(float(_P1_EXIT_PARTIAL_REDUCE_RATIO), max(0.05, float(reduce_ratio_component_sum)))
            else:
                suggest_reduce_ratio = _P1_EXIT_OBSERVE_REDUCE_RATIO
        elif clear_family in ('take_profit', 'beta_reduce') and len(confirm_items) >= 1:
            clear_level_hint = 'observe'
            observe_only_hint = True
            suggest_reduce_ratio = _P1_EXIT_OBSERVE_REDUCE_RATIO
        else:
            return _pool1_reject(
                result,
                'confirm_missing',
                {
                    'holding_days': round(hold_days, 2),
                    'min_confirm': int(min_confirm),
                    'long_hold': bool(long_hold),
                    'clear_family': clear_family,
                    'atr_bucket': atr_bucket,
                    'threshold': th_meta,
                },
            )

    strength = 0
    if clear_family == 'defense':
        if break_ma10:
            strength += 20
        if break_mid:
            strength += 20
        if break_ma20:
            strength += 30
        if entry_cost_lost:
            strength += 12
        if entry_avwap_lost:
            strength += 14
        if breakout_anchor_lost:
            strength += 12
        if event_anchor_lost:
            strength += 10
        if avwap_flow_shift:
            strength += 12
        elif avwap_stack_ready and avwap_control_count <= 0:
            strength += 8
        if clear_level_hint == 'partial':
            strength -= 8
        elif clear_level_hint == 'observe':
            strength -= 14
        if lost_anchor_count >= 2:
            strength += 10 + min(6, (lost_anchor_count - 2) * 3)
        if trend_reversal:
            strength += 15
        if vol_down:
            strength += 10
        if hard_drop:
            strength += 15
        if severe_flow:
            strength += 10
        if atr_bucket == 'high':
            strength += 6
        elif atr_bucket == 'low' and not early_risk_exit:
            strength -= 4
        strength += min(20, len(confirm_items) * 6)
        if hold_days >= 10:
            strength += 5
    elif clear_family == 'take_profit':
        if near_upper:
            strength += 24
        if anchor_take_profit_ready:
            strength += 10
        if rsi_hot:
            strength += 20
        if gain_hot:
            strength += 10
        if pace_hot:
            strength += 10 if str(volume_pace_state or '').strip().lower() == 'surge' else 6
        if resonance_60m is False:
            strength += 8
        if mild_bid_ask_weak:
            strength += 8
        if mild_big_out:
            strength += 8
        if mild_super_out:
            strength += 10
        strength += min(18, len(confirm_items) * 5)
        if hold_days >= _P1_EXIT_MIN_HOLD_DAYS:
            strength += 5
        if atr_bucket == 'low':
            strength -= 4
    else:
        if anchor_beta_reduce_ready:
            strength += 8
        if breakout_beta_reduce_ready:
            strength += 8
        if event_beta_reduce_ready:
            strength += 8
        if resonance_60m is False:
            strength += 18
        if mild_bid_ask_weak:
            strength += 12
        if mild_big_out:
            strength += 10
        if mild_super_out:
            strength += 12
        if break_mid:
            strength += 8
        if break_ma10:
            strength += 10
        if pace_hot and pct_chg is not None and pct_chg > 0:
            strength += 6
        strength += min(18, len(confirm_items) * 5)
        if hold_days >= _P1_EXIT_MIN_HOLD_DAYS:
            strength += 4
        if atr_bucket == 'high':
            strength += 5
        elif atr_bucket == 'low':
            strength -= 3

    if clear_level_hint == 'observe':
        strength = min(max(int(round(strength)), int(_P1_SCORE_OBSERVE + 2)), int(_P1_SCORE_EXECUTABLE - 2))
    elif clear_level_hint == 'partial':
        strength = max(int(round(strength)), int(_P1_SCORE_OBSERVE + 3))
    else:
        strength = max(int(round(strength)), int(_P1_SCORE_EXECUTABLE))
    strength = min(100, max(0, int(round(strength))))

    if clear_family == 'defense':
        if clear_level_hint == 'observe':
            msg_head = '防守观察信号'
        elif clear_level_hint == 'partial':
            msg_head = '防守减仓信号'
        else:
            msg_head = '防守清仓信号'
    elif clear_family == 'take_profit':
        msg_head = '分批止盈信号' if clear_level_hint != 'observe' else '止盈观察信号'
    else:
        msg_head = '被动减仓信号' if clear_level_hint != 'observe' else '减仓观察信号'

    msg = (
        f"{msg_head}@{price:.2f} | "
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
            'near_upper': near_upper,
            'rsi_hot': rsi_hot,
            'gain_hot': gain_hot,
            'clear_family': clear_family,
            'clear_level_hint': clear_level_hint,
            'suggest_reduce_ratio': round(float(suggest_reduce_ratio), 2),
            'reduce_ratio_source': reduce_ratio_source or None,
            'reduce_ratio_components': reduce_ratio_components,
            'vol_down': vol_down,
            'rsi6': rsi6,
            'pct_chg': pct_chg,
            'atr_14': atr_val,
            'atr_pct': round(float(atr_pct), 4) if atr_pct is not None else None,
            'atr_bucket': atr_bucket,
            'vwap': round(float(vwap), 4) if vwap is not None else None,
            'entry_anchor': entry_anchor,
            'avwap_stack': entry_anchor,
            'avwap_stack_ready': bool(avwap_stack_ready),
            'avwap_control_count': int(avwap_control_count),
            'avwap_control_labels': avwap_control_labels,
            'avwap_loss_labels': avwap_loss_labels,
            'avwap_flow_shift': bool(avwap_flow_shift),
            'avwap_exit_tier': avwap_exit_tier or None,
            'avwap_exit_reason': avwap_exit_reason or None,
            'holding_anchor_loss_count': int(holding_anchor_loss_count),
            'holding_anchor_lost_labels': holding_anchor_lost_labels,
            'session_avwap_lost': bool(session_avwap_lost),
            'volume_ratio': volume_ratio,
            'bid_ask_ratio': bid_ask_ratio,
            'resonance_60m': resonance_60m,
            'big_order_bias': big_order_bias,
            'super_order_bias': super_order_bias,
            'big_net_flow_bps': big_net_flow_bps,
            'super_net_flow_bps': super_net_flow_bps,
            'volume_pace_ratio': volume_pace_ratio,
            'volume_pace_state': volume_pace_state,
            'trigger_items': trigger_items,
            'confirm_items': confirm_items,
            'min_confirm': int(min_confirm),
            'observe_only': bool(observe_only_hint),
            'observe_reason': 'timing_clear_observe' if observe_only_hint else None,
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
        result['details'] = {'threshold': th_meta, 'veto': _threshold_block_reason(th_meta)}
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
        result['details'] = {'threshold': th_meta, 'veto': _threshold_block_reason(th_meta)}
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
                    'industry_ecology': m.get('industry_ecology'),
                    'regime_hint': m.get('regime_hint'),
                    'name': m.get('name'),
                    'market_name': m.get('market_name'),
                    'list_date': m.get('list_date'),
                    'concept_boards': m.get('concept_boards'),
                    'concept_codes': m.get('concept_codes'),
                    'core_concept_board': m.get('core_concept_board'),
                    'core_concept_code': m.get('core_concept_code'),
                    'concept_ecology': m.get('concept_ecology'),
                    'concept_ecology_multi': m.get('concept_ecology_multi'),
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
                    price=m.get('price', 0),
                    boll_upper=m.get('boll_upper', 0),
                    boll_mid=m.get('boll_mid', 0),
                    boll_lower=m.get('boll_lower', 0),
                    rsi6=m.get('rsi6'),
                    pct_chg=m.get('pct_chg'),
                    vwap=m.get('vwap'),
                    intraday_prices=m.get('intraday_prices'),
                    bid_ask_ratio=m.get('bid_ask_ratio'),
                    lure_long=m.get('lure_long', False),
                    wash_trade=m.get('wash_trade', False),
                    up_limit=m.get('up_limit'),
                    down_limit=m.get('down_limit'),
                    resonance_60m=bool(resonance_60m_val),
                    volume_pace_ratio=m.get('volume_pace_ratio'),
                    volume_pace_state=m.get('volume_pace_state'),
                    thresholds=p1_left_th,
                    prev_price=m.get('prev_price'),
                    ask_wall_absorb_ratio=m.get('ask_wall_absorb_ratio'),
                    super_net_flow_bps=m.get('super_net_flow_bps'),
                    minute_bars_1m=m.get('minute_bars_1m'),
                    industry=m.get('industry'),
                    industry_ecology=m.get('industry_ecology'),
                    core_concept_board=m.get('core_concept_board'),
                    concept_boards=m.get('concept_boards'),
                    concept_codes=m.get('concept_codes'),
                    core_concept_code=m.get('core_concept_code'),
                    concept_ecology=m.get('concept_ecology'),
                    concept_ecology_multi=m.get('concept_ecology_multi'),
                    position_state={
                        'status': m.get('pool1_position_status'),
                        'last_buy_at': m.get('pool1_last_buy_at'),
                        'last_buy_type': m.get('pool1_last_buy_type'),
                        'last_sell_at': m.get('pool1_last_sell_at'),
                        'last_sell_type': m.get('pool1_last_sell_type'),
                        'left_quick_clear_streak': m.get('pool1_left_quick_clear_streak'),
                        'last_left_quick_clear_at': m.get('pool1_last_left_quick_clear_at'),
                    },
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
                    m.get('vwap'),
                    m.get('prev_price'), m.get('bid_ask_ratio'), m.get('lure_long', False), m.get('wash_trade', False),
                    m.get('up_limit'), m.get('down_limit'), bool(resonance_60m_val),
                    m.get('volume_pace_ratio'), m.get('volume_pace_state'),
                    thresholds=p1_right_th,
                    minute_bars_1m=m.get('minute_bars_1m'),
                    industry=m.get('industry'),
                    industry_code=m.get('industry_code'),
                    industry_ecology=m.get('industry_ecology'),
                    core_concept_board=m.get('core_concept_board'),
                    concept_boards=m.get('concept_boards'),
                    concept_codes=m.get('concept_codes'),
                    core_concept_code=m.get('core_concept_code'),
                    concept_ecology=m.get('concept_ecology'),
                    concept_ecology_multi=m.get('concept_ecology_multi'),
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
                boll_upper=m.get('boll_upper'),
                ma5=m.get('ma5'),
                ma10=m.get('ma10'),
                ma20=m.get('ma20'),
                atr_14=m.get('atr_14'),
                rsi6=m.get('rsi6'),
                pct_chg=m.get('pct_chg'),
                volume_ratio=m.get('volume_ratio'),
                bid_ask_ratio=m.get('bid_ask_ratio'),
                resonance_60m=bool(resonance_60m_val),
                vwap=m.get('vwap'),
                big_order_bias=m.get('big_order_bias'),
                super_order_bias=m.get('super_order_bias'),
                big_net_flow_bps=m.get('big_net_flow_bps'),
                super_net_flow_bps=m.get('super_net_flow_bps'),
                volume_pace_ratio=m.get('volume_pace_ratio'),
                volume_pace_state=m.get('volume_pace_state'),
                last_buy_price=m.get('pool1_last_buy_price'),
                last_buy_at=m.get('pool1_last_buy_at'),
                minute_bars_1m=m.get('minute_bars_1m'),
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








