"""
盘面信号计算框架

两个监控池：
  1号-择时监控池悜战略建仓）
    - 左侧抄底信号悜逆势埋伏式）：价格接近/跌破布林下轨 + RSI 超卖
    - 右侧突破信号悜顺势突破）：价格有效突破布林中轨 上轨 + 成交量放大
  2号-T+0 监控池悜战术套利）    - 正T信号悜低吸高抛）：tick 穿透布林下轨 且 VWAP偏置 < -1.5%，GUB5 由升转平
    - 反T信号悜高抛低吸）：日内急拉 Z-Score > 2.0 或触及上轨，量价背离

本文仅提供框架函数，实际计算需要整合：
- 日线 boll 预计算值悜从 DuckDB 读取）- 实时 tick悜mootdx）- 分钟线悜mootdx）- 逐笔成交悜mootdx）
每个信号函数返回：
    {
        'has_signal': bool,
        'type': str,         # 信号类型标识
        'strength': float,   # 0-100 强度评分
        'message': str,      # 可描述
        'triggered_at': int, # 触发时间戳        'details': dict,     # 计算依据详细
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
_EPS_OVER         = _TRIGGER_CFG.get('eps_over', 0.002)    # 上轨超越系数

_CONFIRM_CFG      = _T0_CFG.get('confirm', {})
_BID_ASK_TH       = _CONFIRM_CFG.get('bid_ask_ratio', 1.15)
_ASK_ABSORB_TH    = _CONFIRM_CFG.get('ask_wall_absorb_ratio', 0.6)
_GUB5_TRANSITIONS = set(_CONFIRM_CFG.get('gub5_transitions', ('up->flat', 'up->down')))

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

# 婊炲洖鐘舵）侊跌{ts_code: {'positive_t': last_trigger_price, 'reverse_t': last_trigger_price}}
_hysteresis_state: dict[str, dict[str, float]] = {}

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
        'limit_filter': thresholds.get('limit_filter'),
        'observer_only': bool(thresholds.get('observer_only', False)),
        'blocked': bool(thresholds.get('blocked', False)),
    }


def compute_pool1_resonance_60m(minute_bars_1m: Optional[list[dict]], fallback: bool = False) -> tuple[bool, dict]:
    """
    鐢?1 鍒嗛挓绾胯仛日堜负 60 鍒嗛挓撴灉瀯骞跺垽鏂槸日﹀叡侀）?
    鍒ゅ畾瑕冩佺悜堝亸信濆畧悜栵跌
      1) 60m 鏀控面鍦?MA3 樻格笂悜屼笖 MA3 嬭急急浜?MA4悜級秼鍔夸笂琛岋栵
      2) 本）鏂?60m 鏀控面偣樹簬鍓嶄竴归价术鍔ㄩ噺日归笂悜      3) 本）鏂?60m 偣偣佺佺冩冩本）斿归归归癸癸癸价价价?60m 忚兘嬭嬭急急悜忚忚忚?侀噺噺悜    """
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
    Pool1 闃舵2悜轨有侊栵分信垎悜布供鍔級垎悜屼笉鏀鏂彉触﹀傚鏉′欢。    - 板樻强强）嶉椂椂椂杩返回 0
    - 褰撳墠灏忔嶉忚帴鈥滅有噺泦嬭强鈥?+ 鑾峰埄监樹繚吸ら槇反?    """
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

    # band 瓒婂皬瓒婇泦嬭价绾挎）褎槧灏信埌 [0, weight]
    ratio = max(0.0, min(1.0, (_P1_CHIP_BAND_MAX - band_val) / max(_P1_CHIP_BAND_MAX, 1e-6)))
    bonus = int(round(_P1_CHIP_WEIGHT * ratio))
    info['applied'] = bonus > 0
    info['bonus'] = max(0, bonus)
    info['reason'] = 'ok' if bonus > 0 else 'weak_bonus'
    return max(0, bonus), info


# ============================================================
# P0-2 / 5.1  撴熶竴鐗鏂緛偏勯）級櫒
# ============================================================
def build_t0_features(
    tick_price: float,
    boll_lower: float,
    boll_upper: float,
    vwap: Optional[float],
    intraday_prices: Optional[list[float]],
    gub5_trend: Optional[str],
    gub5_transition: Optional[str],
    bid_ask_ratio: Optional[float],
    # 嬭标姏琛屼负
    lure_short: bool = False,
    wash_trade: bool = False,
    spread_abnormal: bool = False,
    v_power_divergence: bool = False,
    ask_wall_building: bool = False,
    real_buy_fading: bool = False,
    wash_trade_severe: bool = False,
    liquidity_drain: bool = False,
    # 高阶特征(可选)
    ask_wall_absorb_ratio: Optional[float] = None,
    spoofing_vol_ratio: Optional[float] = None,
    big_order_bias: Optional[float] = None,
) -> dict:
    """
    撴熶竴鐗鏂緛偏勯）級櫒悜氬揩岀緞鍜垚參岀緞鍧囪皟鐢ㄦ返回算悜屼繚分冩壒寰佸彛寰勪竴鑷淬）?
    杈撳嚭瀛楀吀鍖呭惈鎵）本変笅娓搁棬鎺?信″类返回算鎵）闇）瀛楁悜    据$撴撴灉灉悜坮obust_zscore/bias_vwap夛栵栵樻湪姝低ゅ屾垚垚悜伩嶉嶉噸嶈嶈椼椼椼）?    """
    # 鈹）鈹） 价虹鍋忕忚?鈹）鈹）
    bias_vwap = None
    if vwap and vwap > 0 and tick_price > 0:
        bias_vwap = (tick_price - vwap) / vwap * 100

    # 鈹）鈹） robust Z-score悜圡AD悜鈹鈹鈹）
    robust_z = _robust_zscore(intraday_prices) if intraday_prices else None

    # 鈹）鈹） eps 绌块）忓垽鏂术閰嶇疆椹卞姩悜鈹鈹鈹）
    boll_break = (boll_lower > 0 and tick_price < boll_lower * (1 - _EPS_BREAK))
    boll_over  = (boll_upper > 0 and tick_price > boll_upper * (1 + _EPS_OVER))

    # 鈹）鈹） 骞伩獥日﹀樻悜.3.2悜鈹鈹鈹）
    spoofing_suspected = (
        spoofing_vol_ratio is not None and spoofing_vol_ratio > _SPOOFING_RATIO
    )

    # 鈹）鈹） 級栧琚悆灞悜.2.2悜鈹鈹鈹）
    ask_wall_absorb = (
        ask_wall_absorb_ratio is not None and ask_wall_absorb_ratio > _ASK_ABSORB_TH
    )

    return {
        # 鍘湪杈撳樻下个紶
        'tick_price': tick_price,
        'boll_lower': boll_lower,
        'boll_upper': boll_upper,
        'vwap': vwap,
        'gub5_trend': gub5_trend,
        'gub5_transition': gub5_transition,
        'bid_ask_ratio': bid_ask_ratio,
        # 嬭标姏琛屼负
        'lure_short': lure_short,
        'wash_trade': wash_trade,
        'spread_abnormal': spread_abnormal,
        'v_power_divergence': v_power_divergence,
        'ask_wall_building': ask_wall_building,
        'real_buy_fading': real_buy_fading,
        'wash_trade_severe': wash_trade_severe,
        'liquidity_drain': liquidity_drain,
        'big_order_bias': big_order_bias,
        # 据$撴琛嶇敓鐗鏂緛
        'bias_vwap': round(bias_vwap, 3) if bias_vwap is not None else None,
        'robust_zscore': round(robust_z, 3) if robust_z is not None else None,
        'boll_break': boll_break,
        'boll_over': boll_over,
        'ask_wall_absorb': ask_wall_absorb,
        'spoofing_suspected': spoofing_suspected,
    }


# ============================================================
# 4.5  婊炲洖鍖洪棿鐘舵）冩鐞?# ============================================================
def check_hysteresis(ts_code: str, sig_type: str, current_price: float) -> bool:
    """
    检查价格滞回:上次触发后价格是否已回到允许重触发的区间。
    返回 True=允许触发,False=仍在滞回区间内(跳过)。
    """
    state = _hysteresis_state.get(ts_code, {})
    last_price = state.get(sig_type)
    if last_price is None:
        return True  # 首次触发,允许
    if sig_type == 'positive_t':
        # 正T:需价格从上次触发点回落 hyst_pct 才允许重触发
        return current_price < last_price * (1 - _HYST_POS_PCT)
    else:  # reverse_t
        # 倒T:需价格从上次触发点回升 hyst_pct 才允许重触发
        return current_price > last_price * (1 + _HYST_REV_PCT)


def update_hysteresis(ts_code: str, sig_type: str, trigger_price: float):
    """信号触发时更新滞回状态。"""
    if ts_code not in _hysteresis_state:
        _hysteresis_state[ts_code] = {}
    _hysteresis_state[ts_code][sig_type] = trigger_price


# ============================================================
# 11.7.1  鐙珛椂ㄦ池分信垎返回算
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
    撴熶竴椂ㄦ池分信垎悜岃繑鍥?(raw_score, gate_multiplier, gate_reason)。
    raw_score = 40悜堝熀灞）悜 触﹀傚傚鍒鍒鍒?+ 灞傚鍒鍒鍒?    gate_multiplier 嬭蒋日日﹀樻樻暟算 [0, 1]
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
            gate = min(gate, 0.7)   # 嬭标姏牸急姝返）樻帮价反扵璋ㄦ厧
            reasons.append(f'big_buy_bias={big_order_bias:.2f}')

    return raw_score, gate, ';'.join(reasons)


def finalize_pool1_signal(signal: dict, thresholds: Optional[dict] = None) -> dict:
    """
    Pool1 撴熶竴鎵褑鍒嗙骇悜      - strength < score_observe     -> 嬭椼袝墽?      - score_observe <= strength < score_executable -> 牸瀵瀵?      - >= score_executable          -> 墽琛琛?    """
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
# 1墽仿牸时间控池鎺褎略
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
    thresholds: Optional[dict] = None,
) -> dict:
    """
    左︿突吸信号信″类悜抛）嗗娍价格接悜    - 牸牸鎺鎺ヨ繎/岀冩冩冩涓涓嬭建建悜級涓嬭建建 < 0.5%悜    - RSI6 < 30悜級栵級級栵
    - 价栧綋间ヨ穼骞?> 3%
    """
    result = _empty('left_side_buy')
    if price <= 0 or boll_lower <= 0:
        return result

    th_meta = _threshold_meta(thresholds)
    if th_meta.get('blocked'):
        result['details'] = {'threshold': th_meta}
        return result

    # 动态阈值已提供 limit_filter 时,以动态引擎为单一口径;

    # 仅在无阈值输入时走本地兜底硬过滤。

    if _P1_LIMIT_ENABLED and not isinstance(thresholds, dict):
        if up_limit and up_limit > 0:
            dist_up = abs(up_limit - price) / up_limit * 100
            if dist_up <= _P1_LIMIT_TH_PCT:
                return result
        if down_limit and down_limit > 0:
            dist_dn = abs(price - down_limit) / down_limit * 100
            if dist_dn <= _P1_LIMIT_TH_PCT:
                return result

    if _P1_ENABLED_V2:
        if _P1_VETO_LURE_LONG and lure_long:
            return result
        if _P1_VETO_WASH_TRADE and wash_trade:
            return result

    dist_to_lower = (price - boll_lower) / boll_lower * 100  # 正值=在下轨上方
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
        return result

    conditions = []
    if near_lower:
        conditions.append(f'贴近下轨({dist_to_lower:+.2f}%≤{near_lower_th:.1f}%)')
    if below_lower:
        conditions.append('岀冩冩嬭建建')
    if rsi6 is not None and rsi6 <= rsi_oversold_th:
        conditions.append(f'RSI6超卖({rsi6:.1f}≤{rsi_oversold_th:.0f})')
    if pct_chg is not None and pct_chg < -3:
        conditions.append(f'鎬ヨ穼{pct_chg:.2f}%')

    if _P1_ENABLED_V2:
        confirm_items = []
        if bid_ask_ratio is not None and bid_ask_ratio >= _P1_BID_ASK_TH:
            confirm_items.append(f'鎵挎帴{bid_ask_ratio:.2f}')
        if _P1_RESONANCE_ENABLED and resonance_60m:
            confirm_items.append('60m共振')
        if not confirm_items:
            return result
        conditions.append('灞:' + '+'.join(confirm_items))

    if not conditions:
        return result

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
    strength = min(100, strength)
    return {
        'has_signal': True,
        'type': 'left_side_buy',
        'direction': 'buy',
        'price': round(price, 3),
        'strength': strength,
        'message': f"吸信号@{price:.2f}路" + ' + '.join(conditions),
        'triggered_at': int(time.time()),
        'details': {
            'price': price, 'boll_lower': boll_lower,
            'dist_to_lower_pct': round(dist_to_lower, 2),
            'near_lower_th_pct': near_lower_th,
            'band_width_pct': round(band_width_pct, 2),
            'rsi6': rsi6, 'pct_chg': pct_chg,
            'resonance_60m': bool(resonance_60m),
            'threshold': th_meta,
        },
    }


def detect_right_side_breakout(
    price: float,
    boll_upper: float,
    boll_mid: float,
    boll_lower: float,
    volume_ratio: Optional[float] = None,  # 褰灉棩忚忔瘮
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
    thresholds: Optional[dict] = None,
) -> dict:
    """
    右侧突破信号（顺势突破）
    - 价格有效突破布林中轨或上轨（正向穿越，过滤贴线噪音）
    - 量比 >= 1.3（放量）
    - MA5 上穿 MA10（金叉辅助）
    - RSI6 保持在趋势可持续区间（5~85）
    """
    result = _empty('right_side_breakout')
    if price <= 0 or boll_mid <= 0:
        return result

    th_meta = _threshold_meta(thresholds)
    if th_meta.get('blocked'):
        result['details'] = {'threshold': th_meta}
        return result

    if _P1_ENABLED_V2 and (prev_price is None or prev_price <= 0):
        return result

    # 动态阈值已提供 limit_filter 时,以动态引擎为单一口径;

    # 仅在无阈值输入时走本地兜底硬过滤。

    if _P1_LIMIT_ENABLED and not isinstance(thresholds, dict):
        if up_limit and up_limit > 0:
            dist_up = abs(up_limit - price) / up_limit * 100
            if dist_up <= _P1_LIMIT_TH_PCT:
                return result
        if down_limit and down_limit > 0:
            dist_dn = abs(price - down_limit) / down_limit * 100
            if dist_dn <= _P1_LIMIT_TH_PCT:
                return result

    if _P1_ENABLED_V2:
        if _P1_VETO_LURE_LONG and lure_long:
            return result
        if _P1_VETO_WASH_TRADE and wash_trade:
            return result

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
        return result

    rsi_ok = (rsi6 is None) or (45 <= rsi6 <= 85)
    vol_break_th = 1.3

    conditions = []
    if break_mid:
        conditions.append(f'佺冩冩嬭建')
    if break_upper:
        conditions.append('佺冩冩嬭婅建')
    if volume_ratio is not None and volume_ratio >= vol_break_th:
        conditions.append(f'忚忔瘮{volume_ratio:.2f}')
    if ma5 and ma10 and ma5 > ma10:
        conditions.append('MA5>MA10')
    if rsi6 is not None and rsi_ok:
        conditions.append(f'RSI6={rsi6:.1f}')

    if _P1_ENABLED_V2:
        confirm_items = []
        if bid_ask_ratio is not None and bid_ask_ratio >= _P1_BID_ASK_TH:
            confirm_items.append(f'鎵挎帴{bid_ask_ratio:.2f}')
        if _P1_RESONANCE_ENABLED and resonance_60m:
            confirm_items.append('60m共振')
        if not confirm_items:
            return result
        conditions.append('灞:' + '+'.join(confirm_items))

    if not conditions or not (break_mid or break_upper):
        return result
    if not rsi_ok:
        return result

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
    strength = min(100, strength)
    return {
        'has_signal': True,
        'type': 'right_side_breakout',
        'direction': 'buy',
        'price': round(price, 3),
        'strength': strength,
        'message': f"佺冩冩@{price:.2f}路" + ' + '.join(conditions),
        'triggered_at': int(time.time()),
        'details': {
            'price': price, 'boll_mid': boll_mid, 'boll_upper': boll_upper,
            'volume_ratio': volume_ratio, 'ma5': ma5, 'ma10': ma10,
            'rsi6': rsi6, 'rsi_ok': rsi_ok,
            'prev_price': prev_price, 'cross_mid': cross_mid, 'cross_upper': cross_upper,
            'resonance_60m': bool(resonance_60m),
            'threshold': th_meta,
        },
    }


# ============================================================
# 2墽仿稵+0 监控池姹? 悜坃robust_zscore 鍦ㄦ浣价敤悜垚晠嶉堝畾樻栵栵
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
    spoofing_suspected: bool = False,
    bias_vwap: Optional[float] = None,
    ts_code: Optional[str] = None,
    thresholds: Optional[dict] = None,
) -> dict:
    """T+0 正T(低吸高抛)信号。"""
    result = _empty('positive_t')
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
            'message': f"正T买入{tick_price:.2f}·V1简单触发",
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

    if not confirm_items:
        return result

    raw_score, micro_gate, gate_reason = _compute_t0_gates(
        'positive_t',
        trigger_items,
        confirm_items,
        wash_trade=wash_trade,
        spoofing_suspected=spoofing_suspected,
    )
    final_score = min(100, round(raw_score * micro_gate))

    if final_score < _SCORE_OBSERVE:
        return result

    if ts_code:
        update_hysteresis(ts_code, 'positive_t', tick_price)

    observe_only = (final_score < _SCORE_EXECUTABLE) or bool(th_meta.get('observer_only'))
    msg_parts = ' + '.join(trigger_items) + ' | 确认:' + ' + '.join(confirm_items)
    msg_suffix = '·仅观察' if observe_only else '·可执行'

    return {
        'has_signal': True,
        'type': 'positive_t',
        'direction': 'buy',
        'price': round(tick_price, 3),
        'strength': final_score,
        'message': f"正T买入{tick_price:.2f}·{msg_parts}{msg_suffix}",
        'triggered_at': int(time.time()),
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
            'spoofing_suspected': spoofing_suspected,
            'raw_score': raw_score,
            'micro_gate': micro_gate,
            'gate_reason': gate_reason,
            'observe_only': observe_only,
            'threshold': th_meta,
        },
    }


def detect_reverse_t(
    tick_price: float,
    boll_upper: float,
    intraday_prices: Optional[list[float]] = None,
    v_power_divergence: bool = False,
    ask_wall_building: bool = False,
    real_buy_fading: bool = False,
    wash_trade_severe: bool = False,
    liquidity_drain: bool = False,
    boll_over: Optional[bool] = None,
    spoofing_suspected: bool = False,
    big_order_bias: Optional[float] = None,
    robust_zscore: Optional[float] = None,
    ts_code: Optional[str] = None,
    thresholds: Optional[dict] = None,
) -> dict:
    """T+0 反T(高抛低吸)信号。"""
    result = _empty('reverse_t')
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
            'message': f"反T卖出{tick_price:.2f}·V1简单触发",
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
    if real_buy_fading:
        confirm_items.append('买入力衰减')

    if not confirm_items:
        return result

    raw_score, risk_gate, gate_reason = _compute_t0_gates(
        'reverse_t',
        trigger_items,
        confirm_items,
        wash_trade=wash_trade_severe,
        liquidity_drain=liquidity_drain,
        spoofing_suspected=spoofing_suspected,
        big_order_bias=big_order_bias,
    )
    final_score = min(100, round(raw_score * risk_gate))

    if final_score < _SCORE_OBSERVE:
        return result

    if ts_code:
        update_hysteresis(ts_code, 'reverse_t', tick_price)

    observe_only = (final_score < _SCORE_EXECUTABLE) or bool(th_meta.get('observer_only'))
    msg_parts = ' + '.join(trigger_items) + ' | 确认:' + ' + '.join(confirm_items)
    msg_suffix = '·仅观察' if observe_only else '·可执行'

    return {
        'has_signal': True,
        'type': 'reverse_t',
        'direction': 'sell',
        'price': round(tick_price, 3),
        'strength': final_score,
        'message': f"反T卖出{tick_price:.2f}·{msg_parts}{msg_suffix}",
        'triggered_at': int(time.time()),
        'details': {
            'tick_price': tick_price,
            'boll_upper': boll_upper,
            'robust_zscore': round(robust_z, 3) if robust_z is not None else None,
            'boll_over': boll_over,
            'spoofing_suspected': spoofing_suspected,
            'big_order_bias': big_order_bias,
            'v_power_divergence': v_power_divergence,
            'ask_wall_building': ask_wall_building,
            'real_buy_fading': real_buy_fading,
            'raw_score': raw_score,
            'risk_gate': risk_gate,
            'gate_reason': gate_reason,
            'observe_only': observe_only,
            'threshold': th_meta,
        },
    }

def evaluate_pool(pool_id: int, members_data: list[dict]) -> list[dict]:
    """
    对监控池成员批量运行对应信号。
    Args:
        pool_id: 1=择时池, 2=T+0池
            {ts_code, name, price, boll_upper, boll_mid, boll_lower,
             rsi6?, pct_chg?, volume_ratio?, ma5?, ma10?, vwap?, gub5_trend?, intraday_prices?}

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
                intraday=intraday or {},
                market={
                    'vol_20': m.get('vol_20'),
                    'atr_14': m.get('atr_14'),
                    'regime_hint': m.get('regime_hint'),
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
            s1 = detect_left_side_buy(
                m.get('price', 0), m.get('boll_upper', 0),
                m.get('boll_mid', 0), m.get('boll_lower', 0),
                m.get('rsi6'), m.get('pct_chg'),
                m.get('bid_ask_ratio'), m.get('lure_long', False), m.get('wash_trade', False),
                m.get('up_limit'), m.get('down_limit'), bool(resonance_60m_val),
                thresholds=p1_left_th,
            )
            if s1['has_signal']:
                s1 = finalize_pool1_signal(s1, thresholds=p1_left_th)
            if s1['has_signal']:
                _attach_pool1_resonance_detail(s1)
                signals.append(s1)
            s2 = detect_right_side_breakout(
                m.get('price', 0), m.get('boll_upper', 0),
                m.get('boll_mid', 0), m.get('boll_lower', 0),
                m.get('volume_ratio'), m.get('ma5'), m.get('ma10'), m.get('rsi6'),
                m.get('prev_price'), m.get('bid_ask_ratio'), m.get('lure_long', False), m.get('wash_trade', False),
                m.get('up_limit'), m.get('down_limit'), bool(resonance_60m_val),
                thresholds=p1_right_th,
            )
            if s2['has_signal']:
                s2 = finalize_pool1_signal(s2, thresholds=p1_right_th)
            if s2['has_signal']:
                _attach_pool1_resonance_detail(s2)
                signals.append(s2)
        elif pool_id == 2:
            ts_code = m.get('ts_code')
            # 统一特征构造(P0-2 / 5.1)
            feat = build_t0_features(
                tick_price=m.get('price', 0),
                boll_lower=m.get('boll_lower', 0),
                boll_upper=m.get('boll_upper', 0),
                vwap=m.get('vwap'),
                intraday_prices=m.get('intraday_prices'),
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
                spoofing_vol_ratio=m.get('spoofing_vol_ratio'),
                big_order_bias=m.get('big_order_bias'),
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
                spoofing_suspected=feat['spoofing_suspected'],
                bias_vwap=feat['bias_vwap'],
                ts_code=ts_code,
                thresholds=t0_pos_th,
            )
            if s1['has_signal']:
                signals.append(s1)
            s2 = detect_reverse_t(
                tick_price=feat['tick_price'],
                boll_upper=feat['boll_upper'],
                intraday_prices=m.get('intraday_prices'),
                v_power_divergence=feat['v_power_divergence'],
                ask_wall_building=feat['ask_wall_building'],
                real_buy_fading=feat['real_buy_fading'],
                wash_trade_severe=feat['wash_trade_severe'],
                liquidity_drain=feat['liquidity_drain'],
                boll_over=feat['boll_over'],
                spoofing_suspected=feat['spoofing_suspected'],
                big_order_bias=feat['big_order_bias'],
                robust_zscore=feat['robust_zscore'],
                ts_code=ts_code,
                thresholds=t0_rev_th,
            )
            if s2['has_signal']:
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
# 杈呭姪
# ============================================================
# 信″类型标识 ->?鏂鏂鏂悜堜堜/級級栵級級犲
SIGNAL_DIRECTION = {
    'left_side_buy': 'buy',           # 左︿突吸信号 ->?樻樻樻
    'right_side_breakout': 'buy',     # 墽侧突佺冩冩 ->?樻樻樻
    'positive_t': 'buy',              # 低悜堝厛樻樻悗級級价触﹀傚癸?樻樻樻牸凤栵
    'reverse_t': 'sell',              # 反扵悜堝厛級栧悗樻帮价触﹀傚癸?級栧嚭牸凤栵
}


def _empty(signal_type: str) -> dict:
    return {
        'has_signal': False, 'type': signal_type,
        'direction': SIGNAL_DIRECTION.get(signal_type, 'buy'),
        'price': 0,
        'strength': 0,
        'message': '', 'triggered_at': 0, 'details': {},
    }








