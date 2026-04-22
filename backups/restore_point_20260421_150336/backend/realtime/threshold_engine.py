"""
动态阈值引擎（多因子 / 多时段 / 全链路）

目标：
1. 统一快路径(gm_tick)与慢路径(realtime_monitor)的阈值口径
2. 支持按 signal_type × market_phase × regime 分层覆盖
3. 输出阈值版本号，支持信号回放与审计
"""
from __future__ import annotations

import copy
import datetime as _dt
import time
from typing import Any, Optional

try:
    from config import DYNAMIC_THRESHOLD_CONFIG as _DYN_CFG
except Exception:
    _DYN_CFG = {}


_CST = _dt.timezone(_dt.timedelta(hours=8))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _deep_update(dst: dict, src: dict) -> dict:
    """递归更新：src 覆盖 dst（仅 dict 递归）。"""
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v
    return dst


def resolve_market_phase(ts_epoch: int) -> str:
    """
    交易时段分层：
      - open      09:30-10:00
      - morning   10:00-11:30
      - afternoon 13:00-14:30
      - close     14:30-15:00
      - off_session 其余时段
    """
    dt = _dt.datetime.fromtimestamp(int(ts_epoch), tz=_CST)
    hhmm = dt.hour * 100 + dt.minute
    if 930 <= hhmm < 1000:
        return "open"
    if 1000 <= hhmm < 1130:
        return "morning"
    if 1300 <= hhmm < 1430:
        return "afternoon"
    if 1430 <= hhmm <= 1500:
        return "close"
    return "off_session"


def resolve_regime(ctx: dict) -> str:
    """
    波动率状态分层：
      - limit_magnet
      - high_vol / mid_vol / low_vol

    风险控制约束：
      - 当 vol_20 缺失/非法时，默认回落到 mid_vol（中性），
        避免误判到 low_vol 导致阈值被意外放松。
    """
    if bool(ctx.get("limit_magnet")):
        return "limit_magnet"

    vol_cfg = (_DYN_CFG.get("volatility_regime", {}) if isinstance(_DYN_CFG, dict) else {})
    high = _safe_float(vol_cfg.get("high"), 0.30)
    mid = _safe_float(vol_cfg.get("mid"), 0.15)
    vol_20 = _safe_float(ctx.get("vol_20"), -1.0)

    # 缺失或无效值 -> 中性分层（保守）
    if vol_20 < 0:
        return "mid_vol"

    if vol_20 >= high:
        return "high_vol"
    if vol_20 >= mid:
        return "mid_vol"
    return "low_vol"


def build_signal_context(
    ts_code: str,
    pool_id: int,
    tick: dict,
    daily: dict,
    intraday: Optional[dict] = None,
    market: Optional[dict] = None,
) -> dict:
    """
    构建统一信号上下文（快慢路径共享）。

    intraday 建议字段（可选）：
      - vwap, robust_zscore, gub5_ratio, gub5_transition, intraday_vol
    market 建议字段（可选）：
      - vol_20, regime_hint
    """
    intraday = intraday or {}
    market = market or {}

    now_ts = int(tick.get("timestamp") or time.time())
    price = _safe_float(tick.get("price"), 0.0)
    pre_close = _safe_float(tick.get("pre_close"), 0.0)
    pct_chg = _safe_float(tick.get("pct_chg"), 0.0)
    if pre_close > 0 and abs(pct_chg) < 1e-9 and price > 0:
        pct_chg = (price - pre_close) / pre_close * 100

    # 五档与价差
    bids = tick.get("bids") or []
    asks = tick.get("asks") or []
    best_bid = _safe_float(bids[0][0], 0.0) if bids else 0.0
    best_ask = _safe_float(asks[0][0], 0.0) if asks else 0.0
    bid_vol = sum(int(v or 0) for _, v in bids[:5]) if bids else 0
    ask_vol = sum(int(v or 0) for _, v in asks[:5]) if asks else 0
    bid_ask_ratio = (bid_vol / ask_vol) if ask_vol > 0 else 1.0
    spread_bps = ((best_ask - best_bid) / price * 10000) if price > 0 and best_ask > 0 and best_bid > 0 else 0.0

    # 日线结构
    boll_upper = _safe_float(daily.get("boll_upper"), 0.0)
    boll_mid = _safe_float(daily.get("boll_mid"), 0.0)
    boll_lower = _safe_float(daily.get("boll_lower"), 0.0)
    band_width_pct = ((boll_upper - boll_lower) / boll_mid * 100) if boll_mid > 0 else 0.0

    up_limit = daily.get("up_limit")
    down_limit = daily.get("down_limit")
    up_limit = _safe_float(up_limit, 0.0)
    down_limit = _safe_float(down_limit, 0.0)

    dist_up_limit_pct = ((up_limit - price) / up_limit * 100) if up_limit > 0 and price > 0 else None
    dist_down_limit_pct = ((price - down_limit) / down_limit * 100) if down_limit > 0 and price > 0 else None
    dists = [d for d in (dist_up_limit_pct, dist_down_limit_pct) if d is not None]
    min_dist_limit_pct = min(dists) if dists else None

    lm_cfg = (_DYN_CFG.get("limit_magnet", {}) if isinstance(_DYN_CFG, dict) else {})
    lm_enabled = bool(lm_cfg.get("enabled", True))
    lm_th = _safe_float(lm_cfg.get("distance_pct"), 1.5)
    limit_magnet = bool(lm_enabled and min_dist_limit_pct is not None and min_dist_limit_pct < lm_th)

    ctx = {
        "ts_code": ts_code,
        "pool_id": int(pool_id),
        "timestamp": now_ts,
        "market_phase": resolve_market_phase(now_ts),

        "price": price,
        "pre_close": pre_close,
        "pct_chg": pct_chg,
        "volume": int(tick.get("volume") or 0),
        "amount": _safe_float(tick.get("amount"), 0.0),

        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_ask_ratio": bid_ask_ratio,
        "spread_bps": spread_bps,

        "boll_upper": boll_upper,
        "boll_mid": boll_mid,
        "boll_lower": boll_lower,
        "band_width_pct": band_width_pct,

        "up_limit": up_limit if up_limit > 0 else None,
        "down_limit": down_limit if down_limit > 0 else None,
        "dist_up_limit_pct": dist_up_limit_pct,
        "dist_down_limit_pct": dist_down_limit_pct,
        "min_dist_limit_pct": min_dist_limit_pct,
        "limit_magnet": limit_magnet,

        # intraday / market 可选扩展
        "vwap": intraday.get("vwap"),
        "bias_vwap": intraday.get("bias_vwap"),
        "robust_zscore": intraday.get("robust_zscore"),
        "gub5_ratio": intraday.get("gub5_ratio"),
        "gub5_transition": intraday.get("gub5_transition"),

        "vol_20": market.get("vol_20", daily.get("vol_20")),
        "atr_14": market.get("atr_14", daily.get("atr_14")),
    }

    # 若外部直接给了 regime_hint，优先沿用；否则自动判断
    regime_hint = market.get("regime_hint")
    ctx["regime"] = str(regime_hint) if regime_hint else resolve_regime(ctx)
    return ctx


def apply_limit_magnet_filter(ctx: dict, threshold_payload: dict, profile: Optional[dict] = None) -> dict:
    """
    根据 Distance_to_Limit 应用磁吸过滤。

    Returns:
      {
        'limit_magnet': bool,
        'distance_pct': float | None,
        'action': 'blocked|observer_only|board_mode|none',
        'blocked': bool,
        'observer_only': bool,
      }
    """
    cfg = profile if profile is not None else _DYN_CFG
    lm_cfg = (cfg.get("limit_magnet", {}) if isinstance(cfg, dict) else {})
    enabled = bool(lm_cfg.get("enabled", True))
    signal_type = str(threshold_payload.get("signal_type", ""))
    dist = ctx.get("min_dist_limit_pct")

    if not enabled:
        return {
            "limit_magnet": False,
            "distance_pct": dist,
            "action": "none",
            "blocked": False,
            "observer_only": False,
        }

    actions = lm_cfg.get("actions", {}) if isinstance(lm_cfg, dict) else {}
    # 支持从上下文覆盖磁吸阈值（用于 Pool1 与 T+0 口径分离）
    # 例如 Pool1 使用 0.3%，T+0 使用 1.5%。
    th = _safe_float(ctx.get("limit_magnet_distance_pct"), -1.0)
    if th <= 0:
        th = _safe_float(lm_cfg.get("distance_pct"), 1.5)
    near_limit = (dist is not None and dist < th)
    action = str(actions.get(signal_type, "none")) if near_limit else "none"

    return {
        "limit_magnet": bool(near_limit),
        "distance_pct": dist,
        "threshold_pct": th,
        "action": action,
        "blocked": action == "blocked",
        "observer_only": action == "observer_only",
    }


def get_thresholds(signal_type: str, ctx: dict, profile: Optional[dict] = None) -> dict:
    """
    解析动态阈值（base + phase_overrides + regime_overrides）。

    返回结果可直接透传给 detect_* 函数。
    """
    cfg = profile if profile is not None else _DYN_CFG
    if not isinstance(cfg, dict):
        cfg = {}

    phase = str(ctx.get("market_phase") or resolve_market_phase(int(time.time())))
    regime = str(ctx.get("regime") or resolve_regime(ctx))
    version = str(cfg.get("version", "dyn-v1"))

    base_all = cfg.get("base", {}) if isinstance(cfg.get("base"), dict) else {}
    base_sig = copy.deepcopy(base_all.get(signal_type, {}))

    phase_over = cfg.get("phase_overrides", {}) if isinstance(cfg.get("phase_overrides"), dict) else {}
    phase_sig = phase_over.get(phase, {}).get(signal_type, {}) if isinstance(phase_over.get(phase), dict) else {}
    if isinstance(phase_sig, dict):
        _deep_update(base_sig, phase_sig)

    regime_over = cfg.get("regime_overrides", {}) if isinstance(cfg.get("regime_overrides"), dict) else {}
    regime_sig = regime_over.get(regime, {}).get(signal_type, {}) if isinstance(regime_over.get(regime), dict) else {}
    if isinstance(regime_sig, dict):
        _deep_update(base_sig, regime_sig)

    # 磁吸过滤结果
    limit_filter = apply_limit_magnet_filter(ctx, {"signal_type": signal_type}, cfg)

    out = copy.deepcopy(base_sig)
    out.update({
        "signal_type": signal_type,
        "threshold_version": version,
        "market_phase": phase,
        "regime": regime,
        "limit_filter": limit_filter,
        "blocked": bool(limit_filter.get("blocked")),
        "observer_only": bool(limit_filter.get("observer_only")),
        "limit_magnet": bool(limit_filter.get("limit_magnet")),
    })
    return out


__all__ = [
    "build_signal_context",
    "resolve_market_phase",
    "resolve_regime",
    "apply_limit_magnet_filter",
    "get_thresholds",
]
