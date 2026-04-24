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
import json
import os
import time
import re
from typing import Any, Optional

try:
    from config import DYNAMIC_THRESHOLD_CONFIG as _DYN_CFG
except Exception:
    _DYN_CFG = {}
try:
    from config import REALTIME_RUNTIME_STATE_CONFIG as _RUNTIME_CFG
except Exception:
    _RUNTIME_CFG = {}
try:
    from config import DYNAMIC_THRESHOLD_CLOSED_LOOP_CONFIG as _CLOSED_LOOP_CFG
except Exception:
    _CLOSED_LOOP_CFG = {}

try:
    import redis as _redis_mod
except Exception:
    _redis_mod = None


_CST = _dt.timezone(_dt.timedelta(hours=8))
_RUNTIME_REDIS_CFG = _RUNTIME_CFG.get("redis", {}) if isinstance(_RUNTIME_CFG, dict) else {}
_CALI_ENABLED = bool((_CLOSED_LOOP_CFG or {}).get("enabled", False))
_CALI_REFRESH_SEC = max(3.0, float(os.getenv("DYNAMIC_THRESHOLD_CLOSED_LOOP_REFRESH_SEC", "10")))
_CALI_KEY_PREFIX = str(
    _RUNTIME_REDIS_CFG.get("key_prefix", os.getenv("RUNTIME_STATE_REDIS_KEY_PREFIX", "quant:runtime"))
)
_CALI_REDIS_URL = str(
    _RUNTIME_REDIS_CFG.get(
        "url", os.getenv("RUNTIME_STATE_REDIS_URL", os.getenv("REALTIME_DB_WRITER_REDIS_URL", "redis://127.0.0.1:6379/0"))
    )
)
_CALI_REDIS_ENABLED = bool(
    _RUNTIME_REDIS_CFG.get("enabled", os.getenv("RUNTIME_STATE_REDIS_ENABLED", "0").lower() in ("1", "true", "yes", "on"))
)
_CALI_REDIS_KEY = f"{_CALI_KEY_PREFIX}:threshold:calibration:latest"
_cali_redis = None
_cali_redis_next_retry = 0.0
_cali_cache_at = 0.0
_cali_cache_payload: dict = {}
_MODEL_CFG = _DYN_CFG.get("instrument_model", {}) if isinstance(_DYN_CFG, dict) else {}
_IPO_EARLY_DAYS = int(_MODEL_CFG.get("ipo_early_days", 60) or 60)
_IPO_YOUNG_DAYS = int(_MODEL_CFG.get("ipo_young_days", 180) or 180)
_ETF_LIMIT_PCT = _safe_float(_MODEL_CFG.get("etf_limit_pct"), 10.0) if "_safe_float" in globals() else 10.0


def _norm_text(v: Any) -> str:
    return str(v or "").strip()


def _parse_list_date(v: Any) -> Optional[_dt.date]:
    text = _norm_text(v)
    if not text:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return _dt.datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def _is_etf_code(ts_code: str) -> bool:
    code = str(ts_code or "").split(".", 1)[0]
    if len(code) != 6 or not code.isdigit():
        return False
    return (
        code.startswith(("159", "510", "511", "512", "513", "515", "516", "517", "518", "588"))
        or code.startswith(("560", "561", "562", "563"))
        or code.startswith("16")
    )


def _infer_board_segment(ts_code: str, market_name: Any = None) -> str:
    code = str(ts_code or "").split(".", 1)[0]
    market_text = _norm_text(market_name)
    if "创业" in market_text or code.startswith(("300", "301")):
        return "gem"
    if "科创" in market_text or code.startswith("688"):
        return "star"
    if ts_code.endswith(".BJ") or code.startswith(("4", "8")):
        return "beijing"
    if code.startswith(("000", "001", "002", "003", "600", "601", "603", "605")):
        return "main_board"
    return "other"


def _infer_security_type(ts_code: str, name: Any = None, market_name: Any = None) -> str:
    name_text = _norm_text(name).upper()
    market_text = _norm_text(market_name).upper()
    if "ETF" in name_text or "ETF" in market_text or _is_etf_code(ts_code):
        return "etf"
    if _infer_risk_warning(name):
        return "risk_warning_stock"
    return "common_stock"


def _infer_risk_warning(name: Any) -> bool:
    text = _norm_text(name).upper().replace(" ", "")
    if not text:
        return False
    return bool(re.search(r"(^|\*)ST|^SST|^S\*ST|退", text))


def _resolve_price_limit_pct(board_segment: str, security_type: str, risk_warning: bool) -> float:
    if risk_warning or security_type == "risk_warning_stock":
        return 5.0
    if board_segment == "gem":
        return 20.0
    if board_segment == "star":
        return 20.0
    if board_segment == "beijing":
        return 30.0
    if security_type == "etf":
        return 10.0
    return 10.0


def calc_theoretical_limits(pre_close: Any, price_limit_pct: Any) -> tuple[Optional[float], Optional[float]]:
    pc = _safe_float(pre_close, 0.0)
    limit_pct = _safe_float(price_limit_pct, 0.0)
    if pc <= 0 or limit_pct <= 0:
        return None, None
    ratio = limit_pct / 100.0
    return pc * (1.0 + ratio), pc * (1.0 - ratio)


def infer_instrument_profile(
    ts_code: str,
    *,
    name: Any = None,
    market_name: Any = None,
    list_date: Any = None,
    ts_epoch: Optional[int] = None,
) -> dict:
    board_segment = _infer_board_segment(ts_code, market_name)
    risk_warning = _infer_risk_warning(name)
    security_type = _infer_security_type(ts_code, name, market_name)
    limit_pct = _resolve_price_limit_pct(board_segment, security_type, risk_warning)
    list_day = _parse_list_date(list_date)
    listing_days = None
    listing_stage = "unknown"
    if list_day is not None:
        now_date = _dt.datetime.fromtimestamp(int(ts_epoch or time.time()), tz=_CST).date()
        listing_days = max(0, (now_date - list_day).days)
        if listing_days <= _IPO_EARLY_DAYS:
            listing_stage = "ipo_early"
        elif listing_days <= _IPO_YOUNG_DAYS:
            listing_stage = "ipo_young"
        else:
            listing_stage = "seasoned"
    return {
        "board_segment": board_segment,
        "security_type": security_type,
        "risk_warning": bool(risk_warning),
        "market_name": _norm_text(market_name),
        "list_date": list_day.isoformat() if list_day else _norm_text(list_date),
        "listing_days": listing_days,
        "listing_stage": listing_stage,
        "price_limit_pct": limit_pct,
    }


def _detect_micro_environment(
    *,
    price: float,
    bid_ask_ratio: float,
    bid_vol: int,
    ask_vol: int,
    dist_up_limit_pct: Optional[float],
    dist_down_limit_pct: Optional[float],
    min_dist_limit_pct: Optional[float],
    price_limit_pct: float,
    volume_pace_ratio: Any,
    volume_pace_state: Any,
    progress_ratio: Any,
) -> dict:
    cfg = _MODEL_CFG if isinstance(_MODEL_CFG, dict) else {}
    drought_cfg = cfg.get("volume_drought", {}) if isinstance(cfg.get("volume_drought"), dict) else {}
    seal_cfg = cfg.get("seal_env", {}) if isinstance(cfg.get("seal_env"), dict) else {}

    drought_min_progress = _safe_float(drought_cfg.get("min_progress"), 0.35)
    drought_max_ratio = _safe_float(drought_cfg.get("max_ratio"), 0.55)
    pace_ratio = _safe_float(volume_pace_ratio, -1.0)
    prog = _safe_float(progress_ratio, 0.0)
    pace_state = _norm_text(volume_pace_state).lower()
    volume_drought = bool(
        prog >= drought_min_progress
        and pace_ratio > 0
        and pace_ratio <= drought_max_ratio
        and pace_state in {"shrink", "unknown", ""}
    )

    seal_distance_pct = _safe_float(seal_cfg.get("distance_pct"), 0.35 * max(price_limit_pct / 10.0, 1.0))
    seal_one_side_ratio = _safe_float(seal_cfg.get("one_side_ratio"), 6.0)
    seal_min_opposite_vol = int(seal_cfg.get("min_opposite_vol", 1000) or 1000)

    near_up = dist_up_limit_pct is not None and dist_up_limit_pct <= seal_distance_pct
    near_down = dist_down_limit_pct is not None and dist_down_limit_pct <= seal_distance_pct
    seal_side = "none"
    board_seal_env = False
    if near_up and (ask_vol <= seal_min_opposite_vol or bid_ask_ratio >= seal_one_side_ratio):
        board_seal_env = True
        seal_side = "up"
    elif near_down and (bid_vol <= seal_min_opposite_vol or bid_ask_ratio <= (1.0 / max(seal_one_side_ratio, 1.0))):
        board_seal_env = True
        seal_side = "down"

    return {
        "volume_drought": bool(volume_drought),
        "board_seal_env": bool(board_seal_env),
        "seal_side": seal_side,
        "seal_distance_pct": seal_distance_pct,
        "book_bid_vol": int(bid_vol or 0),
        "book_ask_vol": int(ask_vol or 0),
        "min_dist_limit_pct": min_dist_limit_pct,
    }


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


def _get_cali_redis():
    global _cali_redis, _cali_redis_next_retry
    if not (_CALI_ENABLED and _CALI_REDIS_ENABLED) or _redis_mod is None:
        return None
    if _cali_redis is not None:
        return _cali_redis
    now = time.time()
    if now < _cali_redis_next_retry:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_CALI_REDIS_URL, decode_responses=True)
        cli.ping()
        _cali_redis = cli
        return _cali_redis
    except Exception:
        _cali_redis_next_retry = now + 5.0
        return None


def _load_runtime_calibration() -> dict:
    global _cali_cache_at, _cali_cache_payload
    now = time.time()
    if _cali_cache_payload and (now - _cali_cache_at) < _CALI_REFRESH_SEC:
        return _cali_cache_payload
    cli = _get_cali_redis()
    if cli is None:
        return _cali_cache_payload or {}
    try:
        raw = cli.get(_CALI_REDIS_KEY)
        if raw:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                _cali_cache_payload = payload
        _cali_cache_at = now
    except Exception:
        _cali_cache_at = now
    return _cali_cache_payload or {}


def _best_runtime_override(signal_type: str, ctx: dict) -> Optional[dict]:
    payload = _load_runtime_calibration()
    rules = payload.get("rules") if isinstance(payload, dict) else None
    if not isinstance(rules, list) or not rules:
        return None
    phase = str(ctx.get("market_phase") or "")
    regime = str(ctx.get("regime") or "")
    industry = str(ctx.get("industry") or "")
    board_segment = str(ctx.get("board_segment") or "")
    security_type = str(ctx.get("security_type") or "")
    listing_stage = str(ctx.get("listing_stage") or "")
    best = None
    best_score = -1
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if str(rule.get("signal_type") or "") != str(signal_type):
            continue
        # wildcard using "*" means "any"
        rp = str(rule.get("market_phase", "*") or "*")
        rr = str(rule.get("regime", "*") or "*")
        ri = str(rule.get("industry", "*") or "*")
        rb = str(rule.get("board_segment", "*") or "*")
        rs = str(rule.get("security_type", "*") or "*")
        rl = str(rule.get("listing_stage", "*") or "*")
        if rp != "*" and rp != phase:
            continue
        if rr != "*" and rr != regime:
            continue
        if ri != "*" and ri != industry:
            continue
        if rb != "*" and rb != board_segment:
            continue
        if rs != "*" and rs != security_type:
            continue
        if rl != "*" and rl != listing_stage:
            continue
        score = 0
        score += 6 if rs != "*" else 0
        score += 5 if rb != "*" else 0
        score += 4 if ri != "*" else 0
        score += 2 if rr != "*" else 0
        score += 1 if rp != "*" else 0
        score += 1 if rl != "*" else 0
        if score > best_score:
            best_score = score
            best = rule
    return best


def _apply_runtime_override(base_sig: dict, signal_type: str, ctx: dict) -> tuple[dict, dict]:
    out = copy.deepcopy(base_sig)
    meta = {"applied": False}
    rule = _best_runtime_override(signal_type, ctx)
    if not isinstance(rule, dict):
        return out, meta
    # 两种写法都支持：
    # 1) delta: {"z_th": +0.1}（按加减）
    # 2) params: {"z_th": 2.5}（直接覆盖）
    delta = rule.get("delta")
    if isinstance(delta, dict):
        for k, v in delta.items():
            try:
                out[k] = _safe_float(out.get(k), 0.0) + float(v)
            except Exception:
                continue
    params = rule.get("params")
    if isinstance(params, dict):
        _deep_update(out, params)
    meta = {
        "applied": True,
        "rule": {
            "signal_type": rule.get("signal_type"),
            "market_phase": rule.get("market_phase", "*"),
            "regime": rule.get("regime", "*"),
            "industry": rule.get("industry", "*"),
            "board_segment": rule.get("board_segment", "*"),
            "security_type": rule.get("security_type", "*"),
            "listing_stage": rule.get("listing_stage", "*"),
            "reason": rule.get("reason", ""),
        },
        "metrics": rule.get("metrics", {}),
    }
    return out, meta


def _rule_match_value(rule_val: Any, ctx_val: Any) -> bool:
    rv = "*" if rule_val is None else rule_val
    if isinstance(rv, str):
        rv = rv.strip()
        if rv in {"", "*"}:
            return True
        return str(ctx_val or "") == rv
    if isinstance(rv, bool):
        return bool(ctx_val) == rv
    return rv == ctx_val


def _profile_rule_score(rule: dict) -> int:
    score = 0
    for key, weight in (
        ("security_type", 6),
        ("board_segment", 5),
        ("listing_stage", 4),
        ("risk_warning", 4),
        ("industry", 3),
        ("regime", 2),
        ("market_phase", 1),
    ):
        value = rule.get(key, "*")
        if isinstance(value, str):
            if value.strip() not in {"", "*"}:
                score += weight
        elif value is not None:
            score += weight
    return score


def _matching_profile_rules(signal_type: str, ctx: dict, cfg: dict) -> list[dict]:
    rules = cfg.get("profile_overrides") if isinstance(cfg, dict) else None
    if not isinstance(rules, list) or not rules:
        return []
    matched: list[tuple[int, int, dict]] = []
    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            continue
        if str(rule.get("signal_type") or "") != str(signal_type):
            continue
        if not _rule_match_value(rule.get("market_phase", "*"), ctx.get("market_phase")):
            continue
        if not _rule_match_value(rule.get("regime", "*"), ctx.get("regime")):
            continue
        if not _rule_match_value(rule.get("industry", "*"), ctx.get("industry")):
            continue
        if not _rule_match_value(rule.get("board_segment", "*"), ctx.get("board_segment")):
            continue
        if not _rule_match_value(rule.get("security_type", "*"), ctx.get("security_type")):
            continue
        if not _rule_match_value(rule.get("listing_stage", "*"), ctx.get("listing_stage")):
            continue
        if not _rule_match_value(rule.get("risk_warning", "*"), ctx.get("risk_warning")):
            continue
        matched.append((_profile_rule_score(rule), idx, rule))
    matched.sort(key=lambda x: (x[0], x[1]))
    return [x[2] for x in matched]


def _apply_static_profile_overrides(base_sig: dict, signal_type: str, ctx: dict, cfg: dict) -> tuple[dict, dict]:
    out = copy.deepcopy(base_sig)
    rules = _matching_profile_rules(signal_type, ctx, cfg)
    if not rules:
        return out, {"applied": False, "rules": []}
    applied_rules: list[dict] = []
    for rule in rules:
        delta = rule.get("delta")
        if isinstance(delta, dict):
            for k, v in delta.items():
                try:
                    out[k] = _safe_float(out.get(k), 0.0) + float(v)
                except Exception:
                    continue
        params = rule.get("params")
        if isinstance(params, dict):
            _deep_update(out, params)
        applied_rules.append(
            {
                "signal_type": rule.get("signal_type"),
                "market_phase": rule.get("market_phase", "*"),
                "regime": rule.get("regime", "*"),
                "industry": rule.get("industry", "*"),
                "board_segment": rule.get("board_segment", "*"),
                "security_type": rule.get("security_type", "*"),
                "listing_stage": rule.get("listing_stage", "*"),
                "risk_warning": rule.get("risk_warning", "*"),
                "reason": rule.get("reason", ""),
            }
        )
    return out, {"applied": True, "rules": applied_rules}


def resolve_market_phase_detail(ts_epoch: int) -> str:
    """
    更细粒度的盘中阶段：
      - open_strict  09:30-09:35
      - open         09:35-10:00
      - morning      10:00-11:30
      - lunch_break  11:30-13:00
      - afternoon    13:00-14:30
      - close        14:30-14:57
      - close_reduce 14:57-15:00
      - off_session  其余时段
    """
    dt = _dt.datetime.fromtimestamp(int(ts_epoch), tz=_CST)
    hm = dt.hour * 60 + dt.minute
    if 555 <= hm < 570:
        return "pre_auction"
    if 570 <= hm < 575:
        return "open_strict"
    if 575 <= hm < 600:
        return "open"
    if 600 <= hm < 690:
        return "morning"
    if 690 <= hm < 780:
        return "lunch_break"
    if 780 <= hm < 870:
        return "afternoon"
    if 870 <= hm < 897:
        return "close"
    if 897 <= hm < 900:
        return "close_reduce"
    return "off_session"


def resolve_session_policy(ts_epoch: int) -> str:
    detail = resolve_market_phase_detail(ts_epoch)
    if detail == "pre_auction":
        return "auction_pause"
    if detail == "open_strict":
        return "open_strict"
    if detail == "lunch_break":
        return "lunch_pause"
    if detail == "close_reduce":
        return "close_reduce"
    return "none"


def resolve_market_phase(ts_epoch: int) -> str:
    """
    交易时段分层：
      - open      09:30-10:00
      - morning   10:00-11:30
      - afternoon 13:00-14:30
      - close     14:30-15:00
      - off_session 其余时段
    """
    detail = resolve_market_phase_detail(ts_epoch)
    if detail in {"open_strict", "open"}:
        return "open"
    if detail == "morning":
        return "morning"
    if detail == "afternoon":
        return "afternoon"
    if detail in {"close", "close_reduce"}:
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
    if bool(ctx.get("board_seal_env")):
        return "seal_env"
    if bool(ctx.get("limit_magnet")):
        return "limit_magnet"
    if bool(ctx.get("volume_drought")):
        return "drought"

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


def _apply_session_policy_overrides(base_sig: dict, signal_type: str, ctx: dict, cfg: dict) -> tuple[dict, dict]:
    out = copy.deepcopy(base_sig)
    policy = str(ctx.get("session_policy") or "none")
    if policy in {"", "none"}:
        return out, {"applied": False, "policy": "none", "params": {}}
    session_cfg = cfg.get("session_policies") if isinstance(cfg, dict) else None
    if not isinstance(session_cfg, dict):
        return out, {"applied": False, "policy": policy, "params": {}}
    policy_cfg = session_cfg.get(policy)
    if not isinstance(policy_cfg, dict):
        return out, {"applied": False, "policy": policy, "params": {}}
    params = policy_cfg.get(signal_type)
    if not isinstance(params, dict):
        return out, {"applied": False, "policy": policy, "params": {}}
    _deep_update(out, params)
    return out, {
        "applied": True,
        "policy": policy,
        "market_phase_detail": str(ctx.get("market_phase_detail") or ""),
        "params": {
            k: params.get(k)
            for k in ("blocked", "blocked_reason", "observer_only", "observer_reason", "session_hint")
            if k in params
        },
    }


def build_signal_context(
    ts_code: str,
    pool_id: int,
    tick: dict,
    daily: dict,
    intraday: Optional[dict] = None,
    market: Optional[dict] = None,
) -> dict:
    """Build a shared signal context for fast and slow paths."""
    intraday = intraday or {}
    market = market or {}

    now_ts = int(tick.get("timestamp") or time.time())
    price = _safe_float(tick.get("price"), 0.0)
    pre_close = _safe_float(tick.get("pre_close"), 0.0)
    pct_chg = _safe_float(tick.get("pct_chg"), 0.0)
    if pre_close > 0 and abs(pct_chg) < 1e-9 and price > 0:
        pct_chg = (price - pre_close) / pre_close * 100

    bids = tick.get("bids") or []
    asks = tick.get("asks") or []
    best_bid = _safe_float(bids[0][0], 0.0) if bids else 0.0
    best_ask = _safe_float(asks[0][0], 0.0) if asks else 0.0
    bid_vol = sum(int(v or 0) for _, v in bids[:5]) if bids else 0
    ask_vol = sum(int(v or 0) for _, v in asks[:5]) if asks else 0
    bid_ask_ratio = (bid_vol / ask_vol) if ask_vol > 0 else 1.0
    spread_bps = ((best_ask - best_bid) / price * 10000) if price > 0 and best_ask > 0 and best_bid > 0 else 0.0

    boll_upper = _safe_float(daily.get("boll_upper"), 0.0)
    boll_mid = _safe_float(daily.get("boll_mid"), 0.0)
    boll_lower = _safe_float(daily.get("boll_lower"), 0.0)
    band_width_pct = ((boll_upper - boll_lower) / boll_mid * 100) if boll_mid > 0 else 0.0

    up_limit = _safe_float(daily.get("up_limit"), 0.0)
    down_limit = _safe_float(daily.get("down_limit"), 0.0)
    instrument_profile = market.get("instrument_profile") if isinstance(market.get("instrument_profile"), dict) else None
    if not isinstance(instrument_profile, dict):
        instrument_profile = infer_instrument_profile(
            ts_code,
            name=market.get("name", tick.get("name")),
            market_name=market.get("market_name", market.get("market", daily.get("market"))),
            list_date=market.get("list_date", daily.get("list_date")),
            ts_epoch=now_ts,
        )
    else:
        instrument_profile = dict(instrument_profile)

    price_limit_pct = _safe_float(
        instrument_profile.get("price_limit_pct"),
        _resolve_price_limit_pct(
            str(instrument_profile.get("board_segment") or ""),
            str(instrument_profile.get("security_type") or ""),
            bool(instrument_profile.get("risk_warning")),
        ),
    )
    if (up_limit <= 0 or down_limit <= 0) and pre_close > 0 and price_limit_pct > 0:
        theo_up_limit, theo_down_limit = calc_theoretical_limits(pre_close, price_limit_pct)
        if up_limit <= 0 and theo_up_limit is not None:
            up_limit = theo_up_limit
        if down_limit <= 0 and theo_down_limit is not None:
            down_limit = theo_down_limit

    dist_up_limit_pct = ((up_limit - price) / up_limit * 100) if up_limit > 0 and price > 0 else None
    dist_down_limit_pct = ((price - down_limit) / down_limit * 100) if down_limit > 0 and price > 0 else None
    dists = [d for d in (dist_up_limit_pct, dist_down_limit_pct) if d is not None]
    min_dist_limit_pct = min(dists) if dists else None

    lm_cfg = (_DYN_CFG.get("limit_magnet", {}) if isinstance(_DYN_CFG, dict) else {})
    lm_enabled = bool(lm_cfg.get("enabled", True))
    lm_th = _safe_float(lm_cfg.get("distance_pct"), 1.5)
    limit_magnet = bool(lm_enabled and min_dist_limit_pct is not None and min_dist_limit_pct < lm_th)
    progress_ratio = intraday.get("progress_ratio", intraday.get("volume_pace_progress"))
    concept_boards = market.get("concept_boards") if isinstance(market.get("concept_boards"), list) else []
    concept_boards = [str(x or "").strip() for x in concept_boards if str(x or "").strip()]
    core_concept_board = str(market.get("core_concept_board") or "").strip()
    concept_ecology = market.get("concept_ecology") if isinstance(market.get("concept_ecology"), dict) else {}
    industry_ecology = market.get("industry_ecology") if isinstance(market.get("industry_ecology"), dict) else {}
    micro_environment = _detect_micro_environment(
        price=price,
        bid_ask_ratio=bid_ask_ratio,
        bid_vol=bid_vol,
        ask_vol=ask_vol,
        dist_up_limit_pct=dist_up_limit_pct,
        dist_down_limit_pct=dist_down_limit_pct,
        min_dist_limit_pct=min_dist_limit_pct,
        price_limit_pct=price_limit_pct,
        volume_pace_ratio=intraday.get("volume_pace_ratio"),
        volume_pace_state=intraday.get("volume_pace_state"),
        progress_ratio=progress_ratio,
    )

    ctx = {
        "ts_code": ts_code,
        "pool_id": int(pool_id),
        "timestamp": now_ts,
        "market_phase": resolve_market_phase(now_ts),
        "market_phase_detail": resolve_market_phase_detail(now_ts),
        "session_policy": resolve_session_policy(now_ts),
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
        "board_segment": instrument_profile.get("board_segment"),
        "security_type": instrument_profile.get("security_type"),
        "risk_warning": bool(instrument_profile.get("risk_warning")),
        "market_name": instrument_profile.get("market_name"),
        "list_date": instrument_profile.get("list_date"),
        "listing_days": instrument_profile.get("listing_days"),
        "listing_stage": instrument_profile.get("listing_stage"),
        "price_limit_pct": price_limit_pct,
        "concept_boards": concept_boards,
        "core_concept_board": core_concept_board,
        "concept_ecology": dict(concept_ecology),
        "industry_ecology": dict(industry_ecology),
        "volume_drought": bool(micro_environment.get("volume_drought")),
        "board_seal_env": bool(micro_environment.get("board_seal_env")),
        "seal_side": micro_environment.get("seal_side"),
        "seal_distance_pct": micro_environment.get("seal_distance_pct"),
        "instrument_profile": instrument_profile,
        "micro_environment": micro_environment,
        "vwap": intraday.get("vwap"),
        "bias_vwap": intraday.get("bias_vwap"),
        "robust_zscore": intraday.get("robust_zscore"),
        "gub5_ratio": intraday.get("gub5_ratio"),
        "gub5_transition": intraday.get("gub5_transition"),
        "volume_pace_ratio": intraday.get("volume_pace_ratio"),
        "volume_pace_state": intraday.get("volume_pace_state"),
        "progress_ratio": progress_ratio,
        "vol_20": market.get("vol_20", daily.get("vol_20")),
        "atr_14": market.get("atr_14", daily.get("atr_14")),
        "industry": market.get("industry", daily.get("industry")),
    }

    regime_hint = market.get("regime_hint")
    ctx["regime"] = str(regime_hint) if regime_hint else resolve_regime(ctx)
    return ctx



def apply_limit_magnet_filter(ctx: dict, threshold_payload: dict, profile: Optional[dict] = None) -> dict:
    """
    ??? Distance_to_Limit ???????????
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
    th = _safe_float(ctx.get("limit_magnet_distance_pct"), -1.0)
    base_th = _safe_float(lm_cfg.get("distance_pct"), 1.5)
    if th <= 0:
        th = base_th
    price_limit_pct = _safe_float(ctx.get("price_limit_pct"), 10.0)
    if price_limit_pct > 0:
        scale = min(2.5, max(0.5, price_limit_pct / 10.0))
        th = th * scale
    near_limit = (dist is not None and dist < th)
    action = str(actions.get(signal_type, "none")) if near_limit else "none"

    return {
        "limit_magnet": bool(near_limit),
        "distance_pct": dist,
        "base_threshold_pct": base_th,
        "threshold_pct": th,
        "action": action,
        "blocked": action == "blocked",
        "observer_only": action == "observer_only",
    }



def get_thresholds(signal_type: str, ctx: dict, profile: Optional[dict] = None) -> dict:
    """
    ???????????base + phase_overrides + regime_overrides????
    ????????????????detect_* ?????    """
    cfg = profile if profile is not None else _DYN_CFG
    if not isinstance(cfg, dict):
        cfg = {}

    ts_now = int(ctx.get("timestamp") or time.time())
    phase = str(ctx.get("market_phase") or resolve_market_phase(ts_now))
    phase_detail = str(ctx.get("market_phase_detail") or resolve_market_phase_detail(ts_now))
    session_policy = str(ctx.get("session_policy") or resolve_session_policy(ts_now))
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

    profile_sig, profile_meta = _apply_static_profile_overrides(base_sig, signal_type, ctx, cfg)
    session_sig, session_meta = _apply_session_policy_overrides(profile_sig, signal_type, ctx, cfg)
    calibrated_sig, cali_meta = _apply_runtime_override(session_sig, signal_type, ctx)
    limit_filter = apply_limit_magnet_filter(ctx, {"signal_type": signal_type}, cfg)

    out = copy.deepcopy(calibrated_sig)
    version_suffix = []
    if bool(profile_meta.get("applied")):
        version_suffix.append("profile")
    if bool(session_meta.get("applied")):
        version_suffix.append("session")
    if bool(cali_meta.get("applied")):
        version_suffix.append("calib")
    version_text = version if not version_suffix else f"{version}+{'+'.join(version_suffix)}"
    pre_blocked = bool(out.get("blocked"))
    pre_observer_only = bool(out.get("observer_only"))
    out.update({
        "signal_type": signal_type,
        "threshold_version": version_text,
        "market_phase": phase,
        "market_phase_detail": phase_detail,
        "session_policy": session_policy,
        "regime": regime,
        "industry": str(ctx.get("industry") or ""),
        "board_segment": str(ctx.get("board_segment") or ""),
        "security_type": str(ctx.get("security_type") or ""),
        "listing_stage": str(ctx.get("listing_stage") or ""),
        "price_limit_pct": _safe_float(ctx.get("price_limit_pct"), 0.0),
        "concept_boards": list(ctx.get("concept_boards") or []),
        "core_concept_board": str(ctx.get("core_concept_board") or ""),
        "concept_ecology": ctx.get("concept_ecology") if isinstance(ctx.get("concept_ecology"), dict) else {},
        "industry_ecology": ctx.get("industry_ecology") if isinstance(ctx.get("industry_ecology"), dict) else {},
        "risk_warning": bool(ctx.get("risk_warning")),
        "volume_drought": bool(ctx.get("volume_drought")),
        "board_seal_env": bool(ctx.get("board_seal_env")),
        "seal_side": str(ctx.get("seal_side") or ""),
        "instrument_profile": ctx.get("instrument_profile") if isinstance(ctx.get("instrument_profile"), dict) else {},
        "micro_environment": ctx.get("micro_environment") if isinstance(ctx.get("micro_environment"), dict) else {},
        "profile_override": profile_meta,
        "session_override": session_meta,
        "limit_filter": limit_filter,
        "blocked": bool(pre_blocked or limit_filter.get("blocked")),
        "observer_only": bool(pre_observer_only or limit_filter.get("observer_only")),
        "limit_magnet": bool(limit_filter.get("limit_magnet")),
        "calibration": cali_meta,
    })
    if bool(out.get("blocked")) and not out.get("blocked_reason"):
        if bool(session_meta.get("applied")):
            out["blocked_reason"] = str((session_meta.get("params") or {}).get("blocked_reason") or session_policy)
        else:
            out["blocked_reason"] = "threshold_blocked"
    if bool(out.get("observer_only")) and not out.get("observer_reason") and bool(session_meta.get("applied")):
        out["observer_reason"] = str((session_meta.get("params") or {}).get("observer_reason") or session_policy)
    return out



__all__ = [
    "build_signal_context",
    "infer_instrument_profile",
    "calc_theoretical_limits",
    "resolve_market_phase",
    "resolve_market_phase_detail",
    "resolve_session_policy",
    "resolve_regime",
    "apply_limit_magnet_filter",
    "get_thresholds",
]
