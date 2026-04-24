"""Realtime pool service: pool members, signal evaluation, market/tick access, indices."""

from __future__ import annotations

import warnings
import re
import unicodedata
from difflib import SequenceMatcher

from scripts.akshare_proxy_patch_free import install_patch
install_patch(proxy_api_scheme="socks5h", proxy_api_url="http://bapi.51daili.com/getapi2?linePoolIndex=0,1&packid=2&time=1&qty=1&port=2&format=txt&dt=4&ct=1&dtc=1&usertype=17&uid=42083&accessName=sword721&accessPassword=CE2BFE18E746F92ECDB5479063290EAE&skey=autoaddwhiteip",
    log_file="akshare_proxy_patch.log",
    log_console=False,
    hook_domains=[
    "fund.eastmoney.com",
    "push2.eastmoney.com",
    "push2his.eastmoney.com",
    "emweb.securities.eastmoney.com",
    ],)

from .common import *  # noqa: F401,F403
from .persistence_service import *  # noqa: F401,F403

_CONCEPT_FLOW_CACHE_TTL_SEC = 60.0
_concept_flow_cache_lock = threading.Lock()
_concept_flow_cache_at: float = 0.0
_concept_flow_cache_rows: list[dict] = []
_concept_flow_cache_error: str = ""
_CONCEPT_SNAPSHOT_CACHE_TTL_SEC = 60.0
_concept_snapshot_cache_lock = threading.Lock()
_concept_snapshot_cache_at: float = 0.0
_concept_snapshot_cache_map: dict[str, dict] = {}
_concept_snapshot_cache_error: str = ""
_INDUSTRY_SNAPSHOT_CACHE_TTL_SEC = 60.0
_industry_snapshot_cache_lock = threading.Lock()
_industry_snapshot_cache_at: float = 0.0
_industry_snapshot_cache_map: dict[str, dict] = {}
_industry_snapshot_cache_error: str = ""
_CONCEPT_SNAPSHOT_REDIS_KEY_LATEST = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:concept:snapshot:latest"
_CONCEPT_SNAPSHOT_REDIS_KEY_META = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:concept:snapshot:meta"
_AKSHARE_PROXY_PATCH_ENABLED = os.getenv("REALTIME_AKSHARE_PROXY_PATCH_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
_concept_flow_patch_ready = False
_MARKET_CHANGES_CACHE_TTL_SEC = 5.0
_market_changes_cache_lock = threading.Lock()
_market_changes_cache: dict[str, dict] = {}
_news_feed_cache_lock = threading.Lock()
_news_feed_cache: dict[str, dict] = {}

try:
    from pandas.errors import SettingWithCopyWarning as _SettingWithCopyWarning
except Exception:
    _SettingWithCopyWarning = Warning

_NEWS_FEED_SOURCES: dict[str, dict[str, object]] = {
    "cjzc_em": {
        "label": "财经早餐-东方财富",
        "func_name": "stock_info_cjzc_em",
        "ttl_sec": 300.0,
    },
    "global_em": {
        "label": "全球财经快讯-东方财富",
        "func_name": "stock_info_global_em",
        "ttl_sec": 30.0,
    },
    "global_sina": {
        "label": "全球财经快讯-新浪财经",
        "func_name": "stock_info_global_sina",
        "ttl_sec": 30.0,
    },
    "global_futu": {
        "label": "快讯-富途牛牛",
        "func_name": "stock_info_global_futu",
        "ttl_sec": 30.0,
    },
    "global_ths": {
        "label": "全球财经直播-同花顺财经",
        "func_name": "stock_info_global_ths",
        "ttl_sec": 30.0,
    },
    "global_cls": {
        "label": "电报-财联社",
        "func_name": "stock_info_global_cls",
        "ttl_sec": 30.0,
    },
}

_MARKET_CHANGE_SYMBOLS = [
    "火箭发射",
    "快速反弹",
    "大笔买入",
    "封涨停板",
    "打开跌停板",
    "有大买盘",
    "竞价上涨",
    "高开5日线",
    "向上缺口",
    "60日新高",
    "60日大幅上涨",
    "加速下跌",
    "高台跳水",
    "大笔卖出",
    "封跌停板",
    "打开涨停板",
    "有大卖盘",
    "竞价下跌",
    "低开5日线",
    "向下缺口",
    "60日新低",
    "60日大幅下跌",
]


def _safe_news_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _format_news_datetime(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime.time):
        return value.strftime("%H:%M:%S")
    return str(value).strip()


def _compose_news_datetime(date_value: object, time_value: object) -> str:
    date_text = _format_news_datetime(date_value)
    time_text = _format_news_datetime(time_value)
    if date_text and time_text:
        return f"{date_text} {time_text}".strip()
    return date_text or time_text

def _get_latest_factor_date() -> Optional[str]:
    with _data_ro_conn_ctx() as conn:
        try:
            row = conn.execute("SELECT MAX(trade_date) FROM stk_factor_pro").fetchone()
            if not row or not row[0]:
                return None
            val = row[0]
            if hasattr(val, "strftime"):
                return val.strftime("%Y%m%d")
            return str(val).replace("-", "")
        except Exception as e:
            logger.warning(f"latest factor date query failed: {e}")
            return None


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def _pick_existing_column(columns: set[str], *candidates: str) -> Optional[str]:
    for cand in candidates:
        if cand and cand.lower() in columns:
            return cand
    return None


def _normalize_code6(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return ""
    if len(digits) < 6:
        digits = digits.zfill(6)
    elif len(digits) > 6:
        digits = digits[-6:]
    return digits


def _normalize_concept_board_code(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("BK"):
        digits = "".join(ch for ch in s[2:] if ch.isdigit())
        if digits and len(digits) < 4:
            digits = digits.zfill(4)
            return f"BK{digits}"
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return ""
    if len(digits) < 4:
        digits = digits.zfill(4)
    return f"BK{digits}"


def _normalize_industry_board_code(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("BK"):
        digits = "".join(ch for ch in s[2:] if ch.isdigit())
        if digits:
            return f"BK{digits.zfill(4)}"
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return f"BK{digits.zfill(4)}"
    return s


_INDUSTRY_SNAPSHOT_ALIAS_MAP: dict[str, str] = {
    "IT设备": "计算机设备",
    "专用机械": "专用设备",
    "中成药": "中药",
    "乳制品": "乳品",
    "互联网": "互联网服务",
    "供气供热": "燃气",
    "全国地产": "房地产开发",
    "区域地产": "房地产开发",
    "房产服务": "房地产服务",
    "园区开发": "产业地产",
    "建筑工程": "房屋建设",
    "公路": "高速公路",
    "路桥": "基础建设",
    "水运": "航运港口",
    "空运": "航空运输",
    "服饰": "服装家纺",
    "纺织": "纺织制造",
    "玻璃": "玻璃玻纤",
    "红黄酒": "非白酒",
    "软件服务": "软件开发",
    "电信运营": "通信服务",
    "电器连锁": "家电行业",
    "石油加工": "炼化及贸易",
    "石油开采": "油气开采及服务",
    "石油贸易": "炼化及贸易",
    "环境保护": "环保行业",
    "生物制药": "生物制品",
    "超市连锁": "一般零售",
    "综合类": "综合",
    "船舶": "航海装备",
    "运输设备": "交运设备",
    "旅游服务": "旅游酒店",
    "旅游景点": "旅游及景区",
    "食品": "食品加工制造",
    "装修装饰": "装修建材",
}

_INDUSTRY_SNAPSHOT_ALIAS_MAP.update({
    "???": "??",
    "????": "??",
    "????": "????",
    "????": "????",
    "??": "????",
    "????": "????",
    "????": "????",
    "????": "????",
    "????": "????",
    "????": "???????",
    "???": "????",
    "????": "????",
    "????": "?????",
    "????": "???",
    "????": "????",
    "????": "??????",
    "???": "???",
    "????": "??????",
    "????": "?????",
    "????": "????",
    "????": "?????",
    "????": "??????",
    "????": "??????",
    "???": "??",
    "??": "????",
    "????": "?????",
})


def _normalize_industry_name(raw: object) -> str:
    s = unicodedata.normalize("NFKC", str(raw or "").strip())
    if not s:
        return ""
    s = s.replace(" ", "")
    s = re.sub(r"[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩIVX]+$", "", s, flags=re.IGNORECASE)
    if s.endswith("类") and len(s) > 1:
        s = s[:-1]
    for suffix in ("行业", "板块"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[:-len(suffix)]
    return s.strip()


def _iter_unique_industry_snapshots(snapshot_map: dict[str, dict]) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    seen: set[str] = set()
    for snapshot in snapshot_map.values():
        if not isinstance(snapshot, dict):
            continue
        name = str(snapshot.get("industry_name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append((name, dict(snapshot)))
    return out


def _match_industry_snapshot(
    snapshot_map: dict[str, dict],
    industry_name: object,
) -> tuple[dict, str, float]:
    raw_name = str(industry_name or "").strip()
    if not raw_name:
        return {}, "", 0.0

    direct = dict(snapshot_map.get(raw_name) or {})
    if direct:
        return direct, "exact", 1.0

    normalized = _normalize_industry_name(raw_name)
    if normalized and normalized != raw_name:
        direct_norm = dict(snapshot_map.get(normalized) or {})
        if direct_norm:
            return direct_norm, "normalized", 0.96

    alias_target = _INDUSTRY_SNAPSHOT_ALIAS_MAP.get(raw_name) or _INDUSTRY_SNAPSHOT_ALIAS_MAP.get(normalized)
    if alias_target:
        alias_snapshot = dict(snapshot_map.get(alias_target) or snapshot_map.get(_normalize_industry_name(alias_target)) or {})
        if alias_snapshot:
            return alias_snapshot, "alias", 0.98

    candidates = _iter_unique_industry_snapshots(snapshot_map)
    best_snapshot: dict = {}
    best_method = ""
    best_score = 0.0
    for candidate_name, candidate_snapshot in candidates:
        candidate_norm = _normalize_industry_name(candidate_name)
        if not candidate_norm:
            continue
        score = 0.0
        method = ""
        if normalized and (normalized in candidate_norm or candidate_norm in normalized):
            overlap = min(len(normalized), len(candidate_norm))
            score = 0.84 + min(0.12, overlap * 0.01)
            method = "contains"
        else:
            seq = SequenceMatcher(None, normalized or raw_name, candidate_norm).ratio()
            if seq >= 0.74:
                score = seq
                method = "fuzzy"
        if score > best_score:
            best_snapshot = dict(candidate_snapshot)
            best_method = method
            best_score = score

    if best_snapshot and best_score >= 0.82:
        return best_snapshot, best_method, round(float(best_score), 4)
    return {}, "", 0.0


def _register_concept_snapshot_alias(
    snapshot_map: dict[str, dict],
    *,
    concept_name: object = "",
    concept_code: object = "",
    snapshot: Optional[dict] = None,
) -> None:
    if not isinstance(snapshot, dict) or not snapshot:
        return
    name = str(concept_name or snapshot.get("concept_name") or "").strip()
    board_code = _normalize_concept_board_code(concept_code or snapshot.get("board_code"))
    payload = dict(snapshot)
    if name:
        payload["concept_name"] = name
    if board_code:
        payload["board_code"] = board_code
    if board_code:
        snapshot_map[board_code] = dict(payload)
    if name:
        snapshot_map[name] = dict(payload)


def _resolve_concept_snapshot(
    concept_snapshot_map: dict[str, dict],
    concept_name: object = "",
    concept_code: object = "",
) -> dict:
    board_code = _normalize_concept_board_code(concept_code)
    if board_code:
        snapshot = dict(concept_snapshot_map.get(board_code) or {})
        if snapshot:
            return snapshot
    name = str(concept_name or "").strip()
    if name:
        return dict(concept_snapshot_map.get(name) or {})
    return {}


def _register_industry_snapshot_alias(
    snapshot_map: dict[str, dict],
    *,
    industry_name: object = "",
    industry_code: object = "",
    snapshot: Optional[dict] = None,
) -> None:
    if not isinstance(snapshot, dict) or not snapshot:
        return
    name = str(industry_name or snapshot.get("industry_name") or "").strip()
    board_code = _normalize_industry_board_code(industry_code or snapshot.get("industry_code"))
    if not name and not board_code:
        return
    payload = dict(snapshot)
    payload["industry_name"] = name
    if board_code:
        payload["industry_code"] = board_code
        snapshot_map[board_code] = dict(payload)
    if name:
        snapshot_map[name] = dict(payload)
    normalized = _normalize_industry_name(name)
    if normalized:
        snapshot_map[normalized] = dict(payload)


def _attach_industry_snapshot_aliases(
    snapshot_map: dict[str, dict],
    industry_names: list[str] | set[str] | tuple[str, ...],
) -> dict[str, dict]:
    if not snapshot_map:
        return snapshot_map
    for raw_name in sorted({str(x or "").strip() for x in (industry_names or []) if str(x or "").strip()}):
        if raw_name in snapshot_map or _normalize_industry_name(raw_name) in snapshot_map:
            continue
        matched_snapshot, match_method, match_score = _match_industry_snapshot(snapshot_map, raw_name)
        if not matched_snapshot:
            continue
        alias_payload = dict(matched_snapshot)
        alias_payload["industry_query_name"] = raw_name
        alias_payload["industry_match_method"] = match_method
        alias_payload["industry_match_score"] = round(float(match_score), 4)
        snapshot_map[raw_name] = dict(alias_payload)
        normalized = _normalize_industry_name(raw_name)
        if normalized:
            snapshot_map[normalized] = dict(alias_payload)
    return snapshot_map


def _resolve_industry_snapshot(
    industry_snapshot_map: dict[str, dict],
    industry_name: object = "",
    industry_code: object = "",
) -> dict:
    board_code = _normalize_industry_board_code(industry_code)
    if board_code:
        snapshot = dict(industry_snapshot_map.get(board_code) or {})
        if snapshot:
            return snapshot
    name = str(industry_name or "").strip()
    if not name:
        return {}
    snapshot = dict(industry_snapshot_map.get(name) or {})
    if snapshot:
        return snapshot
    normalized = _normalize_industry_name(name)
    if normalized:
        snapshot = dict(industry_snapshot_map.get(normalized) or {})
        if snapshot:
            return snapshot
    matched_snapshot, match_method, match_score = _match_industry_snapshot(industry_snapshot_map, name)
    if matched_snapshot:
        matched_snapshot["industry_query_name"] = name
        matched_snapshot["industry_match_method"] = match_method
        matched_snapshot["industry_match_score"] = round(float(match_score), 4)
        return matched_snapshot
    return {}


def _build_concept_ecology_multi(
    concept_snapshot_map: dict[str, dict],
    concept_names: Optional[list[str]] = None,
    concept_codes: Optional[list[str]] = None,
) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    names = list(concept_names or [])
    codes = list(concept_codes or [])
    for idx, concept_name in enumerate(names):
        board_code = codes[idx] if idx < len(codes) else ""
        snapshot = _resolve_concept_snapshot(
            concept_snapshot_map,
            concept_name=concept_name,
            concept_code=board_code,
        )
        if not snapshot:
            continue
        item = dict(snapshot)
        name = str(concept_name or item.get("concept_name") or "").strip()
        code = _normalize_concept_board_code(board_code or item.get("board_code"))
        if name:
            item["concept_name"] = name
        if code:
            item["board_code"] = code
        key = f"{code}|{name}"
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _clone_concept_snapshot_map(snapshot_map: dict[str, dict], *, source_override: str = "") -> dict[str, dict]:
    out: dict[str, dict] = {}
    for concept_name, payload in (snapshot_map or {}).items():
        name = str(concept_name or "").strip()
        if not name:
            continue
        item = dict(payload or {})
        if source_override:
            if item.get("source"):
                item["source_origin"] = str(item.get("source") or "").strip()
            item["source"] = source_override
        out[name] = item
    return out


def _write_concept_snapshot_runtime_state(snapshot_map: dict[str, dict], fetched_at: float, error_text: str = "") -> bool:
    cli = _get_runtime_state_redis()
    if cli is None or not isinstance(snapshot_map, dict) or not snapshot_map:
        return False
    fetched_ts = int(fetched_at or time.time())
    payload = {
        "updated_at": fetched_ts,
        "updated_at_iso": datetime.datetime.fromtimestamp(fetched_ts).isoformat(timespec="seconds"),
        "count": int(len(snapshot_map)),
        "source": "akshare_eastmoney_concept_realtime",
        "error": str(error_text or ""),
        "snapshot_map": _clone_concept_snapshot_map(snapshot_map),
    }
    ttl = max(int(_runtime_state_ttl_sec()), 300)
    try:
        pipe = cli.pipeline(transaction=False)
        pipe.set(_CONCEPT_SNAPSHOT_REDIS_KEY_LATEST, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        pipe.hset(
            _CONCEPT_SNAPSHOT_REDIS_KEY_META,
            mapping={
                "updated_at": str(payload["updated_at"]),
                "updated_at_iso": str(payload["updated_at_iso"]),
                "count": str(payload["count"]),
                "source": str(payload["source"]),
                "error": str(payload["error"]),
            },
        )
        pipe.expire(_CONCEPT_SNAPSHOT_REDIS_KEY_LATEST, ttl)
        pipe.expire(_CONCEPT_SNAPSHOT_REDIS_KEY_META, ttl)
        pipe.execute()
        return True
    except Exception as e:
        logger.debug(f"写入概念生态 Redis 快照失败: {e}")
        return False


def _read_concept_snapshot_runtime_state(source_override: str = "redis_bootstrap") -> tuple[dict[str, dict], str, float]:
    cli = _get_runtime_state_redis()
    if cli is None:
        return {}, "", 0.0
    try:
        raw = cli.get(_CONCEPT_SNAPSHOT_REDIS_KEY_LATEST)
        if not raw:
            return {}, "", 0.0
        obj = json.loads(raw)
        snapshot_map = obj.get("snapshot_map") if isinstance(obj, dict) else {}
        if not isinstance(snapshot_map, dict) or not snapshot_map:
            return {}, "", 0.0
        fetched_at = float(obj.get("updated_at") or 0.0)
        error_text = str(obj.get("error") or "")
        return _clone_concept_snapshot_map(snapshot_map, source_override=source_override), error_text, fetched_at
    except Exception as e:
        logger.debug(f"读取概念生态 Redis 快照失败: {e}")
        return {}, "", 0.0


def _safe_float_local(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _compute_concept_ecology_snapshot(
    concept_name: str,
    pct_chg: Optional[float],
    up_count: Optional[int],
    down_count: Optional[int],
    leader_pct: Optional[float],
    turnover: Optional[float],
    fund_flow: Optional[float] = None,
    core_degree: Optional[float] = None,
) -> dict:
    pct = _safe_float_local(pct_chg, 0.0)
    up = int(up_count or 0)
    down = int(down_count or 0)
    leader = _safe_float_local(leader_pct, 0.0)
    turn = _safe_float_local(turnover, 0.0)
    fund = _safe_float_text(fund_flow, 0.0)
    core = _safe_float_local(core_degree, 0.0)
    total = up + down
    breadth = ((up - down) / total) if total > 0 else 0.0

    if pct >= 3.0:
        board_component = 30.0
    elif pct >= 1.2:
        board_component = 18.0
    elif pct <= -3.0:
        board_component = -30.0
    elif pct <= -1.2:
        board_component = -18.0
    else:
        board_component = max(-30.0, min(30.0, (pct / 3.0) * 30.0))

    if breadth >= 0.35:
        breadth_component = 20.0
    elif breadth >= 0.10:
        breadth_component = 12.0
    elif breadth <= -0.35:
        breadth_component = -20.0
    elif breadth <= -0.10:
        breadth_component = -12.0
    else:
        breadth_component = max(-20.0, min(20.0, breadth * 20.0))

    if leader >= 8.0:
        leader_component = 25.0
    elif leader >= 4.0:
        leader_component = 16.0
    elif leader >= 2.0:
        leader_component = 8.0
    elif leader <= -4.0:
        leader_component = -18.0
    elif leader <= -2.0:
        leader_component = -10.0
    else:
        leader_component = max(-25.0, min(25.0, (leader / 8.0) * 25.0))

    if fund > 0:
        fund_component = 15.0
    elif fund < 0:
        fund_component = -15.0
    else:
        fund_component = 0.0

    if abs(core) <= 1.0:
        core_component = max(-10.0, min(10.0, core * 10.0))
    else:
        core_component = max(-10.0, min(10.0, core))

    score_components = {
        "board_strength": round(board_component, 2),
        "leader_status": round(leader_component, 2),
        "strong_breadth": round(breadth_component, 2),
        "fund_flow": round(fund_component, 2),
        "core_degree": round(core_component, 2),
    }
    score = sum(score_components.values())

    if score >= 45.0:
        state = "expand"
    elif score >= 30.0:
        state = "strong"
    elif score <= -20.0:
        state = "retreat"
    elif score <= 5.0:
        state = "weak"
    else:
        state = "neutral"

    return {
        "concept_name": str(concept_name or ""),
        "score": round(score, 2),
        "structured_score": round(score, 2),
        "score_components": score_components,
        "score_weights": {
            "board_strength": 30,
            "leader_status": 25,
            "strong_breadth": 20,
            "fund_flow": 15,
            "core_degree": 10,
        },
        "state": state,
        "pct_chg": round(pct, 2),
        "up_count": up,
        "down_count": down,
        "breadth_ratio": round(breadth, 4),
        "leader_pct": round(leader, 2),
        "turnover": round(turn, 2),
        "main_net_inflow": round(fund, 2),
        "source": "eastmoney_concept",
    }


def _compute_industry_ecology_snapshot(
    industry_name: str,
    pct_chg: Optional[float],
    up_count: Optional[int],
    down_count: Optional[int],
    leader_pct: Optional[float],
    turnover: Optional[float],
) -> dict:
    snapshot = _compute_concept_ecology_snapshot(
        concept_name=industry_name,
        pct_chg=pct_chg,
        up_count=up_count,
        down_count=down_count,
        leader_pct=leader_pct,
        turnover=turnover,
    )
    snapshot["industry_name"] = str(industry_name or "")
    snapshot["source"] = "eastmoney_industry"
    return snapshot


def _fetch_realtime_concept_snapshot_map(force_refresh: bool = False) -> tuple[dict[str, dict], str, float]:
    global _concept_snapshot_cache_at, _concept_snapshot_cache_map, _concept_snapshot_cache_error
    now = time.time()
    with _concept_snapshot_cache_lock:
        if (
            not force_refresh
            and _concept_snapshot_cache_map
            and (now - _concept_snapshot_cache_at) < _CONCEPT_SNAPSHOT_CACHE_TTL_SEC
        ):
            return dict(_concept_snapshot_cache_map), str(_concept_snapshot_cache_error or ""), _concept_snapshot_cache_at

    redis_snapshot_map: dict[str, dict] = {}
    redis_error_text = ""
    redis_fetched_at = 0.0
    redis_snapshot_map, redis_error_text, redis_fetched_at = _read_concept_snapshot_runtime_state(
        source_override="redis_bootstrap"
    )
    if (
        not force_refresh
        and redis_snapshot_map
        and redis_fetched_at > 0
        and (now - redis_fetched_at) < _CONCEPT_SNAPSHOT_CACHE_TTL_SEC
    ):
        with _concept_snapshot_cache_lock:
            _concept_snapshot_cache_map = dict(redis_snapshot_map)
            _concept_snapshot_cache_error = str(redis_error_text or "")
            _concept_snapshot_cache_at = float(redis_fetched_at)
        return dict(redis_snapshot_map), str(redis_error_text or ""), float(redis_fetched_at)

    snapshot_map: dict[str, dict] = {}
    error_text = ""
    fetched_at = time.time()
    try:
        _ensure_akshare_runtime_patch()
        import akshare as ak

        df = _call_akshare_quiet(ak.stock_board_concept_name_em)
        fetched_at = time.time()
        if df is not None and not getattr(df, "empty", True):
            fund_flow_by_name: dict[str, dict] = {}
            try:
                flow_rows, _, _ = _fetch_concept_fund_flow_rows(force_refresh=False)
                fund_flow_by_name = {
                    str(row.get("concept_name") or "").strip(): dict(row)
                    for row in flow_rows
                    if str(row.get("concept_name") or "").strip()
                }
            except Exception:
                fund_flow_by_name = {}
            columns = [str(c) for c in list(df.columns)]
            name_col = _pick_df_column(columns, "板块名称", "概念名称", "名称")
            pct_col = _pick_df_column(columns, "涨跌幅")
            up_col = _pick_df_column(columns, "上涨家数", "上涨数")
            down_col = _pick_df_column(columns, "下跌家数", "下跌数")
            leader_col = _pick_df_column(columns, "领涨股票")
            leader_pct_col = _pick_df_column(columns, "领涨股票-涨跌幅", "领涨股-涨跌幅")
            turnover_col = _pick_df_column(columns, "换手率")
            latest_col = _pick_df_column(columns, "最新价")
            change_col = _pick_df_column(columns, "涨跌额")
            total_mv_col = _pick_df_column(columns, "总市值")
            code_col = _pick_df_column(columns, "板块代码")
            fund_flow_col = _pick_df_column(
                columns,
                "今日主力净流入-净额",
                "主力净流入-净额",
                "主力净流入净额",
                "主力净流入",
                "净额",
                "main_net_inflow",
            )
            if not name_col:
                raise RuntimeError(f"东方财富概念实时接口缺少名称字段: {columns}")

            for rec in df.to_dict(orient="records"):
                concept_name = str(rec.get(name_col) or "").strip()
                if not concept_name:
                    continue
                fund_flow_info = dict(fund_flow_by_name.get(concept_name) or {})
                snapshot = _compute_concept_ecology_snapshot(
                    concept_name=concept_name,
                    pct_chg=rec.get(pct_col) if pct_col else None,
                    up_count=rec.get(up_col) if up_col else None,
                    down_count=rec.get(down_col) if down_col else None,
                    leader_pct=rec.get(leader_pct_col) if leader_pct_col else fund_flow_info.get("leader_pct"),
                    turnover=rec.get(turnover_col) if turnover_col else None,
                    fund_flow=rec.get(fund_flow_col) if fund_flow_col else fund_flow_info.get("main_net_inflow"),
                )
                snapshot.update(
                    {
                        "source": "akshare_eastmoney_concept_realtime",
                        "board_code": str(rec.get(code_col) or "").strip() if code_col else "",
                        "leader_name": str(rec.get(leader_col) or "").strip() if leader_col else "",
                        "latest_price": round(_safe_float_text(rec.get(latest_col), 0.0), 4) if latest_col else 0.0,
                        "change_amount": round(_safe_float_text(rec.get(change_col), 0.0), 4) if change_col else 0.0,
                        "total_mv": round(_safe_float_text(rec.get(total_mv_col), 0.0), 2) if total_mv_col else 0.0,
                        "main_net_inflow_ratio": round(_safe_float_local(fund_flow_info.get("main_net_inflow_ratio"), 0.0), 3),
                        "updated_at": int(fetched_at),
                        "updated_at_iso": datetime.datetime.fromtimestamp(fetched_at).isoformat(timespec="seconds"),
                    }
                )
                snapshot_map[concept_name] = snapshot
    except Exception as e:
        error_text = f"获取东方财富概念实时快照失败: {e}"
        logger.warning(error_text)
        with _concept_snapshot_cache_lock:
            if _concept_snapshot_cache_map and not force_refresh:
                return dict(_concept_snapshot_cache_map), error_text, _concept_snapshot_cache_at
        if redis_snapshot_map:
            redis_fallback = _clone_concept_snapshot_map(redis_snapshot_map, source_override="redis_fallback")
            with _concept_snapshot_cache_lock:
                _concept_snapshot_cache_map = dict(redis_fallback)
                _concept_snapshot_cache_error = str(error_text or "")
                _concept_snapshot_cache_at = float(redis_fetched_at or fetched_at)
            return dict(redis_fallback), error_text, float(redis_fetched_at or fetched_at)

    if snapshot_map:
        with _concept_snapshot_cache_lock:
            _concept_snapshot_cache_map = dict(snapshot_map)
            _concept_snapshot_cache_error = error_text
            _concept_snapshot_cache_at = fetched_at
        _write_concept_snapshot_runtime_state(snapshot_map, fetched_at, error_text)
        return dict(snapshot_map), error_text, fetched_at

    if redis_snapshot_map:
        redis_fallback = _clone_concept_snapshot_map(redis_snapshot_map, source_override="redis_fallback")
        with _concept_snapshot_cache_lock:
            _concept_snapshot_cache_map = dict(redis_fallback)
            _concept_snapshot_cache_error = str(error_text or redis_error_text or "")
            _concept_snapshot_cache_at = float(redis_fetched_at or fetched_at)
        return dict(redis_fallback), str(error_text or redis_error_text or ""), float(redis_fetched_at or fetched_at)

    return {}, error_text, fetched_at


def _fetch_realtime_industry_snapshot_map(force_refresh: bool = False) -> tuple[dict[str, dict], str, float]:
    global _industry_snapshot_cache_at, _industry_snapshot_cache_map, _industry_snapshot_cache_error
    now = time.time()
    with _industry_snapshot_cache_lock:
        if (
            not force_refresh
            and _industry_snapshot_cache_map
            and (now - _industry_snapshot_cache_at) < _INDUSTRY_SNAPSHOT_CACHE_TTL_SEC
        ):
            return dict(_industry_snapshot_cache_map), str(_industry_snapshot_cache_error or ""), _industry_snapshot_cache_at

    snapshot_map: dict[str, dict] = {}
    error_text = ""
    fetched_at = time.time()
    try:
        _ensure_akshare_runtime_patch()
        import akshare as ak

        df = _call_akshare_quiet(ak.stock_board_industry_name_em)
        fetched_at = time.time()
        if df is not None and not getattr(df, "empty", True):
            columns = [str(c) for c in list(df.columns)]
            name_col = _pick_df_column(columns, "板块名称", "行业名称", "名称", "行业")
            code_col = _pick_df_column(columns, "????", "????", "??", "board_code", "code")
            pct_col = _pick_df_column(columns, "涨跌幅", "行业-涨跌幅")
            up_col = _pick_df_column(columns, "上涨家数", "上涨数")
            down_col = _pick_df_column(columns, "下跌家数", "下跌数")
            leader_col = _pick_df_column(columns, "领涨股票", "领涨股")
            leader_pct_col = _pick_df_column(columns, "领涨股票-涨跌幅", "领涨股-涨跌幅", "领涨股涨跌幅")
            turnover_col = _pick_df_column(columns, "换手率")
            latest_col = _pick_df_column(columns, "最新价")
            change_col = _pick_df_column(columns, "涨跌额")
            total_mv_col = _pick_df_column(columns, "总市值")
            if not name_col:
                raise RuntimeError(f"东方财富行业实时接口缺少名称字段: {columns}")

            for rec in df.to_dict(orient="records"):
                industry_name = str(rec.get(name_col) or "").strip()
                if not industry_name:
                    continue
                snapshot = _compute_industry_ecology_snapshot(
                    industry_name=industry_name,
                    pct_chg=rec.get(pct_col) if pct_col else None,
                    up_count=rec.get(up_col) if up_col else None,
                    down_count=rec.get(down_col) if down_col else None,
                    leader_pct=rec.get(leader_pct_col) if leader_pct_col else None,
                    turnover=rec.get(turnover_col) if turnover_col else None,
                )
                snapshot.update(
                    {
                        "source": "akshare_eastmoney_industry_realtime",
                        "industry_code": _normalize_industry_board_code(rec.get(code_col)) if code_col else "",
                        "leader_name": str(rec.get(leader_col) or "").strip() if leader_col else "",
                        "latest_price": round(_safe_float_text(rec.get(latest_col), 0.0), 4) if latest_col else 0.0,
                        "change_amount": round(_safe_float_text(rec.get(change_col), 0.0), 4) if change_col else 0.0,
                        "total_mv": round(_safe_float_text(rec.get(total_mv_col), 0.0), 2) if total_mv_col else 0.0,
                        "updated_at": int(fetched_at),
                        "updated_at_iso": datetime.datetime.fromtimestamp(fetched_at).isoformat(timespec="seconds"),
                    }
                )
                _register_industry_snapshot_alias(
                    snapshot_map,
                    industry_name=industry_name,
                    industry_code=snapshot.get("industry_code"),
                    snapshot=snapshot,
                )
    except Exception as e:
        error_text = f"获取东方财富行业实时快照失败: {e}"
        logger.warning(error_text)
        with _industry_snapshot_cache_lock:
            if _industry_snapshot_cache_map and not force_refresh:
                return dict(_industry_snapshot_cache_map), error_text, _industry_snapshot_cache_at

    if snapshot_map:
        with _industry_snapshot_cache_lock:
            _industry_snapshot_cache_map = dict(snapshot_map)
            _industry_snapshot_cache_error = error_text
            _industry_snapshot_cache_at = fetched_at
        return dict(snapshot_map), error_text, fetched_at

    return {}, error_text, fetched_at


def _load_concept_board_maps(
    data_conn: duckdb.DuckDBPyConnection,
    ts_codes: list[str],
) -> tuple[dict[str, list[str]], dict[str, str], dict[str, dict], dict[str, list[str]], dict[str, str]]:
    stock_concepts_map: dict[str, list[str]] = {}
    core_concept_map: dict[str, str] = {}
    concept_snapshot_map: dict[str, dict] = {}
    stock_concept_code_map: dict[str, list[str]] = {}
    core_concept_code_map: dict[str, str] = {}
    if not ts_codes:
        return stock_concepts_map, core_concept_map, concept_snapshot_map, stock_concept_code_map, core_concept_code_map

    code6_to_ts: dict[str, str] = {}
    query_code_values: set[str] = set()
    for ts_code in ts_codes:
        raw_ts_code = str(ts_code or "").strip().upper()
        code6 = _normalize_code6(raw_ts_code)
        if code6:
            code6_to_ts[code6] = str(ts_code)
            query_code_values.add(code6)
            query_code_values.add(raw_ts_code)
            try:
                query_code_values.add(str(int(code6)))
            except Exception:
                pass
            if "." in raw_ts_code:
                code_part, market_part = raw_ts_code.split(".", 1)
                market_part = market_part.strip().upper()
                if market_part:
                    query_code_values.add(f"{market_part}{code_part}")
    if not code6_to_ts:
        return stock_concepts_map, core_concept_map, concept_snapshot_map, stock_concept_code_map, core_concept_code_map

    stock_concept_pairs: dict[str, list[tuple[str, str]]] = {}

    try:
        cd_schema_rows = data_conn.execute("PRAGMA table_info('concept_detail')").fetchall()
        cd_columns = {str(r[1]).lower() for r in cd_schema_rows}
    except Exception:
        cd_columns = set()

    code_col = _pick_existing_column(
        cd_columns,
        "代码",
        "ts_code",
        "股票代码",
        "证券代码",
        "symbol",
        "code",
    )
    concept_col = _pick_existing_column(
        cd_columns,
        "concept_name",
        "概念名称",
        "板块名称",
    )
    board_code_col = _pick_existing_column(
        cd_columns,
        "板块代码",
        "概念代码",
        "concept_code",
        "board_code",
        "bk_code",
    )
    if code_col and concept_col:
        query_code_list = sorted({str(x or "").strip() for x in query_code_values if str(x or "").strip()})
        placeholders = ",".join(["?"] * len(query_code_list))
        board_code_expr = f", CAST({_quote_ident(board_code_col)} AS VARCHAR) AS board_code" if board_code_col else ", NULL AS board_code"
        concept_rows = data_conn.execute(
            f"""
            SELECT CAST({_quote_ident(code_col)} AS VARCHAR) AS raw_code,
                   {_quote_ident(concept_col)} AS concept_name
                   {board_code_expr}
            FROM concept_detail
            WHERE CAST({_quote_ident(code_col)} AS VARCHAR) IN ({placeholders})
            """,
            query_code_list,
        ).fetchall()
        for raw_code, concept_name, raw_board_code in concept_rows:
            ts_code = code6_to_ts.get(_normalize_code6(raw_code))
            name = str(concept_name or "").strip()
            board_code = _normalize_concept_board_code(raw_board_code)
            if not ts_code or not name:
                continue
            stock_concept_pairs.setdefault(ts_code, []).append((name, board_code))

    all_concepts = sorted({name for pairs in stock_concept_pairs.values() for name, _ in pairs if name})
    concept_codes_by_name: dict[str, set[str]] = {}
    for pairs in stock_concept_pairs.values():
        for name, board_code in pairs:
            if name and board_code:
                concept_codes_by_name.setdefault(name, set()).add(board_code)

    if all_concepts:
        realtime_snapshot_map, _, _ = _fetch_realtime_concept_snapshot_map(force_refresh=False)
        for name, snapshot in realtime_snapshot_map.items():
            concept_name = str(name or "").strip()
            if not concept_name:
                continue
            _register_concept_snapshot_alias(
                concept_snapshot_map,
                concept_name=concept_name,
                concept_code=(snapshot or {}).get("board_code"),
                snapshot=dict(snapshot or {}),
            )

        missing_concepts = [
            name
            for name in all_concepts
            if not _resolve_concept_snapshot(
                concept_snapshot_map,
                concept_name=name,
                concept_code=next(iter(concept_codes_by_name.get(name, set())), ""),
            )
        ]
        if missing_concepts:
            try:
                concept_schema_rows = data_conn.execute("PRAGMA table_info('concept')").fetchall()
                concept_columns = {str(r[1]).lower() for r in concept_schema_rows}
            except Exception:
                concept_columns = set()
            name_col = _pick_existing_column(concept_columns, "板块名称", "concept_name", "name")
            pct_col = _pick_existing_column(concept_columns, "涨跌幅", "pct_chg", "change_pct")
            up_col = _pick_existing_column(concept_columns, "上涨家数", "上涨数", "up_count")
            down_col = _pick_existing_column(concept_columns, "下跌家数", "下跌数", "down_count")
            leader_pct_col = _pick_existing_column(concept_columns, "领涨股票-涨跌幅", "领涨股票涨跌幅", "leader_pct_chg")
            turnover_col = _pick_existing_column(concept_columns, "换手率", "turnover")

            if name_col:
                placeholders = ",".join(["?"] * len(missing_concepts))
                pct_expr = f"{_quote_ident(pct_col)}" if pct_col else "NULL"
                up_expr = f"{_quote_ident(up_col)}" if up_col else "NULL"
                down_expr = f"{_quote_ident(down_col)}" if down_col else "NULL"
                leader_expr = f"{_quote_ident(leader_pct_col)}" if leader_pct_col else "NULL"
                turnover_expr = f"{_quote_ident(turnover_col)}" if turnover_col else "NULL"
                concept_meta_rows = data_conn.execute(
                    f"""
                    SELECT {_quote_ident(name_col)} AS concept_name,
                           {pct_expr} AS pct_chg,
                           {up_expr} AS up_count,
                           {down_expr} AS down_count,
                           {leader_expr} AS leader_pct,
                           {turnover_expr} AS turnover
                    FROM concept
                    WHERE {_quote_ident(name_col)} IN ({placeholders})
                    """,
                    missing_concepts,
                ).fetchall()
                for concept_name, pct_chg, up_count, down_count, leader_pct, turnover in concept_meta_rows:
                    name = str(concept_name or "").strip()
                    if not name:
                        continue
                    snapshot = _compute_concept_ecology_snapshot(
                        concept_name=name,
                        pct_chg=pct_chg,
                        up_count=up_count,
                        down_count=down_count,
                        leader_pct=leader_pct,
                        turnover=turnover,
                    )
                    codes = sorted(concept_codes_by_name.get(name, set()))
                    if codes:
                        for board_code in codes:
                            _register_concept_snapshot_alias(
                                concept_snapshot_map,
                                concept_name=name,
                                concept_code=board_code,
                                snapshot=snapshot,
                            )
                    else:
                        _register_concept_snapshot_alias(
                            concept_snapshot_map,
                            concept_name=name,
                            snapshot=snapshot,
                        )

    for ts_code, concept_pairs in stock_concept_pairs.items():
        name_to_code: dict[str, str] = {}
        for concept_name, board_code in concept_pairs:
            name = str(concept_name or "").strip()
            code = _normalize_concept_board_code(board_code)
            if not name:
                continue
            if name not in name_to_code or (not name_to_code[name] and code):
                name_to_code[name] = code
        sorted_names = sorted(name_to_code.keys())
        stock_concepts_map[ts_code] = sorted_names
        stock_concept_code_map[ts_code] = [name_to_code[name] for name in sorted_names if name_to_code[name]]
        if not sorted_names:
            core_concept_map[ts_code] = ""
            core_concept_code_map[ts_code] = ""
            continue
        ranked = sorted(
            sorted_names,
            key=lambda name: float(
                _resolve_concept_snapshot(
                    concept_snapshot_map,
                    concept_name=name,
                    concept_code=name_to_code.get(name, ""),
                ).get("score", -9999.0)
            ),
            reverse=True,
        )
        core_name = ranked[0] if ranked else sorted_names[0]
        core_snapshot = _resolve_concept_snapshot(
            concept_snapshot_map,
            concept_name=core_name,
            concept_code=name_to_code.get(core_name, ""),
        )
        core_concept_map[ts_code] = core_name
        core_concept_code_map[ts_code] = (
            _normalize_concept_board_code(name_to_code.get(core_name, ""))
            or _normalize_concept_board_code(core_snapshot.get("board_code"))
        )

    return stock_concepts_map, core_concept_map, concept_snapshot_map, stock_concept_code_map, core_concept_code_map


def _guess_ts_code_from_code6(code6: str) -> str:
    code6 = _normalize_code6(code6)
    if not code6:
        return ""
    if code6.startswith(("4", "8")):
        return f"{code6}.BJ"
    if code6.startswith(("5", "6", "9")):
        return f"{code6}.SH"
    return f"{code6}.SZ"


def _load_concept_member_counts(
    data_conn: duckdb.DuckDBPyConnection,
    concept_names: list[str],
) -> dict[str, int]:
    out: dict[str, int] = {}
    if not concept_names:
        return out
    try:
        cd_schema_rows = data_conn.execute("PRAGMA table_info('concept_detail')").fetchall()
        cd_columns = {str(r[1]).lower() for r in cd_schema_rows}
    except Exception:
        cd_columns = set()
    concept_col = _pick_existing_column(
        cd_columns,
        "concept_name",
        "概念名称",
        "板块名称",
    )
    if not concept_col:
        return out
    placeholders = ",".join(["?"] * len(concept_names))
    try:
        rows = data_conn.execute(
            f"""
            SELECT {_quote_ident(concept_col)} AS concept_name,
                   COUNT(*) AS member_count
            FROM concept_detail
            WHERE {_quote_ident(concept_col)} IN ({placeholders})
            GROUP BY 1
            """,
            concept_names,
        ).fetchall()
        for concept_name, member_count in rows:
            name = str(concept_name or "").strip()
            if name:
                out[name] = int(member_count or 0)
    except Exception:
        return out
    return out


def _load_em_industry_member_maps(
    data_conn: duckdb.DuckDBPyConnection,
    ts_codes: list[str],
) -> tuple[dict[str, str], dict[str, str]]:
    industry_name_map: dict[str, str] = {}
    industry_code_map: dict[str, str] = {}
    if not ts_codes:
        return industry_name_map, industry_code_map
    try:
        schema_rows = data_conn.execute("PRAGMA table_info('em_industry_member')").fetchall()
        columns = {str(r[1]).lower() for r in schema_rows}
    except Exception:
        return industry_name_map, industry_code_map

    ts_code_col = _pick_existing_column(columns, "ts_code")
    symbol_col = _pick_existing_column(columns, "symbol", "代码", "股票代码", "证券代码", "code")
    industry_name_col = _pick_existing_column(columns, "industry_name", "板块名称", "行业名称", "name")
    industry_code_col = _pick_existing_column(columns, "industry_code", "板块代码", "行业代码", "board_code", "bk_code")
    if not industry_name_col or (not ts_code_col and not symbol_col):
        return industry_name_map, industry_code_map

    params: list[str] = []
    where_sql = ""
    if ts_code_col:
        placeholders = ",".join(["?"] * len(ts_codes))
        where_sql = f"{_quote_ident(ts_code_col)} IN ({placeholders})"
        params = list(ts_codes)
    else:
        code6_to_ts = { _normalize_code6(ts_code): str(ts_code) for ts_code in ts_codes if _normalize_code6(ts_code) }
        if not code6_to_ts:
            return industry_name_map, industry_code_map
        query_values: set[str] = set()
        for code6 in code6_to_ts:
            query_values.add(code6)
            try:
                query_values.add(str(int(code6)))
            except Exception:
                pass
        placeholders = ",".join(["?"] * len(query_values))
        where_sql = f"CAST({_quote_ident(symbol_col)} AS VARCHAR) IN ({placeholders})"
        params = sorted(query_values)

    select_symbol_expr = (
        f"CAST({_quote_ident(symbol_col)} AS VARCHAR) AS raw_symbol"
        if symbol_col else
        "NULL AS raw_symbol"
    )
    select_ts_expr = (
        f"CAST({_quote_ident(ts_code_col)} AS VARCHAR) AS ts_code"
        if ts_code_col else
        "NULL AS ts_code"
    )
    select_code_expr = (
        f"CAST({_quote_ident(industry_code_col)} AS VARCHAR) AS industry_code"
        if industry_code_col else
        "NULL AS industry_code"
    )
    rows = data_conn.execute(
        f"""
        SELECT {select_ts_expr},
               {select_symbol_expr},
               {_quote_ident(industry_name_col)} AS industry_name,
               {select_code_expr}
        FROM em_industry_member
        WHERE {where_sql}
        """,
        params,
    ).fetchall()
    for raw_ts_code, raw_symbol, industry_name, industry_code in rows:
        ts_code = str(raw_ts_code or "").strip().upper()
        if not ts_code:
            ts_code = _guess_ts_code_from_code6(_normalize_code6(raw_symbol))
        if not ts_code:
            continue
        name = str(industry_name or "").strip()
        code = _normalize_industry_board_code(industry_code)
        if name:
            industry_name_map[ts_code] = name
        if code:
            industry_code_map[ts_code] = code
    return industry_name_map, industry_code_map


def _load_stock_basic_meta_by_symbol(
    data_conn: duckdb.DuckDBPyConnection,
    code6_list: list[str],
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not code6_list:
        return out
    placeholders = ",".join(["?"] * len(code6_list))
    try:
        rows = data_conn.execute(
            f"""
            SELECT ts_code, symbol, name, industry, market
            FROM stock_basic
            WHERE symbol IN ({placeholders})
            """,
            code6_list,
        ).fetchall()
    except Exception:
        return out
    for ts_code, symbol, name, industry, market in rows:
        code6 = _normalize_code6(symbol)
        if not code6:
            continue
        out[code6] = {
            "ts_code": str(ts_code or ""),
            "symbol": code6,
            "name": str(name or ""),
            "industry": str(industry or ""),
            "market": str(market or ""),
        }
    return out


def _load_stock_basic_ts_code_by_names(
    data_conn: duckdb.DuckDBPyConnection,
    names: list[str],
) -> dict[str, str]:
    out: dict[str, str] = {}
    normalized = sorted({str(name or "").strip() for name in names if str(name or "").strip()})
    if not normalized:
        return out
    placeholders = ",".join(["?"] * len(normalized))
    try:
        rows = data_conn.execute(
            f"""
            SELECT name, ts_code
            FROM stock_basic
            WHERE name IN ({placeholders})
            """,
            normalized,
        ).fetchall()
    except Exception:
        return out
    for name, ts_code in rows:
        key = str(name or "").strip()
        val = str(ts_code or "").strip()
        if key and val and key not in out:
            out[key] = val
    return out


def _load_latest_daily_basic_map(
    data_conn: duckdb.DuckDBPyConnection,
    ts_codes: list[str],
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not ts_codes:
        return out
    placeholders = ",".join(["?"] * len(ts_codes))
    try:
        rows = data_conn.execute(
            f"""
            SELECT ts_code, total_mv, circ_mv
            FROM (
                SELECT ts_code, total_mv, circ_mv,
                       ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM daily_basic
                WHERE ts_code IN ({placeholders})
            ) t
            WHERE rn = 1
            """,
            ts_codes,
        ).fetchall()
    except Exception:
        return out
    for ts_code, total_mv, circ_mv in rows:
        out[str(ts_code or "")] = {
            "total_mv": _safe_float_local(total_mv, 0.0),
            "circ_mv": _safe_float_local(circ_mv, 0.0),
        }
    return out


def _load_latest_daily_price_map(
    data_conn: duckdb.DuckDBPyConnection,
    ts_codes: list[str],
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not ts_codes:
        return out
    placeholders = ",".join(["?"] * len(ts_codes))
    try:
        rows = data_conn.execute(
            f"""
            SELECT ts_code, close, pre_close, pct_chg
            FROM (
                SELECT ts_code, close, pre_close, pct_chg,
                       ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM daily
                WHERE ts_code IN ({placeholders})
            ) t
            WHERE rn = 1
            """,
            ts_codes,
        ).fetchall()
    except Exception:
        return out
    for ts_code, close, pre_close, pct_chg in rows:
        out[str(ts_code or "")] = {
            "close": _safe_float_local(close, 0.0),
            "pre_close": _safe_float_local(pre_close, 0.0),
            "pct_chg": _safe_float_local(pct_chg, 0.0),
        }
    return out


def _pick_df_column(columns: list[str], *candidates: str) -> Optional[str]:
    col_map = {str(col).strip().lower(): str(col) for col in columns}
    for cand in candidates:
        key = str(cand or "").strip().lower()
        if key and key in col_map:
            return col_map[key]
    return None


def _safe_float_text(value: object, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return float(default)
    text = str(value).strip().replace(",", "")
    if not text or text in {"--", "None", "nan", "NaN"}:
        return float(default)
    mult = 1.0
    if text.endswith("%"):
        text = text[:-1]
    elif text.endswith("亿"):
        mult = 10000.0
        text = text[:-1]
    elif text.endswith("万"):
        mult = 1.0
        text = text[:-1]
    try:
        return float(text) * mult
    except Exception:
        return float(default)


def _call_akshare_quiet(func, *args, **kwargs):
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="A value is trying to be set on a copy of a slice from a DataFrame.*",
            category=_SettingWithCopyWarning,
            module=r"akshare\.stock\.stock_board_concept_em",
        )
        return func(*args, **kwargs)


def _ensure_akshare_runtime_patch() -> None:
    global _concept_flow_patch_ready
    if _concept_flow_patch_ready:
        return
    if not _AKSHARE_PROXY_PATCH_ENABLED:
        _concept_flow_patch_ready = True
        return
    try:
       
        from scripts.akshare_proxy_patch_free import install_patch
        install_patch(proxy_api_scheme="socks5h", proxy_api_url="http://bapi.51daili.com/getapi2?linePoolIndex=0,1&packid=2&time=1&qty=1&port=2&format=txt&dt=4&ct=1&dtc=1&usertype=17&uid=42083&accessName=sword721&accessPassword=CE2BFE18E746F92ECDB5479063290EAE&skey=autoaddwhiteip",
            log_file="akshare_proxy_patch.log",
            log_console=False,
            hook_domains=[
            "fund.eastmoney.com",
            "push2.eastmoney.com",
            "push2his.eastmoney.com",
            "emweb.securities.eastmoney.com",
            ],)
    except Exception:
        pass
    _concept_flow_patch_ready = True


def _fetch_concept_fund_flow_rows(force_refresh: bool = False) -> tuple[list[dict], str, float]:
    global _concept_flow_cache_at, _concept_flow_cache_rows, _concept_flow_cache_error
    now = time.time()
    with _concept_flow_cache_lock:
        if (
            not force_refresh
            and _concept_flow_cache_rows
            and (now - _concept_flow_cache_at) < _CONCEPT_FLOW_CACHE_TTL_SEC
        ):
            return list(_concept_flow_cache_rows), str(_concept_flow_cache_error or ""), _concept_flow_cache_at

    _ensure_akshare_runtime_patch()
    rows: list[dict] = []
    error_text = ""
    fetched_at = now
    try:
        import akshare as ak

        df = ak.stock_fund_flow_concept()
        if df is None or getattr(df, "empty", True):
            rows = []
        else:
            columns = [str(c) for c in list(df.columns)]
            concept_col = _pick_df_column(columns, "名称", "概念名称", "板块名称", "行业")
            pct_col = _pick_df_column(columns, "今日涨跌幅", "涨跌幅", "行业-涨跌幅", "行业涨跌幅")
            main_amt_col = _pick_df_column(columns, "今日主力净流入-净额", "主力净流入-净额", "主力净流入净额", "主力净流入", "净额")
            main_ratio_col = _pick_df_column(columns, "今日主力净流入-净占比", "主力净流入-净占比", "主力净流入净占比", "主力净流入占比")
            inflow_amt_col = _pick_df_column(columns, "流入资金", "今日流入资金")
            outflow_amt_col = _pick_df_column(columns, "流出资金", "今日流出资金")
            company_count_col = _pick_df_column(columns, "公司家数", "成分股数量", "个股数")
            leader_col = _pick_df_column(columns, "领涨股", "领涨股票", "领涨股名称")
            leader_pct_col = _pick_df_column(columns, "领涨股-涨跌幅", "领涨股票-涨跌幅", "领涨股涨跌幅")
            if not concept_col:
                raise RuntimeError(f"概念资金流向字段缺少名称列: {columns}")

            for idx, rec in enumerate(df.to_dict(orient="records")):
                concept_name = str(rec.get(concept_col) or "").strip()
                if not concept_name:
                    continue
                pct = _safe_float_text(rec.get(pct_col), 0.0) if pct_col else 0.0
                main_amt = _safe_float_text(rec.get(main_amt_col), 0.0) if main_amt_col else 0.0
                inflow_amt = _safe_float_text(rec.get(inflow_amt_col), 0.0) if inflow_amt_col else 0.0
                outflow_amt = _safe_float_text(rec.get(outflow_amt_col), 0.0) if outflow_amt_col else 0.0
                if main_ratio_col:
                    main_ratio = _safe_float_text(rec.get(main_ratio_col), 0.0)
                else:
                    denom = inflow_amt + outflow_amt
                    main_ratio = (main_amt / denom * 100.0) if denom > 0 else 0.0
                company_count = int(_safe_float_text(rec.get(company_count_col), 0.0)) if company_count_col else 0

                state = "neutral"
                if main_amt >= 10 and pct >= 1.5:
                    state = "expand"
                elif main_amt > 0 and main_ratio >= 5:
                    state = "strong"
                elif main_amt <= -10 and pct <= -1.5:
                    state = "retreat"
                elif main_amt < 0 or main_ratio < 0:
                    state = "weak"

                rows.append(
                    {
                        "rank": idx + 1,
                        "concept_name": concept_name,
                        "pct_chg": round(pct, 3),
                        "main_net_inflow": round(main_amt, 2),
                        "main_net_inflow_ratio": round(main_ratio, 3),
                        "inflow_amount": round(inflow_amt, 2),
                        "outflow_amount": round(outflow_amt, 2),
                        "company_count": company_count,
                        "super_net_inflow": None,
                        "big_net_inflow": None,
                        "mid_net_inflow": None,
                        "small_net_inflow": None,
                        "leader_name": str(rec.get(leader_col) or "").strip() if leader_col else "",
                        "leader_ts_code": "",
                        "leader_pct": round(_safe_float_text(rec.get(leader_pct_col), 0.0), 3) if leader_pct_col else 0.0,
                        "state": state,
                        "raw": {str(k): rec.get(k) for k in columns},
                    }
                )
            leader_name_map: dict[str, str] = {}
            leader_names = [str(row.get("leader_name") or "").strip() for row in rows]
            if leader_names:
                try:
                    with _data_ro_conn_ctx() as data_conn:
                        leader_name_map = _load_stock_basic_ts_code_by_names(data_conn, leader_names)
                except Exception:
                    leader_name_map = {}
            for row in rows:
                leader_name = str(row.get("leader_name") or "").strip()
                if leader_name and leader_name in leader_name_map:
                    row["leader_ts_code"] = str(leader_name_map.get(leader_name) or "")
            rows.sort(
                key=lambda x: (
                    _safe_float_local(x.get("main_net_inflow"), 0.0),
                    _safe_float_local(x.get("main_net_inflow_ratio"), 0.0),
                    _safe_float_local(x.get("pct_chg"), 0.0),
                ),
                reverse=True,
            )
            for idx, row in enumerate(rows, start=1):
                row["rank"] = idx
        fetched_at = time.time()
    except Exception as e:
        error_text = str(e)
        logger.warning(f"get concept fund flow failed: {e}")
        with _concept_flow_cache_lock:
            if _concept_flow_cache_rows and not force_refresh:
                return list(_concept_flow_cache_rows), error_text, _concept_flow_cache_at

    with _concept_flow_cache_lock:
        _concept_flow_cache_rows = list(rows)
        _concept_flow_cache_error = error_text
        _concept_flow_cache_at = fetched_at
    return list(rows), error_text, fetched_at


def list_concept_boards(
    q: str = Query("", description="概念名称搜索"),
    limit: int = Query(500, ge=1, le=2000),
):
    items: list[dict] = []
    realtime_snapshot_map, _, _ = _fetch_realtime_concept_snapshot_map(force_refresh=False)
    with _data_ro_conn_ctx() as data_conn:
        keyword = str(q or "").strip()
        concept_names = sorted(
            [
                str(name or "").strip()
                for name in realtime_snapshot_map.keys()
                if str(name or "").strip() and (not keyword or keyword in str(name or "").strip())
            ]
        )
        if not concept_names:
            try:
                concept_schema_rows = data_conn.execute("PRAGMA table_info('concept')").fetchall()
                concept_columns = {str(r[1]).lower() for r in concept_schema_rows}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"读取 concept 表结构失败: {e}")
            name_col = _pick_existing_column(concept_columns, "板块名称", "concept_name", "name")
            if not name_col:
                raise HTTPException(status_code=500, detail="concept 表缺少概念名称字段")
            params: list[object] = []
            where_sql = ""
            if keyword:
                where_sql = f"WHERE CAST({_quote_ident(name_col)} AS VARCHAR) LIKE ?"
                params.append(f"%{keyword}%")
            params.append(int(limit))
            db_rows = data_conn.execute(
                f"""
                SELECT {_quote_ident(name_col)} AS concept_name
                FROM concept
                {where_sql}
                ORDER BY {_quote_ident(name_col)}
                LIMIT ?
                """,
                params,
            ).fetchall()
            concept_names = [str(x[0] or "").strip() for x in db_rows if str(x[0] or "").strip()]

        concept_names = concept_names[: int(limit)]
        member_count_map = _load_concept_member_counts(data_conn, concept_names)
        for name in concept_names:
            snapshot = dict(realtime_snapshot_map.get(name) or {})
            if not snapshot:
                snapshot = {
                    "concept_name": name,
                    "score": 0.0,
                    "state": "neutral",
                    "pct_chg": 0.0,
                    "up_count": 0,
                    "down_count": 0,
                    "breadth_ratio": 0.0,
                    "leader_pct": 0.0,
                    "turnover": 0.0,
                    "source": "concept_table_fallback",
                }
            items.append(
                {
                    **snapshot,
                    "member_count": int(member_count_map.get(name, 0) or 0),
                }
            )

    items.sort(
        key=lambda x: (
            _safe_float_local(x.get("score"), 0.0),
            _safe_float_local(x.get("pct_chg"), 0.0),
            int(x.get("member_count", 0) or 0),
        ),
        reverse=True,
    )
    return {"count": len(items), "data": items}


def list_concept_fund_flow(
    q: str = Query("", description="概念名称搜索"),
    limit: int = Query(500, ge=1, le=2000),
    force_refresh: bool = Query(False, description="是否强制刷新"),
):
    rows, error_text, fetched_at = _fetch_concept_fund_flow_rows(force_refresh=bool(force_refresh))
    keyword = str(q or "").strip().lower()
    data = rows
    if keyword:
        data = [row for row in data if keyword in str(row.get("concept_name") or "").strip().lower()]
    data = list(data[: int(limit)])
    fetched_iso = datetime.datetime.fromtimestamp(float(fetched_at)).isoformat() if fetched_at else ""
    return {
        "count": len(data),
        "fetched_at": fetched_iso,
        "cache_ttl_sec": _CONCEPT_FLOW_CACHE_TTL_SEC,
        "stale": bool(error_text and bool(rows)),
        "error": error_text or None,
        "data": data,
    }


def _fetch_market_changes_rows(symbol: str, force_refresh: bool = False) -> tuple[list[dict], str, float]:
    symbol = str(symbol or "").strip() or "大笔买入"
    if symbol not in _MARKET_CHANGE_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"不支持的盘口异动类型: {symbol}")

    now = time.time()
    with _market_changes_cache_lock:
        cached = _market_changes_cache.get(symbol) or {}
        if (
            not force_refresh
            and cached.get("rows")
            and (now - float(cached.get("at") or 0.0)) < _MARKET_CHANGES_CACHE_TTL_SEC
        ):
            return (
                list(cached.get("rows") or []),
                str(cached.get("error") or ""),
                float(cached.get("at") or 0.0),
            )

    rows: list[dict] = []
    error_text = ""
    fetched_at = time.time()
    try:
        _ensure_akshare_runtime_patch()
        import akshare as ak

        df = ak.stock_changes_em(symbol=symbol)
        fetched_at = time.time()
        if df is None or getattr(df, "empty", False):
            rows = []
        else:
            columns = [str(c) for c in list(df.columns)]
            time_col = _pick_df_column(columns, "时间")
            code_col = _pick_df_column(columns, "代码", "股票代码", "证券代码")
            name_col = _pick_df_column(columns, "名称", "股票名称")
            board_col = _pick_df_column(columns, "板块", "所属板块")
            info_col = _pick_df_column(columns, "相关信息", "异动信息")
            if not time_col or not code_col or not name_col:
                raise RuntimeError(f"盘口异动字段缺失: {columns}")

            raw_items: list[dict] = []
            code6_list: list[str] = []
            for idx, row in enumerate(df.to_dict("records"), 1):
                code6 = _normalize_code6(row.get(code_col))
                ts_code = _guess_ts_code_from_code6(code6)
                item = {
                    "rank": idx,
                    "time": str(row.get(time_col) or "").strip(),
                    "code": code6 or str(row.get(code_col) or "").strip(),
                    "ts_code": ts_code,
                    "name": str(row.get(name_col) or "").strip(),
                    "board": str(row.get(board_col) or "").strip() if board_col else "",
                    "related_info": str(row.get(info_col) or "").strip() if info_col else "",
                    "price": None,
                    "pct_chg": None,
                    "total_mv": None,
                    "circ_mv": None,
                    "raw": row,
                }
                raw_items.append(item)
                if code6:
                    code6_list.append(code6)

            code6_list = sorted({x for x in code6_list if x})
            price_map: dict[str, dict] = {}
            mv_map: dict[str, dict] = {}
            if code6_list:
                with _data_ro_conn_ctx() as data_conn:
                    meta_by_code = _load_stock_basic_meta_by_symbol(data_conn, code6_list)
                    ts_codes = [str((meta_by_code.get(code6) or {}).get("ts_code") or "") for code6 in code6_list]
                    ts_codes = [x for x in ts_codes if x]
                    if ts_codes:
                        price_map = _load_latest_daily_price_map(data_conn, ts_codes)
                        mv_map = _load_latest_daily_basic_map(data_conn, ts_codes)
                    for item in raw_items:
                        meta = meta_by_code.get(str(item.get("code") or "")) or {}
                        ts_code = str(meta.get("ts_code") or item.get("ts_code") or "")
                        item["ts_code"] = ts_code
                        price_info = price_map.get(ts_code) or {}
                        mv_info = mv_map.get(ts_code) or {}
                        close = _safe_float_local(price_info.get("close"), 0.0)
                        pct = _safe_float_local(price_info.get("pct_chg"), 0.0)
                        item["price"] = round(close, 3) if close > 0 else None
                        item["pct_chg"] = round(pct, 3) if close > 0 else None
                        total_mv = _safe_float_local(mv_info.get("total_mv"), 0.0)
                        circ_mv = _safe_float_local(mv_info.get("circ_mv"), 0.0)
                        item["total_mv"] = round(total_mv, 2) if total_mv > 0 else None
                        item["circ_mv"] = round(circ_mv, 2) if circ_mv > 0 else None

            rows = raw_items
    except HTTPException:
        raise
    except Exception as e:
        error_text = f"获取盘口异动失败: {e}"
        logger.warning(error_text)
        with _market_changes_cache_lock:
            cached = _market_changes_cache.get(symbol) or {}
            if cached.get("rows") and not force_refresh:
                return (
                    list(cached.get("rows") or []),
                    error_text,
                    float(cached.get("at") or 0.0),
                )
        rows = []

    with _market_changes_cache_lock:
        _market_changes_cache[symbol] = {
            "rows": list(rows),
            "error": error_text,
            "at": fetched_at,
        }
    return rows, error_text, fetched_at


def list_market_changes(
    symbol: str = Query("大笔买入", description="盘口异动类型"),
    q: str = Query("", description="代码/名称/板块/相关信息搜索"),
    limit: int = Query(300, ge=1, le=3000),
    force_refresh: bool = Query(False, description="是否强制刷新"),
):
    rows, error_text, fetched_at = _fetch_market_changes_rows(symbol=str(symbol or "").strip(), force_refresh=bool(force_refresh))
    keyword = str(q or "").strip().lower()
    data = rows
    if keyword:
        data = [
            row
            for row in data
            if keyword in str(row.get("code") or "").lower()
            or keyword in str(row.get("name") or "").lower()
            or keyword in str(row.get("board") or "").lower()
            or keyword in str(row.get("related_info") or "").lower()
        ]
    data = list(data[: int(limit)])
    fetched_iso = datetime.datetime.fromtimestamp(float(fetched_at)).isoformat() if fetched_at else ""
    return {
        "symbol": str(symbol or "").strip() or "大笔买入",
        "symbols": list(_MARKET_CHANGE_SYMBOLS),
        "count": len(data),
        "fetched_at": fetched_iso,
        "cache_ttl_sec": _MARKET_CHANGES_CACHE_TTL_SEC,
        "stale": bool(error_text and bool(rows)),
        "error": error_text or None,
        "data": data,
    }


def _fetch_news_feed_rows(source: str, force_refresh: bool = False) -> tuple[list[dict], str, float]:
    source = str(source or "").strip().lower() or "global_em"
    if source not in _NEWS_FEED_SOURCES:
        raise HTTPException(status_code=400, detail=f"不支持的资讯源: {source}")

    cfg = _NEWS_FEED_SOURCES[source]
    ttl_sec = float(cfg.get("ttl_sec", 30.0) or 30.0)
    now = time.time()
    with _news_feed_cache_lock:
        cached = _news_feed_cache.get(source) or {}
        if (
            not force_refresh
            and cached.get("rows") is not None
            and (now - float(cached.get("at") or 0.0)) < ttl_sec
        ):
            return (
                list(cached.get("rows") or []),
                str(cached.get("error") or ""),
                float(cached.get("at") or 0.0),
            )

    rows: list[dict] = []
    error_text = ""
    fetched_at = time.time()
    try:
        _ensure_akshare_runtime_patch()
        import akshare as ak

        func_name = str(cfg.get("func_name") or "").strip()
        fn = getattr(ak, func_name, None)
        if fn is None:
            raise RuntimeError(f"AKShare 缺少接口: {func_name}")

        df = fn()
        fetched_at = time.time()
        if df is None or getattr(df, "empty", False):
            rows = []
        else:
            columns = [str(c) for c in list(df.columns)]
            title_col = _pick_df_column(columns, "标题")
            summary_col = _pick_df_column(columns, "摘要", "内容")
            link_col = _pick_df_column(columns, "链接", "link", "url")
            published_col = _pick_df_column(columns, "发布时间", "时间")
            publish_date_col = _pick_df_column(columns, "发布日期")
            publish_time_col = _pick_df_column(columns, "发布时间")

            for idx, rec in enumerate(df.to_dict(orient="records"), start=1):
                title = _safe_news_text(rec.get(title_col)) if title_col else ""
                summary = _safe_news_text(rec.get(summary_col)) if summary_col else ""
                link = _safe_news_text(rec.get(link_col)) if link_col else ""
                if publish_date_col or publish_time_col:
                    published_at = _compose_news_datetime(
                        rec.get(publish_date_col) if publish_date_col else None,
                        rec.get(publish_time_col) if publish_time_col else None,
                    )
                else:
                    published_at = _format_news_datetime(rec.get(published_col)) if published_col else ""

                if not title:
                    fallback = summary or _safe_news_text(rec.get("内容")) or _safe_news_text(rec.get("摘要"))
                    title = fallback[:64] + ("..." if len(fallback) > 64 else "")
                if not summary:
                    summary = _safe_news_text(rec.get("内容")) or _safe_news_text(rec.get("摘要"))

                rows.append(
                    {
                        "rank": idx,
                        "source": source,
                        "source_label": str(cfg.get("label") or source),
                        "title": title,
                        "summary": summary,
                        "published_at": published_at,
                        "link": link,
                        "raw": {str(k): rec.get(k) for k in columns},
                    }
                )

            rows.sort(
                key=lambda x: str(x.get("published_at") or ""),
                reverse=True,
            )
            for idx, row in enumerate(rows, start=1):
                row["rank"] = idx
    except HTTPException:
        raise
    except Exception as e:
        error_text = f"获取资讯失败: {e}"
        logger.warning(error_text)
        with _news_feed_cache_lock:
            cached = _news_feed_cache.get(source) or {}
            if cached.get("rows") is not None and not force_refresh:
                return (
                    list(cached.get("rows") or []),
                    error_text,
                    float(cached.get("at") or 0.0),
                )
        rows = []

    with _news_feed_cache_lock:
        _news_feed_cache[source] = {
            "rows": list(rows),
            "error": error_text,
            "at": fetched_at,
        }
    return rows, error_text, fetched_at


def list_news_feed(
    source: str = Query("global_em", description="资讯源"),
    q: str = Query("", description="标题/摘要搜索"),
    limit: int = Query(100, ge=1, le=500),
    force_refresh: bool = Query(False, description="是否强制刷新"),
):
    source = str(source or "").strip().lower() or "global_em"
    rows, error_text, fetched_at = _fetch_news_feed_rows(source=source, force_refresh=bool(force_refresh))
    keyword = str(q or "").strip().lower()
    data = rows
    if keyword:
        data = [
            row
            for row in data
            if keyword in str(row.get("title") or "").lower()
            or keyword in str(row.get("summary") or "").lower()
            or keyword in str(row.get("published_at") or "").lower()
        ]
    data = list(data[: int(limit)])
    fetched_iso = datetime.datetime.fromtimestamp(float(fetched_at)).isoformat() if fetched_at else ""
    cfg = _NEWS_FEED_SOURCES.get(source) or {}
    return {
        "source": source,
        "source_label": str(cfg.get("label") or source),
        "sources": [
            {"key": key, "label": str(item.get("label") or key), "ttl_sec": float(item.get("ttl_sec", 30.0) or 30.0)}
            for key, item in _NEWS_FEED_SOURCES.items()
        ],
        "count": len(data),
        "fetched_at": fetched_iso,
        "cache_ttl_sec": float(cfg.get("ttl_sec", 30.0) or 30.0),
        "stale": bool(error_text and bool(rows)),
        "error": error_text or None,
        "data": data,
    }


def get_concept_board_members(
    concept_name: str = Query(..., min_length=1, description="概念名称"),
    limit: int = Query(300, ge=1, le=1000),
):
    concept_name = str(concept_name or "").strip()
    if not concept_name:
        raise HTTPException(status_code=400, detail="concept_name 不能为空")

    realtime_snapshot_map, _, _ = _fetch_realtime_concept_snapshot_map(force_refresh=False)
    with _data_ro_conn_ctx() as data_conn:
        board_meta = dict(realtime_snapshot_map.get(concept_name) or {})
        if not board_meta:
            board_meta = {
                "concept_name": concept_name,
                "score": 0.0,
                "state": "neutral",
                "pct_chg": 0.0,
                "up_count": 0,
                "down_count": 0,
                "breadth_ratio": 0.0,
                "leader_pct": 0.0,
                "turnover": 0.0,
                "source": "concept_table_fallback",
                "member_count": 0,
            }

        try:
            cd_schema_rows = data_conn.execute("PRAGMA table_info('concept_detail')").fetchall()
            cd_columns = {str(r[1]).lower() for r in cd_schema_rows}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"读取 concept_detail 表结构失败: {e}")

        concept_col = _pick_existing_column(
            cd_columns,
            "concept_name",
            "概念名称",
            "板块名称",
        )
        code_col = _pick_existing_column(
            cd_columns,
            "代码",
            "ts_code",
            "股票代码",
            "证券代码",
            "symbol",
            "code",
        )
        stock_name_col = _pick_existing_column(cd_columns, "名称", "股票名称", "name")
        if not concept_col or not code_col:
            raise HTTPException(status_code=500, detail="concept_detail 表缺少概念名称或股票代码字段")

        name_expr = f", {_quote_ident(stock_name_col)} AS stock_name" if stock_name_col else ", NULL AS stock_name"
        detail_rows = data_conn.execute(
            f"""
            SELECT CAST({_quote_ident(code_col)} AS VARCHAR) AS raw_code
                   {name_expr}
            FROM concept_detail
            WHERE {_quote_ident(concept_col)} = ?
            LIMIT ?
            """,
            [concept_name, int(limit)],
        ).fetchall()

        code6_list: list[str] = []
        detail_items: list[dict] = []
        for raw_code, stock_name in detail_rows:
            code6 = _normalize_code6(raw_code)
            if not code6:
                continue
            code6_list.append(code6)
            detail_items.append(
                {
                    "code6": code6,
                    "raw_name": str(stock_name or "").strip(),
                }
            )

        unique_code6 = sorted(set(code6_list))
        basic_map = _load_stock_basic_meta_by_symbol(data_conn, unique_code6)
        ts_codes = [str((basic_map.get(code6) or {}).get("ts_code") or _guess_ts_code_from_code6(code6)) for code6 in unique_code6]
        daily_basic_map = _load_latest_daily_basic_map(data_conn, [c for c in ts_codes if c])
        daily_price_map = _load_latest_daily_price_map(data_conn, [c for c in ts_codes if c])

    provider = get_tick_provider()
    data: list[dict] = []
    for item in detail_items:
        code6 = item["code6"]
        basic = dict(basic_map.get(code6, {}) or {})
        ts_code = str(basic.get("ts_code") or _guess_ts_code_from_code6(code6))
        try:
            tick = provider.get_tick(ts_code) or {}
        except Exception:
            tick = {}
        fallback_daily = dict(daily_price_map.get(ts_code, {}) or {})
        price = _safe_float_local(tick.get("price"), 0.0)
        pct_chg = _safe_float_local(tick.get("pct_chg"), 0.0)
        if price <= 0:
            price = _safe_float_local(fallback_daily.get("close"), 0.0)
            pct_chg = _safe_float_local(fallback_daily.get("pct_chg"), 0.0)
        mv = dict(daily_basic_map.get(ts_code, {}) or {})
        profile = infer_instrument_profile(ts_code, name=str(basic.get("name") or item["raw_name"])) if infer_instrument_profile is not None else {}
        data.append(
            {
                "ts_code": ts_code,
                "symbol": code6,
                "name": str(basic.get("name") or item["raw_name"] or ts_code),
                "industry": str(basic.get("industry") or ""),
                "market": str(basic.get("market") or ""),
                "board_segment": str((profile or {}).get("board_segment") or ""),
                "price": round(price, 3) if price > 0 else 0.0,
                "pct_chg": round(pct_chg, 3),
                "total_mv": round(_safe_float_local(mv.get("total_mv"), 0.0), 2),
                "circ_mv": round(_safe_float_local(mv.get("circ_mv"), 0.0), 2),
                "price_source": str(tick.get("source") or ("daily_fallback" if fallback_daily else "")),
            }
        )

    board_meta["member_count"] = len(data)
    data.sort(
        key=lambda x: (
            _safe_float_local(x.get("total_mv"), 0.0),
            _safe_float_local(x.get("pct_chg"), 0.0),
            str(x.get("ts_code") or ""),
        ),
        reverse=True,
    )
    return {
        "concept_name": concept_name,
        "board": board_meta,
        "count": len(data),
        "data": data,
    }


def _refresh_daily_factors_cache(pool_id: Optional[int] = None) -> int:
    provider = get_tick_provider()
    try:
        with _ro_conn_ctx() as rt_conn:
            pool_rows = rt_conn.execute("SELECT ts_code, pool_id, industry FROM monitor_pools").fetchall()
        stock_pool_map: dict[str, set] = {}
        stock_industry_map: dict[str, str] = {}
        stock_industry_code_map: dict[str, str] = {}
        for ts_code, pid, industry in pool_rows:
            stock_pool_map.setdefault(ts_code, set()).add(int(pid))
            if ts_code and ts_code not in stock_industry_map:
                stock_industry_map[str(ts_code)] = str(industry or "")
        instrument_profile_map: dict[str, dict] = {}
        stock_concepts_map: dict[str, list[str]] = {}
        core_concept_map: dict[str, str] = {}
        concept_snapshot_map: dict[str, dict] = {}
        industry_snapshot_map: dict[str, dict] = {}
        stock_concept_code_map: dict[str, list[str]] = {}
        core_concept_code_map: dict[str, str] = {}
        ts_codes = list(stock_pool_map.keys()) if pool_id is None else [
            code for code, pools in stock_pool_map.items() if int(pool_id) in pools
        ]
        if ts_codes:
            try:
                with _data_ro_conn_ctx() as data_conn:
                    placeholders = ",".join(["?"] * len(ts_codes))
                    meta_rows = data_conn.execute(
                        f"""
                        SELECT ts_code, name, market, list_date
                        FROM stock_basic
                        WHERE ts_code IN ({placeholders})
                        """,
                        ts_codes,
                    ).fetchall()
                    (
                        stock_concepts_map,
                        core_concept_map,
                        concept_snapshot_map,
                        stock_concept_code_map,
                        core_concept_code_map,
                    ) = _load_concept_board_maps(data_conn, ts_codes)
                    em_industry_name_map, em_industry_code_map = _load_em_industry_member_maps(data_conn, ts_codes)
                    for code, industry_name in em_industry_name_map.items():
                        if industry_name:
                            stock_industry_map[str(code)] = str(industry_name)
                    for code, industry_code in em_industry_code_map.items():
                        if industry_code:
                            stock_industry_code_map[str(code)] = str(industry_code)
                    industry_snapshot_map, _, _ = _fetch_realtime_industry_snapshot_map(force_refresh=False)
                    _attach_industry_snapshot_aliases(
                        industry_snapshot_map,
                        list(stock_industry_map.values()),
                    )
                    for ts_code, name, market_name, list_date in meta_rows:
                        if infer_instrument_profile is None:
                            continue
                        profile = infer_instrument_profile(
                            str(ts_code or ""),
                            name=name,
                            market_name=market_name,
                            list_date=list_date,
                            ts_epoch=int(time.time()),
                        )
                        profile["name"] = str(name or "")
                        instrument_profile_map[str(ts_code)] = profile
            except Exception:
                instrument_profile_map = {}
                stock_concepts_map = {}
                core_concept_map = {}
                concept_snapshot_map = {}
                stock_concept_code_map = {}
                core_concept_code_map = {}
                industry_snapshot_map = {}
        try:
            provider.bulk_update_stock_pools(stock_pool_map)
        except Exception:
            pass
        try:
            provider.bulk_update_stock_industry({
                ts_code: {
                    "industry_name": stock_industry_map.get(ts_code, ""),
                    "industry_code": stock_industry_code_map.get(ts_code, ""),
                }
                for ts_code in ts_codes
            })
        except Exception:
            pass
        try:
            provider.bulk_update_stock_concepts({
                ts_code: {
                    "concept_boards": stock_concepts_map.get(ts_code, []),
                    "concept_codes": stock_concept_code_map.get(ts_code, []),
                    "core_concept_board": core_concept_map.get(ts_code, ""),
                    "core_concept_code": core_concept_code_map.get(ts_code, ""),
                }
                for ts_code in ts_codes
            })
        except Exception:
            pass
        try:
            provider.bulk_update_concept_snapshots(concept_snapshot_map)
        except Exception:
            pass
        try:
            provider.bulk_update_industry_snapshots(industry_snapshot_map)
        except Exception:
            pass
        try:
            provider.bulk_update_instrument_profiles(instrument_profile_map)
        except Exception:
            pass

        # 启动和全量刷新时，统一订阅 Pool1+Pool2 股票（去重后）。
        # 这样即使前端尚未连接 WS，也能提前建立行情订阅。
        if pool_id is None:
            try:
                target_codes = sorted(
                    {
                        str(code)
                        for code, pools in stock_pool_map.items()
                        if code and ({1, 2} & set(pools or set()))
                    }
                )
                if target_codes:
                    provider.subscribe_symbols(target_codes)
                    logger.info(f"[Layer2] startup merged subscription synced: {len(target_codes)}")
            except Exception as e:
                logger.warning(f"[Layer2] startup merged subscription failed: {e}")

        if not ts_codes:
            return 0

        with _data_ro_conn_ctx() as data_conn:
            # Compatible column fallback: vol_20/vol20/vol and atr_14/atr14/atr.
            try:
                schema_rows = data_conn.execute("PRAGMA table_info('stk_factor_pro')").fetchall()
                columns = {str(r[1]).lower() for r in schema_rows}
            except Exception:
                columns = set()

            vol_col = None
            for c in ("vol_20", "vol20", "vol"):
                if c in columns:
                    vol_col = c
                    break
            atr_col = None
            for c in ("atr_14", "atr14", "atr"):
                if c in columns:
                    atr_col = c
                    break
            ma20_col = "ma_qfq_20" if "ma_qfq_20" in columns else None

            vol_expr = f"f.{vol_col}" if vol_col else "NULL"
            atr_expr = f"f.{atr_col}" if atr_col else "NULL"
            ma20_expr = f"f.{ma20_col}" if ma20_col else "NULL"
            try:
                daily_schema_rows = data_conn.execute("PRAGMA table_info('daily')").fetchall()
                daily_columns = {str(r[1]).lower() for r in daily_schema_rows}
            except Exception:
                daily_columns = set()
            daily_vol_col = None
            for c in ("vol", "volume", "trade_vol", "total_vol"):
                if c in daily_columns:
                    daily_vol_col = c
                    break
            daily_vol_scale = 100.0 if daily_vol_col == "vol" else 1.0

            placeholders = ",".join(["?"] * len(ts_codes))
            rows = data_conn.execute(
                f"""
                WITH latest AS (
                    SELECT ts_code, MAX(trade_date) AS max_date
                    FROM stk_factor_pro
                    WHERE ts_code IN ({placeholders})
                    GROUP BY ts_code
                )
                SELECT f.ts_code,
                       f.boll_upper_qfq AS boll_upper,
                       f.boll_mid_qfq AS boll_mid,
                       f.boll_lower_qfq AS boll_lower,
                       f.rsi_qfq_6 AS rsi6,
                       f.ma_qfq_5 AS ma5,
                       f.ma_qfq_10 AS ma10,
                       {ma20_expr} AS ma20,
                       f.volume_ratio AS volume_ratio,
                       {vol_expr} AS vol_20,
                       {atr_expr} AS atr_14
                FROM stk_factor_pro f
                JOIN latest l ON f.ts_code = l.ts_code AND f.trade_date = l.max_date
                """,
                ts_codes,
            ).fetchall()
            prev_day_volume_map: dict[str, float] = {}
            vol5_median_volume_map: dict[str, float] = {}
            if daily_vol_col:
                daily_rows = data_conn.execute(
                    f"""
                    WITH ranked_daily AS (
                        SELECT ts_code,
                               {daily_vol_col} AS vol_raw,
                               ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                        FROM daily
                        WHERE ts_code IN ({placeholders})
                    ),
                    agg_daily AS (
                        SELECT ts_code,
                               MAX(CASE WHEN rn = 1 THEN vol_raw END) AS prev_day_volume,
                               median(vol_raw) AS vol5_median_volume
                        FROM ranked_daily
                        WHERE rn <= 5
                        GROUP BY ts_code
                    )
                    SELECT ts_code, prev_day_volume, vol5_median_volume
                    FROM agg_daily
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, prev_vol, med5_vol in daily_rows:
                    try:
                        base_vol = float(prev_vol) if prev_vol is not None else 0.0
                        prev_day_volume_map[str(ts_code)] = base_vol * daily_vol_scale
                    except Exception:
                        prev_day_volume_map[str(ts_code)] = 0.0
                    try:
                        base_med5 = float(med5_vol) if med5_vol is not None else 0.0
                        vol5_median_volume_map[str(ts_code)] = base_med5 * daily_vol_scale
                    except Exception:
                        vol5_median_volume_map[str(ts_code)] = 0.0

        factors_map: dict[str, dict] = {}
        for r in rows:
            factors_map[r[0]] = {
                "boll_upper": r[1] or 0,
                "boll_mid": r[2] or 0,
                "boll_lower": r[3] or 0,
                "rsi6": r[4],
                "ma5": r[5],
                "ma10": r[6],
                "ma20": r[7],
                "volume_ratio": r[8],
                "vol_20": r[9],
                "atr_14": r[10],
                "prev_day_volume": prev_day_volume_map.get(str(r[0]), 0.0),
                "vol5_median_volume": vol5_median_volume_map.get(str(r[0]), 0.0),
            }
        try:
            provider.bulk_update_daily_factors(factors_map)
        except Exception:
            pass

        # Pool1 筹码特征链路：cyq_perf -> provider.bulk_update_chip_features
        chip_map: dict[str, dict] = {}
        try:
            with _data_ro_conn_ctx() as data_conn:
                try:
                    cyq_schema = data_conn.execute("PRAGMA table_info('cyq_perf')").fetchall()
                    cyq_cols = {str(r[1]).lower() for r in cyq_schema}
                except Exception:
                    cyq_cols = set()

                cost5_col = next((c for c in ("cost_5pct", "cost5") if c in cyq_cols), None)
                cost95_col = next((c for c in ("cost_95pct", "cost95") if c in cyq_cols), None)
                winner_col = next((c for c in ("winner_rate", "win_rate") if c in cyq_cols), None)

                if cost5_col and cost95_col and winner_col:
                    placeholders = ",".join(["?"] * len(ts_codes))
                    cyq_rows = data_conn.execute(
                        f"""
                        WITH latest AS (
                            SELECT ts_code, MAX(trade_date) AS max_date
                            FROM cyq_perf
                            WHERE ts_code IN ({placeholders})
                            GROUP BY ts_code
                        )
                        SELECT c.ts_code,
                               c.{cost5_col} AS cost_5pct,
                               c.{cost95_col} AS cost_95pct,
                               c.{winner_col} AS winner_rate
                        FROM cyq_perf c
                        JOIN latest l ON c.ts_code = l.ts_code AND c.trade_date = l.max_date
                        """,
                        ts_codes,
                    ).fetchall()
                    for ts_code, c5, c95, winner in cyq_rows:
                        try:
                            c5f = float(c5) if c5 is not None else 0.0
                            c95f = float(c95) if c95 is not None else 0.0
                            wf = float(winner) if winner is not None else 0.0
                            mid = (c95f + c5f) / 2.0
                            conc_pct = (abs(c95f - c5f) / abs(mid) * 100.0) if abs(mid) > 1e-9 else 0.0
                            chip_map[str(ts_code)] = {
                                "cost_5pct": c5f,
                                "cost_95pct": c95f,
                                "winner_rate": wf,
                                "chip_concentration_pct": conc_pct,
                            }
                        except Exception:
                            continue
        except Exception:
            chip_map = {}

        if chip_map:
            try:
                provider.bulk_update_chip_features(chip_map)
            except Exception:
                pass
        return len(factors_map)
    except Exception as e:
        logger.warning(f"refresh_daily_factors_cache failed: {e}")
        return 0
def _build_member_data(conn: duckdb.DuckDBPyConnection, pool_id: int) -> list[dict]:
    members = conn.execute(
        "SELECT ts_code, name, industry FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
        [pool_id],
    ).fetchall()
    if not members:
        return []
    ts_codes = [str(m[0]) for m in members]
    daily_map: dict[str, dict] = {}
    chip_map: dict[str, dict] = {}
    prev_day_volume_map: dict[str, float] = {}
    vol5_median_volume_map: dict[str, float] = {}
    instrument_profile_map: dict[str, dict] = {}
    stock_industry_name_map: dict[str, str] = {}
    stock_industry_code_map: dict[str, str] = {}
    stock_concepts_map: dict[str, list[str]] = {}
    core_concept_map: dict[str, str] = {}
    concept_snapshot_map: dict[str, dict] = {}
    industry_snapshot_map: dict[str, dict] = {}
    try:
        placeholders = ",".join(["?"] * len(ts_codes))
        with _data_ro_conn_ctx() as dconn:
            (
                stock_concepts_map,
                core_concept_map,
                concept_snapshot_map,
                stock_concept_code_map,
                core_concept_code_map,
            ) = _load_concept_board_maps(dconn, ts_codes)
            stock_industry_name_map, stock_industry_code_map = _load_em_industry_member_maps(dconn, ts_codes)
            industry_snapshot_map, _, _ = _fetch_realtime_industry_snapshot_map(force_refresh=False)
            _attach_industry_snapshot_aliases(
                industry_snapshot_map,
                {
                    str(stock_industry_name_map.get(str(m[0]), "") or str(m[2] or "")).strip()
                    for m in members
                },
            )
            try:
                schema_rows = dconn.execute("PRAGMA table_info('stk_factor_pro')").fetchall()
                columns = {str(r[1]).lower() for r in schema_rows}
            except Exception:
                columns = set()
            ma20_expr = "f.ma_qfq_20" if "ma_qfq_20" in columns else "NULL"
            rows = dconn.execute(
                f"""
                WITH latest AS (
                    SELECT ts_code, MAX(trade_date) AS max_date
                    FROM stk_factor_pro
                    WHERE ts_code IN ({placeholders})
                    GROUP BY ts_code
                )
                SELECT f.ts_code,
                       f.boll_upper_qfq AS boll_upper,
                       f.boll_mid_qfq AS boll_mid,
                       f.boll_lower_qfq AS boll_lower,
                       f.rsi_qfq_6 AS rsi6,
                       f.ma_qfq_5 AS ma5,
                       f.ma_qfq_10 AS ma10,
                       {ma20_expr} AS ma20,
                       f.volume_ratio AS volume_ratio
                FROM stk_factor_pro f
                JOIN latest l ON f.ts_code = l.ts_code AND f.trade_date = l.max_date
                """,
                ts_codes,
                ).fetchall()
            try:
                daily_schema_rows = dconn.execute("PRAGMA table_info('daily')").fetchall()
                daily_cols = {str(r[1]).lower() for r in daily_schema_rows}
            except Exception:
                daily_cols = set()
            daily_vol_col = next((c for c in ("vol", "volume", "trade_vol", "total_vol") if c in daily_cols), None)
            daily_vol_scale = 100.0 if daily_vol_col == "vol" else 1.0
            if daily_vol_col:
                vol_rows = dconn.execute(
                    f"""
                    WITH ranked_daily AS (
                        SELECT ts_code,
                               {daily_vol_col} AS vol_raw,
                               ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                        FROM daily
                        WHERE ts_code IN ({placeholders})
                    ),
                    agg_daily AS (
                        SELECT ts_code,
                               MAX(CASE WHEN rn = 1 THEN vol_raw END) AS prev_day_volume,
                               median(vol_raw) AS vol5_median_volume
                        FROM ranked_daily
                        WHERE rn <= 5
                        GROUP BY ts_code
                    )
                    SELECT ts_code, prev_day_volume, vol5_median_volume
                    FROM agg_daily
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, prev_vol, med5_vol in vol_rows:
                    try:
                        base_vol = float(prev_vol) if prev_vol is not None else 0.0
                        prev_day_volume_map[str(ts_code)] = base_vol * daily_vol_scale
                    except Exception:
                        prev_day_volume_map[str(ts_code)] = 0.0
                    try:
                        base_med5 = float(med5_vol) if med5_vol is not None else 0.0
                        vol5_median_volume_map[str(ts_code)] = base_med5 * daily_vol_scale
                    except Exception:
                        vol5_median_volume_map[str(ts_code)] = 0.0
            # Optional: cyq_perf chip features for Pool1 consistency.
            try:
                cyq_schema = dconn.execute("PRAGMA table_info('cyq_perf')").fetchall()
                cyq_cols = {str(r[1]).lower() for r in cyq_schema}
            except Exception:
                cyq_cols = set()
            cost5_col = next((c for c in ("cost_5pct", "cost5") if c in cyq_cols), None)
            cost95_col = next((c for c in ("cost_95pct", "cost95") if c in cyq_cols), None)
            winner_col = next((c for c in ("winner_rate", "win_rate") if c in cyq_cols), None)
            if cost5_col and cost95_col and winner_col:
                cyq_rows = dconn.execute(
                    f"""
                    WITH latest AS (
                        SELECT ts_code, MAX(trade_date) AS max_date
                        FROM cyq_perf
                        WHERE ts_code IN ({placeholders})
                        GROUP BY ts_code
                    )
                    SELECT c.ts_code,
                           c.{cost5_col} AS cost_5pct,
                           c.{cost95_col} AS cost_95pct,
                           c.{winner_col} AS winner_rate
                    FROM cyq_perf c
                    JOIN latest l ON c.ts_code = l.ts_code AND c.trade_date = l.max_date
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, c5, c95, winner in cyq_rows:
                    try:
                        c5f = float(c5) if c5 is not None else 0.0
                        c95f = float(c95) if c95 is not None else 0.0
                        wf = float(winner) if winner is not None else 0.0
                        mid = (c95f + c5f) / 2.0
                        conc_pct = (abs(c95f - c5f) / abs(mid) * 100.0) if abs(mid) > 1e-9 else 0.0
                        chip_map[str(ts_code)] = {
                            "winner_rate": wf,
                            "chip_concentration_pct": conc_pct,
                        }
                    except Exception:
                        continue
            try:
                meta_rows = dconn.execute(
                    f"""
                    SELECT ts_code, name, market, list_date
                    FROM stock_basic
                    WHERE ts_code IN ({placeholders})
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, sb_name, market_name, list_date in meta_rows:
                    if infer_instrument_profile is None:
                        continue
                    profile = infer_instrument_profile(
                        str(ts_code or ""),
                        name=sb_name,
                        market_name=market_name,
                        list_date=list_date,
                        ts_epoch=int(time.time()),
                    )
                    profile["name"] = str(sb_name or "")
                    instrument_profile_map[str(ts_code)] = profile
            except Exception:
                instrument_profile_map = {}
        for r in rows:
            daily_map[str(r[0])] = {
                "boll_upper": r[1],
                "boll_mid": r[2],
                "boll_lower": r[3],
                "rsi6": r[4],
                "ma5": r[5],
                "ma10": r[6],
                "ma20": r[7],
                "volume_ratio": r[8],
                "prev_day_volume": prev_day_volume_map.get(str(r[0]), 0.0),
                "vol5_median_volume": vol5_median_volume_map.get(str(r[0]), 0.0),
            }
    except Exception:
        pass
    provider = get_tick_provider()
    out = []
    for ts_code, name, industry in members:
        tick = provider.get_tick(ts_code)
        daily = dict(daily_map.get(str(ts_code), {}))
        instrument_profile = dict(instrument_profile_map.get(str(ts_code), {}) or {})
        runtime_industry = str(stock_industry_name_map.get(str(ts_code), "") or industry or "")
        runtime_industry_code = str(stock_industry_code_map.get(str(ts_code), "") or "")

        m = {
            "ts_code": ts_code,
            "name": name,
            "industry": runtime_industry,
            "industry_code": runtime_industry_code,
            "instrument_profile": instrument_profile,
            "concept_boards": list(stock_concepts_map.get(str(ts_code), []) or []),
            "concept_codes": list(stock_concept_code_map.get(str(ts_code), []) or []),
            "core_concept_board": str(core_concept_map.get(str(ts_code), "") or ""),
            "core_concept_code": str(core_concept_code_map.get(str(ts_code), "") or ""),
            "concept_ecology": _resolve_concept_snapshot(
                concept_snapshot_map,
                concept_name=core_concept_map.get(str(ts_code), ""),
                concept_code=core_concept_code_map.get(str(ts_code), ""),
            ),
            "concept_ecology_multi": _build_concept_ecology_multi(
                concept_snapshot_map,
                concept_names=list(stock_concepts_map.get(str(ts_code), []) or []),
                concept_codes=list(stock_concept_code_map.get(str(ts_code), []) or []),
            ),
            "industry_ecology": _resolve_industry_snapshot(
                industry_snapshot_map,
                industry_name=runtime_industry,
                industry_code=runtime_industry_code,
            ),
            "market_name": instrument_profile.get("market_name"),
            "list_date": instrument_profile.get("list_date"),
            "board_segment": instrument_profile.get("board_segment"),
            "security_type": instrument_profile.get("security_type"),
            "risk_warning": bool(instrument_profile.get("risk_warning")),
            "listing_stage": instrument_profile.get("listing_stage"),
            "listing_days": instrument_profile.get("listing_days"),
            "price_limit_pct": instrument_profile.get("price_limit_pct"),
            "price": tick.get("price", 0),
            "pct_chg": tick.get("pct_chg", 0),
            **{k: (v if v is not None else 0) for k, v in daily.items()},
        }
        pace = _calc_intraday_volume_pace_member(
            tick=tick,
            prev_day_volume=m.get("prev_day_volume"),
            vol5_median_volume=m.get("vol5_median_volume"),
            pool_id=pool_id,
        )
        m["volume_pace_ratio"] = pace.get("ratio")
        m["volume_pace_ratio_prev"] = pace.get("ratio_prev")
        m["volume_pace_ratio_med5"] = pace.get("ratio_med5")
        m["volume_pace_state"] = pace.get("state")
        m["volume_pace_progress"] = pace.get("progress_ratio")
        m["volume_pace_baseline_volume"] = pace.get("baseline_volume")
        m["volume_pace_baseline_mode"] = pace.get("baseline_mode")
        m["cum_volume"] = pace.get("cum_volume")

        if pool_id == 1:
            bids = tick.get("bids") or []
            asks = tick.get("asks") or []
            bid_vol = sum(v for _, v in bids[:5]) if bids else 0
            ask_vol = sum(v for _, v in asks[:5]) if asks else 0
            pre_close = float(tick.get("pre_close", 0) or 0)
            m["bid_ask_ratio"] = round(bid_vol / ask_vol, 3) if ask_vol > 0 else None
            if pre_close > 0 and calc_theoretical_limits is not None:
                m["up_limit"], m["down_limit"] = calc_theoretical_limits(pre_close, instrument_profile.get("price_limit_pct"))
            else:
                m["up_limit"] = pre_close * 1.1 if pre_close > 0 else None
                m["down_limit"] = pre_close * 0.9 if pre_close > 0 else None
            prev_price = None
            try:
                prev_price = provider.get_prev_price(ts_code)
            except Exception:
                prev_price = None
            if prev_price is not None and prev_price > 0:
                m["prev_price"] = float(prev_price)
            else:
                m["prev_price"] = None
            try:
                pos = provider.get_pool1_position_state(ts_code)
            except Exception:
                pos = None
            if isinstance(pos, dict):
                status = str(pos.get("status") or "observe")
                m["pool1_position_status"] = status
                m["pool1_position_ratio"] = round(float(pos.get("position_ratio", 0.0) or 0.0), 4)
                m["pool1_holding_days"] = float(pos.get("holding_days", 0.0) or 0.0)
                m["pool1_in_holding"] = bool(status == "holding")
                m["pool1_last_buy_at"] = int(pos.get("last_buy_at", 0) or 0)
                m["pool1_last_buy_price"] = round(float(pos.get("last_buy_price", 0.0) or 0.0), 4)
                m["pool1_last_buy_type"] = str(pos.get("last_buy_type") or "")
                m["pool1_last_sell_at"] = int(pos.get("last_sell_at", 0) or 0)
                m["pool1_last_sell_type"] = str(pos.get("last_sell_type") or "")
                m["pool1_last_reduce_at"] = int(pos.get("last_reduce_at", 0) or 0)
                m["pool1_last_reduce_type"] = str(pos.get("last_reduce_type") or "")
                m["pool1_last_reduce_ratio"] = round(float(pos.get("last_reduce_ratio", 0.0) or 0.0), 4)
                m["pool1_last_reduce_source"] = str(pos.get("last_reduce_source") or "")
                m["pool1_last_reduce_avwap_tier"] = str(pos.get("last_reduce_avwap_tier") or "")
                m["pool1_reduce_streak"] = int(pos.get("reduce_streak", 0) or 0)
                m["pool1_reduce_ratio_cum"] = round(float(pos.get("reduce_ratio_cum", 0.0) or 0.0), 4)
                m["pool1_last_rebuild_at"] = int(pos.get("last_rebuild_at", 0) or 0)
                m["pool1_last_rebuild_type"] = str(pos.get("last_rebuild_type") or "")
                m["pool1_last_rebuild_add_ratio_base"] = round(float(pos.get("last_rebuild_add_ratio_base", 0.0) or 0.0), 4)
                m["pool1_last_rebuild_add_ratio_bias"] = round(float(pos.get("last_rebuild_add_ratio_bias", 0.0) or 0.0), 4)
                m["pool1_last_rebuild_from_partial_count"] = int(pos.get("last_rebuild_from_partial_count", 0) or 0)
                m["pool1_last_rebuild_from_partial_ratio"] = round(float(pos.get("last_rebuild_from_partial_ratio", 0.0) or 0.0), 4)
                m["pool1_last_rebuild_from_partial_source"] = str(pos.get("last_rebuild_from_partial_source") or "")
                m["pool1_last_rebuild_from_partial_avwap_tier"] = str(pos.get("last_rebuild_from_partial_avwap_tier") or "")
                m["pool1_last_exit_after_partial"] = bool(pos.get("last_exit_after_partial", False))
                m["pool1_last_exit_partial_count"] = int(pos.get("last_exit_partial_count", 0) or 0)
                m["pool1_last_exit_reduce_ratio_cum"] = round(float(pos.get("last_exit_reduce_ratio_cum", 0.0) or 0.0), 4)
                m["pool1_last_exit_reduce_source"] = str(pos.get("last_exit_reduce_source") or "")
                m["pool1_last_exit_reduce_avwap_tier"] = str(pos.get("last_exit_reduce_avwap_tier") or "")
                m["pool1_left_quick_clear_streak"] = int(pos.get("left_quick_clear_streak", 0) or 0)
                m["pool1_last_left_quick_clear_at"] = int(pos.get("last_left_quick_clear_at", 0) or 0)
            else:
                m["pool1_position_status"] = "observe"
                m["pool1_position_ratio"] = 0.0
                m["pool1_holding_days"] = 0.0
                m["pool1_in_holding"] = False
                m["pool1_last_buy_at"] = 0
                m["pool1_last_buy_price"] = 0.0
                m["pool1_last_buy_type"] = ""
                m["pool1_last_sell_at"] = 0
                m["pool1_last_sell_type"] = ""
                m["pool1_last_reduce_at"] = 0
                m["pool1_last_reduce_type"] = ""
                m["pool1_last_reduce_ratio"] = 0.0
                m["pool1_last_reduce_source"] = ""
                m["pool1_last_reduce_avwap_tier"] = ""
                m["pool1_reduce_streak"] = 0
                m["pool1_reduce_ratio_cum"] = 0.0
                m["pool1_last_rebuild_at"] = 0
                m["pool1_last_rebuild_type"] = ""
                m["pool1_last_rebuild_add_ratio_base"] = 0.0
                m["pool1_last_rebuild_add_ratio_bias"] = 0.0
                m["pool1_last_rebuild_from_partial_count"] = 0
                m["pool1_last_rebuild_from_partial_ratio"] = 0.0
                m["pool1_last_rebuild_from_partial_source"] = ""
                m["pool1_last_rebuild_from_partial_avwap_tier"] = ""
                m["pool1_last_exit_after_partial"] = False
                m["pool1_last_exit_partial_count"] = 0
                m["pool1_last_exit_reduce_ratio_cum"] = 0.0
                m["pool1_last_exit_reduce_source"] = ""
                m["pool1_last_exit_reduce_avwap_tier"] = ""
                m["pool1_left_quick_clear_streak"] = 0
                m["pool1_last_left_quick_clear_at"] = 0
            chip = chip_map.get(str(ts_code), {})
            if chip:
                m["winner_rate"] = chip.get("winner_rate")
                m["chip_concentration_pct"] = chip.get("chip_concentration_pct")
            try:
                bars_1m = provider.get_cached_bars(ts_code)
                if not bars_1m:
                    bars_1m = mootdx_client.get_minute_bars(ts_code, 240)
                if bars_1m:
                    m["minute_bars_1m"] = bars_1m
                    try:
                        cum_amt = sum(float(b.get("amount", 0) or 0) for b in bars_1m)
                        cum_vol = sum(float(b.get("volume", 0) or 0) for b in bars_1m)
                        m["vwap"] = cum_amt / cum_vol if cum_vol > 0 else None
                    except Exception:
                        m["vwap"] = None
                    try:
                        m["intraday_prices"] = [
                            float(b.get("close", 0) or 0)
                            for b in bars_1m
                            if float(b.get("close", 0) or 0) > 0
                        ]
                    except Exception:
                        m["intraday_prices"] = None
                    rv, rinfo = sig.compute_pool1_resonance_60m(bars_1m, fallback=False)
                    if isinstance(rinfo, dict):
                        rinfo = dict(rinfo)
                        rinfo.setdefault("source", "1m_aggregate")
                    m["resonance_60m"] = bool(rv)
                    m["resonance_60m_info"] = rinfo
            except Exception:
                pass

        if pool_id == 2:
            try:
                bars = mootdx_client.get_minute_bars(ts_code, 240)
                if bars:
                    cum_amt = sum(b.get("amount", 0) for b in bars)
                    cum_vol = sum(b.get("volume", 0) for b in bars)
                    m["vwap"] = cum_amt / cum_vol if cum_vol > 0 else None
                    m["intraday_prices"] = [b.get("close", 0) for b in bars]
                    m["volume_pace_progress"] = m.get("volume_pace_progress")
            except Exception:
                pass
            try:
                txns = provider.get_cached_transactions(ts_code)
                if txns is None:
                    txns = mootdx_client.get_transactions(ts_code, int(_TXN_ANALYZE_COUNT))
                if txns:
                    sells = [t for t in txns if t.get("direction", 0) == 1 and t.get("volume", 0) < 50]
                    ratio = len(sells) / len(txns)
                    if ratio > 0.4:
                        m["gub5_trend"] = "up"
                    elif ratio > 0.2:
                        m["gub5_trend"] = "flat"
                    else:
                        m["gub5_trend"] = "down"
            except Exception:
                pass
        out.append(m)
    return out
def _evaluate_signals_internal(pool_id: int) -> list:
    try:
        if not _is_signal_processing_allowed():
            return []
        with _ro_conn_ctx() as conn:
            members_data = _build_member_data(conn, pool_id)
            if not members_data:
                return []
            evaluated = sig.evaluate_pool(pool_id, members_data)
            _save_signal_history(pool_id, evaluated)
            return evaluated
    except Exception as e:
        logger.error(f"evaluate signals internal failed pool={pool_id}: {e}")
        return []
def _evaluate_signals_fast_internal(pool_id: int, members: list, provider, tick_hint: Optional[dict] = None) -> list:
    allowed_types = POOL_SIGNAL_TYPES.get(pool_id, set())
    signal_processing_allowed = bool(_is_signal_processing_allowed())
    data = []
    hint = tick_hint or {}
    keep_pool1_until_eod = bool(POOL1_SIGNAL_CONFIG.get("keep_signals_until_eod", True))
    today_date = datetime.datetime.now().date()

    def _pool1_keep_today_signal(sig: dict) -> bool:
        if int(pool_id) != 1 or not keep_pool1_until_eod:
            return True
        if not isinstance(sig, dict):
            return False
        ts = sig.get("triggered_at")
        try:
            if isinstance(ts, datetime.datetime):
                sig_date = ts.date()
            elif isinstance(ts, datetime.date):
                sig_date = ts
            else:
                sig_date = datetime.datetime.fromtimestamp(int(float(ts or 0))).date()
            return sig_date == today_date
        except Exception:
            return False

    def _attach_pool1_position_fields(row: dict, code: str) -> None:
        if int(pool_id) != 1:
            return
        status = "observe"
        position_ratio = 0.0
        holding_days = 0.0
        row["pool1_last_buy_at"] = 0
        row["pool1_last_buy_type"] = ""
        row["pool1_last_sell_at"] = 0
        row["pool1_last_sell_type"] = ""
        row["pool1_last_reduce_at"] = 0
        row["pool1_last_reduce_type"] = ""
        row["pool1_last_reduce_ratio"] = 0.0
        row["pool1_last_reduce_source"] = ""
        row["pool1_last_reduce_avwap_tier"] = ""
        row["pool1_reduce_streak"] = 0
        row["pool1_reduce_ratio_cum"] = 0.0
        row["pool1_last_rebuild_at"] = 0
        row["pool1_last_rebuild_type"] = ""
        row["pool1_last_rebuild_transition"] = ""
        row["pool1_last_rebuild_add_ratio"] = 0.0
        row["pool1_last_rebuild_add_ratio_base"] = 0.0
        row["pool1_last_rebuild_add_ratio_bias"] = 0.0
        row["pool1_last_rebuild_from_partial_count"] = 0
        row["pool1_last_rebuild_from_partial_ratio"] = 0.0
        row["pool1_last_rebuild_from_partial_source"] = ""
        row["pool1_last_rebuild_from_partial_avwap_tier"] = ""
        row["pool1_last_exit_after_partial"] = False
        row["pool1_last_exit_partial_count"] = 0
        row["pool1_last_exit_reduce_ratio_cum"] = 0.0
        row["pool1_last_exit_reduce_source"] = ""
        row["pool1_last_exit_reduce_avwap_tier"] = ""
        row["pool1_left_quick_clear_streak"] = 0
        row["pool1_last_left_quick_clear_at"] = 0
        row["pool1_reduce_source"] = ""
        row["pool1_reduce_avwap_tier"] = ""
        try:
            pos = provider.get_pool1_position_state(code)
            if isinstance(pos, dict):
                status = str(pos.get("status") or "observe").strip().lower()
                if status not in ("holding", "observe"):
                    status = "observe"
                position_ratio = float(pos.get("position_ratio", 0.0) or 0.0)
                holding_days = float(pos.get("holding_days", 0.0) or 0.0)
                row["pool1_last_buy_at"] = int(pos.get("last_buy_at", 0) or 0)
                row["pool1_last_buy_type"] = str(pos.get("last_buy_type") or "")
                row["pool1_last_sell_at"] = int(pos.get("last_sell_at", 0) or 0)
                row["pool1_last_sell_type"] = str(pos.get("last_sell_type") or "")
                row["pool1_last_reduce_at"] = int(pos.get("last_reduce_at", 0) or 0)
                row["pool1_last_reduce_type"] = str(pos.get("last_reduce_type") or "")
                row["pool1_last_reduce_ratio"] = round(float(pos.get("last_reduce_ratio", 0.0) or 0.0), 4)
                row["pool1_last_reduce_source"] = str(pos.get("last_reduce_source") or "")
                row["pool1_last_reduce_avwap_tier"] = str(pos.get("last_reduce_avwap_tier") or "")
                row["pool1_reduce_streak"] = int(pos.get("reduce_streak", 0) or 0)
                row["pool1_reduce_ratio_cum"] = round(float(pos.get("reduce_ratio_cum", 0.0) or 0.0), 4)
                row["pool1_last_rebuild_at"] = int(pos.get("last_rebuild_at", 0) or 0)
                row["pool1_last_rebuild_type"] = str(pos.get("last_rebuild_type") or "")
                row["pool1_last_rebuild_transition"] = str(pos.get("last_rebuild_transition") or "")
                row["pool1_last_rebuild_add_ratio"] = round(float(pos.get("last_rebuild_add_ratio", 0.0) or 0.0), 4)
                row["pool1_last_rebuild_add_ratio_base"] = round(float(pos.get("last_rebuild_add_ratio_base", 0.0) or 0.0), 4)
                row["pool1_last_rebuild_add_ratio_bias"] = round(float(pos.get("last_rebuild_add_ratio_bias", 0.0) or 0.0), 4)
                row["pool1_last_rebuild_from_partial_count"] = int(pos.get("last_rebuild_from_partial_count", 0) or 0)
                row["pool1_last_rebuild_from_partial_ratio"] = round(float(pos.get("last_rebuild_from_partial_ratio", 0.0) or 0.0), 4)
                row["pool1_last_rebuild_from_partial_source"] = str(pos.get("last_rebuild_from_partial_source") or "")
                row["pool1_last_rebuild_from_partial_avwap_tier"] = str(pos.get("last_rebuild_from_partial_avwap_tier") or "")
                row["pool1_last_exit_after_partial"] = bool(pos.get("last_exit_after_partial", False))
                row["pool1_last_exit_partial_count"] = int(pos.get("last_exit_partial_count", 0) or 0)
                row["pool1_last_exit_reduce_ratio_cum"] = round(float(pos.get("last_exit_reduce_ratio_cum", 0.0) or 0.0), 4)
                row["pool1_last_exit_reduce_source"] = str(pos.get("last_exit_reduce_source") or "")
                row["pool1_last_exit_reduce_avwap_tier"] = str(pos.get("last_exit_reduce_avwap_tier") or "")
                row["pool1_left_quick_clear_streak"] = int(pos.get("left_quick_clear_streak", 0) or 0)
                row["pool1_last_left_quick_clear_at"] = int(pos.get("last_left_quick_clear_at", 0) or 0)
        except Exception:
            pass
        row["pool1_position_status"] = status
        row["pool1_position_ratio"] = round(max(0.0, min(1.0, position_ratio)), 4)
        row["pool1_holding_days"] = round(max(0.0, holding_days), 4)
        row["pool1_in_holding"] = bool(status == "holding")

    def _attach_pool1_decision_fields(row: dict) -> None:
        if int(pool_id) != 1:
            return
        signals = row.get("signals") if isinstance(row.get("signals"), list) else []
        status = str(row.get("pool1_position_status") or "observe").strip().lower()

        def _sig_actionable(s: dict) -> bool:
            if not isinstance(s, dict) or not s.get("has_signal"):
                return False
            details = s.get("details") if isinstance(s.get("details"), dict) else {}
            return not bool(details.get("observe_only", False))

        def _sig_strength(s: dict) -> float:
            try:
                return float(s.get("current_strength", s.get("strength", 0)) or 0)
            except Exception:
                return 0.0

        buy_types = {"left_side_buy", "right_side_breakout"}
        clear_types = {"timing_clear"}

        buy_actionable = [s for s in signals if str(s.get("type") or "") in buy_types and _sig_actionable(s)]
        clear_actionable = [s for s in signals if str(s.get("type") or "") in clear_types and _sig_actionable(s)]
        buy_watch = [s for s in signals if str(s.get("type") or "") in buy_types]
        clear_watch = [s for s in signals if str(s.get("type") or "") in clear_types]

        decision = "observe"
        decision_label = "建议观望"
        decision_reason = "未出现确认级建仓信号"
        decision_strength = 0.0
        decision_signal_types: list[str] = []
        decision_mode = "neutral"
        decision_clear_level = ""
        decision_clear_family = ""
        decision_reduce_ratio = 0.0
        decision_reduce_source = ""
        decision_reduce_avwap_tier = ""

        if status == "holding":
            if clear_actionable:
                best = max(clear_actionable, key=_sig_strength)
                best_details = best.get("details") if isinstance(best.get("details"), dict) else {}
                clear_level = str(best_details.get("clear_level") or best_details.get("clear_level_hint") or "full").strip().lower()
                clear_family = str(best_details.get("clear_family") or "defense").strip().lower()
                try:
                    reduce_ratio = float(best_details.get("suggest_reduce_ratio", 0.0) or 0.0)
                except Exception:
                    reduce_ratio = 0.0
                decision = "clear"
                decision_label = "建议减仓" if clear_level == "partial" else "建议清仓"
                decision_reason = str(best.get("message") or "出现确认级清仓信号")
                decision_strength = _sig_strength(best)
                decision_signal_types = [str(s.get("type") or "") for s in clear_actionable]
                decision_mode = "actionable"
                decision_clear_level = clear_level
                decision_clear_family = clear_family
                decision_reduce_ratio = round(max(0.0, min(1.0, reduce_ratio)), 4)
                decision_reduce_source = str(best_details.get("reduce_ratio_source") or "")
                decision_reduce_avwap_tier = str(best_details.get("avwap_exit_tier") or "")
            else:
                decision = "hold"
                decision_label = "建议持有"
                if clear_watch:
                    best = max(clear_watch, key=_sig_strength)
                    best_details = best.get("details") if isinstance(best.get("details"), dict) else {}
                    clear_level = str(best_details.get("clear_level") or best_details.get("clear_level_hint") or "").strip().lower()
                    clear_family = str(best_details.get("clear_family") or "").strip().lower()
                    try:
                        reduce_ratio = float(best_details.get("suggest_reduce_ratio", 0.0) or 0.0)
                    except Exception:
                        reduce_ratio = 0.0
                    decision_reason = str(best.get("message") or "存在清仓观察信号，但未到执行级")
                    decision_strength = _sig_strength(best)
                    decision_signal_types = [str(s.get("type") or "") for s in clear_watch]
                    decision_mode = "watch"
                    decision_clear_level = clear_level
                    decision_clear_family = clear_family
                    decision_reduce_ratio = round(max(0.0, min(1.0, reduce_ratio)), 4)
                    decision_reduce_source = str(best_details.get("reduce_ratio_source") or "")
                    decision_reduce_avwap_tier = str(best_details.get("avwap_exit_tier") or "")
                else:
                    decision_reason = "主线趋势未出现确认级清仓条件"
        else:
            if buy_actionable:
                best = max(buy_actionable, key=_sig_strength)
                decision = "build"
                decision_label = "建议建仓"
                decision_reason = str(best.get("message") or "出现确认级建仓信号")
                decision_strength = _sig_strength(best)
                decision_signal_types = [str(s.get("type") or "") for s in buy_actionable]
                decision_mode = "actionable"
            else:
                if buy_watch:
                    best = max(buy_watch, key=_sig_strength)
                    decision_reason = str(best.get("message") or "存在建仓观察信号，但未到执行级")
                    decision_strength = _sig_strength(best)
                    decision_signal_types = [str(s.get("type") or "") for s in buy_watch]
                    decision_mode = "watch"
                else:
                    decision_reason = "未出现确认级建仓信号"

        row["pool1_decision"] = decision
        row["pool1_decision_label"] = decision_label
        row["pool1_decision_reason"] = decision_reason
        row["pool1_decision_strength"] = round(float(decision_strength), 1)
        row["pool1_decision_mode"] = decision_mode
        row["pool1_decision_signal_types"] = decision_signal_types
        row["pool1_clear_level"] = decision_clear_level
        row["pool1_clear_family"] = decision_clear_family
        row["pool1_reduce_ratio"] = round(float(decision_reduce_ratio), 4)
        row["pool1_reduce_source"] = decision_reduce_source
        row["pool1_reduce_avwap_tier"] = decision_reduce_avwap_tier

        if signals:
            patched = []
            for s in signals:
                if not isinstance(s, dict):
                    patched.append(s)
                    continue
                sc = dict(s)
                details = dict(sc.get("details") or {})
                details["pool1_decision"] = {
                    "decision": decision,
                    "label": decision_label,
                    "reason": decision_reason,
                    "strength": round(float(decision_strength), 1),
                    "mode": decision_mode,
                    "signal_types": decision_signal_types,
                    "position_status": status,
                    "clear_level": decision_clear_level,
                    "clear_family": decision_clear_family,
                    "reduce_ratio": round(float(decision_reduce_ratio), 4),
                    "reduce_source": decision_reduce_source,
                    "reduce_avwap_tier": decision_reduce_avwap_tier,
                }
                sc["details"] = details
                patched.append(sc)
            row["signals"] = patched

    for ts_code, name in members:
        entry = provider.get_cached_signals(ts_code)
        if entry is None:
            tick = hint.get(ts_code)
            if not tick:
                try:
                    tick = provider.get_cached_tick(ts_code)
                except Exception:
                    tick = None
            if not tick:
                if str(getattr(provider, "name", "")) == "gm":
                    tick = _empty_tick_payload(ts_code)
                else:
                    tick = provider.get_tick(ts_code)
            row = {
                "ts_code": ts_code,
                "name": name,
                "price": tick.get("price", 0),
                "pct_chg": tick.get("pct_chg", 0),
                "signals": [],
                "evaluated_at": 0,
            }
            _attach_pool1_position_fields(row, ts_code)
            _attach_pool1_decision_fields(row)
            data.append(row)
            continue
        e = dict(entry)
        e["name"] = name
        e["signals"] = [
            dict(s) for s in e.get("signals", [])
            if signal_processing_allowed and s.get("type") in allowed_types and _pool1_keep_today_signal(s)
        ]
        _attach_pool1_position_fields(e, ts_code)
        _attach_pool1_decision_fields(e)
        data.append(e)
    return data
def tick_provider_info():
    p = get_tick_provider()
    return {"name": p.name, "display_name": p.display_name}
def realtime_ui_config():
    cfg = dict(REALTIME_UI_CONFIG or {})
    rebuild_cfg = dict((POOL1_SIGNAL_CONFIG or {}).get("rebuild") or {})
    cfg["pool1_rebuild_ui"] = {
        "enabled": bool(rebuild_cfg.get("enabled", False)),
        "require_stage1_pass": bool(rebuild_cfg.get("require_stage1_pass", True)),
        "min_position_gap": round(float(rebuild_cfg.get("min_position_gap", 0.12) or 0.12), 4),
        "min_minutes_after_reduce": round(float(rebuild_cfg.get("min_minutes_after_reduce", 20.0) or 20.0), 2),
        "min_signal_strength": round(float(rebuild_cfg.get("min_signal_strength", 78.0) or 78.0), 2),
        "add_ratio": round(float(rebuild_cfg.get("add_ratio", 0.50) or 0.50), 4),
        "observe_tier_add_ratio_bias": round(float(rebuild_cfg.get("observe_tier_add_ratio_bias", 0.15) or 0.15), 4),
        "full_tier_add_ratio_bias": round(float(rebuild_cfg.get("full_tier_add_ratio_bias", -0.15) or -0.15), 4),
        "soft_source_add_ratio_bias": round(float(rebuild_cfg.get("soft_source_add_ratio_bias", 0.10) or 0.10), 4),
        "core_source_add_ratio_bias": round(float(rebuild_cfg.get("core_source_add_ratio_bias", -0.15) or -0.15), 4),
        "full_tier_extra_delay_minutes": round(float(rebuild_cfg.get("full_tier_extra_delay_minutes", 20.0) or 20.0), 2),
        "core_source_extra_delay_minutes": round(float(rebuild_cfg.get("core_source_extra_delay_minutes", 15.0) or 15.0), 2),
        "allow_left_side_buy": bool(rebuild_cfg.get("allow_left_side_buy", True)),
        "allow_right_side_breakout": bool(rebuild_cfg.get("allow_right_side_breakout", True)),
    }
    return {"data": cfg}
def pool1_observe_stats():
    provider = get_tick_provider()
    stats = provider.get_pool1_observe_stats()
    if not stats:
        return {
            "pool_id": 1,
            "provider": provider.name,
            "supported": False,
            "message": "observe stats not supported",
        }
    storage_check = {
        "expected": str(stats.get("storage_expected") or ("redis" if bool(stats.get("redis_enabled", False)) else "memory")),
        "source": str(stats.get("storage_source") or "memory"),
        "verified": bool(stats.get("storage_verified", False)),
        "degraded": bool(stats.get("storage_degraded", False)),
        "note": str(stats.get("storage_note") or ""),
        "redis_enabled": bool(stats.get("redis_enabled", False)),
        "redis_ready": bool(stats.get("redis_ready", False)),
    }
    return {
        "pool_id": 1,
        "provider": provider.name,
        "supported": True,
        "trade_date": stats.get("trade_date"),
        "updated_at": stats.get("updated_at"),
        "updated_at_iso": stats.get("updated_at_iso"),
        "storage_check": storage_check,
        "data": stats,
    }
def pool1_position_summary():
    provider = get_tick_provider()
    with _ro_conn_ctx() as conn:
        rows = conn.execute(
            """
            SELECT ts_code, name
            FROM monitor_pools
            WHERE pool_id = 1
            ORDER BY sort_order, added_at
            """
        ).fetchall()
    total = len(rows)
    holding = 0
    observe = 0
    holding_days_arr: list[float] = []
    transitions_today = 0
    today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_ts = int(today_start.timestamp())
    holdings_detail: list[dict] = []
    for r in rows:
        ts_code = str(r[0] or "")
        name = str(r[1] or "")
        pos = {}
        try:
            got = provider.get_pool1_position_state(ts_code)
            if isinstance(got, dict):
                pos = got
        except Exception:
            pos = {}
        status = str(pos.get("status") or "observe").strip().lower()
        if status not in ("observe", "holding"):
            status = "observe"
        hd = float(pos.get("holding_days", 0.0) or 0.0)
        position_ratio = round(float(pos.get("position_ratio", 0.0) or 0.0), 4)
        last_buy_at = int(pos.get("last_buy_at", 0) or 0)
        last_sell_at = int(pos.get("last_sell_at", 0) or 0)
        last_reduce_at = int(pos.get("last_reduce_at", 0) or 0)
        if last_buy_at >= today_ts:
            transitions_today += 1
        if last_sell_at >= today_ts:
            transitions_today += 1
        if last_reduce_at >= today_ts:
            transitions_today += 1
        if status == "holding":
            holding += 1
            holding_days_arr.append(max(0.0, hd))
            holdings_detail.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "holding_days": round(max(0.0, hd), 4),
                    "position_ratio": position_ratio,
                    "last_buy_at": last_buy_at,
                    "last_buy_type": str(pos.get("last_buy_type") or ""),
                    "last_buy_price": float(pos.get("last_buy_price", 0.0) or 0.0),
                    "last_reduce_at": last_reduce_at,
                    "last_reduce_type": str(pos.get("last_reduce_type") or ""),
                    "last_reduce_ratio": round(float(pos.get("last_reduce_ratio", 0.0) or 0.0), 4),
                }
            )
        else:
            observe += 1
    holdings_detail.sort(key=lambda x: float(x.get("holding_days", 0.0) or 0.0), reverse=True)
    avg_holding_days = (sum(holding_days_arr) / len(holding_days_arr)) if holding_days_arr else 0.0
    max_holding_days = max(holding_days_arr) if holding_days_arr else 0.0
    storage = None
    try:
        s = provider.get_pool1_position_storage_status()
        if isinstance(s, dict):
            storage = s
    except Exception:
        storage = None
    return {
        "pool_id": 1,
        "provider": provider.name,
        "checked_at": datetime.datetime.now().isoformat(),
        "summary": {
            "member_count": int(total),
            "holding_count": int(holding),
            "observe_count": int(observe),
            "holding_ratio": round((holding / total), 4) if total > 0 else 0.0,
            "avg_holding_days": round(avg_holding_days, 4),
            "max_holding_days": round(max_holding_days, 4),
            "transitions_today": int(transitions_today),
        },
        "storage": storage or {},
        "holdings": holdings_detail[:50],
    }
def pool1_decision_summary():
    provider = get_tick_provider()
    trend_days = 5
    today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    clear_reason_meta = [
        ("entry_cost_lost", "成本锚失守"),
        ("entry_avwap_lost", "建仓AVWAP失守"),
        ("breakout_anchor_lost", "突破锚失守"),
        ("event_anchor_lost", "事件锚失守"),
        ("multi_anchor_lost", "多锚失守"),
    ]
    clear_reason_summary = {
        key: {"key": key, "label": label, "count": 0, "actionable": 0}
        for key, label in clear_reason_meta
    }
    with _ro_conn_ctx() as conn:
        members = conn.execute(
            """
            SELECT ts_code, name, industry
            FROM monitor_pools
            WHERE pool_id = 1
            ORDER BY sort_order, added_at
            """
        ).fetchall()
        trend_since_dt = today_start - datetime.timedelta(days=max(0, trend_days - 1))
        history_rows = conn.execute(
            """
            SELECT ts_code, name, signal_type, details_json, triggered_at
            FROM signal_history
            WHERE pool_id = 1
              AND signal_type IN ('left_side_buy', 'right_side_breakout', 'timing_clear')
              AND triggered_at >= ?
            ORDER BY triggered_at DESC
            """,
            [trend_since_dt],
        ).fetchall()

    eval_members = [(str(m[0] or ""), str(m[1] or "")) for m in members]
    rows = _evaluate_signals_fast_internal(1, eval_members, provider) if eval_members else []
    total = len(rows)
    today_ts = int(today_start.timestamp())
    concept_board_map: dict[str, list[str]] = {}
    core_concept_map: dict[str, str] = {}
    concept_snapshot_map: dict[str, dict] = {}
    industry_snapshot_map: dict[str, dict] = {}
    concept_code_map: dict[str, list[str]] = {}
    core_concept_code_map: dict[str, str] = {}
    stock_industry_name_map: dict[str, str] = {}
    stock_industry_code_map: dict[str, str] = {}
    member_meta: dict[str, dict] = {}
    if eval_members:
        try:
            with _data_ro_conn_ctx() as data_conn:
                (
                    concept_board_map,
                    core_concept_map,
                    concept_snapshot_map,
                    concept_code_map,
                    core_concept_code_map,
                ) = _load_concept_board_maps(data_conn, [x[0] for x in eval_members])
                stock_industry_name_map, stock_industry_code_map = _load_em_industry_member_maps(
                    data_conn,
                    [x[0] for x in eval_members],
                )
                industry_snapshot_map, _, _ = _fetch_realtime_industry_snapshot_map(force_refresh=False)
                _attach_industry_snapshot_aliases(
                    industry_snapshot_map,
                    {
                        str(stock_industry_name_map.get(str(m[0]), "") or str(m[2] or "")).strip()
                        for m in members
                    },
                )
        except Exception:
            concept_board_map = {}
            core_concept_map = {}
            concept_snapshot_map = {}
            industry_snapshot_map = {}
            concept_code_map = {}
            core_concept_code_map = {}
            stock_industry_name_map = {}
            stock_industry_code_map = {}
    for m in members:
        code = str(m[0] or "")
        name = str(m[1] or "")
        industry = str(stock_industry_name_map.get(code, "") or m[2] or "")
        industry_code = str(stock_industry_code_map.get(code, "") or "")
        board_segment = "unknown"
        if infer_instrument_profile is not None:
            try:
                profile = infer_instrument_profile(code, name=name)
                board_segment = str(profile.get("board_segment") or "unknown")
            except Exception:
                board_segment = "unknown"
        member_meta[code] = {
            "name": name,
            "industry": industry,
            "industry_code": industry_code,
            "board_segment": board_segment,
            "concept_boards": list(concept_board_map.get(code, []) or []),
            "concept_codes": list(concept_code_map.get(code, []) or []),
            "core_concept_board": str(core_concept_map.get(code, "") or ""),
            "core_concept_code": str(core_concept_code_map.get(code, "") or ""),
            "concept_ecology": _resolve_concept_snapshot(
                concept_snapshot_map,
                concept_name=core_concept_map.get(code, ""),
                concept_code=core_concept_code_map.get(code, ""),
            ),
            "concept_ecology_multi": _build_concept_ecology_multi(
                concept_snapshot_map,
                concept_names=list(concept_board_map.get(code, []) or []),
                concept_codes=list(concept_code_map.get(code, []) or []),
            ),
            "industry_ecology": _resolve_industry_snapshot(
                industry_snapshot_map,
                industry_name=industry,
                industry_code=industry_code,
            ),
        }
    decision_counts = {"build": 0, "hold": 0, "clear": 0, "observe": 0}
    mode_counts = {"actionable": 0, "watch": 0, "neutral": 0}
    position_counts = {"holding": 0, "observe": 0}
    transition_summary = {
        "build_today": 0,
        "rebuild_today": 0,
        "partial_today": 0,
        "chained_partial_today": 0,
        "clear_today": 0,
        "clear_after_partial_today": 0,
        "rebuild_stabilized_today": 0,
        "rebuild_reduced_again_today": 0,
        "rebuild_cleared_again_today": 0,
        "transition_today": 0,
        "net_build_today": 0,
    }
    left_suppressed_count = 0
    left_repeat_suppressed_count = 0
    concept_retreat_watch_count = 0

    def _pack_item(row: dict) -> dict:
        try:
            strength = float(row.get("pool1_decision_strength", 0.0) or 0.0)
        except Exception:
            strength = 0.0
        return {
            "ts_code": str(row.get("ts_code") or ""),
            "name": str(row.get("name") or ""),
            "decision": str(row.get("pool1_decision") or "observe"),
            "decision_label": str(row.get("pool1_decision_label") or "建议观望"),
            "decision_reason": str(row.get("pool1_decision_reason") or ""),
            "decision_strength": round(strength, 1),
            "decision_mode": str(row.get("pool1_decision_mode") or "neutral"),
            "position_status": str(row.get("pool1_position_status") or "observe"),
            "position_ratio": round(float(row.get("pool1_position_ratio", 0.0) or 0.0), 4),
            "clear_level": str(row.get("pool1_clear_level") or ""),
            "clear_family": str(row.get("pool1_clear_family") or ""),
            "reduce_ratio": round(float(row.get("pool1_reduce_ratio", 0.0) or 0.0), 4),
            "reduce_source": str(row.get("pool1_reduce_source") or ""),
            "reduce_avwap_tier": str(row.get("pool1_reduce_avwap_tier") or ""),
            "last_reduce_source": str(row.get("pool1_last_reduce_source") or ""),
            "last_reduce_avwap_tier": str(row.get("pool1_last_reduce_avwap_tier") or ""),
            "reduce_streak": int(row.get("pool1_reduce_streak", 0) or 0),
            "reduce_ratio_cum": round(float(row.get("pool1_reduce_ratio_cum", 0.0) or 0.0), 4),
            "last_rebuild_at": int(row.get("pool1_last_rebuild_at", 0) or 0),
            "last_rebuild_type": str(row.get("pool1_last_rebuild_type") or ""),
            "last_rebuild_transition": str(row.get("pool1_last_rebuild_transition") or ""),
            "last_rebuild_add_ratio": round(float(row.get("pool1_last_rebuild_add_ratio", 0.0) or 0.0), 4),
            "last_rebuild_add_ratio_base": round(float(row.get("pool1_last_rebuild_add_ratio_base", 0.0) or 0.0), 4),
            "last_rebuild_add_ratio_bias": round(float(row.get("pool1_last_rebuild_add_ratio_bias", 0.0) or 0.0), 4),
            "last_rebuild_from_partial_count": int(row.get("pool1_last_rebuild_from_partial_count", 0) or 0),
            "last_rebuild_from_partial_ratio": round(float(row.get("pool1_last_rebuild_from_partial_ratio", 0.0) or 0.0), 4),
            "last_rebuild_from_partial_source": str(row.get("pool1_last_rebuild_from_partial_source") or ""),
            "last_rebuild_from_partial_avwap_tier": str(row.get("pool1_last_rebuild_from_partial_avwap_tier") or ""),
            "last_exit_after_partial": bool(row.get("pool1_last_exit_after_partial", False)),
            "last_exit_partial_count": int(row.get("pool1_last_exit_partial_count", 0) or 0),
            "last_exit_reduce_ratio_cum": round(float(row.get("pool1_last_exit_reduce_ratio_cum", 0.0) or 0.0), 4),
            "last_exit_reduce_source": str(row.get("pool1_last_exit_reduce_source") or ""),
            "last_exit_reduce_avwap_tier": str(row.get("pool1_last_exit_reduce_avwap_tier") or ""),
            "holding_days": round(float(row.get("pool1_holding_days", 0.0) or 0.0), 4),
            "signal_types": list(row.get("pool1_decision_signal_types") or []),
            "industry": str((member_meta.get(str(row.get("ts_code") or ""), {}) or {}).get("industry") or ""),
            "board_segment": str((member_meta.get(str(row.get("ts_code") or ""), {}) or {}).get("board_segment") or "unknown"),
            "concept_boards": list((member_meta.get(str(row.get("ts_code") or ""), {}) or {}).get("concept_boards") or []),
            "core_concept_board": str((member_meta.get(str(row.get("ts_code") or ""), {}) or {}).get("core_concept_board") or ""),
            "concept_state": str((((member_meta.get(str(row.get("ts_code") or ""), {}) or {}).get("concept_ecology") or {}).get("state") or "")),
            "concept_score": round(float((((member_meta.get(str(row.get("ts_code") or ""), {}) or {}).get("concept_ecology") or {}).get("score") or 0.0)), 2),
            "price": float(row.get("price", 0.0) or 0.0),
            "pct_chg": float(row.get("pct_chg", 0.0) or 0.0),
        }

    decision_examples = {k: [] for k in decision_counts.keys()}
    mode_examples = {k: [] for k in mode_counts.keys()}
    recent_transitions: list[dict] = []
    daily_map: dict[str, dict] = {}
    history_events: list[dict] = []

    for offset in range(max(1, trend_days)):
        dt = today_start.date() - datetime.timedelta(days=(trend_days - 1 - offset))
        day_key = dt.isoformat()
        daily_map[day_key] = {
            "trade_date": day_key,
            "build_actionable": 0,
            "build_watch": 0,
            "clear_actionable": 0,
            "clear_watch": 0,
            "partial_actionable": 0,
            "partial_watch": 0,
            "partial_reduce_ratio_sum": 0.0,
            "left_side_buy": 0,
            "right_side_breakout": 0,
            "timing_clear": 0,
            "net_build_actionable": 0,
            "net_exposure_change": 0.0,
            "entry_cost_lost_count": 0,
            "entry_cost_lost_actionable": 0,
            "entry_avwap_lost_count": 0,
            "entry_avwap_lost_actionable": 0,
            "breakout_anchor_lost_count": 0,
            "breakout_anchor_lost_actionable": 0,
            "event_anchor_lost_count": 0,
            "event_anchor_lost_actionable": 0,
            "multi_anchor_lost_count": 0,
            "multi_anchor_lost_actionable": 0,
        }

    for hr in history_rows:
        sig_type = str(hr[2] or "")
        triggered_at = hr[4]
        if not triggered_at:
            continue
        try:
            day_key = triggered_at.date().isoformat() if hasattr(triggered_at, "date") else str(triggered_at)[:10]
        except Exception:
            continue
        bucket = daily_map.get(day_key)
        if not isinstance(bucket, dict):
            continue
        meta = member_meta.get(str(hr[0] or ""), {})
        industry = str(meta.get("industry") or "")
        board_segment = str(meta.get("board_segment") or "unknown")
        details = {}
        raw_details = hr[3]
        if raw_details:
            try:
                parsed = json.loads(str(raw_details))
                if isinstance(parsed, dict):
                    details = parsed
            except Exception:
                details = {}
        observe_only = bool(details.get("observe_only", False))
        clear_level = str(details.get("clear_level") or details.get("clear_level_hint") or "").strip().lower()
        clear_family = str(details.get("clear_family") or "").strip().lower()
        entry_anchor = details.get("entry_anchor") if isinstance(details.get("entry_anchor"), dict) else {}
        entry_cost_lost = bool(entry_anchor.get("entry_cost_lost", False))
        entry_avwap_lost = bool(entry_anchor.get("entry_avwap_lost", False))
        breakout_anchor_lost = bool(entry_anchor.get("breakout_anchor_lost", False))
        event_anchor_lost = bool(entry_anchor.get("event_anchor_lost", False))
        try:
            lost_anchor_count = int(entry_anchor.get("lost_anchor_count", 0) or 0)
        except Exception:
            lost_anchor_count = 0
        multi_anchor_lost = lost_anchor_count >= 2
        clear_reason_flags = {
            "entry_cost_lost": entry_cost_lost,
            "entry_avwap_lost": entry_avwap_lost,
            "breakout_anchor_lost": breakout_anchor_lost,
            "event_anchor_lost": event_anchor_lost,
            "multi_anchor_lost": multi_anchor_lost,
        }
        dominant_clear_reason = ""
        for key in ("multi_anchor_lost", "event_anchor_lost", "breakout_anchor_lost", "entry_avwap_lost", "entry_cost_lost"):
            if clear_reason_flags.get(key):
                dominant_clear_reason = key
                break
        try:
            reduce_ratio = float(details.get("suggest_reduce_ratio", 0.0) or 0.0)
        except Exception:
            reduce_ratio = 0.0
        reduce_ratio = max(0.0, min(1.0, reduce_ratio))
        if sig_type in ("left_side_buy", "right_side_breakout"):
            if observe_only:
                bucket["build_watch"] += 1
            else:
                bucket["build_actionable"] += 1
        elif sig_type == "timing_clear":
            if observe_only:
                bucket["clear_watch"] += 1
            else:
                bucket["clear_actionable"] += 1
            if clear_level == "partial":
                if observe_only:
                    bucket["partial_watch"] += 1
                else:
                    bucket["partial_actionable"] += 1
                    bucket["partial_reduce_ratio_sum"] += reduce_ratio
            for key, _label in clear_reason_meta:
                if not clear_reason_flags.get(key):
                    continue
                bucket[f"{key}_count"] += 1
                clear_reason_summary[key]["count"] += 1
                if not observe_only:
                    bucket[f"{key}_actionable"] += 1
                    clear_reason_summary[key]["actionable"] += 1
        if sig_type in bucket:
            bucket[sig_type] += 1
        history_events.append(
            {
                "trade_date": day_key,
                "ts_code": str(hr[0] or ""),
                "name": str(hr[1] or meta.get("name") or ""),
                "industry": industry,
                "board_segment": board_segment,
                "concept_boards": list(meta.get("concept_boards") or []),
                "core_concept_board": str(meta.get("core_concept_board") or ""),
                "concept_state": str(((meta.get("concept_ecology") or {}).get("state") or "")),
                "concept_score": round(float(((meta.get("concept_ecology") or {}).get("score") or 0.0)), 2),
                "signal_type": sig_type,
                "observe_only": observe_only,
                "actionable": not observe_only,
                "clear_level": clear_level,
                "clear_family": clear_family,
                "reduce_ratio": round(reduce_ratio, 4),
                "entry_cost_lost": entry_cost_lost,
                "entry_avwap_lost": entry_avwap_lost,
                "breakout_anchor_lost": breakout_anchor_lost,
                "event_anchor_lost": event_anchor_lost,
                "multi_anchor_lost": multi_anchor_lost,
                "lost_anchor_count": lost_anchor_count,
                "support_anchor_type": str(entry_anchor.get("support_anchor_type") or ""),
                "support_anchor_line": round(float(entry_anchor.get("support_anchor_line", 0.0) or 0.0), 6) if entry_anchor.get("support_anchor_line") is not None else None,
                "dominant_clear_reason": dominant_clear_reason,
                "dominant_clear_reason_label": str(clear_reason_summary.get(dominant_clear_reason, {}).get("label") or ""),
                "triggered_at": triggered_at.isoformat() if hasattr(triggered_at, "isoformat") else str(triggered_at),
            }
        )

    for row in rows:
        decision = str(row.get("pool1_decision") or "observe").strip().lower()
        if decision not in decision_counts:
            decision = "observe"
        mode = str(row.get("pool1_decision_mode") or "neutral").strip().lower()
        if mode not in mode_counts:
            mode = "neutral"
        position_status = str(row.get("pool1_position_status") or "observe").strip().lower()
        if position_status not in position_counts:
            position_status = "observe"
        decision_counts[decision] += 1
        mode_counts[mode] += 1
        position_counts[position_status] += 1
        decision_reason = str(row.get("pool1_decision_reason") or "")
        if "左侧抑制:" in decision_reason:
            left_suppressed_count += 1
        if (
            "左侧抑制:repeat_retreat_after_quick_clear" in decision_reason
            or "左侧抑制:repeat_weak_after_quick_clear" in decision_reason
        ):
            left_repeat_suppressed_count += 1
        if "概念生态:" in decision_reason and "retreat" in decision_reason and mode == "watch":
            concept_retreat_watch_count += 1
        item = _pack_item(row)
        decision_examples[decision].append(item)
        mode_examples[mode].append(item)

        ts_code = str(row.get("ts_code") or "")
        pos = {}
        try:
            got = provider.get_pool1_position_state(ts_code)
            if isinstance(got, dict):
                pos = got
        except Exception:
            pos = {}
        last_buy_at = int(pos.get("last_buy_at", 0) or 0)
        last_sell_at = int(pos.get("last_sell_at", 0) or 0)
        last_reduce_at = int(pos.get("last_reduce_at", 0) or 0)
        last_rebuild_at = int(pos.get("last_rebuild_at", 0) or 0)
        last_rebuild_transition = str(pos.get("last_rebuild_transition") or "")
        last_rebuild_add_ratio = round(float(pos.get("last_rebuild_add_ratio", 0.0) or 0.0), 4)
        reduce_streak = int(pos.get("reduce_streak", 0) or 0)
        reduce_ratio_cum = round(float(pos.get("reduce_ratio_cum", 0.0) or 0.0), 4)
        last_exit_after_partial = bool(pos.get("last_exit_after_partial", False))
        last_exit_partial_count = int(pos.get("last_exit_partial_count", 0) or 0)
        last_exit_reduce_ratio_cum = round(float(pos.get("last_exit_reduce_ratio_cum", 0.0) or 0.0), 4)
        rebuild_today_flag = last_rebuild_at >= today_ts and last_rebuild_transition in ("observe->holding(rebuild_after_partial)", "holding->holding(rebuild)")
        if rebuild_today_flag:
            if last_sell_at > last_rebuild_at:
                transition_summary["rebuild_cleared_again_today"] += 1
            elif last_reduce_at > last_rebuild_at:
                transition_summary["rebuild_reduced_again_today"] += 1
            else:
                transition_summary["rebuild_stabilized_today"] += 1
        if last_buy_at >= today_ts and last_rebuild_transition != "observe->holding(rebuild_after_partial)":
            transition_summary["build_today"] += 1
            transition_summary["transition_today"] += 1
            recent_transitions.append(
                {
                    "ts_code": ts_code,
                    "name": str(row.get("name") or ""),
                    "transition": "observe->holding",
                    "label": "观望->持仓",
                    "at": last_buy_at,
                    "at_iso": datetime.datetime.fromtimestamp(last_buy_at).isoformat() if last_buy_at > 0 else None,
                    "signal_type": str(pos.get("last_buy_type") or ""),
                    "signal_price": float(pos.get("last_buy_price", 0.0) or 0.0),
                    "rebuild_after_partial": False,
                    "rebuild_from_partial_count": 0,
                    "rebuild_from_partial_ratio": 0.0,
                    "current_position_status": position_status,
                    "current_decision": decision,
                    "current_decision_label": str(row.get("pool1_decision_label") or "建议观望"),
                    "current_decision_mode": mode,
                    "current_decision_strength": round(float(row.get("pool1_decision_strength", 0.0) or 0.0), 1),
                    "current_signal_types": list(row.get("pool1_decision_signal_types") or []),
                    "holding_days": round(float(row.get("pool1_holding_days", 0.0) or 0.0), 4),
                    "industry": str((member_meta.get(ts_code, {}) or {}).get("industry") or ""),
                    "board_segment": str((member_meta.get(ts_code, {}) or {}).get("board_segment") or "unknown"),
                    "concept_boards": list((member_meta.get(ts_code, {}) or {}).get("concept_boards") or []),
                    "core_concept_board": str((member_meta.get(ts_code, {}) or {}).get("core_concept_board") or ""),
                    "concept_state": str((((member_meta.get(ts_code, {}) or {}).get("concept_ecology") or {}).get("state") or "")),
                    "concept_score": round(float((((member_meta.get(ts_code, {}) or {}).get("concept_ecology") or {}).get("score") or 0.0)), 2),
                }
            )
        if last_rebuild_at >= today_ts and last_rebuild_transition in ("observe->holding(rebuild_after_partial)", "holding->holding(rebuild)"):
            transition_summary["rebuild_today"] += 1
            transition_summary["transition_today"] += 1
            recent_transitions.append(
                {
                    "ts_code": ts_code,
                    "name": str(row.get("name") or ""),
                    "transition": last_rebuild_transition,
                    "label": "减仓后回补" if last_rebuild_transition == "observe->holding(rebuild_after_partial)" else "持仓->回补",
                    "at": last_rebuild_at,
                    "at_iso": datetime.datetime.fromtimestamp(last_rebuild_at).isoformat() if last_rebuild_at > 0 else None,
                    "signal_type": str(pos.get("last_rebuild_type") or ""),
                    "signal_price": float(pos.get("last_rebuild_price", 0.0) or 0.0),
                    "rebuild_after_partial": True,
                    "rebuild_from_partial_count": int(pos.get("last_rebuild_from_partial_count", 0) or 0),
                    "rebuild_from_partial_ratio": round(float(pos.get("last_rebuild_from_partial_ratio", 0.0) or 0.0), 4),
                    "rebuild_add_ratio": last_rebuild_add_ratio,
                    "rebuild_add_ratio_base": round(float(pos.get("last_rebuild_add_ratio_base", 0.0) or 0.0), 4),
                    "rebuild_add_ratio_bias": round(float(pos.get("last_rebuild_add_ratio_bias", 0.0) or 0.0), 4),
                    "rebuild_from_partial_source": str(pos.get("last_rebuild_from_partial_source") or ""),
                    "rebuild_from_partial_avwap_tier": str(pos.get("last_rebuild_from_partial_avwap_tier") or ""),
                    "current_position_status": position_status,
                    "current_position_ratio": round(float(row.get("pool1_position_ratio", 0.0) or 0.0), 4),
                    "current_decision": decision,
                    "current_decision_label": str(row.get("pool1_decision_label") or "建议观望"),
                    "current_decision_mode": mode,
                    "current_decision_strength": round(float(row.get("pool1_decision_strength", 0.0) or 0.0), 1),
                    "current_signal_types": list(row.get("pool1_decision_signal_types") or []),
                    "holding_days": round(float(row.get("pool1_holding_days", 0.0) or 0.0), 4),
                    "industry": str((member_meta.get(ts_code, {}) or {}).get("industry") or ""),
                    "board_segment": str((member_meta.get(ts_code, {}) or {}).get("board_segment") or "unknown"),
                    "concept_boards": list((member_meta.get(ts_code, {}) or {}).get("concept_boards") or []),
                    "core_concept_board": str((member_meta.get(ts_code, {}) or {}).get("core_concept_board") or ""),
                    "concept_state": str((((member_meta.get(ts_code, {}) or {}).get("concept_ecology") or {}).get("state") or "")),
                    "concept_score": round(float((((member_meta.get(ts_code, {}) or {}).get("concept_ecology") or {}).get("score") or 0.0)), 2),
                }
            )
        if last_reduce_at >= today_ts:
            transition_summary["partial_today"] += 1
            if reduce_streak >= 2:
                transition_summary["chained_partial_today"] += 1
            transition_summary["transition_today"] += 1
            recent_transitions.append(
                {
                    "ts_code": ts_code,
                    "name": str(row.get("name") or ""),
                    "transition": "holding->holding(partial_chain)" if reduce_streak >= 2 else "holding->holding(partial)",
                    "label": "连续减仓" if reduce_streak >= 2 else "持仓->减仓",
                    "at": last_reduce_at,
                    "at_iso": datetime.datetime.fromtimestamp(last_reduce_at).isoformat() if last_reduce_at > 0 else None,
                    "signal_type": str(pos.get("last_reduce_type") or ""),
                    "signal_price": float(pos.get("last_reduce_price", 0.0) or 0.0),
                    "reduce_ratio": round(float(pos.get("last_reduce_ratio", 0.0) or 0.0), 4),
                    "reduce_source": str(pos.get("last_reduce_source") or ""),
                    "reduce_avwap_tier": str(pos.get("last_reduce_avwap_tier") or ""),
                    "reduce_streak": reduce_streak,
                    "reduce_ratio_cum": reduce_ratio_cum,
                    "current_position_status": position_status,
                    "current_position_ratio": round(float(row.get("pool1_position_ratio", 0.0) or 0.0), 4),
                    "current_decision": decision,
                    "current_decision_label": str(row.get("pool1_decision_label") or "建议观望"),
                    "current_decision_mode": mode,
                    "current_decision_strength": round(float(row.get("pool1_decision_strength", 0.0) or 0.0), 1),
                    "current_signal_types": list(row.get("pool1_decision_signal_types") or []),
                    "holding_days": round(float(row.get("pool1_holding_days", 0.0) or 0.0), 4),
                    "industry": str((member_meta.get(ts_code, {}) or {}).get("industry") or ""),
                    "board_segment": str((member_meta.get(ts_code, {}) or {}).get("board_segment") or "unknown"),
                    "concept_boards": list((member_meta.get(ts_code, {}) or {}).get("concept_boards") or []),
                    "core_concept_board": str((member_meta.get(ts_code, {}) or {}).get("core_concept_board") or ""),
                    "concept_state": str((((member_meta.get(ts_code, {}) or {}).get("concept_ecology") or {}).get("state") or "")),
                    "concept_score": round(float((((member_meta.get(ts_code, {}) or {}).get("concept_ecology") or {}).get("score") or 0.0)), 2),
                }
            )
        if last_sell_at >= today_ts:
            transition_summary["clear_today"] += 1
            if last_exit_after_partial or last_exit_partial_count > 0 or str(pos.get("last_sell_type") or "").endswith("partial_exhausted"):
                transition_summary["clear_after_partial_today"] += 1
            transition_summary["transition_today"] += 1
            recent_transitions.append(
                {
                    "ts_code": ts_code,
                    "name": str(row.get("name") or ""),
                    "transition": "holding->observe(after_partial)" if (last_exit_after_partial or last_exit_partial_count > 0 or str(pos.get("last_sell_type") or "").endswith("partial_exhausted")) else "holding->observe",
                    "label": "减仓后清仓" if (last_exit_after_partial or last_exit_partial_count > 0 or str(pos.get("last_sell_type") or "").endswith("partial_exhausted")) else "持仓->观望",
                    "at": last_sell_at,
                    "at_iso": datetime.datetime.fromtimestamp(last_sell_at).isoformat() if last_sell_at > 0 else None,
                    "signal_type": str(pos.get("last_sell_type") or ""),
                    "signal_price": float(pos.get("last_sell_price", 0.0) or 0.0),
                    "exit_after_partial": bool(last_exit_after_partial or last_exit_partial_count > 0 or str(pos.get("last_sell_type") or "").endswith("partial_exhausted")),
                    "exit_partial_count": last_exit_partial_count,
                    "exit_reduce_ratio_cum": last_exit_reduce_ratio_cum,
                    "exit_reduce_source": str(pos.get("last_exit_reduce_source") or ""),
                    "exit_reduce_avwap_tier": str(pos.get("last_exit_reduce_avwap_tier") or ""),
                    "current_position_status": position_status,
                    "current_decision": decision,
                    "current_decision_label": str(row.get("pool1_decision_label") or "建议观望"),
                    "current_decision_mode": mode,
                    "current_decision_strength": round(float(row.get("pool1_decision_strength", 0.0) or 0.0), 1),
                    "current_signal_types": list(row.get("pool1_decision_signal_types") or []),
                    "holding_days": round(float(row.get("pool1_holding_days", 0.0) or 0.0), 4),
                    "industry": str((member_meta.get(ts_code, {}) or {}).get("industry") or ""),
                    "board_segment": str((member_meta.get(ts_code, {}) or {}).get("board_segment") or "unknown"),
                    "concept_boards": list((member_meta.get(ts_code, {}) or {}).get("concept_boards") or []),
                    "core_concept_board": str((member_meta.get(ts_code, {}) or {}).get("core_concept_board") or ""),
                    "concept_state": str((((member_meta.get(ts_code, {}) or {}).get("concept_ecology") or {}).get("state") or "")),
                    "concept_score": round(float((((member_meta.get(ts_code, {}) or {}).get("concept_ecology") or {}).get("score") or 0.0)), 2),
                }
            )

    def _sort_items(items: list[dict]) -> list[dict]:
        return sorted(
            items,
            key=lambda x: (
                -float(x.get("decision_strength", 0.0) or 0.0),
                x.get("ts_code") or "",
            ),
        )

    for bucket in list(decision_examples.keys()):
        decision_examples[bucket] = _sort_items(decision_examples[bucket])
    for bucket in list(mode_examples.keys()):
        mode_examples[bucket] = _sort_items(mode_examples[bucket])
    decision_daily = []
    for k in sorted(daily_map.keys()):
        item = dict(daily_map[k])
        item["net_build_actionable"] = int(item["build_actionable"] - item["clear_actionable"])
        item["actionable_total"] = int(item["build_actionable"] + item["clear_actionable"])
        item["watch_total"] = int(item["build_watch"] + item["clear_watch"])
        item["partial_reduce_ratio_sum"] = round(float(item.get("partial_reduce_ratio_sum", 0.0) or 0.0), 4)
        item["net_exposure_change"] = round(
            float(item["build_actionable"]) - float(item["clear_actionable"] - item["partial_actionable"]) - float(item["partial_reduce_ratio_sum"]),
            4,
        )
        dominant_clear_reason = ""
        dominant_clear_reason_label = ""
        dominant_clear_reason_count = 0
        dominant_clear_reason_actionable = 0
        for key, label in clear_reason_meta:
            total_count = int(item.get(f"{key}_count", 0) or 0)
            actionable_count = int(item.get(f"{key}_actionable", 0) or 0)
            if total_count > dominant_clear_reason_count:
                dominant_clear_reason = key
                dominant_clear_reason_label = label
                dominant_clear_reason_count = total_count
                dominant_clear_reason_actionable = actionable_count
        item["dominant_clear_reason"] = dominant_clear_reason
        item["dominant_clear_reason_label"] = dominant_clear_reason_label
        item["dominant_clear_reason_count"] = dominant_clear_reason_count
        item["dominant_clear_reason_actionable"] = dominant_clear_reason_actionable
        decision_daily.append(item)
    recent_transitions.sort(key=lambda x: (-int(x.get("at", 0) or 0), str(x.get("ts_code") or "")))
    transition_summary["net_build_today"] = int(transition_summary["build_today"] - transition_summary["clear_today"])

    checked_dt = datetime.datetime.now()
    checked_at_ts = int(checked_dt.timestamp())
    rebuild_today_total = int(transition_summary["rebuild_today"])
    rebuild_success_rate = (
        round(float(transition_summary["rebuild_stabilized_today"]) / rebuild_today_total, 4)
        if rebuild_today_total > 0 else 0.0
    )
    rebuild_reduce_again_rate = (
        round(float(transition_summary["rebuild_reduced_again_today"]) / rebuild_today_total, 4)
        if rebuild_today_total > 0 else 0.0
    )
    return {
        "pool_id": 1,
        "provider": provider.name,
        "checked_at": checked_dt.isoformat(),
        "checked_at_ts": checked_at_ts,
        "summary": {
            "member_count": int(total),
            "build_count": int(decision_counts["build"]),
            "hold_count": int(decision_counts["hold"]),
            "clear_count": int(decision_counts["clear"]),
            "observe_count": int(decision_counts["observe"]),
            "actionable_count": int(mode_counts["actionable"]),
            "watch_count": int(mode_counts["watch"]),
            "neutral_count": int(mode_counts["neutral"]),
            "holding_count": int(position_counts["holding"]),
            "observe_position_count": int(position_counts["observe"]),
            "holding_ratio": round((position_counts["holding"] / total), 4) if total > 0 else 0.0,
            "actionable_ratio": round((mode_counts["actionable"] / total), 4) if total > 0 else 0.0,
            "watch_ratio": round((mode_counts["watch"] / total), 4) if total > 0 else 0.0,
            "transition_today": int(transition_summary["transition_today"]),
            "build_today": int(transition_summary["build_today"]),
            "rebuild_today": int(transition_summary["rebuild_today"]),
            "partial_today": int(transition_summary["partial_today"]),
            "chained_partial_today": int(transition_summary["chained_partial_today"]),
            "clear_today": int(transition_summary["clear_today"]),
            "clear_after_partial_today": int(transition_summary["clear_after_partial_today"]),
            "rebuild_stabilized_today": int(transition_summary["rebuild_stabilized_today"]),
            "rebuild_reduced_again_today": int(transition_summary["rebuild_reduced_again_today"]),
            "rebuild_cleared_again_today": int(transition_summary["rebuild_cleared_again_today"]),
            "rebuild_success_rate": rebuild_success_rate,
            "rebuild_reduce_again_rate": rebuild_reduce_again_rate,
            "net_build_today": int(transition_summary["net_build_today"]),
            "left_suppressed_count": int(left_suppressed_count),
            "left_repeat_suppressed_count": int(left_repeat_suppressed_count),
            "concept_retreat_watch_count": int(concept_retreat_watch_count),
        },
        "decision_counts": decision_counts,
        "mode_counts": mode_counts,
        "position_counts": position_counts,
        "transition_summary": transition_summary,
        "recent_transitions": recent_transitions[:20],
        "decision_daily": decision_daily,
        "decision_history_events": history_events[:500],
        "clear_reason_summary": clear_reason_summary,
        "decision_examples": {k: v[:6] for k, v in decision_examples.items()},
        "mode_examples": {k: v[:6] for k, v in mode_examples.items()},
    }
def refresh_daily_cache(pool_id: Optional[int] = None):
    n = _refresh_daily_factors_cache(pool_id)
    return {"refreshed": n, "pool_id": pool_id}
def list_pools():
    with _ro_conn_ctx() as conn:
        result = []
        for pid, info in POOLS.items():
            cnt = conn.execute("SELECT COUNT(*) FROM monitor_pools WHERE pool_id = ?", [pid]).fetchone()[0]
            result.append({**info, "member_count": cnt})
        return {"data": result}
class MemberItem(BaseModel):
    ts_code: str
    name: str
    industry: Optional[str] = None
    note: Optional[str] = None
def get_members(pool_id: int):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    with _ro_conn_ctx() as conn:
        rows = conn.execute(
            """
            SELECT ts_code, name, industry, added_at, note, sort_order
            FROM monitor_pools
            WHERE pool_id = ?
            ORDER BY sort_order, added_at
            """,
            [pool_id],
        ).fetchall()
        stock_concepts_map: dict[str, list[str]] = {}
        core_concept_map: dict[str, str] = {}
        stock_concept_code_map: dict[str, list[str]] = {}
        core_concept_code_map: dict[str, str] = {}
        if rows:
            try:
                with _data_ro_conn_ctx() as data_conn:
                    (
                        stock_concepts_map,
                        core_concept_map,
                        _concept_snapshot_map,
                        stock_concept_code_map,
                        core_concept_code_map,
                    ) = _load_concept_board_maps(
                        data_conn,
                        [str(r[0] or "") for r in rows],
                    )
            except Exception:
                stock_concepts_map = {}
                core_concept_map = {}
                stock_concept_code_map = {}
                core_concept_code_map = {}
        data = []
        for r in rows:
            ts_code = str(r[0] or "")
            data.append(
                {
                    "ts_code": ts_code,
                    "name": r[1],
                    "industry": r[2],
                    "added_at": r[3].isoformat() if hasattr(r[3], "isoformat") else str(r[3]) if r[3] else None,
                    "note": r[4],
                    "sort_order": r[5],
                    "concept_boards": list(stock_concepts_map.get(ts_code, []) or []),
                    "concept_codes": list(stock_concept_code_map.get(ts_code, []) or []),
                    "core_concept_board": str(core_concept_map.get(ts_code, "") or ""),
                    "core_concept_code": str(core_concept_code_map.get(ts_code, "") or ""),
                }
            )
        return {"pool_id": pool_id, "count": len(data), "data": data}
def add_member(pool_id: int, item: MemberItem):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        exists = conn.execute(
            "SELECT 1 FROM monitor_pools WHERE pool_id = ? AND ts_code = ? LIMIT 1",
            [pool_id, item.ts_code],
        ).fetchone()
        if exists:
            return {"ok": False, "msg": f"{item.ts_code} already exists in pool {pool_id}"}
        conn.execute(
            """
            INSERT INTO monitor_pools (pool_id, ts_code, name, industry, added_at, note, sort_order)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 0)
            """,
            [pool_id, item.ts_code, item.name, item.industry, item.note],
        )
        _refresh_daily_factors_cache(pool_id=None)
        return {"ok": True, "msg": "added"}
    finally:
        conn.close()
def remove_member(pool_id: int, ts_code: str):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM monitor_pools WHERE pool_id = ? AND ts_code = ?", [pool_id, ts_code])
        _refresh_daily_factors_cache(pool_id=None)
        return {"ok": True, "msg": "removed"}
    finally:
        conn.close()
def update_note(pool_id: int, ts_code: str, note: str = ""):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE monitor_pools SET note = ? WHERE pool_id = ? AND ts_code = ?",
            [note, pool_id, ts_code],
        )
        return {"ok": True, "msg": "updated"}
    finally:
        conn.close()
def evaluate_signals_fast(pool_id: int):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    with _ro_conn_ctx() as conn:
        members = conn.execute(
            "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
            [pool_id],
        ).fetchall()
    if not members:
        return {"pool_id": pool_id, "count": 0, "data": []}
    provider = get_tick_provider()
    data = _evaluate_signals_fast_internal(pool_id, members, provider)
    return {"pool_id": pool_id, "count": len(data), "data": data}
def evaluate_signals(pool_id: int):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    evaluated = _evaluate_signals_internal(pool_id)
    return {"pool_id": pool_id, "count": len(evaluated), "data": evaluated}
class BatchRequest(BaseModel):
    ts_codes: list[str]
def batch_tick(req: BatchRequest):
    provider = get_tick_provider()
    out = {}
    for ts_code in req.ts_codes:
        try:
            out[ts_code] = provider.get_tick(ts_code)
        except Exception as e:
            out[ts_code] = {"error": str(e)}
    return {"data": out}
def batch_minute(req: BatchRequest, n: int = Query(240, ge=1, le=480)):
    out = {}
    for ts_code in req.ts_codes:
        try:
            out[ts_code] = mootdx_client.get_minute_bars(ts_code, n)
        except Exception:
            out[ts_code] = []
    return {"data": out}
def batch_transactions(req: BatchRequest, n: int = Query(15, ge=1, le=500)):
    provider = get_tick_provider()
    out = {}
    for ts_code in req.ts_codes:
        try:
            txns = provider.get_cached_transactions(ts_code)
            if txns is None:
                txns = mootdx_client.get_transactions(ts_code, n)
            out[ts_code] = txns or []
        except Exception:
            out[ts_code] = []
    return {"data": out}
def get_tick(ts_code: str):
    return get_tick_provider().get_tick(ts_code)
def get_minute(ts_code: str, n: int = Query(240, ge=1, le=480)):
    try:
        return {"ts_code": ts_code, "data": mootdx_client.get_minute_bars(ts_code, n)}
    except Exception:
        return {"ts_code": ts_code, "data": []}
def get_transactions(ts_code: str, n: int = Query(15, ge=1, le=500)):
    provider = get_tick_provider()
    txns = provider.get_cached_transactions(ts_code)
    if txns is None:
        try:
            txns = mootdx_client.get_transactions(ts_code, n)
        except Exception:
            txns = []
    return {"ts_code": ts_code, "data": txns or []}
def market_status():
    return _is_market_open()
def _load_indices_snapshot(force_refresh: bool = False) -> list[dict]:
    global _indices_cache_data, _indices_cache_at
    now = time.time()
    ttl = max(0.2, float(_INDICES_CACHE_TTL_SEC))
    if not force_refresh:
        with _indices_cache_lock:
            if _indices_cache_data and (now - float(_indices_cache_at)) < ttl:
                return list(_indices_cache_data)

    data = []
    now_ts = int(now)
    for ts_code, name in INDICES:
        try:
            source = "mootdx_index"
            try:
                t = mootdx_client.get_index_tick(ts_code)
            except Exception:
                t = {}
                source = "index_fetch_error"

            price = float(t.get("price", 0) or 0)
            pre_close = float(t.get("pre_close", 0) or 0)
            raw_pct_chg = float(t.get("pct_chg", 0) or 0)
            valid = price > 0 and pre_close > 0

            # Avoid stock/index code collision fallback; prefer last-good index cache.
            if not valid:
                cached = _INDEX_LAST_GOOD.get(ts_code)
                if cached and float(cached.get("price", 0) or 0) > 0 and float(cached.get("pre_close", 0) or 0) > 0:
                    t = dict(cached)
                    price = float(t.get("price", 0) or 0)
                    pre_close = float(t.get("pre_close", 0) or 0)
                    raw_pct_chg = float(t.get("pct_chg", 0) or 0)
                    source = "cache_last_good"
                    valid = True

            if pre_close <= 0 and price > 0 and abs(raw_pct_chg) > 1e-9 and abs(raw_pct_chg) < 90:
                try:
                    pre_close = price / (1.0 + raw_pct_chg / 100.0)
                except Exception:
                    pre_close = 0.0
            pct_chg = ((price - pre_close) / pre_close * 100) if (price > 0 and pre_close > 0) else raw_pct_chg
            updated_at = int(t.get("timestamp", 0) or 0)
            if updated_at <= 0:
                updated_at = now_ts

            if price > 0 and pre_close > 0:
                _INDEX_LAST_GOOD[ts_code] = {
                    "price": price,
                    "pre_close": pre_close,
                    "pct_chg": pct_chg,
                    "amount": float(t.get("amount", 0) or 0),
                    "timestamp": updated_at,
                    "is_mock": bool(t.get("is_mock", False)),
                }

            data.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "price": price,
                    "pre_close": pre_close,
                    "pct_chg": pct_chg,
                    "amount": t.get("amount", 0),
                    "is_mock": t.get("is_mock", False),
                    "source": source,
                    "updated_at": updated_at,
                }
            )
        except Exception:
            data.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "price": 0,
                    "pre_close": 0,
                    "pct_chg": 0,
                    "amount": 0,
                    "is_mock": True,
                    "source": "error_fallback",
                    "updated_at": int(time.time()),
                }
            )
    with _indices_cache_lock:
        _indices_cache_data = list(data)
        _indices_cache_at = float(time.time())
    return data
def indices():
    return {"data": _load_indices_snapshot(force_refresh=False)}
def get_signal_history(
    pool_id: int,
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(200, ge=1, le=2000),
):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    since_dt = datetime.datetime.now() - datetime.timedelta(hours=int(hours))
    with _ro_conn_ctx() as conn:
        rows = conn.execute(
            """
            SELECT id, pool_id, ts_code, name, signal_type, channel, signal_source, strength, message, price, pct_chg, triggered_at
            FROM signal_history
            WHERE pool_id = ?
              AND triggered_at >= ?
            ORDER BY triggered_at DESC
            LIMIT ?
            """,
            [pool_id, since_dt, int(limit)],
        ).fetchall()
        data = []
        for r in rows:
            data.append(
                {
                    "id": r[0],
                    "pool_id": r[1],
                    "ts_code": r[2],
                    "name": r[3],
                    "signal_type": r[4],
                    "channel": r[5],
                    "signal_source": r[6],
                    "strength": r[7],
                    "message": r[8],
                    "price": r[9],
                    "pct_chg": r[10],
                    "triggered_at": r[11].isoformat() if hasattr(r[11], "isoformat") else str(r[11]),
                }
            )
        return {"pool_id": pool_id, "hours": hours, "limit": limit, "count": len(data), "data": data}
def get_t0_quality_summary(hours: int = Query(24, ge=1, le=168)):
    with _ro_conn_ctx() as conn:
        h = int(hours)
        rows = conn.execute(
            f"""
            SELECT signal_type, market_phase, eval_horizon_sec, ret_bps, mfe_bps, mae_bps, direction_correct,
                   COALESCE(channel, '') AS channel,
                   COALESCE(signal_source, '') AS signal_source
            FROM t0_signal_quality
            WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '{h} hours'
            """
        ).fetchall()
        df_quality = pd.DataFrame(
            rows,
            columns=[
                "signal_type",
                "market_phase",
                "eval_horizon_sec",
                "ret_bps",
                "mfe_bps",
                "mae_bps",
                "direction_correct",
                "channel",
                "signal_source",
            ],
        )

        sig_rows = conn.execute(
            f"""
            SELECT ts_code, signal_type, triggered_at
            FROM signal_history
            WHERE pool_id = 2
              AND triggered_at >= CURRENT_TIMESTAMP - INTERVAL '{h} hours'
            ORDER BY ts_code, triggered_at
            """
        ).fetchall()
        df_signal = pd.DataFrame(sig_rows, columns=["ts_code", "signal_type", "triggered_at"])
        churn = _calc_churn_stats(df_signal, hours=h)
        return _build_t0_quality_payload(df_quality, hours=h, churn=churn)
def get_t0_drift_status(days: int = Query(3, ge=1, le=30)):
    with _ro_conn_ctx() as conn:
        d = int(days)
        rows = conn.execute(
            f"""
            SELECT checked_at, feature_name, psi, ks_stat, ks_p, severity, drifted, precision_drop, alerted
            FROM t0_feature_drift
            WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '{d} days'
            ORDER BY checked_at DESC
            """
        ).fetchall()
        split_rows = conn.execute(
            f"""
            WITH recent AS (
                SELECT
                    COALESCE(channel, 'unknown') AS channel,
                    COALESCE(signal_source, 'unknown') AS signal_source,
                    COUNT(*) AS recent_cnt,
                    AVG(CASE WHEN direction_correct THEN 1.0 ELSE 0.0 END) AS recent_precision,
                    AVG(ret_bps) AS recent_ret_bps
                FROM t0_signal_quality
                WHERE eval_horizon_sec = 300
                  AND created_at >= CURRENT_TIMESTAMP - INTERVAL '{d} days'
                GROUP BY 1,2
            ),
            baseline AS (
                SELECT
                    COALESCE(channel, 'unknown') AS channel,
                    COALESCE(signal_source, 'unknown') AS signal_source,
                    COUNT(*) AS base_cnt,
                    AVG(CASE WHEN direction_correct THEN 1.0 ELSE 0.0 END) AS base_precision,
                    AVG(ret_bps) AS base_ret_bps
                FROM t0_signal_quality
                WHERE eval_horizon_sec = 300
                  AND created_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '7 days'
                                      AND CURRENT_TIMESTAMP - INTERVAL '3 days'
                GROUP BY 1,2
            )
            SELECT
                COALESCE(r.channel, b.channel) AS channel,
                COALESCE(r.signal_source, b.signal_source) AS signal_source,
                COALESCE(r.recent_cnt, 0) AS recent_cnt,
                r.recent_precision,
                r.recent_ret_bps,
                COALESCE(b.base_cnt, 0) AS base_cnt,
                b.base_precision,
                b.base_ret_bps
            FROM recent r
            FULL OUTER JOIN baseline b
              ON r.channel = b.channel
             AND r.signal_source = b.signal_source
            ORDER BY recent_cnt DESC, base_cnt DESC, channel, signal_source
            """
        ).fetchall()
        quality_split = []
        for sr in split_rows:
            channel = str(sr[0] or "unknown")
            signal_source = str(sr[1] or "unknown")
            recent_cnt = int(sr[2] or 0)
            recent_precision = _round_or_none(sr[3], 4)
            recent_ret_bps = _round_or_none(sr[4], 2)
            base_cnt = int(sr[5] or 0)
            base_precision = _round_or_none(sr[6], 4)
            base_ret_bps = _round_or_none(sr[7], 2)
            precision_drop = None
            if base_precision is not None and recent_precision is not None:
                precision_drop = _round_or_none(base_precision - recent_precision, 4)
            quality_split.append(
                {
                    "channel": channel,
                    "signal_source": signal_source,
                    "recent_count": recent_cnt,
                    "recent_precision_5m": recent_precision,
                    "recent_avg_ret_bps": recent_ret_bps,
                    "baseline_count": base_cnt,
                    "baseline_precision_5m": base_precision,
                    "baseline_avg_ret_bps": base_ret_bps,
                    "precision_drop": precision_drop,
                }
            )
        if not rows:
            return {
                "days": d,
                "total_checks": 0,
                "has_active_alert": False,
                "latest_checked_at": None,
                "latest": {},
                "alerts": 0,
                "quality_split_by_channel_source": quality_split,
            }
        latest_at = rows[0][0]
        latest_rows = [r for r in rows if r[0] == latest_at]
        latest = {}
        for r in latest_rows:
            latest[str(r[1])] = {
                "psi": r[2],
                "ks_stat": r[3],
                "severity": r[5],
                "drifted": bool(r[6]),
                "precision_drop": r[7],
                "alerted": bool(r[8]),
            }
        return {
            "days": d,
            "total_checks": len(rows),
            "has_active_alert": any(bool(r[8]) for r in latest_rows),
            "latest_checked_at": latest_at.isoformat() if hasattr(latest_at, "isoformat") else str(latest_at),
            "latest": latest,
            "alerts": sum(1 for r in rows if bool(r[8])),
            "quality_split_by_channel_source": quality_split,
        }
