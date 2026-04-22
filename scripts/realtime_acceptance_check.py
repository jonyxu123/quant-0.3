"""One-click acceptance checks for realtime monitor main chain.

Usage:
  python scripts/realtime_acceptance_check.py
  python scripts/realtime_acceptance_check.py --base-url http://127.0.0.1:8000 --pool-id 1 --hours 24
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import warnings
from typing import Any, Optional

# Silence known local requests dependency warning noise before importing requests.
warnings.filterwarnings("ignore", category=Warning, module="requests")
import requests
from requests import RequestsDependencyWarning


def _health_level(ok: bool, warn: bool = False) -> str:
    if not ok:
        return "red"
    if warn:
        return "yellow"
    return "green"


def _safe_get(
    session: requests.Session,
    base_url: str,
    path: str,
    params: Optional[dict[str, Any]] = None,
    timeout_sec: float = 8.0,
) -> tuple[bool, Any]:
    url = base_url.rstrip("/") + path
    try:
        resp = session.get(url, params=params or {}, timeout=timeout_sec)
        resp.raise_for_status()
        return True, resp.json()
    except Exception as e:
        return False, {"error": str(e), "url": url, "params": params or {}}


def _is_nonempty_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def main() -> int:
    warnings.filterwarnings("ignore", category=RequestsDependencyWarning)
    ap = argparse.ArgumentParser(description="Realtime monitor one-click acceptance checks")
    ap.add_argument("--base-url", default="http://127.0.0.1:8000", help="API base URL")
    ap.add_argument("--pool-id", type=int, default=1, help="Pool id for fast/slow checks")
    ap.add_argument("--hours", type=int, default=24, help="Window hours for split/trend checks")
    ap.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds")
    args = ap.parse_args()

    base_url = str(args.base_url).rstrip("/")
    pool_id = int(args.pool_id)
    hours = int(args.hours)
    timeout_sec = float(args.timeout)

    checks: list[dict[str, Any]] = []
    session = requests.Session()
    checked_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    ok_ms, market = _safe_get(session, base_url, "/api/realtime/market_status", timeout_sec=timeout_sec)
    market_open = bool(ok_ms and bool((market or {}).get("is_open", False)))

    ok_sc, selfcheck = _safe_get(session, base_url, "/api/realtime/runtime/selfcheck", timeout_sec=timeout_sec)
    checks.append(
        {
            "name": "runtime_selfcheck",
            "ok": bool(ok_sc and bool(selfcheck.get("ok", False))),
            "level": _health_level(bool(ok_sc and bool(selfcheck.get("ok", False)))),
            "detail": selfcheck if ok_sc else selfcheck.get("error"),
        }
    )

    ok_hs, health = _safe_get(session, base_url, "/api/realtime/runtime/health_summary", timeout_sec=timeout_sec)
    hs_level = str((health or {}).get("level", "red")) if ok_hs else "red"
    checks.append(
        {
            "name": "runtime_health_summary",
            "ok": bool(ok_hs and hs_level != "red"),
            "level": hs_level if hs_level in {"green", "yellow", "red"} else "red",
            "detail": {"level": hs_level, "summary": (health or {}).get("summary")} if ok_hs else health.get("error"),
        }
    )

    ok_mem, members = _safe_get(session, base_url, f"/api/realtime/pool/{pool_id}/members", timeout_sec=timeout_sec)
    member_count = int(len((members or {}).get("data", []))) if ok_mem else 0
    checks.append(
        {
            "name": "pool_members",
            "ok": member_count > 0,
            "level": _health_level(member_count > 0, warn=(member_count > 0 and member_count < 5)),
            "detail": {"pool_id": pool_id, "count": member_count} if ok_mem else members.get("error"),
        }
    )

    ok_sf, sig_fast = _safe_get(session, base_url, f"/api/realtime/pool/{pool_id}/signals_fast", timeout_sec=timeout_sec)
    fast_count = int((sig_fast or {}).get("count", 0) or 0) if ok_sf else 0
    count_warn = ok_sf and member_count > 0 and fast_count != member_count
    checks.append(
        {
            "name": "signals_fast",
            "ok": bool(ok_sf),
            "level": _health_level(bool(ok_sf), warn=bool(count_warn)),
            "detail": {"pool_id": pool_id, "count": fast_count, "member_count": member_count}
            if ok_sf
            else sig_fast.get("error"),
        }
    )

    ok_idx, indices = _safe_get(session, base_url, "/api/realtime/indices", timeout_sec=timeout_sec)
    idx_rows = (indices or {}).get("data", []) if ok_idx else []
    idx_count = int(len(idx_rows)) if ok_idx else 0
    idx_name_ok = bool(ok_idx and idx_rows and all(_is_nonempty_str(x.get("name")) for x in idx_rows))
    idx_pct_bad = 0
    idx_source_err = 0
    if ok_idx:
        for x in idx_rows:
            try:
                price = float(x.get("price", 0) or 0)
                pre_close = float(x.get("pre_close", 0) or 0)
                pct = float(x.get("pct_chg", 0) or 0)
                if pre_close > 0 and price > 0:
                    calc = (price - pre_close) / pre_close * 100
                    if abs(calc - pct) > 0.20:
                        idx_pct_bad += 1
            except Exception:
                idx_pct_bad += 1
            src = str(x.get("source", "") or "")
            if src in {"index_fetch_error", "error_fallback"}:
                idx_source_err += 1
    idx_ok = bool(ok_idx and idx_count >= 6 and idx_name_ok and idx_pct_bad == 0)
    idx_warn = bool(
        ok_idx
        and (
            (idx_count > 0 and idx_count < 6)
            or not idx_name_ok
            or idx_source_err > 0
            or (idx_pct_bad > 0 and idx_pct_bad <= 2)
        )
    )
    checks.append(
        {
            "name": "indices",
            "ok": idx_ok,
            "level": _health_level(idx_ok, warn=idx_warn),
            "detail": {
                "count": idx_count,
                "name_ok": idx_name_ok,
                "pct_inconsistent_count": idx_pct_bad,
                "source_error_count": idx_source_err,
            }
            if ok_idx
            else indices.get("error"),
        }
    )

    ok_split, split_stats = _safe_get(
        session,
        base_url,
        "/api/realtime/runtime/channel_split_stats",
        params={"hours": hours},
        timeout_sec=timeout_sec,
    )
    split_ok = bool(ok_split and bool(split_stats.get("ok", False)))
    checks.append(
        {
            "name": "channel_split_stats",
            "ok": split_ok,
            "level": _health_level(split_ok),
            "detail": split_stats.get("summary") if ok_split else split_stats.get("error"),
        }
    )

    ok_diff, diff = _safe_get(
        session,
        base_url,
        "/api/realtime/runtime/fast_slow_diff",
        params={"pool_id": pool_id, "limit": 200},
        timeout_sec=timeout_sec,
    )
    mismatch_ratio = float(((diff or {}).get("summary") or {}).get("mismatch_ratio", 0.0) or 0.0) if ok_diff else 0.0
    field_missing = ((diff or {}).get("field_missing") or {}) if ok_diff else {}
    missing_total = 0
    if isinstance(field_missing, dict):
        for v in field_missing.values():
            try:
                missing_total += int(v or 0)
            except Exception:
                continue
    diff_ok = bool(ok_diff and mismatch_ratio < 0.10)
    diff_warn = bool(ok_diff and ((0.05 <= mismatch_ratio < 0.10) or (market_open and missing_total > 0)))
    checks.append(
        {
            "name": "fast_slow_diff",
            "ok": diff_ok,
            "level": _health_level(diff_ok, warn=diff_warn),
            "detail": {
                **((diff or {}).get("summary") or {}),
                "field_missing": field_missing,
            }
            if ok_diff
            else diff.get("error"),
        }
    )

    ok_trend, trend = _safe_get(
        session,
        base_url,
        "/api/realtime/runtime/fast_slow_trend",
        params={"pool_id": pool_id, "hours": hours, "limit": 500},
        timeout_sec=timeout_sec,
    )
    trend_count = int((trend or {}).get("count", 0) or 0) if ok_trend else 0
    checks.append(
        {
            "name": "fast_slow_trend",
            "ok": bool(ok_trend),
            "level": _health_level(bool(ok_trend), warn=bool(ok_trend and trend_count == 0)),
            "detail": {"count": trend_count, "storage_source": (trend or {}).get("storage_source")}
            if ok_trend
            else trend.get("error"),
        }
    )

    if pool_id == 1:
        ok_obs, obs = _safe_get(session, base_url, "/api/realtime/pool/1/observe_stats", timeout_sec=timeout_sec)
        obs_supported = bool(ok_obs and bool(obs.get("supported", False)))
        obs_data = ((obs or {}).get("data") or {}) if ok_obs else {}
        obs_storage = ((obs or {}).get("storage_check") or {}) if ok_obs else {}
        obs_sample = int(obs_data.get("screen_total", 0) or 0) if ok_obs else 0
        redis_enabled = bool(obs_storage.get("redis_enabled", obs_data.get("redis_enabled", False))) if ok_obs else False
        redis_ready = bool(obs_storage.get("redis_ready", obs_data.get("redis_ready", False))) if ok_obs else False
        storage_source = str(obs_storage.get("source", obs_data.get("storage_source", "memory"))) if ok_obs else "unknown"
        storage_expected = str(obs_storage.get("expected", obs_data.get("storage_expected", "memory"))) if ok_obs else "unknown"
        storage_verified = bool(obs_storage.get("verified", obs_data.get("storage_verified", False))) if ok_obs else False
        storage_degraded = bool(obs_storage.get("degraded", obs_data.get("storage_degraded", False))) if ok_obs else False
        storage_note = str(obs_storage.get("note", obs_data.get("storage_note", ""))) if ok_obs else ""
        strict_storage_fail = bool(redis_enabled and market_open and obs_sample > 0 and not storage_verified)
        obs_ok = bool(obs_supported and not strict_storage_fail)
        obs_warn = bool(ok_obs and (not obs_supported or storage_degraded or (redis_enabled and not redis_ready)))
        checks.append(
            {
                "name": "pool1_observe_stats",
                "ok": obs_ok,
                "level": _health_level(obs_ok, warn=obs_warn),
                "detail": {
                    "supported": bool(obs.get("supported", False)) if ok_obs else False,
                    "sample": obs_sample if ok_obs else None,
                    "redis_enabled": redis_enabled if ok_obs else None,
                    "redis_ready": redis_ready if ok_obs else None,
                    "storage_source": storage_source if ok_obs else None,
                    "storage_expected": storage_expected if ok_obs else None,
                    "storage_verified": storage_verified if ok_obs else None,
                    "storage_degraded": storage_degraded if ok_obs else None,
                    "storage_note": storage_note if ok_obs else None,
                }
                if ok_obs
                else obs.get("error"),
            }
        )

    reds = sum(1 for x in checks if x["level"] == "red")
    yellows = sum(1 for x in checks if x["level"] == "yellow")
    payload = {
        "ok": reds == 0,
        "checked_at": checked_at,
        "base_url": base_url,
        "pool_id": pool_id,
        "hours": hours,
        "market": market if ok_ms else {"is_open": None, "status": "unknown"},
        "summary": {
            "green": sum(1 for x in checks if x["level"] == "green"),
            "yellow": yellows,
            "red": reds,
        },
        "checks": checks,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
