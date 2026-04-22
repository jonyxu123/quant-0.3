"""Open-session regression checks for realtime monitor.

Focus:
1) Intraday/open profile thresholds.
2) Signal payload field completeness.
3) Fast/slow path consistency + missing-field diagnostics.
4) Pool1 observe storage source verification.

Usage:
  python scripts/realtime_open_session_regression.py
  python scripts/realtime_open_session_regression.py --simulate-open
  python scripts/realtime_open_session_regression.py --allow-closed --expected-phase open
"""

from __future__ import annotations

import argparse
import json
import time
import warnings
from typing import Any, Optional

warnings.filterwarnings("ignore", category=Warning, module="requests")
import requests
from requests import RequestsDependencyWarning


def _level(ok: bool, warn: bool = False) -> str:
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
    timeout_sec: float = 20.0,
    retries: int = 1,
) -> tuple[bool, Any]:
    url = base_url.rstrip("/") + path
    last_err: Exception | None = None
    total = max(1, int(retries) + 1)
    for i in range(total):
        try:
            resp = session.get(url, params=params or {}, timeout=timeout_sec)
            resp.raise_for_status()
            return True, resp.json()
        except Exception as e:
            last_err = e
            if i < total - 1:
                time.sleep(0.25 * (i + 1))
                continue
    return False, {"error": str(last_err), "url": url, "params": params or {}}


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _parse_pool_ids(raw: str) -> list[int]:
    out: list[int] = []
    for p in str(raw or "").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            v = int(p)
        except Exception:
            continue
        if v in (1, 2):
            out.append(v)
    return out or [1, 2]


def main() -> int:
    warnings.filterwarnings("ignore", category=RequestsDependencyWarning)
    ap = argparse.ArgumentParser(description="Open-session regression checks")
    ap.add_argument("--base-url", default="http://127.0.0.1:8000", help="API base URL")
    ap.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds")
    ap.add_argument("--pool-ids", default="1,2", help="Comma separated pool ids, e.g. 1,2")
    ap.add_argument("--expected-phase", default="open", help="Expected threshold market_phase for active signals")
    ap.add_argument("--hours", type=int, default=6, help="Window hours for split/trend checks")
    ap.add_argument("--limit", type=int, default=200, help="Row limit for fast_slow_diff")
    ap.add_argument("--simulate-open", action="store_true", help="Force open-profile branch checks")
    ap.add_argument("--allow-closed", action="store_true", help="Do not fail hard when market is closed")
    args = ap.parse_args()

    base_url = str(args.base_url).rstrip("/")
    timeout_sec = float(args.timeout)
    pool_ids = _parse_pool_ids(args.pool_ids)
    expected_phase = str(args.expected_phase or "").strip().lower() or "open"
    hours = max(1, int(args.hours))
    limit = max(20, int(args.limit))

    checked_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    session = requests.Session()
    checks: list[dict[str, Any]] = []

    ok_ms, market = _safe_get(session, base_url, "/api/realtime/market_status", timeout_sec=timeout_sec)
    market_open = bool(ok_ms and bool((market or {}).get("is_open", False)))
    effective_open = bool(args.simulate_open or market_open)
    if not ok_ms:
        checks.append(
            {
                "name": "market_status",
                "ok": False,
                "level": "red",
                "detail": market.get("error"),
            }
        )
    else:
        if market_open:
            checks.append(
                {
                    "name": "market_status",
                    "ok": True,
                    "level": "green",
                    "detail": market,
                }
            )
        else:
            closed_ok = bool(args.allow_closed or args.simulate_open)
            checks.append(
                {
                    "name": "market_status",
                    "ok": closed_ok,
                    "level": _level(closed_ok, warn=closed_ok),
                    "detail": {
                        **(market or {}),
                        "note": "market closed, using simulated-open branch"
                        if args.simulate_open
                        else "market closed",
                    },
                }
            )

    # Open profile health branch.
    ok_h, health = _safe_get(
        session,
        base_url,
        "/api/realtime/runtime/health_summary",
        params={"profile": "open", "simulate_open": "true" if effective_open else "false"},
        timeout_sec=timeout_sec,
    )
    h_level = str((health or {}).get("level", "red")) if ok_h else "red"
    checks.append(
        {
            "name": "health_open_profile",
            "ok": bool(ok_h and h_level != "red"),
            "level": h_level if h_level in {"green", "yellow", "red"} else "red",
            "detail": {"level": h_level, "summary": (health or {}).get("summary")} if ok_h else health.get("error"),
        }
    )

    # Closed-loop threshold engine runtime state + snapshot.
    ok_mx, metrics = _safe_get(session, base_url, "/api/realtime/runtime/db_metrics", timeout_sec=timeout_sec)
    t_enabled = bool(((metrics or {}).get("data") or {}).get("threshold_closed_loop_enabled", False)) if ok_mx else False
    t_started = bool(((metrics or {}).get("data") or {}).get("threshold_closed_loop_started", False)) if ok_mx else False
    checks.append(
        {
            "name": "threshold_closed_loop_state",
            "ok": bool(ok_mx and ((not t_enabled) or t_started)),
            "level": _level(bool(ok_mx and ((not t_enabled) or t_started))),
            "detail": {
                "enabled": t_enabled,
                "started": t_started,
            }
            if ok_mx
            else metrics.get("error"),
        }
    )

    ok_ts, t_snap = _safe_get(session, base_url, "/api/realtime/runtime/threshold_calibration_snapshot", timeout_sec=timeout_sec)
    t_ok = bool(ok_ts and bool(t_snap.get("ok", False)))
    t_rules = _to_int(((t_snap or {}).get("summary") or {}).get("rule_count", 0), 0)
    t_warn = bool(ok_ts and not t_ok)
    if ok_ts and t_ok and t_rules == 0:
        # Can happen when sample is insufficient; keep as warning.
        t_warn = True
    checks.append(
        {
            "name": "threshold_calibration_snapshot",
            "ok": bool(ok_ts),
            "level": _level(bool(ok_ts), warn=t_warn),
            "detail": {
                "ok": t_ok,
                "rule_count": t_rules,
                "storage_source": (t_snap or {}).get("storage_source"),
                "checked_at": (t_snap or {}).get("checked_at"),
            }
            if ok_ts
            else t_snap.get("error"),
        }
    )

    # Channel split conflict.
    ok_sp, split = _safe_get(
        session,
        base_url,
        "/api/realtime/runtime/channel_split_stats",
        params={"hours": hours},
        timeout_sec=timeout_sec,
    )
    conflicts = _to_int(((split or {}).get("summary") or {}).get("conflicts", 0), 0) if ok_sp else 0
    checks.append(
        {
            "name": "channel_split_conflict",
            "ok": bool(ok_sp and conflicts == 0),
            "level": _level(bool(ok_sp and conflicts == 0), warn=bool(ok_sp and conflicts > 0)),
            "detail": (split or {}).get("summary") if ok_sp else split.get("error"),
        }
    )

    # Pool checks.
    for pool_id in pool_ids:
        ok_mem, members = _safe_get(session, base_url, f"/api/realtime/pool/{pool_id}/members", timeout_sec=timeout_sec)
        member_count = _to_int((members or {}).get("count", 0), 0) if ok_mem else 0
        checks.append(
            {
                "name": f"pool{pool_id}_members",
                "ok": bool(ok_mem and member_count > 0),
                "level": _level(bool(ok_mem and member_count > 0), warn=bool(ok_mem and 0 < member_count < 5)),
                "detail": {"count": member_count} if ok_mem else members.get("error"),
            }
        )

        ok_fast, sig_fast = _safe_get(session, base_url, f"/api/realtime/pool/{pool_id}/signals_fast", timeout_sec=timeout_sec)
        rows = (sig_fast or {}).get("data", []) if ok_fast else []
        count = _to_int((sig_fast or {}).get("count", 0), 0) if ok_fast else 0

        required_row_keys = {"ts_code", "name", "price", "pct_chg", "signals"}
        bad_rows = 0
        active_signals = 0
        phase_missing = 0
        phase_mismatch = 0
        threshold_missing = 0
        for r in rows if isinstance(rows, list) else []:
            if not isinstance(r, dict):
                bad_rows += 1
                continue
            if not required_row_keys.issubset(set(r.keys())):
                bad_rows += 1
            for s in r.get("signals", []) or []:
                if not isinstance(s, dict):
                    continue
                if s.get("has_signal") is False:
                    continue
                active_signals += 1
                details = s.get("details") or {}
                if not isinstance(details, dict):
                    threshold_missing += 1
                    continue
                th = details.get("threshold")
                if not isinstance(th, dict):
                    threshold_missing += 1
                    continue
                phase = str(th.get("market_phase") or "").strip().lower()
                if not phase:
                    phase_missing += 1
                elif expected_phase and phase != expected_phase and effective_open:
                    phase_mismatch += 1

        count_warn = bool(ok_fast and member_count > 0 and count != member_count)
        shape_ok = bool(ok_fast and bad_rows == 0)
        checks.append(
            {
                "name": f"pool{pool_id}_signals_fast_shape",
                "ok": shape_ok,
                "level": _level(shape_ok, warn=count_warn),
                "detail": {
                    "count": count,
                    "member_count": member_count,
                    "bad_rows": bad_rows,
                    "active_signals": active_signals,
                }
                if ok_fast
                else sig_fast.get("error"),
            }
        )

        # Phase/threshold completeness for active signals.
        if active_signals == 0:
            checks.append(
                {
                    "name": f"pool{pool_id}_active_signal_threshold_phase",
                    "ok": True,
                    "level": "yellow",
                    "detail": {
                        "active_signals": 0,
                        "note": "no active signals in current snapshot",
                    },
                }
            )
        else:
            phase_ok = (threshold_missing == 0 and phase_missing == 0 and phase_mismatch == 0)
            phase_warn = bool((threshold_missing > 0 or phase_missing > 0 or phase_mismatch > 0) and not effective_open)
            checks.append(
                {
                    "name": f"pool{pool_id}_active_signal_threshold_phase",
                    "ok": phase_ok,
                    "level": _level(phase_ok, warn=phase_warn),
                    "detail": {
                        "active_signals": active_signals,
                        "expected_phase": expected_phase,
                        "threshold_missing": threshold_missing,
                        "phase_missing": phase_missing,
                        "phase_mismatch": phase_mismatch,
                    },
                }
            )

        ok_diff, diff = _safe_get(
            session,
            base_url,
            "/api/realtime/runtime/fast_slow_diff",
            params={"pool_id": pool_id, "limit": limit},
            timeout_sec=timeout_sec,
        )
        ratio = _to_float(((diff or {}).get("summary") or {}).get("mismatch_ratio", 0.0), 0.0) if ok_diff else 0.0
        field_missing = (diff or {}).get("field_missing", {}) if ok_diff else {}
        missing_total = 0
        if isinstance(field_missing, dict):
            for v in field_missing.values():
                missing_total += _to_int(v, 0)
        diff_ok = bool(ok_diff and ratio < 0.10 and ((not effective_open) or missing_total == 0))
        diff_warn = bool(ok_diff and ((0.05 <= ratio < 0.10) or (missing_total > 0 and not effective_open)))
        checks.append(
            {
                "name": f"pool{pool_id}_fast_slow_consistency",
                "ok": diff_ok,
                "level": _level(diff_ok, warn=diff_warn),
                "detail": {
                    **(((diff or {}).get("summary") or {}) if ok_diff else {}),
                    "field_missing": field_missing,
                    "effective_open": effective_open,
                }
                if ok_diff
                else diff.get("error"),
            }
        )

    # Pool1 observe storage source verification.
    ok_obs, obs = _safe_get(session, base_url, "/api/realtime/pool/1/observe_stats", timeout_sec=timeout_sec)
    if ok_obs and bool(obs.get("supported", False)):
        data = (obs or {}).get("data") or {}
        sc = (obs or {}).get("storage_check") or {}
        sample = _to_int(data.get("screen_total", 0), 0)
        redis_enabled = bool(sc.get("redis_enabled", data.get("redis_enabled", False)))
        redis_ready = bool(sc.get("redis_ready", data.get("redis_ready", False)))
        verified = bool(sc.get("verified", data.get("storage_verified", False)))
        degraded = bool(sc.get("degraded", data.get("storage_degraded", False)))
        strict_fail = bool(effective_open and sample > 0 and redis_enabled and not verified)
        warn = bool(degraded or (redis_enabled and not redis_ready))
        checks.append(
            {
                "name": "pool1_observe_storage_check",
                "ok": not strict_fail,
                "level": _level(not strict_fail, warn=warn),
                "detail": {
                    "sample": sample,
                    "redis_enabled": redis_enabled,
                    "redis_ready": redis_ready,
                    "verified": verified,
                    "degraded": degraded,
                    "expected": sc.get("expected", data.get("storage_expected")),
                    "source": sc.get("source", data.get("storage_source")),
                    "note": sc.get("note", data.get("storage_note")),
                },
            }
        )
    else:
        checks.append(
            {
                "name": "pool1_observe_storage_check",
                "ok": False,
                "level": "red",
                "detail": obs.get("error") if not ok_obs else {"supported": False},
            }
        )

    reds = sum(1 for x in checks if x.get("level") == "red")
    yellows = sum(1 for x in checks if x.get("level") == "yellow")
    payload = {
        "ok": reds == 0,
        "checked_at": checked_at,
        "mode": "open_session_regression",
        "base_url": base_url,
        "market": market if ok_ms else {"is_open": None, "status": "unknown"},
        "effective_open": effective_open,
        "expected_phase": expected_phase,
        "summary": {
            "green": sum(1 for x in checks if x.get("level") == "green"),
            "yellow": yellows,
            "red": reds,
        },
        "checks": checks,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
