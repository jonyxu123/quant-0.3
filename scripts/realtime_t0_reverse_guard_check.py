"""T+0 reverse_t trend-guard acceptance checks.

Focus:
1) reverse_t 主升浪门控字段完整性
2) 诱多/量价背离增强链路是否打通
3) 反T当前候选/门控/触发分布

Usage:
  python scripts/realtime_t0_reverse_guard_check.py
  python scripts/realtime_t0_reverse_guard_check.py --base-url http://127.0.0.1:8000 --limit 80
  python scripts/realtime_t0_reverse_guard_check.py --all
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
) -> tuple[bool, Any]:
    url = base_url.rstrip("/") + path
    try:
        resp = session.get(url, params=params or {}, timeout=timeout_sec)
        resp.raise_for_status()
        return True, resp.json()
    except Exception as e:
        return False, {"error": str(e), "url": url, "params": params or {}}


def _as_dict(v: Any) -> dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _as_list(v: Any) -> list[Any]:
    return v if isinstance(v, list) else []


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def main() -> int:
    warnings.filterwarnings("ignore", category=RequestsDependencyWarning)
    ap = argparse.ArgumentParser(description="T+0 reverse_t trend-guard acceptance checks")
    ap.add_argument("--base-url", default="http://127.0.0.1:8000", help="API base URL")
    ap.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds")
    ap.add_argument("--limit", type=int, default=60, help="Diagnostic row limit")
    ap.add_argument("--all", action="store_true", help="Include idle rows")
    ap.add_argument("--hours", type=int, default=24, help="Window hours for quality summary")
    args = ap.parse_args()

    base_url = str(args.base_url).rstrip("/")
    timeout_sec = float(args.timeout)
    limit = max(1, int(args.limit))
    interesting_only = not bool(args.all)
    hours = max(1, int(args.hours))

    session = requests.Session()
    checked_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    checks: list[dict[str, Any]] = []

    ok_market, market = _safe_get(session, base_url, "/api/realtime/market_status", timeout_sec=timeout_sec)
    market_open = bool(ok_market and bool(_as_dict(market).get("is_open", False)))
    checks.append(
        {
            "name": "market_status",
            "ok": bool(ok_market),
            "level": _level(bool(ok_market), warn=bool(ok_market and not market_open)),
            "detail": market if ok_market else market.get("error"),
        }
    )

    ok_diag, diag = _safe_get(
        session,
        base_url,
        "/api/realtime/runtime/t0_reverse_diagnostics",
        params={"limit": limit, "interesting_only": "true" if interesting_only else "false"},
        timeout_sec=timeout_sec,
    )
    diag_rows = _as_list(_as_dict(diag).get("rows"))
    diag_summary = _as_dict(_as_dict(diag).get("summary"))
    diag_ok = bool(ok_diag and _as_dict(diag).get("ok", False))
    checks.append(
        {
            "name": "reverse_diag_endpoint",
            "ok": diag_ok,
            "level": _level(diag_ok),
            "detail": diag_summary if diag_ok else diag.get("error"),
        }
    )

    guard_rows = [
        r for r in diag_rows
        if str(_as_dict(r).get("status") or "") in {"guard_veto", "surge_guard_veto"}
    ]
    triggered_rows = [
        r for r in diag_rows
        if str(_as_dict(r).get("status") or "") in {"triggered", "trigger_observe_only"}
    ]

    field_bad = 0
    guard_bad = 0
    for row in diag_rows:
        r = _as_dict(row)
        if not _as_dict(r.get("feature_snapshot")):
            field_bad += 1
        if not _as_dict(r.get("market_structure")):
            field_bad += 1
        if not _as_dict(r.get("momentum_divergence")):
            field_bad += 1
        if not isinstance(r.get("trigger_items"), list):
            field_bad += 1
        if not isinstance(r.get("confirm_items"), list):
            field_bad += 1
    for row in guard_rows:
        r = _as_dict(row)
        if str(r.get("veto") or "") not in {"trend_extension_guard", "trend_surge_absorb_guard"}:
            guard_bad += 1
            continue
        tg = _as_dict(r.get("trend_guard"))
        if not bool(tg.get("guard", tg.get("active", False))):
            guard_bad += 1

    field_ok = bool(diag_ok and field_bad == 0)
    field_warn = bool(diag_ok and field_bad > 0 and field_bad <= 3)
    checks.append(
        {
            "name": "reverse_diag_fields",
            "ok": field_ok,
            "level": _level(field_ok, warn=field_warn),
            "detail": {
                "row_count": len(diag_rows),
                "field_bad": field_bad,
            } if diag_ok else "diag_unavailable",
        }
    )

    guard_ok = bool(diag_ok and guard_bad == 0)
    guard_warn = bool(diag_ok and len(guard_rows) == 0)
    checks.append(
        {
            "name": "trend_guard_semantics",
            "ok": guard_ok,
            "level": _level(guard_ok, warn=guard_warn),
            "detail": {
                "guard_veto_rows": len(guard_rows),
                "surge_guard_veto_rows": sum(
                    1 for r in guard_rows if str(_as_dict(r).get("status") or "") == "surge_guard_veto"
                ),
                "off_session_skip_rows": sum(
                    1 for r in diag_rows if str(_as_dict(r).get("status") or "") == "off_session_skip"
                ),
                "guard_bad_rows": guard_bad,
                "triggered_rows": len(triggered_rows),
            } if diag_ok else "diag_unavailable",
        }
    )

    ok_quality, quality = _safe_get(
        session,
        base_url,
        "/api/realtime/pool/2/quality_summary",
        params={"hours": hours},
        timeout_sec=timeout_sec,
    )
    q_ok = bool(ok_quality)
    q_summary = {
        "signal_count": _to_int(_as_dict(quality).get("signal_count", 0), 0),
        "quality_rows": _to_int(_as_dict(quality).get("quality_rows", 0), 0),
        "split_count": len(_as_list(_as_dict(quality).get("quality_split_by_channel_source"))),
    } if q_ok else quality.get("error")
    checks.append(
        {
            "name": "t0_quality_summary",
            "ok": q_ok,
            "level": _level(q_ok),
            "detail": q_summary,
        }
    )

    ok_health, health = _safe_get(
        session,
        base_url,
        "/api/realtime/runtime/health_summary",
        timeout_sec=timeout_sec,
    )
    h_level = str(_as_dict(health).get("level", "red")) if ok_health else "red"
    checks.append(
        {
            "name": "runtime_health_summary",
            "ok": bool(ok_health and h_level != "red"),
            "level": h_level if h_level in {"green", "yellow", "red"} else "red",
            "detail": _as_dict(health).get("summary") if ok_health else health.get("error"),
        }
    )

    green = sum(1 for c in checks if c.get("level") == "green")
    yellow = sum(1 for c in checks if c.get("level") == "yellow")
    red = sum(1 for c in checks if c.get("level") == "red")
    overall_ok = red == 0
    payload = {
        "ok": overall_ok,
        "checked_at": checked_at,
        "market_open": market_open,
        "summary": {
            "green": green,
            "yellow": yellow,
            "red": red,
        },
        "reverse_diag": {
            "interesting_only": interesting_only,
            "rows_total": _to_int(_as_dict(diag).get("rows_total", 0), 0) if diag_ok else 0,
            "returned_rows": len(diag_rows),
            "summary": diag_summary if diag_ok else {},
        },
        "checks": checks,
        "top_rows": diag_rows[: min(12, len(diag_rows))] if diag_ok else [],
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
