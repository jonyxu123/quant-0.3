"""Backfill approximate reverse_t history snapshots into signal_history.details_json.

Goals:
- Reconstruct compact historical snapshots for old reverse_t rows that were
  persisted before details_json existed.
- Mark reconstructed payloads with snapshot_mode=backfill so the UI can
  distinguish approximate backfills from native persisted snapshots.
- Support batch progress + checkpoint-based resume for long runs.

Usage:
  python scripts/realtime_backfill_reverse_history_snapshot.py --dry-run
  python scripts/realtime_backfill_reverse_history_snapshot.py --hours 168 --limit 500
  python scripts/realtime_backfill_reverse_history_snapshot.py --resume
  python scripts/realtime_backfill_reverse_history_snapshot.py --force --batch-size 500
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from typing import Any, Optional

from config import REALTIME_DB_PATH
from backend.data.duckdb_manager import open_duckdb_conn


_MSG_PREFIX_RE = re.compile(r"^反T卖出\s*[0-9]+(?:\.[0-9]+)?")
_ROBUST_Z_RE = re.compile(r"RobustZ\s*=\s*([+-]?[0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
_FLOAT_RE = re.compile(r"([+-]?[0-9]+(?:\.[0-9]+)?)")
_BPS_RE = re.compile(r"([+-]?[0-9]+(?:\.[0-9]+)?)\s*bps", re.IGNORECASE)
_DEFAULT_CHECKPOINT = os.path.join("tmp", "realtime_reverse_snapshot_backfill.checkpoint.json")


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _ensure_signal_history_snapshot_column(conn) -> None:
    try:
        conn.execute("ALTER TABLE signal_history ADD COLUMN details_json VARCHAR")
    except Exception:
        pass


def _load_checkpoint(path: str) -> dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_checkpoint(path: str, payload: dict[str, Any]) -> None:
    if not path:
        return
    abs_path = os.path.abspath(path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    tmp_path = abs_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, abs_path)


def _split_signal_message(message: str) -> tuple[list[str], list[str], str, bool]:
    msg = str(message or "").strip()
    score_status = ""
    observe_only = False
    if "·仅观察" in msg:
        score_status = "observe"
        observe_only = True
    elif "·可执行" in msg:
        score_status = "executable"
        observe_only = False

    core = msg.split("·", 1)[0].strip() if "·" in msg else msg
    if "| 确认:" in core:
        left, right = core.split("| 确认:", 1)
    elif "|确认:" in core:
        left, right = core.split("|确认:", 1)
    else:
        left, right = core, ""
    left = _MSG_PREFIX_RE.sub("", left).strip()
    trigger_items = [x.strip() for x in re.split(r"\s*\+\s*", left) if x.strip()]
    confirm_items = [x.strip() for x in re.split(r"\s*\+\s*", right) if x.strip()]
    return trigger_items, confirm_items, score_status, observe_only


def _infer_bearish_confirms(confirm_items: list[str]) -> list[str]:
    out: list[str] = []
    for item in confirm_items:
        s = str(item or "")
        if "量价背离" in s:
            out.append("v_power_divergence")
        elif "卖墙堆积" in s:
            out.append("ask_wall_building")
        elif "托盘击穿" in s:
            out.append("bid_wall_break")
        elif "买入力衰减" in s:
            out.append("real_buy_fading")
        elif "超大单偏空" in s:
            out.append("super_order_bias")
        elif "超大单净流出" in s:
            out.append("super_net_flow_bps")
    return out


def _extract_robust_z(trigger_items: list[str]) -> Optional[float]:
    for item in trigger_items:
        m = _ROBUST_Z_RE.search(str(item))
        if m:
            return _as_float(m.group(1))
    return None


def _extract_confirm_metric(confirm_items: list[str], keyword: str, *, use_bps: bool = False) -> Optional[float]:
    for item in confirm_items:
        s = str(item or "")
        if keyword not in s:
            continue
        m = _BPS_RE.search(s) if use_bps else _FLOAT_RE.search(s)
        if m:
            return _as_float(m.group(1))
    return None


def _load_quality_rows(conn, since_dt: Optional[dt.datetime]) -> dict[str, list[dict[str, Any]]]:
    sql = """
        SELECT ts_code, trigger_time, market_phase, channel, signal_source
        FROM t0_signal_quality
        WHERE signal_type = 'reverse_t'
    """
    params: list[Any] = []
    if since_dt is not None:
        sql += " AND created_at >= ?"
        params.append(since_dt)
    sql += " ORDER BY ts_code, trigger_time DESC"
    rows = conn.execute(sql, params).fetchall()
    out: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        trigger_dt = r[1] if isinstance(r[1], dt.datetime) else None
        out.setdefault(str(r[0] or ""), []).append(
            {
                "trigger_time": trigger_dt.timestamp() if trigger_dt else 0.0,
                "trigger_time_iso": trigger_dt.isoformat() if trigger_dt else "",
                "market_phase": str(r[2] or ""),
                "channel": str(r[3] or ""),
                "signal_source": str(r[4] or ""),
            }
        )
    return out


def _nearest_quality(
    quality_by_code: dict[str, list[dict[str, Any]]],
    ts_code: str,
    trigger_ts: float,
    tolerance_sec: float,
) -> Optional[dict[str, Any]]:
    best = None
    best_diff: Optional[float] = None
    for cand in quality_by_code.get(ts_code, []):
        diff = abs(float(cand.get("trigger_time", 0.0) or 0.0) - trigger_ts)
        if diff > tolerance_sec:
            continue
        if best_diff is None or diff < best_diff:
            best = cand
            best_diff = diff
    return best


def _build_backfill_snapshot(row: tuple, quality_hint: Optional[dict[str, Any]]) -> Optional[str]:
    hist_id, ts_code, name, strength, message, price, pct_chg, triggered_at, channel, signal_source = row
    trigger_dt = triggered_at if isinstance(triggered_at, dt.datetime) else None
    trigger_ts = trigger_dt.timestamp() if trigger_dt else 0.0
    trigger_items, confirm_items, score_status, observe_only = _split_signal_message(str(message or ""))
    bearish_confirms = _infer_bearish_confirms(confirm_items)
    robust_z = _extract_robust_z(trigger_items)
    super_order_bias = _extract_confirm_metric(confirm_items, "超大单偏空")
    super_net_flow_bps = _extract_confirm_metric(confirm_items, "超大单净流出", use_bps=True)
    bid_wall_break_ratio = _extract_confirm_metric(confirm_items, "托盘击穿")

    payload = {
        "snapshot_mode": "backfill",
        "snapshot_version": "reverse_t_backfill_v1",
        "snapshot_quality": "approx",
        "backfilled_at": dt.datetime.now().isoformat(),
        "type": "reverse_t",
        "direction": "sell",
        "strength": float(strength or 0),
        "message": str(message or ""),
        "veto": "",
        "observe_only": bool(observe_only),
        "score_status": score_status or ("observe" if bool(observe_only) else "executable"),
        "trigger_items": trigger_items,
        "confirm_items": confirm_items,
        "bearish_confirms": bearish_confirms,
        "volume_pace_ratio": None,
        "volume_pace_state": "",
        "pct_chg": _as_float(pct_chg),
        "bid_ask_ratio": None,
        "robust_zscore": robust_z,
        "v_power_divergence": any("量价背离" in x for x in confirm_items),
        "ask_wall_building": any("卖墙堆积" in x for x in confirm_items),
        "ask_wall_absorb": False,
        "ask_wall_absorb_ratio": None,
        "bid_wall_break": any("托盘击穿" in x for x in confirm_items),
        "bid_wall_break_ratio": bid_wall_break_ratio,
        "real_buy_fading": any("买入力衰减" in x for x in confirm_items),
        "super_order_bias": super_order_bias,
        "super_net_flow_bps": super_net_flow_bps,
        "trend_guard_override": False,
        "threshold": {
            "threshold_version": "backfill-v1",
            "market_phase": str((quality_hint or {}).get("market_phase") or ""),
            "regime": "",
            "observer_only": bool(observe_only),
            "blocked": False,
        },
        "trend_guard": {
            "active": False,
            "trend_up": None,
            "pct_chg": _as_float(pct_chg),
            "required_bearish_confirms": len(bearish_confirms) if bearish_confirms else None,
            "surge_absorb_guard": False,
            "guard_reasons": [],
            "structure_reasons": [],
            "negative_flags": [],
            "bearish_confirms": bearish_confirms,
            "backfill_inferred": True,
        },
        "quality_hint": {
            "matched": bool(quality_hint),
            "trigger_time": trigger_ts,
            "trigger_time_iso": trigger_dt.isoformat() if trigger_dt else "",
            "matched_quality_time_iso": str((quality_hint or {}).get("trigger_time_iso") or ""),
            "market_phase": str((quality_hint or {}).get("market_phase") or ""),
            "channel": str(channel or (quality_hint or {}).get("channel") or ""),
            "signal_source": str(signal_source or (quality_hint or {}).get("signal_source") or ""),
        },
        "backfill_meta": {
            "history_id": int(hist_id),
            "ts_code": str(ts_code or ""),
            "name": str(name or ""),
            "price": _as_float(price),
        },
    }
    try:
        return json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except Exception:
        return None


def _fetch_history_batch(
    conn,
    *,
    since_dt: Optional[dt.datetime],
    force: bool,
    batch_size: int,
    cursor_id_lt: Optional[int],
    min_id_gt: int,
) -> list[tuple]:
    sql = """
        SELECT id, ts_code, name, strength, message, price, pct_chg, triggered_at, channel, signal_source
        FROM signal_history
        WHERE pool_id = 2
          AND signal_type = 'reverse_t'
    """
    params: list[Any] = []
    if not force:
        sql += " AND TRIM(COALESCE(details_json, '')) = ''"
    if since_dt is not None:
        sql += " AND triggered_at >= ?"
        params.append(since_dt)
    if min_id_gt > 0:
        sql += " AND id > ?"
        params.append(min_id_gt)
    if cursor_id_lt is not None:
        sql += " AND id < ?"
        params.append(int(cursor_id_lt))
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(max(1, int(batch_size)))
    return conn.execute(sql, params).fetchall()


def _checkpoint_payload(
    *,
    db_path: str,
    started_at: str,
    last_history_id: int,
    processed: int,
    updated: int,
    matched_quality: int,
    unmatched_quality: int,
    hours: int,
    force: bool,
    batch_size: int,
) -> dict[str, Any]:
    return {
        "db_path": db_path,
        "started_at": started_at,
        "updated_at": dt.datetime.now().isoformat(),
        "last_history_id": int(last_history_id),
        "processed": int(processed),
        "updated": int(updated),
        "matched_quality": int(matched_quality),
        "unmatched_quality": int(unmatched_quality),
        "hours": int(hours),
        "force": bool(force),
        "batch_size": int(batch_size),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill reverse_t history snapshots into signal_history.details_json")
    ap.add_argument("--db-path", default=REALTIME_DB_PATH, help="Realtime DuckDB path")
    ap.add_argument("--hours", type=int, default=0, help="Only backfill rows in recent N hours; 0 means no time filter")
    ap.add_argument("--limit", type=int, default=2000, help="Max history rows to process in this run")
    ap.add_argument("--batch-size", type=int, default=300, help="Batch size per DB roundtrip")
    ap.add_argument("--tolerance-sec", type=float, default=180.0, help="Max quality match time diff")
    ap.add_argument("--force", action="store_true", help="Rebuild even if details_json already exists")
    ap.add_argument("--dry-run", action="store_true", help="Preview only, do not write DB")
    ap.add_argument("--resume", action="store_true", help="Resume from checkpoint file if present")
    ap.add_argument("--checkpoint-file", default=_DEFAULT_CHECKPOINT, help="Checkpoint JSON path")
    ap.add_argument("--reset-checkpoint", action="store_true", help="Ignore and overwrite existing checkpoint")
    args = ap.parse_args()

    since_dt: Optional[dt.datetime] = None
    if int(args.hours) > 0:
        since_dt = dt.datetime.now() - dt.timedelta(hours=int(args.hours))

    checkpoint = {}
    if bool(args.resume) and not bool(args.reset_checkpoint):
        checkpoint = _load_checkpoint(str(args.checkpoint_file))

    started_at = str(checkpoint.get("started_at") or dt.datetime.now().isoformat())
    cursor_id_lt = int(checkpoint.get("last_history_id", 0) or 0) if checkpoint else 0
    cursor_id_lt = cursor_id_lt if cursor_id_lt > 0 else None
    processed_total = int(checkpoint.get("processed", 0) or 0) if checkpoint else 0
    updated_total = int(checkpoint.get("updated", 0) or 0) if checkpoint else 0
    matched_quality_total = int(checkpoint.get("matched_quality", 0) or 0) if checkpoint else 0
    unmatched_quality_total = int(checkpoint.get("unmatched_quality", 0) or 0) if checkpoint else 0
    limit_remaining = max(1, int(args.limit))

    conn = open_duckdb_conn(str(args.db_path), retries=5, base_sleep_sec=0.08)
    try:
        _ensure_signal_history_snapshot_column(conn)
        quality_by_code = _load_quality_rows(conn, since_dt)

        print(
            json.dumps(
                {
                    "phase": "start",
                    "db_path": str(args.db_path),
                    "hours": int(args.hours),
                    "limit": int(args.limit),
                    "batch_size": int(args.batch_size),
                    "force": bool(args.force),
                    "dry_run": bool(args.dry_run),
                    "resume": bool(args.resume),
                    "checkpoint_file": str(args.checkpoint_file),
                    "resume_from_history_id": int(cursor_id_lt or 0),
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        sample_snapshot: Optional[dict[str, Any]] = None
        batch_no = 0
        while limit_remaining > 0:
            batch_no += 1
            batch_size = min(max(1, int(args.batch_size)), limit_remaining)
            hist_rows = _fetch_history_batch(
                conn,
                since_dt=since_dt,
                force=bool(args.force),
                batch_size=batch_size,
                cursor_id_lt=cursor_id_lt,
                min_id_gt=0,
            )
            if not hist_rows:
                break

            updates: list[tuple[str, int]] = []
            matched_quality_batch = 0
            last_history_id = int(hist_rows[-1][0])
            for row in hist_rows:
                trigger_dt = row[7] if isinstance(row[7], dt.datetime) else None
                trigger_ts = trigger_dt.timestamp() if trigger_dt else 0.0
                quality_hint = _nearest_quality(
                    quality_by_code,
                    str(row[1] or ""),
                    trigger_ts,
                    float(args.tolerance_sec),
                )
                if quality_hint is not None:
                    matched_quality_batch += 1
                payload = _build_backfill_snapshot(row, quality_hint)
                if payload:
                    updates.append((payload, int(row[0])))
                    if sample_snapshot is None:
                        try:
                            sample_snapshot = json.loads(payload)
                        except Exception:
                            sample_snapshot = None

            processed_total += len(hist_rows)
            updated_total += len(updates)
            matched_quality_total += matched_quality_batch
            unmatched_quality_total += max(0, len(updates) - matched_quality_batch)
            limit_remaining -= len(hist_rows)

            progress = _checkpoint_payload(
                db_path=str(args.db_path),
                started_at=started_at,
                last_history_id=last_history_id,
                processed=processed_total,
                updated=updated_total,
                matched_quality=matched_quality_total,
                unmatched_quality=unmatched_quality_total,
                hours=int(args.hours),
                force=bool(args.force),
                batch_size=int(args.batch_size),
            )
            progress.update(
                {
                    "phase": "batch",
                    "batch_no": int(batch_no),
                    "batch_rows": len(hist_rows),
                    "batch_updates": len(updates),
                    "batch_matched_quality": matched_quality_batch,
                    "limit_remaining": int(limit_remaining),
                }
            )
            print(json.dumps(progress, ensure_ascii=False, indent=2))

            if not bool(args.dry_run) and updates:
                conn.executemany("UPDATE signal_history SET details_json = ? WHERE id = ?", updates)

            if not bool(args.dry_run):
                _save_checkpoint(str(args.checkpoint_file), progress)

            cursor_id_lt = last_history_id

            if len(hist_rows) < batch_size:
                break

        result = {
            "ok": True,
            "phase": "done",
            "db_path": str(args.db_path),
            "dry_run": bool(args.dry_run),
            "force": bool(args.force),
            "resume": bool(args.resume),
            "processed": processed_total,
            "updated": updated_total,
            "matched_quality": matched_quality_total,
            "unmatched_quality": unmatched_quality_total,
            "checkpoint_file": str(args.checkpoint_file),
            "last_history_id": int(cursor_id_lt or 0),
            "sample_snapshot": sample_snapshot,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
