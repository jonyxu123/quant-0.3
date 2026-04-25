"""Realtime diagnostics service: selfcheck, replay, diff, health, calibration snapshot."""
from __future__ import annotations

from .common import *  # noqa: F401,F403
from .persistence_service import *  # noqa: F401,F403
from .pool_service import _build_member_data, _evaluate_signals_fast_internal
from . import pool_service as pool_service_module
import backend.realtime_runtime.runtime_jobs as runtime_jobs
from backend.realtime.gm_runtime.common import load_t0_index_context

def runtime_pool1_left_replay(
    hours: int = Query(72, ge=1, le=240),
    limit: int = Query(120, ge=1, le=500),
    promote_hours: int = Query(48, ge=1, le=168),
):
    h = int(hours)
    lim = int(limit)
    promote_h = int(promote_hours)
    since_dt = datetime.datetime.now() - datetime.timedelta(hours=h)

    def _parse_snapshot(raw) -> dict | None:
        if isinstance(raw, str) and raw:
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                return None
        return None

    def _stage_label(snapshot: dict | None, message: str) -> tuple[str, str]:
        hs = snapshot if isinstance(snapshot, dict) else {}
        lsm = hs.get("left_state_machine") if isinstance(hs.get("left_state_machine"), dict) else {}
        stage_a = lsm.get("stage_a") if isinstance(lsm.get("stage_a"), dict) else {}
        stage_b = lsm.get("stage_b") if isinstance(lsm.get("stage_b"), dict) else {}
        stage_c = lsm.get("stage_c") if isinstance(lsm.get("stage_c"), dict) else {}
        observe_only = bool(hs.get("observe_only", False) or lsm.get("observe_only", False))
        observe_reason = str(hs.get("observe_reason") or lsm.get("observe_reason") or "")
        if bool(stage_a.get("passed")) and not bool(stage_b.get("passed")):
            return "B??", "?????????????????"
        if bool(stage_a.get("passed")) and bool(stage_b.get("passed")) and (not bool(stage_c.get("passed")) or bool(stage_c.get("observe_only"))):
            return "C??", "???????????????????"
        if bool(lsm.get("executable_ready")) and not observe_only:
            return "???", "?? A/B/C ???????????"
        if "?????" in message:
            return "B??", "?????????????????"
        if observe_reason in {
            "concept_retreat",
            "concept_weak",
            "left_high_position_catchdown",
            "left_distribution_structure",
            "left_concept_heat_cliff",
            "left_side_streak_retreat",
            "left_side_repeat_retreat",
            "left_side_streak_weak",
            "left_side_repeat_weak",
        }:
            return "C??", "??????????????????"
        if observe_only:
            return "??", "????????????"
        return "???", "????????????"

    def _stage_c_block(snapshot: dict | None, observe_reason: str) -> tuple[str, str]:
        hs = snapshot if isinstance(snapshot, dict) else {}
        lsm = hs.get("left_state_machine") if isinstance(hs.get("left_state_machine"), dict) else {}
        stage_c = lsm.get("stage_c") if isinstance(lsm.get("stage_c"), dict) else {}
        items = [str(x or "").strip() for x in (stage_c.get("items") or [])] if isinstance(stage_c.get("items"), list) else []
        observe_reason = str(observe_reason or "").strip()
        if bool(stage_c.get("industry_joint_weak")) or observe_reason == "concept_industry_joint_weak" or "行业+概念共弱" in items:
            return "industry_joint_weak", "行业+概念共弱"
        if bool(stage_c.get("industry_retreat")) or "行业退潮" in items:
            return "industry_retreat", "行业退潮"
        if bool(stage_c.get("industry_weak")) or "行业承接转弱" in items:
            return "industry_weak", "行业承接转弱"
        if bool(stage_c.get("concept_heat_cliff")) or observe_reason == "left_concept_heat_cliff" or "??????" in items:
            return "concept_heat_cliff", "??????"
        if "???????" in items or observe_reason == "concept_retreat":
            return "concept_retreat_main", "???????"
        if "??????" in items or observe_reason == "concept_weak":
            return "concept_weak_main", "??????"
        if bool(stage_c.get("high_position_catchdown")) or observe_reason == "left_high_position_catchdown":
            return "high_position_catchdown", "????"
        if bool(stage_c.get("distribution_structure")) or observe_reason == "left_distribution_structure":
            return "distribution_structure", "????"
        return "", ""

    def _future_tick_ret_bps(conn, ts_code: str, trigger_ts: float, trigger_price: float, horizon_sec: int, tolerance_sec: int) -> float | None:
        if not ts_code or trigger_price <= 0 or trigger_ts <= 0:
            return None
        row = conn.execute(
            """
            SELECT price, ts
            FROM tick_history
            WHERE ts_code = ?
              AND ts >= ?
              AND ts <= ?
              AND price > 0
            ORDER BY ts ASC
            LIMIT 1
            """,
            [ts_code, int(trigger_ts + horizon_sec), int(trigger_ts + horizon_sec + tolerance_sec)],
        ).fetchone()
        if not row:
            return None
        try:
            eval_price = float(row[0] or 0)
            if eval_price <= 0:
                return None
            return round((eval_price - float(trigger_price)) / float(trigger_price) * 10000.0, 2)
        except Exception:
            return None

    with _ro_conn_ctx() as conn:
        hist_rows = conn.execute(
            """
            SELECT id, ts_code, name, signal_type, channel, signal_source, strength, message, price, pct_chg, details_json, triggered_at
            FROM signal_history
            WHERE pool_id = 1
              AND signal_type = 'left_side_buy'
              AND triggered_at >= ?
            ORDER BY triggered_at DESC
            LIMIT ?
            """,
            [since_dt, lim],
        ).fetchall()

    rows: list[dict] = []
    summary = {
        "signals": 0,
        "stage_b_pending": 0,
        "stage_c_observe": 0,
        "executable": 0,
        "concept_heat_cliff": 0,
        "concept_retreat_main": 0,
        "concept_weak_main": 0,
        "industry_joint_weak": 0,
        "industry_retreat": 0,
        "industry_weak": 0,
        "high_position_catchdown": 0,
        "distribution_structure": 0,
        "promoted_build": 0,
        "watch_avoided_drop": 0,
        "watch_too_early": 0,
        "good_follow": 0,
        "fast_fail": 0,
        "mixed": 0,
        "pending": 0,
    }

    with _ro_conn_ctx() as conn:
        for r in hist_rows:
            hist_id = r[0]
            ts_code = str(r[1] or "")
            name = str(r[2] or "")
            message = str(r[7] or "")
            price = float(r[8] or 0)
            pct_chg = float(r[9] or 0)
            snapshot = _parse_snapshot(r[10])
            triggered_at = r[11]
            trigger_dt = triggered_at if isinstance(triggered_at, datetime.datetime) else _parse_dt(triggered_at)
            trigger_ts = float(trigger_dt.timestamp()) if isinstance(trigger_dt, datetime.datetime) else 0.0
            stage_label, stage_reason = _stage_label(snapshot, message)
            hs = snapshot if isinstance(snapshot, dict) else {}
            observe_only = bool(hs.get("observe_only", False))
            observe_reason = str(hs.get("observe_reason") or "")
            stage_c_block_type, stage_c_block_label = _stage_c_block(snapshot, observe_reason)
            concept_ecology = hs.get("concept_ecology") if isinstance(hs.get("concept_ecology"), dict) else {}
            concept_state = str(concept_ecology.get("state") or "")
            left_state_machine = hs.get("left_state_machine") if isinstance(hs.get("left_state_machine"), dict) else {}
            stage_b = left_state_machine.get("stage_b") if isinstance(left_state_machine.get("stage_b"), dict) else {}
            stage_c = left_state_machine.get("stage_c") if isinstance(left_state_machine.get("stage_c"), dict) else {}

            ret_5m_bps = _future_tick_ret_bps(conn, ts_code, trigger_ts, price, 300, 900)
            ret_60m_bps = _future_tick_ret_bps(conn, ts_code, trigger_ts, price, 3600, 1800)

            future_rows = conn.execute(
                """
                SELECT signal_type, message, details_json, triggered_at
                FROM signal_history
                WHERE pool_id = 1
                  AND ts_code = ?
                  AND triggered_at > ?
                  AND triggered_at <= ?
                  AND signal_type IN ('left_side_buy', 'right_side_breakout', 'timing_clear')
                ORDER BY triggered_at ASC
                """,
                [ts_code, trigger_dt, trigger_dt + datetime.timedelta(hours=promote_h) if isinstance(trigger_dt, datetime.datetime) else since_dt],
            ).fetchall() if isinstance(trigger_dt, datetime.datetime) else []

            next_exec_build = None
            next_clear = None
            next_any = None
            for fr in future_rows:
                sig_type = str(fr[0] or "")
                fr_message = str(fr[1] or "")
                fr_snapshot = _parse_snapshot(fr[2])
                fr_dt = fr[3] if isinstance(fr[3], datetime.datetime) else _parse_dt(fr[3])
                fr_observe = bool((fr_snapshot or {}).get("observe_only", False))
                item = {
                    "signal_type": sig_type,
                    "message": fr_message,
                    "observe_only": fr_observe,
                    "triggered_at": _to_iso_datetime_text(fr_dt),
                    "time_diff_hours": round(max(0.0, ((fr_dt.timestamp() - trigger_ts) / 3600.0)) if isinstance(fr_dt, datetime.datetime) and trigger_ts > 0 else 0.0, 3),
                }
                if next_any is None:
                    next_any = item
                if sig_type in {"left_side_buy", "right_side_breakout"} and not fr_observe and next_exec_build is None:
                    next_exec_build = item
                if sig_type == "timing_clear" and next_clear is None:
                    next_clear = item

            classification = "pending"
            classification_reason = "????????"
            if stage_label in {"B??", "C??", "??"}:
                if next_exec_build is not None:
                    classification = "promoted_build"
                    classification_reason = "????????????/??"
                elif ret_60m_bps is not None and ret_60m_bps <= -120:
                    classification = "watch_avoided_drop"
                    classification_reason = "??????????????????"
                elif ret_60m_bps is not None and ret_60m_bps >= 80:
                    classification = "watch_too_early"
                    classification_reason = "?????????????????????"
                elif ret_5m_bps is not None or ret_60m_bps is not None:
                    classification = "mixed"
                    classification_reason = "???????????????????????"
            else:
                if next_clear is not None and ret_60m_bps is not None and ret_60m_bps < 0:
                    classification = "fast_fail"
                    classification_reason = "????????????????????"
                elif ret_60m_bps is not None and ret_60m_bps >= 80:
                    classification = "good_follow"
                    classification_reason = "????? 60 ???????"
                elif ret_60m_bps is not None and ret_60m_bps <= -120:
                    classification = "fast_fail"
                    classification_reason = "????? 60 ???????"
                elif ret_5m_bps is not None or ret_60m_bps is not None:
                    classification = "mixed"
                    classification_reason = "??????????????????????"

            summary["signals"] += 1
            if stage_label == "B??":
                summary["stage_b_pending"] += 1
            elif stage_label == "C??":
                summary["stage_c_observe"] += 1
            elif stage_label == "???":
                summary["executable"] += 1
            if stage_c_block_type and stage_c_block_type in summary:
                summary[stage_c_block_type] += 1
            if classification in summary:
                summary[classification] += 1

            rows.append(
                {
                    "id": hist_id,
                    "ts_code": ts_code,
                    "name": name,
                    "signal_type": str(r[3] or ""),
                    "channel": str(r[4] or ""),
                    "signal_source": str(r[5] or ""),
                    "strength": float(r[6] or 0),
                    "message": message,
                    "price": price,
                    "pct_chg": pct_chg,
                    "triggered_at": _to_iso_datetime_text(trigger_dt),
                    "history_snapshot": snapshot,
                    "stage_label": stage_label,
                    "stage_reason": stage_reason,
                    "observe_only": observe_only,
                    "observe_reason": observe_reason,
                    "stage_c_block_type": stage_c_block_type,
                    "stage_c_block_label": stage_c_block_label,
                    "concept_state": concept_state,
                    "ret_5m_bps": ret_5m_bps,
                    "ret_60m_bps": ret_60m_bps,
                    "next_signal": next_any,
                    "next_exec_build": next_exec_build,
                    "next_clear": next_clear,
                    "classification": classification,
                    "classification_reason": classification_reason,
                    "stage_b_items": list(stage_b.get("confirm_items") or []) if isinstance(stage_b.get("confirm_items"), list) else [],
                    "stage_c_items": list(stage_c.get("items") or []) if isinstance(stage_c.get("items"), list) else [],
                }
            )

    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "hours": h,
        "limit": lim,
        "promote_hours": promote_h,
        "summary": summary,
        "count": len(rows),
        "rows": rows,
    }

def runtime_t0_reverse_replay(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(120, ge=1, le=1000),
    fee_bps: float = Query(16.0, ge=0, le=200),
):
    h = int(hours)
    lim = int(limit)
    fee = float(fee_bps)
    since_dt = datetime.datetime.now() - datetime.timedelta(hours=h)
    with _ro_conn_ctx() as conn:
        hist_rows = conn.execute(
            """
            SELECT id, ts_code, name, signal_type, channel, signal_source, strength, message, price, pct_chg, details_json, triggered_at
            FROM signal_history
            WHERE pool_id = 2
              AND signal_type = 'reverse_t'
              AND triggered_at >= ?
            ORDER BY triggered_at DESC
            LIMIT ?
            """,
            [since_dt, lim],
        ).fetchall()
        q_rows = conn.execute(
            """
            SELECT ts_code, signal_type, direction, trigger_time, trigger_price, eval_price,
                   ret_bps, mfe_bps, mae_bps, direction_correct, market_phase,
                   COALESCE(channel, '') AS channel,
                   COALESCE(signal_source, '') AS signal_source,
                   created_at
            FROM t0_signal_quality
            WHERE signal_type = 'reverse_t'
              AND eval_horizon_sec = 300
              AND created_at >= ?
            ORDER BY trigger_time DESC
            """,
            [since_dt],
        ).fetchall()

    quality_by_code: dict[str, list[dict]] = {}
    for r in q_rows:
        item = {
            "ts_code": str(r[0] or ""),
            "signal_type": str(r[1] or ""),
            "direction": str(r[2] or ""),
            "trigger_time": _to_epoch_seconds(r[3]),
            "trigger_time_iso": _to_iso_datetime_text(r[3]),
            "trigger_price": float(r[4] or 0),
            "eval_price": float(r[5] or 0),
            "ret_bps": float(r[6] or 0),
            "mfe_bps": float(r[7] or 0),
            "mae_bps": float(r[8] or 0),
            "direction_correct": bool(r[9]),
            "market_phase": str(r[10] or ""),
            "channel": str(r[11] or ""),
            "signal_source": str(r[12] or ""),
            "created_at": r[13].isoformat() if hasattr(r[13], "isoformat") else str(r[13]),
            "_used": False,
        }
        quality_by_code.setdefault(item["ts_code"], []).append(item)

    def _classify_reverse_row(match: Optional[dict]) -> tuple[str, str]:
        if not isinstance(match, dict):
            return "pending", "等待5分钟质量评估"
        ret_bps = float(match.get("ret_bps", 0.0) or 0.0)
        direction_correct = bool(match.get("direction_correct", False))
        if (not direction_correct) or ret_bps < 0:
            return "bad_sell", "卖出后价格继续上行，疑似误卖/漏杀"
        if ret_bps < fee:
            return "weak_edge", "方向虽对但边际不足，可能覆盖不了手续费"
        return "good_hit", "反T命中，5分钟收益覆盖成本"

    rows: list[dict] = []
    summary = {
        "signals": 0,
        "matched_quality": 0,
        "pending": 0,
        "bad_sell": 0,
        "weak_edge": 0,
        "good_hit": 0,
    }
    tolerance_sec = 180.0
    for r in hist_rows:
        hist_id = r[0]
        ts_code = str(r[1] or "")
        name = str(r[2] or "")
        details_raw = r[10]
        triggered_at = r[11]
        trigger_dt = triggered_at if isinstance(triggered_at, datetime.datetime) else None
        trigger_ts = trigger_dt.timestamp() if trigger_dt is not None else 0.0
        history_snapshot = None
        if isinstance(details_raw, str) and details_raw:
            try:
                parsed_snapshot = json.loads(details_raw)
                if isinstance(parsed_snapshot, dict):
                    history_snapshot = parsed_snapshot
            except Exception:
                history_snapshot = None
        match = None
        best_diff = None
        for cand in quality_by_code.get(ts_code, []):
            if cand.get("_used"):
                continue
            diff = abs(float(cand.get("trigger_time", 0) or 0) - trigger_ts)
            if diff > tolerance_sec:
                continue
            if best_diff is None or diff < best_diff:
                best_diff = diff
                match = cand
        if isinstance(match, dict):
            match["_used"] = True

        classification, classification_reason = _classify_reverse_row(match)
        summary["signals"] += 1
        if classification in summary:
            summary[classification] += 1
        if isinstance(match, dict):
            summary["matched_quality"] += 1

        rows.append(
            {
                "id": hist_id,
                "ts_code": ts_code,
                "name": name,
                "signal_type": str(r[3] or ""),
                "channel": str(r[4] or ""),
                "signal_source": str(r[5] or ""),
                "strength": float(r[6] or 0),
                "message": str(r[7] or ""),
                "price": float(r[8] or 0),
                "pct_chg": float(r[9] or 0),
                "history_snapshot": history_snapshot,
                "triggered_at": trigger_dt.isoformat() if trigger_dt else str(triggered_at),
                "quality": None if not isinstance(match, dict) else {
                    "trigger_time": _to_epoch_seconds(match.get("trigger_time", 0)),
                    "trigger_time_iso": _to_iso_datetime_text(match.get("trigger_time", "")),
                    "trigger_price": float(match.get("trigger_price", 0) or 0),
                    "eval_price": float(match.get("eval_price", 0) or 0),
                    "ret_bps": float(match.get("ret_bps", 0) or 0),
                    "mfe_bps": float(match.get("mfe_bps", 0) or 0),
                    "mae_bps": float(match.get("mae_bps", 0) or 0),
                    "direction_correct": bool(match.get("direction_correct", False)),
                    "market_phase": str(match.get("market_phase", "") or ""),
                    "channel": str(match.get("channel", "") or ""),
                    "signal_source": str(match.get("signal_source", "") or ""),
                    "created_at": str(match.get("created_at", "") or ""),
                    "time_diff_sec": round(float(best_diff or 0.0), 3),
                },
                "classification": classification,
                "classification_reason": classification_reason,
                "fee_bps": fee,
            }
        )

    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "hours": h,
        "limit": lim,
        "fee_bps": fee,
        "summary": summary,
        "count": len(rows),
        "rows": rows,
    }


def runtime_t0_positive_replay(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(120, ge=1, le=1000),
    fee_bps: float = Query(16.0, ge=0, le=200),
):
    h = int(hours)
    lim = int(limit)
    fee = float(fee_bps)
    since_dt = datetime.datetime.now() - datetime.timedelta(hours=h)
    with _ro_conn_ctx() as conn:
        hist_rows = conn.execute(
            """
            SELECT id, ts_code, name, signal_type, channel, signal_source, strength, message, price, pct_chg, details_json, triggered_at
            FROM signal_history
            WHERE pool_id = 2
              AND signal_type = 'positive_t'
              AND triggered_at >= ?
            ORDER BY triggered_at DESC
            LIMIT ?
            """,
            [since_dt, lim],
        ).fetchall()
        q_rows = conn.execute(
            """
            SELECT ts_code, signal_type, direction, trigger_time, trigger_price, eval_price,
                   ret_bps, mfe_bps, mae_bps, direction_correct, market_phase,
                   COALESCE(channel, '') AS channel,
                   COALESCE(signal_source, '') AS signal_source,
                   created_at
            FROM t0_signal_quality
            WHERE signal_type = 'positive_t'
              AND eval_horizon_sec = 300
              AND created_at >= ?
            ORDER BY trigger_time DESC
            """,
            [since_dt],
        ).fetchall()

    quality_by_code: dict[str, list[dict]] = {}
    for r in q_rows:
        item = {
            "ts_code": str(r[0] or ""),
            "signal_type": str(r[1] or ""),
            "direction": str(r[2] or ""),
            "trigger_time": _to_epoch_seconds(r[3]),
            "trigger_time_iso": _to_iso_datetime_text(r[3]),
            "trigger_price": float(r[4] or 0),
            "eval_price": float(r[5] or 0),
            "ret_bps": float(r[6] or 0),
            "mfe_bps": float(r[7] or 0),
            "mae_bps": float(r[8] or 0),
            "direction_correct": bool(r[9]),
            "market_phase": str(r[10] or ""),
            "channel": str(r[11] or ""),
            "signal_source": str(r[12] or ""),
            "created_at": r[13].isoformat() if hasattr(r[13], "isoformat") else str(r[13]),
            "_used": False,
        }
        quality_by_code.setdefault(item["ts_code"], []).append(item)

    def _classify_positive_row(match: Optional[dict], rebuild_quality: Optional[dict]) -> tuple[str, str]:
        if not isinstance(match, dict):
            return "pending", "等待 5 分钟回补质量评估"
        rebuild = rebuild_quality if isinstance(rebuild_quality, dict) else {}
        observe_only = bool(rebuild.get("observe_only", False))
        observe_reasons = [str(x or "").strip() for x in (rebuild.get("observe_reasons") or [])] if isinstance(rebuild.get("observe_reasons"), list) else []
        ret_bps = float(match.get("ret_bps", 0.0) or 0.0)
        direction_correct = bool(match.get("direction_correct", False))
        net_ret_bps = ret_bps - fee

        if observe_only:
            if direction_correct and net_ret_bps >= 0:
                if "inventory_quality_degraded" in observe_reasons:
                    return "observe_missed", "库存质量门偏严，过滤后仍出现覆盖成本回补"
                return "observe_missed", "质量门偏严，过滤后仍出现覆盖成本回补"
            if "inventory_quality_degraded" in observe_reasons:
                return "observe_filtered", "库存质量门拦截后，5 分钟内未形成有效回补"
            return "observe_filtered", "质量门拦截后，5 分钟内未形成有效回补"

        if (not direction_correct) or ret_bps < 0:
            return "false_rebuild", "回补后 5 分钟继续走弱，疑似接在下跌延续段"
        if net_ret_bps < 0:
            return "weak_rebuild", "回补方向正确，但净边际未覆盖成本"
        return "good_rebuild", "回补后 5 分钟反抽覆盖成本"

    rows: list[dict] = []
    summary = {
        "signals": 0,
        "matched_quality": 0,
        "pending": 0,
        "good_rebuild": 0,
        "weak_rebuild": 0,
        "false_rebuild": 0,
        "observe_filtered": 0,
        "observe_missed": 0,
        "quality_pass": 0,
        "observe_only": 0,
    }
    quality_entries: list[dict] = []
    ret_vals: list[float] = []
    net_ret_vals: list[float] = []
    direction_correct_count = 0
    cost_covered_count = 0
    tolerance_sec = 180.0

    for r in hist_rows:
        hist_id = r[0]
        ts_code = str(r[1] or "")
        name = str(r[2] or "")
        strength = float(r[6] or 0)
        details_raw = r[10]
        triggered_at = r[11]
        trigger_dt = triggered_at if isinstance(triggered_at, datetime.datetime) else None
        trigger_ts = trigger_dt.timestamp() if trigger_dt is not None else 0.0
        history_snapshot = pool_service_module._parse_history_snapshot_json(details_raw)
        positive_rebuild_quality = None
        t0_inventory = None
        compact_entry = None
        if isinstance(history_snapshot, dict):
            prq = history_snapshot.get("positive_rebuild_quality")
            if isinstance(prq, dict):
                positive_rebuild_quality = prq
                compact_entry = pool_service_module._compact_positive_rebuild_entry(
                    prq,
                    signal_obj={
                        "triggered_at": int(trigger_ts) if trigger_ts > 0 else 0,
                        "strength": strength,
                    },
                    source="signal_history",
                )
                if isinstance(compact_entry, dict):
                    quality_entries.append(compact_entry)
            inv = history_snapshot.get("t0_inventory")
            if isinstance(inv, dict):
                t0_inventory = inv

        match = None
        best_diff = None
        for cand in quality_by_code.get(ts_code, []):
            if cand.get("_used"):
                continue
            diff = abs(float(cand.get("trigger_time", 0) or 0) - trigger_ts)
            if diff > tolerance_sec:
                continue
            if best_diff is None or diff < best_diff:
                best_diff = diff
                match = cand
        if isinstance(match, dict):
            match["_used"] = True

        classification, classification_reason = _classify_positive_row(match, positive_rebuild_quality)
        summary["signals"] += 1
        if classification in summary:
            summary[classification] += 1
        if isinstance(positive_rebuild_quality, dict):
            if bool(positive_rebuild_quality.get("quality_pass", False)):
                summary["quality_pass"] += 1
            if bool(positive_rebuild_quality.get("observe_only", False)):
                summary["observe_only"] += 1
        if isinstance(match, dict):
            summary["matched_quality"] += 1
            ret_bps = float(match.get("ret_bps", 0) or 0)
            ret_vals.append(ret_bps)
            net_ret_vals.append(ret_bps - fee)
            if bool(match.get("direction_correct", False)):
                direction_correct_count += 1
            if (ret_bps - fee) >= 0:
                cost_covered_count += 1

        rows.append(
            {
                "id": hist_id,
                "ts_code": ts_code,
                "name": name,
                "signal_type": str(r[3] or ""),
                "channel": str(r[4] or ""),
                "signal_source": str(r[5] or ""),
                "strength": strength,
                "message": str(r[7] or ""),
                "price": float(r[8] or 0),
                "pct_chg": float(r[9] or 0),
                "history_snapshot": history_snapshot,
                "positive_rebuild_quality": positive_rebuild_quality,
                "t0_inventory": t0_inventory,
                "triggered_at": trigger_dt.isoformat() if trigger_dt else str(triggered_at),
                "quality": None if not isinstance(match, dict) else {
                    "trigger_time": _to_epoch_seconds(match.get("trigger_time", 0)),
                    "trigger_time_iso": _to_iso_datetime_text(match.get("trigger_time", "")),
                    "trigger_price": float(match.get("trigger_price", 0) or 0),
                    "eval_price": float(match.get("eval_price", 0) or 0),
                    "ret_bps": float(match.get("ret_bps", 0) or 0),
                    "mfe_bps": float(match.get("mfe_bps", 0) or 0),
                    "mae_bps": float(match.get("mae_bps", 0) or 0),
                    "direction_correct": bool(match.get("direction_correct", False)),
                    "market_phase": str(match.get("market_phase", "") or ""),
                    "channel": str(match.get("channel", "") or ""),
                    "signal_source": str(match.get("signal_source", "") or ""),
                    "created_at": str(match.get("created_at", "") or ""),
                    "time_diff_sec": round(float(best_diff or 0.0), 3),
                    "net_ret_bps_after_fee": round(float(match.get("ret_bps", 0) or 0) - fee, 2),
                },
                "classification": classification,
                "classification_reason": classification_reason,
                "fee_bps": fee,
            }
        )

    quality_summary = pool_service_module._aggregate_positive_rebuild_entries(quality_entries)
    if not isinstance(quality_summary, dict):
        quality_summary = {}
    matched_quality = int(summary.get("matched_quality", 0) or 0)
    quality_summary.update(
        {
            "matched_quality_count": matched_quality,
            "avg_ret_bps_5m": round((sum(ret_vals) / len(ret_vals)), 2) if ret_vals else None,
            "avg_net_ret_bps_5m": round((sum(net_ret_vals) / len(net_ret_vals)), 2) if net_ret_vals else None,
            "direction_correct_rate": round((direction_correct_count / matched_quality), 4) if matched_quality > 0 else None,
            "cost_covered_count": int(cost_covered_count),
            "cost_covered_rate": round((cost_covered_count / matched_quality), 4) if matched_quality > 0 else None,
        }
    )

    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "hours": h,
        "limit": lim,
        "fee_bps": fee,
        "summary": summary,
        "quality_summary": quality_summary,
        "count": len(rows),
        "rows": rows,
    }


def _t0_positive_diag_sort_key(row: dict) -> tuple:
    priority = {
        "triggered": 1,
        "trigger_observe_only": 2,
        "veto": 3,
        "trigger_score_filtered": 4,
        "trigger_no_confirm": 5,
        "candidate": 6,
        "off_session_skip": 7,
        "error": 8,
        "idle": 9,
    }
    status = str(row.get("status") or "")
    try:
        strength = -float(row.get("strength", 0) or 0)
    except Exception:
        strength = 0.0
    return (priority.get(status, 99), strength, str(row.get("ts_code") or ""))


def _build_t0_positive_diag_row(member: dict, provider) -> dict:
    from backend.realtime import gm_tick as gm_runtime
    from backend.realtime.threshold_engine import build_signal_context, get_thresholds

    m = dict(member or {})
    ts_code = str(m.get("ts_code") or "")
    name = str(m.get("name") or "")
    if not ts_code:
        return {"status": "error", "error": "missing_ts_code"}
    market_open = bool(_is_market_open().get("is_open", False))

    tick = {}
    try:
        tick = provider.get_cached_tick(ts_code) or {}
    except Exception:
        tick = {}
    if not tick:
        try:
            tick = provider.get_tick(ts_code) or {}
        except Exception:
            tick = {}
    if not isinstance(tick, dict) or not tick:
        return {
            "ts_code": ts_code,
            "name": name,
            "status": "error",
            "error": "tick_unavailable",
        }

    price = float(tick.get("price", m.get("price", 0)) or 0)
    pct_chg = float(tick.get("pct_chg", m.get("pct_chg", 0)) or 0)
    if price > 0:
        m["price"] = price
    m["pct_chg"] = pct_chg
    if (not market_open) and price <= 0:
        return {
            "ts_code": ts_code,
            "name": name,
            "price": round(price, 3),
            "pct_chg": round(pct_chg, 3),
            "status": "off_session_skip",
            "has_signal": False,
            "observe_only": False,
            "strength": 0.0,
            "message": "",
            "veto": None,
            "trigger_items": [],
            "confirm_items": [],
            "candidate_reasons": [],
            "threshold": {},
            "positive_rebuild_quality": {},
            "t0_inventory": {},
            "market_structure": {"reason": "off_session_skip"},
            "feature_snapshot": {
                "boll_break": False,
                "bias_vwap": None,
                "gub5_transition": None,
                "bid_ask_ratio": None,
                "volume_pace_ratio": None,
                "volume_pace_state": "unknown",
                "pct_chg": pct_chg,
            },
        }

    txns = None
    try:
        txns = provider.get_cached_transactions(ts_code)
    except Exception:
        txns = None
    if txns is None:
        try:
            txns = mootdx_client.get_transactions(ts_code, int(_TXN_ANALYZE_COUNT))
        except Exception:
            txns = None
    txns = txns or []

    ms = gm_runtime._analyze_market_structure(tick, txns)
    state = gm_runtime._update_intraday_state(ts_code, tick)
    vwap = m.get("vwap")
    if vwap in (None, 0):
        vwap = gm_runtime._compute_vwap(state)
    gub5_trend = m.get("gub5_trend") or gm_runtime._compute_gub5_trend(state)
    gub5_transition = gm_runtime._compute_gub5_transition(state)
    pw = state.get("price_window") or []
    intraday_prices = m.get("intraday_prices") or [p for _, p, _ in pw]

    ma5 = float(m.get("ma5", 0) or 0)
    ma10 = float(m.get("ma10", 0) or 0)
    ma20 = float(m.get("ma20", 0) or 0)
    trend_up = bool(ma5 > ma10 > ma20 > 0)

    feat = sig.build_t0_features(
        tick_price=price,
        boll_lower=m.get("boll_lower", 0),
        boll_upper=m.get("boll_upper", 0),
        vwap=vwap,
        intraday_prices=intraday_prices,
        pct_chg=pct_chg,
        trend_up=trend_up,
        gub5_trend=gub5_trend,
        gub5_transition=gub5_transition,
        bid_ask_ratio=m.get("bid_ask_ratio"),
        lure_short=bool(ms.get("lure_short", False)),
        wash_trade=bool(ms.get("wash_trade", False)),
        spread_abnormal=bool(m.get("spread_abnormal", False)),
        v_power_divergence=False,
        ask_wall_building=bool(ms.get("lure_long", False)),
        real_buy_fading=bool(ms.get("active_buy_fading", False)),
        wash_trade_severe=bool(ms.get("wash_trade", False) and abs(float(ms.get("big_order_bias", 0.0) or 0.0)) < 0.05),
        liquidity_drain=bool(m.get("liquidity_drain", False)),
        ask_wall_absorb_ratio=ms.get("ask_wall_absorb_ratio"),
        bid_wall_break_ratio=ms.get("bid_wall_break_ratio"),
        spoofing_vol_ratio=m.get("spoofing_vol_ratio"),
        big_order_bias=ms.get("big_order_bias"),
        big_net_flow_bps=ms.get("big_net_flow_bps"),
        super_order_bias=ms.get("super_order_bias"),
        super_net_flow_bps=ms.get("super_net_flow_bps"),
        volume_pace_ratio=m.get("volume_pace_ratio"),
        volume_pace_state=m.get("volume_pace_state"),
    )

    ctx = build_signal_context(
        ts_code,
        2,
        tick=tick,
        daily=m,
        intraday={
            "vwap": feat.get("vwap"),
            "bias_vwap": feat.get("bias_vwap"),
            "robust_zscore": feat.get("robust_zscore"),
            "gub5_ratio": m.get("gub5_ratio"),
            "gub5_transition": feat.get("gub5_transition"),
            "volume_pace_ratio": feat.get("volume_pace_ratio"),
            "volume_pace_state": feat.get("volume_pace_state"),
            "progress_ratio": m.get("volume_pace_progress"),
        },
        market={
            "industry": m.get("industry"),
            "vol_20": m.get("vol_20"),
            "atr_14": m.get("atr_14"),
            "name": m.get("name"),
            "market_name": m.get("market_name"),
            "list_date": m.get("list_date"),
            "industry_ecology": m.get("industry_ecology"),
            "concept_boards": m.get("concept_boards"),
            "concept_codes": m.get("concept_codes"),
            "core_concept_board": m.get("core_concept_board"),
            "core_concept_code": m.get("core_concept_code"),
            "concept_ecology": m.get("concept_ecology"),
            "concept_ecology_multi": m.get("concept_ecology_multi"),
            "instrument_profile": m.get("instrument_profile"),
            "index_context": load_t0_index_context(instrument_profile=m.get("instrument_profile"), ts_code=ts_code),
        },
    )
    thresholds = get_thresholds("positive_t", ctx)

    inventory_state = {}
    try:
        inv = provider.get_pool2_t0_inventory_state(ts_code)
        if isinstance(inv, dict):
            inventory_state = dict(inv)
    except Exception:
        inventory_state = {}

    pos = sig.detect_positive_t(
        tick_price=feat["tick_price"],
        boll_lower=feat["boll_lower"],
        vwap=feat["vwap"],
        gub5_trend=feat.get("gub5_trend"),
        gub5_transition=feat.get("gub5_transition"),
        bid_ask_ratio=feat.get("bid_ask_ratio"),
        lure_short=bool(feat.get("lure_short", False)),
        wash_trade=bool(feat.get("wash_trade", False)),
        spread_abnormal=bool(feat.get("spread_abnormal", False)),
        boll_break=bool(feat.get("boll_break", False)),
        ask_wall_absorb=bool(feat.get("ask_wall_absorb", False)),
        ask_wall_absorb_ratio=feat.get("ask_wall_absorb_ratio"),
        spoofing_suspected=bool(feat.get("spoofing_suspected", False)),
        bias_vwap=feat.get("bias_vwap"),
        super_order_bias=feat.get("super_order_bias"),
        super_net_flow_bps=feat.get("super_net_flow_bps"),
        volume_pace_ratio=feat.get("volume_pace_ratio"),
        volume_pace_state=feat.get("volume_pace_state"),
        ts_code=ts_code,
        thresholds=thresholds,
        position_state=inventory_state,
        industry_ecology=m.get("industry_ecology"),
        concept_ecology=m.get("concept_ecology"),
        concept_ecology_multi=m.get("concept_ecology_multi"),
        index_context=load_t0_index_context(instrument_profile=m.get("instrument_profile"), ts_code=ts_code),
    )

    try:
        bias_vwap_th = float((thresholds or {}).get("bias_vwap_th"))
    except Exception:
        bias_vwap_th = float(getattr(sig, "_BIAS_VWAP_PCT", -1.8))
    try:
        eps_break_th = float((thresholds or {}).get("eps_break"))
    except Exception:
        eps_break_th = float(getattr(sig, "_EPS_BREAK", 0.003))

    trigger_items: list[str] = []
    if bool(feat.get("boll_break", False)):
        trigger_items.append("boll_break")
    try:
        bias_vwap = float(feat.get("bias_vwap")) if feat.get("bias_vwap") is not None else None
    except Exception:
        bias_vwap = None
    if bias_vwap is not None and bias_vwap < bias_vwap_th:
        trigger_items.append("bias_vwap")

    confirm_items: list[str] = []
    if feat.get("gub5_transition") in getattr(sig, "_GUB5_TRANSITIONS", set()):
        confirm_items.append("gub5_transition")
    try:
        if feat.get("bid_ask_ratio") is not None and float(feat.get("bid_ask_ratio")) > float(getattr(sig, "_BID_ASK_TH", 1.05)):
            confirm_items.append("bid_ask_ratio")
    except Exception:
        pass
    if bool(feat.get("lure_short", False)):
        confirm_items.append("lure_short")
    if bool(feat.get("ask_wall_absorb", False)):
        confirm_items.append("ask_wall_absorb")
    try:
        if feat.get("super_order_bias") is not None and float(feat.get("super_order_bias")) >= float(getattr(sig, "_SUPER_ORDER_BIAS_BUY_TH", 0.20)):
            confirm_items.append("super_order_bias")
    except Exception:
        pass
    try:
        if feat.get("super_net_flow_bps") is not None and float(feat.get("super_net_flow_bps")) >= float(getattr(sig, "_SUPER_NET_INFLOW_BUY_BPS", 120.0)):
            confirm_items.append("super_net_flow_bps")
    except Exception:
        pass

    details = pos.get("details") if isinstance(pos.get("details"), dict) else {}
    veto = str(details.get("veto") or "")
    has_signal = bool(pos.get("has_signal", False))
    observe_only = bool(details.get("observe_only", False))
    strength = float(pos.get("strength", 0) or 0)
    positive_rebuild_quality = details.get("positive_rebuild_quality") if isinstance(details.get("positive_rebuild_quality"), dict) else {}
    t0_inventory = details.get("t0_inventory") if isinstance(details.get("t0_inventory"), dict) else inventory_state
    index_context_gate = details.get("index_context_gate") if isinstance(details.get("index_context_gate"), dict) else {}
    index_context = details.get("index_context") if isinstance(details.get("index_context"), dict) else {}

    candidate_reasons: list[str] = []
    boll_lower = float(feat.get("boll_lower", 0) or 0)
    if not trigger_items:
        if boll_lower > 0 and price <= boll_lower * (1 + max(0.001, eps_break_th * 0.25)):
            candidate_reasons.append("near_boll_lower")
        if bias_vwap is not None and bias_vwap <= (bias_vwap_th + 0.5):
            candidate_reasons.append("near_bias_tail")
        if bool(feat.get("ask_wall_absorb", False)) and bool(feat.get("lure_short", False)):
            candidate_reasons.append("repairing_microstructure")

    if has_signal:
        status = "trigger_observe_only" if observe_only else "triggered"
    elif veto:
        status = "veto"
    elif trigger_items and not confirm_items:
        status = "trigger_no_confirm"
    elif trigger_items and confirm_items:
        status = "trigger_score_filtered"
    elif candidate_reasons:
        status = "candidate"
    else:
        status = "idle"

    return {
        "ts_code": ts_code,
        "name": name,
        "price": round(price, 3),
        "pct_chg": round(pct_chg, 3),
        "status": status,
        "has_signal": has_signal,
        "observe_only": observe_only,
        "strength": round(strength, 2),
        "message": str(pos.get("message", "") or ""),
        "veto": veto or None,
        "trigger_items": trigger_items,
        "confirm_items": confirm_items,
        "candidate_reasons": candidate_reasons,
        "threshold": details.get("threshold") if isinstance(details.get("threshold"), dict) else thresholds,
        "positive_rebuild_quality": positive_rebuild_quality,
        "t0_inventory": t0_inventory,
        "index_context_gate": index_context_gate,
        "index_context": index_context,
        "market_structure": {
            "tag": ms.get("tag"),
            "big_order_bias": ms.get("big_order_bias"),
            "big_net_flow_bps": ms.get("big_net_flow_bps"),
            "super_order_bias": ms.get("super_order_bias"),
            "super_net_flow_bps": ms.get("super_net_flow_bps"),
            "bid_ask_ratio": ms.get("bid_ask_ratio"),
            "ask_wall_absorb_ratio": ms.get("ask_wall_absorb_ratio"),
            "bid_wall_break_ratio": ms.get("bid_wall_break_ratio"),
            "ask_wall_absorbed": bool(ms.get("ask_wall_absorbed", False)),
            "lure_short": bool(ms.get("lure_short", False)),
            "wash_trade": bool(ms.get("wash_trade", False)),
            "active_buy_fading": bool(ms.get("active_buy_fading", False)),
            "peak_gain_pct": ms.get("peak_gain_pct"),
            "retrace_from_high_pct": ms.get("retrace_from_high_pct"),
        },
        "feature_snapshot": {
            "boll_break": bool(feat.get("boll_break", False)),
            "bias_vwap": feat.get("bias_vwap"),
            "gub5_trend": feat.get("gub5_trend"),
            "gub5_transition": feat.get("gub5_transition"),
            "bid_ask_ratio": feat.get("bid_ask_ratio"),
            "ask_wall_absorb": bool(feat.get("ask_wall_absorb", False)),
            "super_order_bias": feat.get("super_order_bias"),
            "super_net_flow_bps": feat.get("super_net_flow_bps"),
            "volume_pace_ratio": feat.get("volume_pace_ratio"),
            "volume_pace_state": feat.get("volume_pace_state"),
            "pct_chg": feat.get("pct_chg"),
        },
    }


def runtime_t0_positive_diagnostics(
    limit: int = Query(200, ge=1, le=1000),
    interesting_only: bool = Query(False),
    ts_codes: str = Query("", description="comma separated ts_code list"),
):
    with _ro_conn_ctx() as conn:
        members_data = _build_member_data(conn, 2)

    code_filter = {
        str(x or "").strip().upper()
        for x in str(ts_codes or "").split(",")
        if str(x or "").strip()
    }
    if code_filter:
        members_data = [m for m in (members_data or []) if str((m or {}).get("ts_code") or "").strip().upper() in code_filter]

    provider = get_tick_provider()
    rows: list[dict] = []
    for m in members_data or []:
        try:
            rows.append(_build_t0_positive_diag_row(m, provider))
        except Exception as e:
            rows.append(
                {
                    "ts_code": str((m or {}).get("ts_code") or ""),
                    "name": str((m or {}).get("name") or ""),
                    "status": "error",
                    "error": str(e),
                }
            )

    total_rows = len(rows)
    summary = {
        "members": total_rows,
        "triggered": 0,
        "observe_only": 0,
        "index_observe_only": 0,
        "index_panic_selloff": 0,
        "index_risk_off": 0,
        "veto": 0,
        "trigger_no_confirm": 0,
        "trigger_score_filtered": 0,
        "candidate": 0,
        "off_session_skip": 0,
        "error": 0,
        "quality_pass": 0,
        "recovery_quality_low": 0,
        "net_executable_edge_low": 0,
        "inventory_degraded": 0,
    }
    for row in rows:
        status = str(row.get("status") or "")
        if status == "triggered":
            summary["triggered"] += 1
        elif status == "trigger_observe_only":
            summary["triggered"] += 1
            summary["observe_only"] += 1
        elif status == "veto":
            summary["veto"] += 1
        elif status == "trigger_no_confirm":
            summary["trigger_no_confirm"] += 1
        elif status == "trigger_score_filtered":
            summary["trigger_score_filtered"] += 1
        elif status == "candidate":
            summary["candidate"] += 1
        elif status == "off_session_skip":
            summary["off_session_skip"] += 1
        elif status == "error":
            summary["error"] += 1

        index_gate = row.get("index_context_gate") if isinstance(row.get("index_context_gate"), dict) else {}
        index_reasons = [str(x or "").strip() for x in (index_gate.get("reasons") or [])] if isinstance(index_gate.get("reasons"), list) else []
        if bool(index_gate.get("observe_only")):
            summary["index_observe_only"] += 1
        if "index_panic_selloff" in index_reasons:
            summary["index_panic_selloff"] += 1
        if "index_risk_off" in index_reasons:
            summary["index_risk_off"] += 1

        quality = row.get("positive_rebuild_quality") if isinstance(row.get("positive_rebuild_quality"), dict) else {}
        if bool(quality.get("quality_pass", False)):
            summary["quality_pass"] += 1
        observe_reasons = [str(x or "").strip() for x in (quality.get("observe_reasons") or [])] if isinstance(quality.get("observe_reasons"), list) else []
        if "recovery_quality_low" in observe_reasons:
            summary["recovery_quality_low"] += 1
        if "net_executable_edge_low" in observe_reasons:
            summary["net_executable_edge_low"] += 1
        if "inventory_quality_degraded" in observe_reasons:
            summary["inventory_degraded"] += 1

    if interesting_only:
        rows = [r for r in rows if str(r.get("status") or "") not in {"idle", "off_session_skip"}]
    rows.sort(key=_t0_positive_diag_sort_key)

    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "interesting_only": bool(interesting_only),
        "summary": summary,
        "rows_total": total_rows,
        "count": len(rows[: int(limit)]),
        "rows": rows[: int(limit)],
    }


def runtime_db_metrics():
    m = _metrics_snapshot()
    m["db_realtime_path"] = _REALTIME_DB
    m["db_data_path"] = _DATA_DB
    m["db_read_pool_size"] = int(_db_read_pool.qsize())
    m["db_read_pool_capacity"] = int(_DB_READ_POOL_SIZE)
    m["db_data_read_pool_size"] = int(_db_data_read_pool.qsize())
    m["db_data_read_pool_capacity"] = int(_DB_DATA_READ_POOL_SIZE)
    m["db_write_queue_size"] = int(_db_write_queue.qsize())
    m["db_write_queue_capacity"] = 200000
    m["db_writer_flush_sec"] = float(_DB_WRITER_FLUSH_SEC)
    m["db_writer_max_batch"] = int(_DB_WRITER_MAX_BATCH)
    m["db_writer_redis_enabled"] = bool(_DB_WRITER_REDIS_ENABLED and _redis_mod is not None)
    if m["db_writer_redis_enabled"]:
        cli = _get_db_writer_redis()
        m["db_writer_redis_ready"] = bool(cli is not None)
        if cli is not None:
            try:
                m["db_writer_redis_queue_size"] = int(cli.llen(_DB_WRITER_REDIS_KEY))
            except Exception:
                m["db_writer_redis_queue_size"] = None
    else:
        m["db_writer_redis_ready"] = False
        m["db_writer_redis_queue_size"] = None
    m["runtime_state_redis_enabled"] = bool(_RUNTIME_STATE_REDIS_ENABLED and _redis_mod is not None)
    if m["runtime_state_redis_enabled"]:
        cli2 = _get_runtime_state_redis()
        m["runtime_state_redis_ready"] = bool(cli2 is not None)
    else:
        m["runtime_state_redis_ready"] = False
    m["threshold_closed_loop_enabled"] = bool(_CLOSED_LOOP_ENABLED)
    m["threshold_closed_loop_started"] = runtime_jobs.is_threshold_closed_loop_started()
    try:
        p = get_tick_provider()
        fn = getattr(p, "get_signal_perf_stats", None)
        if callable(fn):
            perf = fn() or {}
            if isinstance(perf, dict) and perf:
                for k, v in perf.items():
                    m[f"signal_perf_{k}"] = v
    except Exception:
        pass
    _runtime_state_write_metrics(m)
    return {"data": m}
def runtime_t0_quality_live(
    trade_date: Optional[str] = Query(None, description="YYYYMMDD, default today"),
    signal_type: Optional[str] = Query(None, description="positive_t|reverse_t"),
    horizon_sec: Optional[int] = Query(None, description="e.g. 60/180/300"),
    window_limit: int = Query(50, ge=1, le=500),
):
    td = trade_date.strip() if isinstance(trade_date, str) and trade_date.strip() else _trade_date_from_dt()
    sig = signal_type.strip() if isinstance(signal_type, str) and signal_type.strip() else None
    hz = int(horizon_sec) if isinstance(horizon_sec, int) else None
    wlim = int(window_limit) if isinstance(window_limit, int) else 50
    wlim = max(1, min(500, wlim))
    cli = _get_runtime_state_redis()
    if cli is None:
        return {
            "redis_ready": False,
            "trade_date": td,
            "storage_source": "memory",
            "aggregate": {},
            "window": {"count": 0, "items": []},
        }

    k_agg = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:t0_quality:agg:{td}"
    agg_raw = {}
    try:
        agg_raw = cli.hgetall(k_agg) or {}
    except Exception:
        agg_raw = {}

    def _to_i(x):
        try:
            return int(float(x))
        except Exception:
            return 0

    def _to_f(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    total = _to_i(agg_raw.get("total"))
    correct = _to_i(agg_raw.get("correct"))
    ret_sum = _to_f(agg_raw.get("ret_bps_sum"))
    mfe_sum = _to_f(agg_raw.get("mfe_bps_sum"))
    mae_sum = _to_f(agg_raw.get("mae_bps_sum"))
    aggregate = {
        "total": total,
        "correct": correct,
        "precision": round((correct / total), 4) if total > 0 else None,
        "ret_bps_avg": round((ret_sum / total), 2) if total > 0 else None,
        "mfe_bps_avg": round((mfe_sum / total), 2) if total > 0 else None,
        "mae_bps_avg": round((mae_sum / total), 2) if total > 0 else None,
        "updated_at": agg_raw.get("updated_at"),
    }
    by_channel: dict[str, dict] = {}
    by_source: dict[str, dict] = {}
    by_channel_source: dict[str, dict] = {}
    for k in list(agg_raw.keys()):
        if isinstance(k, str) and k.startswith("c:") and k.endswith(":total") and ":src:" not in k:
            channel = k[2:-6]
            c_total = _to_i(agg_raw.get(k))
            c_correct = _to_i(agg_raw.get(f"c:{channel}:correct"))
            by_channel[channel] = {
                "total": c_total,
                "correct": c_correct,
                "precision": round((c_correct / c_total), 4) if c_total > 0 else None,
            }
        if isinstance(k, str) and k.startswith("src:") and k.endswith(":total"):
            source = k[4:-6]
            s_total = _to_i(agg_raw.get(k))
            s_correct = _to_i(agg_raw.get(f"src:{source}:correct"))
            by_source[source] = {
                "total": s_total,
                "correct": s_correct,
                "precision": round((s_correct / s_total), 4) if s_total > 0 else None,
            }
        if isinstance(k, str) and k.startswith("c:") and ":src:" in k and k.endswith(":total"):
            mid = k[2:-6]
            idx = mid.find(":src:")
            if idx > 0:
                channel = mid[:idx]
                source = mid[idx + 5 :]
                cs_total = _to_i(agg_raw.get(k))
                cs_correct = _to_i(agg_raw.get(f"c:{channel}:src:{source}:correct"))
                by_channel_source[f"{channel}|{source}"] = {
                    "channel": channel,
                    "signal_source": source,
                    "total": cs_total,
                    "correct": cs_correct,
                    "precision": round((cs_correct / cs_total), 4) if cs_total > 0 else None,
                }
    if by_channel:
        aggregate["by_channel"] = by_channel
    if by_source:
        aggregate["by_source"] = by_source
    if by_channel_source:
        aggregate["by_channel_source"] = by_channel_source

    if isinstance(hz, int):
        h_total = _to_i(agg_raw.get(f"h:{hz}:total"))
        h_correct = _to_i(agg_raw.get(f"h:{hz}:correct"))
        aggregate["horizon"] = {
            "sec": int(hz),
            "total": h_total,
            "correct": h_correct,
            "precision": round((h_correct / h_total), 4) if h_total > 0 else None,
        }
    if isinstance(sig, str) and sig:
        s_total = _to_i(agg_raw.get(f"s:{sig}:total"))
        s_correct = _to_i(agg_raw.get(f"s:{sig}:correct"))
        aggregate["signal_type"] = {
            "name": sig,
            "total": s_total,
            "correct": s_correct,
            "precision": round((s_correct / s_total), 4) if s_total > 0 else None,
        }
        if isinstance(hz, int):
            sh_total = _to_i(agg_raw.get(f"s:{sig}:h:{hz}:total"))
            sh_correct = _to_i(agg_raw.get(f"s:{sig}:h:{hz}:correct"))
            aggregate["signal_type_horizon"] = {
                "name": sig,
                "sec": int(hz),
                "total": sh_total,
                "correct": sh_correct,
                "precision": round((sh_correct / sh_total), 4) if sh_total > 0 else None,
            }

    items = []
    if sig and isinstance(hz, int):
        k_win = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:t0_quality:window:{td}:{sig}:{hz}"
        try:
            raws = cli.lrange(k_win, 0, max(0, int(wlim) - 1))
            for raw in raws or []:
                try:
                    items.append(json.loads(raw))
                except Exception:
                    continue
        except Exception:
            items = []

    return {
        "redis_ready": True,
        "trade_date": td,
        "storage_source": "redis",
        "window_size": int(_RUNTIME_T0_QUALITY_WINDOW_SIZE),
        "aggregate": aggregate,
        "window": {
            "count": len(items),
            "items": items,
        },
    }
def runtime_threshold_calibration_snapshot():
    cli = _get_runtime_state_redis()
    key_latest = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:threshold:calibration:latest"
    if cli is not None:
        try:
            raw = cli.get(key_latest)
            if raw:
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    payload["storage_source"] = "redis"
                    return payload
        except Exception:
            pass
    payload = runtime_jobs.get_threshold_calibration_snapshot_local()
    if not payload:
        return {
            "ok": False,
            "checked_at": datetime.datetime.now().isoformat(),
            "message": "threshold calibration snapshot not ready",
            "storage_source": "memory",
        }
    payload["storage_source"] = "memory"
    return payload


def runtime_concept_snapshot_status():
    checked_at = datetime.datetime.now()

    def _summarize_snapshot_map(snapshot_map: dict[str, dict]) -> dict:
        concept_names: set[str] = set()
        source_set: set[str] = set()
        latest_updated = 0.0
        latest_updated_iso = ""
        for payload in (snapshot_map or {}).values():
            if not isinstance(payload, dict):
                continue
            concept_name = str(payload.get("concept_name") or "").strip()
            if concept_name:
                concept_names.add(concept_name)
            source = str(payload.get("source") or "").strip()
            if source:
                source_set.add(source)
            try:
                updated_at = float(payload.get("updated_at") or 0.0)
            except Exception:
                updated_at = 0.0
            if updated_at > latest_updated:
                latest_updated = updated_at
                latest_updated_iso = str(payload.get("updated_at_iso") or "").strip()
        return {
            "count": len(concept_names),
            "sources": sorted(source_set),
            "updated_at": int(latest_updated) if latest_updated > 0 else None,
            "updated_at_iso": latest_updated_iso or None,
            "age_sec": (checked_at.timestamp() - latest_updated) if latest_updated > 0 else None,
        }

    with pool_service_module._concept_snapshot_cache_lock:
        cache_map = dict(pool_service_module._concept_snapshot_cache_map or {})
        cache_error = str(pool_service_module._concept_snapshot_cache_error or "")
        cache_at = float(pool_service_module._concept_snapshot_cache_at or 0.0)

    cache_summary = _summarize_snapshot_map(cache_map)
    if cache_at > 0 and not cache_summary.get("updated_at"):
        cache_summary["updated_at"] = int(cache_at)
        cache_summary["updated_at_iso"] = datetime.datetime.fromtimestamp(cache_at).isoformat(timespec="seconds")
        cache_summary["age_sec"] = checked_at.timestamp() - cache_at

    redis_ready = False
    redis_summary = {
        "enabled": bool(_RUNTIME_STATE_REDIS_ENABLED),
        "ready": False,
        "count": 0,
        "updated_at": None,
        "updated_at_iso": None,
        "age_sec": None,
        "sources": [],
        "error": "",
    }
    try:
        cli = _get_runtime_state_redis()
        redis_ready = cli is not None
        redis_summary["ready"] = redis_ready
    except Exception as e:
        redis_summary["error"] = str(e)
        cli = None
    redis_map, redis_error, redis_at = pool_service_module._read_concept_snapshot_runtime_state(
        source_override="redis_status"
    )
    redis_compact = _summarize_snapshot_map(redis_map)
    redis_summary.update(redis_compact)
    if redis_at > 0 and not redis_summary.get("updated_at"):
        redis_summary["updated_at"] = int(redis_at)
        redis_summary["updated_at_iso"] = datetime.datetime.fromtimestamp(redis_at).isoformat(timespec="seconds")
        redis_summary["age_sec"] = checked_at.timestamp() - redis_at
    if redis_error:
        redis_summary["error"] = str(redis_error)

    runtime_summary = {
        "count": 0,
        "sources": [],
        "updated_at": None,
        "updated_at_iso": None,
        "age_sec": None,
    }
    runtime_error = ""
    try:
        from backend.realtime import gm_tick

        runtime_map = dict(getattr(gm_tick, "_main_concept_snapshot", {}) or {})
        runtime_summary = _summarize_snapshot_map(runtime_map)
    except Exception as e:
        runtime_error = str(e)

    cache_fresh = bool(cache_summary.get("updated_at")) and float(cache_summary.get("age_sec") or 10**9) <= float(
        pool_service_module._CONCEPT_SNAPSHOT_CACHE_TTL_SEC
    ) * 2.0
    runtime_fresh = bool(runtime_summary.get("updated_at")) and float(runtime_summary.get("age_sec") or 10**9) <= float(
        runtime_jobs._CONCEPT_SNAPSHOT_OFF_SESSION_SEC
    ) * 1.5
    redis_fresh = bool(redis_summary.get("updated_at")) and float(redis_summary.get("age_sec") or 10**9) <= max(
        float(pool_service_module._CONCEPT_SNAPSHOT_CACHE_TTL_SEC) * 3.0,
        300.0,
    )

    active_source = ""
    active_updated_iso = None
    if runtime_summary.get("sources"):
        active_source = str((runtime_summary.get("sources") or [""])[0] or "")
        active_updated_iso = runtime_summary.get("updated_at_iso")
    elif cache_summary.get("sources"):
        active_source = str((cache_summary.get("sources") or [""])[0] or "")
        active_updated_iso = cache_summary.get("updated_at_iso")
    elif redis_summary.get("sources"):
        active_source = str((redis_summary.get("sources") or [""])[0] or "")
        active_updated_iso = redis_summary.get("updated_at_iso")

    ok = bool(runtime_summary.get("count")) and runtime_fresh
    if not ok and bool(cache_summary.get("count")) and cache_fresh:
        ok = True
    if not ok and bool(redis_summary.get("count")) and redis_fresh:
        ok = True

    return {
        "ok": ok,
        "checked_at": checked_at.isoformat(),
        "refresh_ttl_sec": float(pool_service_module._CONCEPT_SNAPSHOT_CACHE_TTL_SEC),
        "off_session_refresh_sec": float(runtime_jobs._CONCEPT_SNAPSHOT_OFF_SESSION_SEC),
        "active_source": active_source or None,
        "active_updated_at": runtime_summary.get("updated_at") or cache_summary.get("updated_at") or redis_summary.get("updated_at"),
        "active_updated_at_iso": active_updated_iso,
        "cache": {
            **cache_summary,
            "error": cache_error,
            "fresh": cache_fresh,
        },
        "runtime": {
            **runtime_summary,
            "fresh": runtime_fresh,
            "error": runtime_error,
        },
        "redis": {
            **redis_summary,
            "fresh": redis_fresh,
        },
    }


def runtime_industry_mapping_coverage(
    pool_id: Optional[int] = Query(None),
    limit_unmatched: int = Query(80, ge=1, le=500),
):
    checked_at = datetime.datetime.now()
    limit_n = int(limit_unmatched)

    with _ro_conn_ctx() as conn:
        if pool_id is None:
            member_rows = conn.execute(
                """
                SELECT pool_id, ts_code, name, industry
                FROM monitor_pools
                ORDER BY pool_id, sort_order, added_at
                """
            ).fetchall()
        else:
            member_rows = conn.execute(
                """
                SELECT pool_id, ts_code, name, industry
                FROM monitor_pools
                WHERE pool_id = ?
                ORDER BY sort_order, added_at
                """,
                [int(pool_id)],
            ).fetchall()

    if not member_rows:
        return {
            "ok": True,
            "checked_at": checked_at.isoformat(timespec="seconds"),
            "pool_scope": int(pool_id) if pool_id is not None else None,
            "totals": {
                "member_count": 0,
                "unique_ts_count": 0,
                "pool_count": 0,
            },
            "em_tables": {
                "board_count": 0,
                "member_count": 0,
            },
            "source_counts": {},
            "match_counts": {},
            "coverage": {
                "matched": 0,
                "unmatched": 0,
                "match_rate": 0.0,
                "code_first_rate": 0.0,
            },
            "unmatched_items": [],
            "fallback_items": [],
            "snapshot": {
                "count": 0,
                "updated_at": None,
                "updated_at_iso": None,
                "error": "",
                "sources": [],
            },
        }

    ts_codes = sorted({str(r[1] or "").strip().upper() for r in member_rows if str(r[1] or "").strip()})
    pool_counts: dict[str, int] = {}
    for row in member_rows:
        pool_counts[str(int(row[0]))] = pool_counts.get(str(int(row[0])), 0) + 1

    industry_snapshot_map, snapshot_error, snapshot_at = pool_service_module._fetch_realtime_industry_snapshot_map(
        force_refresh=False
    )
    snapshot_sources = sorted(
        {
            str(v.get("source") or "").strip()
            for v in (industry_snapshot_map or {}).values()
            if isinstance(v, dict) and str(v.get("source") or "").strip()
        }
    )

    em_name_map: dict[str, str] = {}
    em_code_map: dict[str, str] = {}
    stock_basic_map: dict[str, dict] = {}
    em_board_count = 0
    em_member_count = 0
    with pool_service_module._data_ro_conn_ctx() as dconn:
        em_name_map, em_code_map = pool_service_module._load_em_industry_member_maps(dconn, ts_codes)
        try:
            row = dconn.execute("SELECT COUNT(*) FROM em_industry_board").fetchone()
            em_board_count = int(row[0] or 0) if row else 0
        except Exception:
            em_board_count = 0
        try:
            row = dconn.execute("SELECT COUNT(*) FROM em_industry_member").fetchone()
            em_member_count = int(row[0] or 0) if row else 0
        except Exception:
            em_member_count = 0
        if ts_codes:
            placeholders = ",".join(["?"] * len(ts_codes))
            try:
                rows = dconn.execute(
                    f"""
                    SELECT ts_code, name, industry, market
                    FROM stock_basic
                    WHERE ts_code IN ({placeholders})
                    """,
                    ts_codes,
                ).fetchall()
                for ts_code, name, industry, market in rows:
                    code = str(ts_code or "").strip().upper()
                    if not code:
                        continue
                    stock_basic_map[code] = {
                        "name": str(name or "").strip(),
                        "industry": str(industry or "").strip(),
                        "market": str(market or "").strip(),
                    }
            except Exception:
                stock_basic_map = {}

    alias_inputs = {
        str(em_name_map.get(ts_code, "") or "").strip()
        for ts_code in ts_codes
        if str(em_name_map.get(ts_code, "") or "").strip()
    }
    alias_inputs.update(
        str((stock_basic_map.get(ts_code) or {}).get("industry") or "").strip()
        for ts_code in ts_codes
        if str((stock_basic_map.get(ts_code) or {}).get("industry") or "").strip()
    )
    alias_inputs.update(str(r[3] or "").strip() for r in member_rows if str(r[3] or "").strip())
    pool_service_module._attach_industry_snapshot_aliases(industry_snapshot_map, alias_inputs)

    source_counts = {
        "em_industry_member": 0,
        "stock_basic_fallback": 0,
        "monitor_pool_fallback": 0,
        "missing": 0,
    }
    match_counts = {
        "code": 0,
        "exact": 0,
        "normalized": 0,
        "alias": 0,
        "contains": 0,
        "fuzzy": 0,
        "resolved": 0,
        "unmatched": 0,
    }
    matched_count = 0
    code_first_count = 0
    unmatched_items: list[dict] = []
    fallback_items: list[dict] = []

    for row in member_rows:
        member_pool_id = int(row[0])
        ts_code = str(row[1] or "").strip().upper()
        name = str(row[2] or "").strip()
        pool_industry = str(row[3] or "").strip()
        if not ts_code:
            continue

        em_industry_name = str(em_name_map.get(ts_code, "") or "").strip()
        em_industry_code = str(em_code_map.get(ts_code, "") or "").strip().upper()
        basic_meta = stock_basic_map.get(ts_code) or {}
        stock_basic_industry = str(basic_meta.get("industry") or "").strip()

        if em_industry_name or em_industry_code:
            source = "em_industry_member"
        elif stock_basic_industry:
            source = "stock_basic_fallback"
        elif pool_industry:
            source = "monitor_pool_fallback"
        else:
            source = "missing"
        source_counts[source] = source_counts.get(source, 0) + 1

        effective_industry = em_industry_name or stock_basic_industry or pool_industry
        effective_code = em_industry_code

        resolved = pool_service_module._resolve_industry_snapshot(
            industry_snapshot_map,
            industry_name=effective_industry,
            industry_code=effective_code,
        )
        resolved_name = str((resolved or {}).get("industry_name") or "").strip()
        resolved_code = str((resolved or {}).get("industry_code") or "").strip().upper()
        resolved_state = str((resolved or {}).get("state") or "").strip()
        if effective_code and resolved_code and resolved_code == pool_service_module._normalize_industry_board_code(effective_code):
            match_method = "code"
        elif resolved:
            match_method = str(resolved.get("industry_match_method") or "").strip()
            if not match_method:
                if effective_industry and effective_industry in industry_snapshot_map:
                    match_method = "exact"
                elif effective_industry and pool_service_module._normalize_industry_name(effective_industry) in industry_snapshot_map:
                    match_method = "normalized"
                else:
                    match_method = "resolved"
        else:
            match_method = "unmatched"

        if match_method not in match_counts:
            match_counts[match_method] = 0
        match_counts[match_method] += 1
        if match_method != "unmatched":
            matched_count += 1
        if match_method == "code":
            code_first_count += 1

        item = {
            "pool_id": member_pool_id,
            "ts_code": ts_code,
            "name": name,
            "source": source,
            "pool_industry": pool_industry,
            "stock_basic_industry": stock_basic_industry,
            "em_industry_name": em_industry_name,
            "em_industry_code": em_industry_code,
            "effective_industry": effective_industry,
            "effective_code": effective_code,
            "match_method": match_method,
            "resolved_industry": resolved_name,
            "resolved_code": resolved_code,
            "resolved_state": resolved_state,
        }
        if match_method == "unmatched" and len(unmatched_items) < limit_n:
            unmatched_items.append(item)
        if source != "em_industry_member" and len(fallback_items) < limit_n:
            fallback_items.append(item)

    member_count = len(member_rows)
    return {
        "ok": True,
        "checked_at": checked_at.isoformat(timespec="seconds"),
        "pool_scope": int(pool_id) if pool_id is not None else None,
        "totals": {
            "member_count": member_count,
            "unique_ts_count": len(ts_codes),
            "pool_count": len(pool_counts),
            "pool_breakdown": pool_counts,
        },
        "em_tables": {
            "board_count": em_board_count,
            "member_count": em_member_count,
            "mapped_name_count": len(em_name_map),
            "mapped_code_count": len(em_code_map),
        },
        "source_counts": source_counts,
        "match_counts": match_counts,
        "coverage": {
            "matched": matched_count,
            "unmatched": member_count - matched_count,
            "match_rate": round((matched_count / member_count) * 100.0, 2) if member_count else 0.0,
            "code_first_count": code_first_count,
            "code_first_rate": round((code_first_count / member_count) * 100.0, 2) if member_count else 0.0,
        },
        "snapshot": {
            "count": len(
                {
                    str(v.get("industry_name") or "").strip()
                    for v in (industry_snapshot_map or {}).values()
                    if isinstance(v, dict) and str(v.get("industry_name") or "").strip()
                }
            ),
            "updated_at": int(snapshot_at) if snapshot_at > 0 else None,
            "updated_at_iso": datetime.datetime.fromtimestamp(snapshot_at).isoformat(timespec="seconds")
            if snapshot_at > 0
            else None,
            "age_sec": round(checked_at.timestamp() - snapshot_at, 3) if snapshot_at > 0 else None,
            "sources": snapshot_sources,
            "error": str(snapshot_error or ""),
        },
        "unmatched_items": unmatched_items,
        "fallback_items": fallback_items,
    }


def runtime_concept_snapshot_refresh():
    checked_at = datetime.datetime.now()
    rows_loaded = 0
    error_text = ""
    try:
        raw_snapshot_map, error_text, fetched_at = pool_service_module._fetch_realtime_concept_snapshot_map(force_refresh=True)
        if raw_snapshot_map:
            runtime_snapshot_map: dict[str, dict] = {}
            seen_names: set[str] = set()
            for snapshot in raw_snapshot_map.values():
                if not isinstance(snapshot, dict):
                    continue
                concept_name = str(snapshot.get("concept_name") or "").strip()
                if not concept_name or concept_name in seen_names:
                    continue
                seen_names.add(concept_name)
                pool_service_module._register_concept_snapshot_alias(
                    runtime_snapshot_map,
                    concept_name=concept_name,
                    concept_code=snapshot.get("board_code"),
                    snapshot=dict(snapshot),
                )
            rows_loaded = len(seen_names)
            if runtime_snapshot_map:
                provider = get_tick_provider()
                provider.bulk_update_concept_snapshots(runtime_snapshot_map)
        status = runtime_concept_snapshot_status()
        return {
            "ok": bool(status.get("ok")),
            "checked_at": checked_at.isoformat(),
            "message": "concept snapshot refreshed" if rows_loaded > 0 else "concept snapshot refresh finished",
            "rows_loaded": int(rows_loaded),
            "fetch_error": str(error_text or ""),
            "status": status,
        }
    except Exception as e:
        error_text = str(e)
        status = runtime_concept_snapshot_status()
        return {
            "ok": False,
            "checked_at": checked_at.isoformat(),
            "message": "concept snapshot refresh failed",
            "rows_loaded": int(rows_loaded),
            "fetch_error": error_text,
            "status": status,
        }


def runtime_board_catalog_status():
    status = runtime_jobs._get_board_catalog_status()
    return status


def runtime_board_catalog_refresh(
    force: bool = Query(False, description="是否强制重刷今天已同步过的板块目录与成分股"),
):
    checked_at = datetime.datetime.now()
    before = runtime_jobs._get_board_catalog_status()
    ok = runtime_jobs._run_board_catalog_sync_once(reason="api_manual", force=bool(force))
    after = runtime_jobs._get_board_catalog_status()
    message = "board catalog already fresh" if (before.get("ok") and not bool(force)) else "board catalog refresh finished"
    if not ok:
        message = "board catalog refresh failed"
    return {
        "ok": bool(ok and after.get("ok")),
        "checked_at": checked_at.isoformat(timespec="seconds"),
        "force": bool(force),
        "message": message,
        "before": before,
        "after": after,
    }


def _table_probe(conn: duckdb.DuckDBPyConnection, table_name: str) -> tuple[bool, Optional[str]]:
    try:
        conn.execute(f'SELECT 1 FROM "{table_name}" LIMIT 1').fetchone()
        return True, None
    except Exception as e:
        return False, str(e)
def runtime_selfcheck():
    now = datetime.datetime.now()
    checks: list[dict] = []
    ok_all = True

    rt_conn = None
    data_conn = None
    try:
        try:
            rt_conn = _get_conn()
            checks.append({"name": "realtime_db_open", "ok": True, "db": _REALTIME_DB})
        except Exception as e:
            ok_all = False
            checks.append({"name": "realtime_db_open", "ok": False, "db": _REALTIME_DB, "error": str(e)})

        try:
            data_conn = open_duckdb_conn(_DATA_DB, retries=2, base_sleep_sec=0.05)
            checks.append({"name": "data_db_open", "ok": True, "db": _DATA_DB})
        except Exception as e:
            ok_all = False
            checks.append({"name": "data_db_open", "ok": False, "db": _DATA_DB, "error": str(e)})

        if rt_conn is not None:
            for t in ("monitor_pools", "tick_history", "signal_history", "t0_signal_quality", "t0_feature_drift"):
                ok, err = _table_probe(rt_conn, t)
                checks.append({"name": f"realtime_table:{t}", "ok": ok, "error": err})
                if not ok:
                    ok_all = False
            try:
                c = rt_conn.execute("SELECT COUNT(*) FROM monitor_pools").fetchone()
                checks.append({"name": "realtime_pool_member_count", "ok": True, "value": int(c[0] or 0)})
            except Exception as e:
                ok_all = False
                checks.append({"name": "realtime_pool_member_count", "ok": False, "error": str(e)})

        if data_conn is not None:
            for t in ("stock_basic", "daily", "daily_basic", "stk_factor_pro", "trade_cal"):
                ok, err = _table_probe(data_conn, t)
                checks.append({"name": f"data_table:{t}", "ok": ok, "error": err})
                if not ok:
                    ok_all = False
            try:
                row = data_conn.execute("SELECT MAX(trade_date) FROM stk_factor_pro").fetchone()
                latest = row[0] if row else None
                checks.append(
                    {
                        "name": "data_latest_factor_date",
                        "ok": bool(latest),
                        "value": (latest.isoformat() if hasattr(latest, "isoformat") else str(latest)) if latest else None,
                    }
                )
            except Exception as e:
                ok_all = False
                checks.append({"name": "data_latest_factor_date", "ok": False, "error": str(e)})

        market = _is_market_open()
        checks.append({"name": "market_status", "ok": True, "value": market})

        return {
            "ok": ok_all,
            "checked_at": now.isoformat(),
            "checks": checks,
        }
    finally:
        if rt_conn is not None:
            try:
                rt_conn.close()
            except Exception:
                pass
        if data_conn is not None:
            try:
                data_conn.close()
            except Exception:
                pass
def runtime_channel_split_stats(hours: int = Query(24, ge=1, le=240)):
    """
    统计近 N 小时 Pool1/Pool2 的通道分布，检测是否存在串池污染。
    """
    since_ts = int(time.time()) - int(hours) * 3600
    since_dt = datetime.datetime.fromtimestamp(since_ts)
    rows_out: list[dict] = []
    conflicts: list[dict] = []
    try:
        with _ro_conn_ctx() as conn:
            rows = conn.execute(
                """
                SELECT
                    pool_id,
                    COALESCE(channel, '') AS channel,
                    signal_type,
                    COUNT(*) AS cnt,
                    COUNT(DISTINCT ts_code) AS symbols,
                    MIN(triggered_at) AS min_ts,
                    MAX(triggered_at) AS max_ts
                FROM signal_history
                WHERE triggered_at >= ?
                GROUP BY 1,2,3
                ORDER BY pool_id, channel, cnt DESC
                """,
                [since_dt],
            ).fetchall()
            src_rows = conn.execute(
                """
                SELECT
                    pool_id,
                    COALESCE(channel, '') AS channel,
                    COALESCE(signal_source, '') AS signal_source,
                    COUNT(*) AS cnt
                FROM signal_history
                WHERE triggered_at >= ?
                GROUP BY 1,2,3
                ORDER BY pool_id, channel, cnt DESC
                """,
                [since_dt],
            ).fetchall()
    except Exception as e:
        return {
            "ok": False,
            "checked_at": datetime.datetime.now().isoformat(),
            "hours": int(hours),
            "error": str(e),
        }

    for r in rows:
        pool_id = int(r[0] or 0)
        channel = str(r[1] or "")
        signal_type = str(r[2] or "")
        cnt = int(r[3] or 0)
        symbols = int(r[4] or 0)
        min_dt = r[5]
        max_dt = r[6]
        expected_channel = "pool1_timing" if pool_id == 1 else ("pool2_t0" if pool_id == 2 else "")
        conflict = bool(expected_channel and channel and channel != expected_channel)
        item = {
            "pool_id": pool_id,
            "channel": channel,
            "expected_channel": expected_channel,
            "signal_type": signal_type,
            "count": cnt,
            "symbols": symbols,
            "first_at": min_dt.isoformat() if hasattr(min_dt, "isoformat") else (str(min_dt) if min_dt else None),
            "last_at": max_dt.isoformat() if hasattr(max_dt, "isoformat") else (str(max_dt) if max_dt else None),
            "channel_conflict": conflict,
        }
        rows_out.append(item)
        if conflict:
            conflicts.append(item)

    source_rows = [
        {
            "pool_id": int(r[0] or 0),
            "channel": str(r[1] or ""),
            "signal_source": str(r[2] or ""),
            "count": int(r[3] or 0),
        }
        for r in src_rows
    ]

    return {
        "ok": len(conflicts) == 0,
        "checked_at": datetime.datetime.now().isoformat(),
        "hours": int(hours),
        "summary": {
            "groups": len(rows_out),
            "source_groups": len(source_rows),
            "conflicts": len(conflicts),
        },
        "conflicts": conflicts,
        "rows": rows_out,
        "source_rows": source_rows,
    }
def runtime_fast_slow_diff(
    pool_id: int = Query(1, ge=1, le=2),
    limit: int = Query(200, ge=1, le=2000),
):
    """
    Fast/slow signal path consistency check.
    - fast: provider cached signal path
    - slow: rebuild member context + evaluate_pool
    """
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")

    with _ro_conn_ctx() as conn:
        members = conn.execute(
            "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
            [pool_id],
        ).fetchall()
    if not members:
        return {
            "ok": True,
            "checked_at": datetime.datetime.now().isoformat(),
            "pool_id": int(pool_id),
            "summary": {"members": 0, "mismatch": 0, "exact_match": 0},
            "rows": [],
            "field_missing": {},
        }

    provider = get_tick_provider()
    fast_rows = _evaluate_signals_fast_internal(pool_id, members, provider)

    with _ro_conn_ctx() as conn:
        member_ctx = _build_member_data(conn, pool_id)
    slow_rows = sig.evaluate_pool(pool_id, member_ctx) if member_ctx else []

    def _signal_map(row: Optional[dict]) -> dict[str, dict]:
        if not row:
            return {}
        out: dict[str, dict] = {}
        for s in row.get("signals", []) or []:
            if not isinstance(s, dict):
                continue
            if s.get("has_signal") is False:
                continue
            t = str(s.get("type") or "")
            if t:
                out[t] = s
        return out

    def _active_types(row: Optional[dict]) -> set[str]:
        return set(_signal_map(row).keys())

    def _norm_value(v):
        if isinstance(v, float):
            return round(float(v), 6)
        if isinstance(v, dict):
            return {str(k): _norm_value(x) for k, x in sorted(v.items(), key=lambda kv: str(kv[0]))}
        if isinstance(v, list):
            return [_norm_value(x) for x in v]
        return v

    def _value_changed(a, b) -> bool:
        try:
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                return abs(float(a) - float(b)) > 1e-6
            return _norm_value(a) != _norm_value(b)
        except Exception:
            return str(a) != str(b)

    def _extract_conditions(signal_obj: Optional[dict]) -> list[str]:
        if not isinstance(signal_obj, dict):
            return []
        details = signal_obj.get("details")
        if isinstance(details, dict):
            cond = details.get("conditions")
            if isinstance(cond, list):
                return [str(x).strip() for x in cond if str(x).strip()]
        msg = str(signal_obj.get("message") or "")
        if not msg:
            return []
        # message schema usually: "<title> | 条件1 + 条件2 + 确认:..."
        tail = msg.split("|", 1)[1] if "|" in msg else msg
        return [p.strip() for p in tail.split("+") if p and p.strip()]

    trigger_keys_by_type: dict[str, list[str]] = {
        "left_side_buy": ["dist_to_lower_pct", "near_lower_th_pct", "rsi6", "pct_chg", "resonance_60m"],
        "right_side_breakout": ["cross_mid", "cross_upper", "volume_ratio", "ma5", "ma10", "rsi6", "resonance_60m"],
        "timing_clear": ["holding_days", "break_mid", "break_ma10", "break_ma20", "trend_reversal", "early_risk_exit", "bid_ask_ratio"],
        "positive_t": ["boll_break", "bias_vwap", "gub5_transition", "bid_ask_ratio", "lure_short", "ask_wall_absorb"],
        "reverse_t": ["boll_over", "robust_zscore", "v_power_divergence", "ask_wall_building", "real_buy_fading"],
    }
    threshold_extra_keys_by_type: dict[str, list[str]] = {
        "left_side_buy": ["near_lower_th_pct"],
        "right_side_breakout": [],
        "timing_clear": [],
        "positive_t": [],
        "reverse_t": [],
    }

    def _trigger_diff(signal_type: str, fast_sig: dict, slow_sig: dict) -> dict:
        fd = fast_sig.get("details") if isinstance(fast_sig.get("details"), dict) else {}
        sd = slow_sig.get("details") if isinstance(slow_sig.get("details"), dict) else {}
        f_conds = _extract_conditions(fast_sig)
        s_conds = _extract_conditions(slow_sig)
        cond_only_fast = sorted(list(set(f_conds) - set(s_conds)))
        cond_only_slow = sorted(list(set(s_conds) - set(f_conds)))
        detail_delta: dict[str, dict] = {}
        for k in trigger_keys_by_type.get(signal_type, []):
            fv = fd.get(k)
            sv = sd.get(k)
            if _value_changed(fv, sv):
                detail_delta[k] = {"fast": _norm_value(fv), "slow": _norm_value(sv)}
        return {
            "fast_conditions": f_conds,
            "slow_conditions": s_conds,
            "condition_only_fast": cond_only_fast,
            "condition_only_slow": cond_only_slow,
            "detail_delta": detail_delta,
            "has_diff": bool(cond_only_fast or cond_only_slow or detail_delta),
        }

    def _threshold_diff(signal_type: str, fast_sig: dict, slow_sig: dict) -> dict:
        fd = fast_sig.get("details") if isinstance(fast_sig.get("details"), dict) else {}
        sd = slow_sig.get("details") if isinstance(slow_sig.get("details"), dict) else {}
        f_th = fd.get("threshold") if isinstance(fd.get("threshold"), dict) else {}
        s_th = sd.get("threshold") if isinstance(sd.get("threshold"), dict) else {}
        delta: dict[str, dict] = {}
        for k in sorted(set(f_th.keys()) | set(s_th.keys())):
            fv = f_th.get(k)
            sv = s_th.get(k)
            if _value_changed(fv, sv):
                delta[str(k)] = {"fast": _norm_value(fv), "slow": _norm_value(sv)}
        f_extra: dict[str, object] = {}
        s_extra: dict[str, object] = {}
        for k in threshold_extra_keys_by_type.get(signal_type, []):
            fv = fd.get(k)
            sv = sd.get(k)
            if fv is not None:
                f_extra[k] = _norm_value(fv)
            if sv is not None:
                s_extra[k] = _norm_value(sv)
            if _value_changed(fv, sv):
                delta[k] = {"fast": _norm_value(fv), "slow": _norm_value(sv)}
        return {
            "fast_threshold": _norm_value(f_th),
            "slow_threshold": _norm_value(s_th),
            "fast_threshold_extra": f_extra,
            "slow_threshold_extra": s_extra,
            "delta": delta,
            "has_diff": bool(delta),
        }

    fast_map = {str(r.get("ts_code")): r for r in fast_rows}
    slow_map = {str(r.get("ts_code")): r for r in slow_rows}

    # Context completeness diagnostics (slow path input fields).
    # Use pool-specific required fields to avoid false positives.
    field_missing: dict[str, int] = {}
    required_fields: list[str] = []
    market_open = bool(_is_market_open().get("is_open", False))
    if int(pool_id) == 1:
        required_fields = [
            "ma20",
            "bid_ask_ratio",
            "up_limit",
            "down_limit",
            "winner_rate",
            "chip_concentration_pct",
            "pool1_in_holding",
            "pool1_holding_days",
        ]
        # prev_price/resonance are runtime-sensitive intraday fields;
        # only treat as required in open session.
        if market_open:
            required_fields.extend(["prev_price", "resonance_60m"])
    member_ctx_map = {str(m.get("ts_code") or ""): m for m in (member_ctx or []) if isinstance(m, dict)}

    def _missing_fields_for_code(code: str) -> list[str]:
        m = member_ctx_map.get(code, {})
        out: list[str] = []
        for field in required_fields:
            v = m.get(field) if isinstance(m, dict) else None
            miss = v is None
            if isinstance(v, (int, float)) and field in ("up_limit", "down_limit") and float(v) <= 0:
                miss = True
            if miss:
                out.append(field)
                field_missing[field] = field_missing.get(field, 0) + 1
        return out

    rows: list[dict] = []
    mismatch = 0
    exact = 0
    condition_mismatch = 0
    threshold_mismatch = 0
    missing_field_members = 0
    full_exact_match = 0
    for ts_code, name in members:
        code = str(ts_code)
        f = fast_map.get(code, {})
        s = slow_map.get(code, {})
        f_sig_map = _signal_map(f)
        s_sig_map = _signal_map(s)
        f_types = _active_types(f)
        s_types = _active_types(s)
        only_fast = sorted(list(f_types - s_types))
        only_slow = sorted(list(s_types - f_types))
        same = (not only_fast) and (not only_slow)  # keep backward-compatible semantics
        if same:
            exact += 1
        else:
            mismatch += 1

        missing_fields = _missing_fields_for_code(code)
        if missing_fields:
            missing_field_members += 1

        common_types = sorted(list(f_types & s_types))
        trigger_condition_diff: dict[str, dict] = {}
        threshold_diff: dict[str, dict] = {}
        for sig_type in common_types:
            f_sig = f_sig_map.get(sig_type, {})
            s_sig = s_sig_map.get(sig_type, {})
            td = _trigger_diff(sig_type, f_sig, s_sig)
            if td.get("has_diff"):
                trigger_condition_diff[sig_type] = td
            thd = _threshold_diff(sig_type, f_sig, s_sig)
            if thd.get("has_diff"):
                threshold_diff[sig_type] = thd

        if trigger_condition_diff:
            condition_mismatch += 1
        if threshold_diff:
            threshold_mismatch += 1

        full_match = bool(same and not missing_fields and not trigger_condition_diff and not threshold_diff)
        if full_match:
            full_exact_match += 1

        rows.append(
            {
                "ts_code": code,
                "name": str(name or ""),
                "fast_types": sorted(list(f_types)),
                "slow_types": sorted(list(s_types)),
                "only_fast": only_fast,
                "only_slow": only_slow,
                "fast_price": float(f.get("price", 0) or 0),
                "slow_price": float(s.get("price", 0) or 0),
                "price_delta": round(float(f.get("price", 0) or 0) - float(s.get("price", 0) or 0), 6),
                "match": same,
                "full_match": full_match,
                "missing_fields": missing_fields,
                "signal_type_diff": {
                    "only_fast": only_fast,
                    "only_slow": only_slow,
                    "common": common_types,
                },
                "trigger_condition_diff": trigger_condition_diff,
                "threshold_diff": threshold_diff,
            }
        )

    rows.sort(key=lambda x: (x.get("match", True), x.get("ts_code", "")))
    rows = rows[: int(limit)]

    ok = mismatch == 0
    return {
        "ok": ok,
        "checked_at": datetime.datetime.now().isoformat(),
        "pool_id": int(pool_id),
        "provider": provider.name,
        "summary": {
            "members": len(members),
            "mismatch": mismatch,
            "exact_match": exact,
            "mismatch_ratio": round((mismatch / len(members)) if members else 0.0, 6),
            "full_exact_match": full_exact_match,
            "full_mismatch": len(members) - full_exact_match,
            "missing_field_members": missing_field_members,
            "condition_mismatch_members": condition_mismatch,
            "threshold_mismatch_members": threshold_mismatch,
        },
        "rows": rows,
        "field_missing": field_missing,
    }
def _build_t0_reverse_diag_row(member: dict, provider) -> dict:
    from backend.realtime import gm_tick as gm_runtime
    from backend.realtime.threshold_engine import build_signal_context, get_thresholds

    m = dict(member or {})
    ts_code = str(m.get("ts_code") or "")
    name = str(m.get("name") or "")
    if not ts_code:
        return {"status": "error", "error": "missing_ts_code"}
    market_open = bool(_is_market_open().get("is_open", False))

    tick = {}
    try:
        tick = provider.get_cached_tick(ts_code) or {}
    except Exception:
        tick = {}
    if not tick:
        try:
            tick = provider.get_tick(ts_code) or {}
        except Exception:
            tick = {}
    if not isinstance(tick, dict) or not tick:
        return {
            "ts_code": ts_code,
            "name": name,
            "status": "error",
            "error": "tick_unavailable",
        }

    price = float(tick.get("price", m.get("price", 0)) or 0)
    pct_chg = float(tick.get("pct_chg", m.get("pct_chg", 0)) or 0)
    if price > 0:
        m["price"] = price
    m["pct_chg"] = pct_chg
    if (not market_open) and price <= 0:
        return {
            "ts_code": ts_code,
            "name": name,
            "price": round(price, 3),
            "pct_chg": round(pct_chg, 3),
            "status": "off_session_skip",
            "has_signal": False,
            "observe_only": False,
            "strength": 0.0,
            "message": "",
            "veto": None,
            "main_rally_guard": False,
            "trend_guard": {"enabled": True, "reason": "off_session_skip"},
            "trigger_items": [],
            "confirm_items": [],
            "bearish_confirm_count": 0,
            "threshold": {},
            "momentum_divergence": {"reason": "off_session_skip"},
            "market_structure": {},
            "feature_snapshot": {
                "trend_up": False,
                "boll_over": False,
                "robust_zscore": None,
                "bid_ask_ratio": None,
                "volume_pace_ratio": None,
                "volume_pace_state": "unknown",
                "main_rally_guard": False,
                "pct_chg": pct_chg,
            },
        }

    txns = None
    try:
        txns = provider.get_cached_transactions(ts_code)
    except Exception:
        txns = None
    if txns is None:
        try:
            txns = mootdx_client.get_transactions(ts_code, int(_TXN_ANALYZE_COUNT))
        except Exception:
            txns = None
    txns = txns or []

    ms = gm_runtime._analyze_market_structure(tick, txns)
    state = gm_runtime._update_intraday_state(ts_code, tick)
    vwap = m.get("vwap")
    if vwap in (None, 0):
        vwap = gm_runtime._compute_vwap(state)
    gub5_trend = m.get("gub5_trend") or gm_runtime._compute_gub5_trend(state)
    gub5_transition = gm_runtime._compute_gub5_transition(state)
    pw = state.get("price_window") or []
    intraday_prices = m.get("intraday_prices") or [p for _, p, _ in pw]
    v_power_divergence, vpower_info = gm_runtime._detect_v_power_divergence(state, txns)

    ma5 = float(m.get("ma5", 0) or 0)
    ma10 = float(m.get("ma10", 0) or 0)
    ma20 = float(m.get("ma20", 0) or 0)
    trend_up = bool(ma5 > ma10 > ma20 > 0)

    feat = sig.build_t0_features(
        tick_price=price,
        boll_lower=m.get("boll_lower", 0),
        boll_upper=m.get("boll_upper", 0),
        vwap=vwap,
        intraday_prices=intraday_prices,
        pct_chg=pct_chg,
        trend_up=trend_up,
        gub5_trend=gub5_trend,
        gub5_transition=gub5_transition,
        bid_ask_ratio=m.get("bid_ask_ratio"),
        lure_short=bool(ms.get("lure_short", False)),
        wash_trade=bool(ms.get("wash_trade", False)),
        spread_abnormal=bool(m.get("spread_abnormal", False)),
        v_power_divergence=bool(v_power_divergence),
        ask_wall_building=bool(ms.get("lure_long", False)),
        real_buy_fading=bool(ms.get("active_buy_fading", False)),
        wash_trade_severe=bool(ms.get("wash_trade", False) and abs(float(ms.get("big_order_bias", 0.0) or 0.0)) < 0.05),
        liquidity_drain=bool(m.get("liquidity_drain", False)),
        ask_wall_absorb_ratio=ms.get("ask_wall_absorb_ratio"),
        bid_wall_break_ratio=ms.get("bid_wall_break_ratio"),
        spoofing_vol_ratio=m.get("spoofing_vol_ratio"),
        big_order_bias=ms.get("big_order_bias"),
        big_net_flow_bps=ms.get("big_net_flow_bps"),
        super_order_bias=ms.get("super_order_bias"),
        super_net_flow_bps=ms.get("super_net_flow_bps"),
        volume_pace_ratio=m.get("volume_pace_ratio"),
        volume_pace_state=m.get("volume_pace_state"),
    )

    ctx = build_signal_context(
        ts_code,
        2,
        tick=tick,
        daily=m,
        intraday={
            "vwap": feat.get("vwap"),
            "bias_vwap": feat.get("bias_vwap"),
            "robust_zscore": feat.get("robust_zscore"),
            "gub5_ratio": m.get("gub5_ratio"),
            "gub5_transition": feat.get("gub5_transition"),
            "volume_pace_ratio": feat.get("volume_pace_ratio"),
            "volume_pace_state": feat.get("volume_pace_state"),
            "progress_ratio": m.get("volume_pace_progress"),
        },
        market={
            "industry": m.get("industry"),
            "vol_20": m.get("vol_20"),
            "atr_14": m.get("atr_14"),
            "name": m.get("name"),
            "market_name": m.get("market_name"),
            "list_date": m.get("list_date"),
            "industry_ecology": m.get("industry_ecology"),
            "concept_boards": m.get("concept_boards"),
            "concept_codes": m.get("concept_codes"),
            "core_concept_board": m.get("core_concept_board"),
            "core_concept_code": m.get("core_concept_code"),
            "concept_ecology": m.get("concept_ecology"),
            "concept_ecology_multi": m.get("concept_ecology_multi"),
            "instrument_profile": m.get("instrument_profile"),
            "index_context": load_t0_index_context(instrument_profile=m.get("instrument_profile"), ts_code=ts_code),
        },
    )
    thresholds = get_thresholds("reverse_t", ctx)
    rev = sig.detect_reverse_t(
        tick_price=feat["tick_price"],
        boll_upper=feat["boll_upper"],
        vwap=feat["vwap"],
        intraday_prices=intraday_prices,
        pct_chg=feat.get("pct_chg"),
        trend_up=bool(feat.get("trend_up", False)),
        bid_ask_ratio=feat["bid_ask_ratio"],
        v_power_divergence=feat["v_power_divergence"],
        ask_wall_building=feat["ask_wall_building"],
        real_buy_fading=feat["real_buy_fading"],
        wash_trade_severe=feat["wash_trade_severe"],
        liquidity_drain=feat["liquidity_drain"],
        ask_wall_absorb=bool(feat.get("ask_wall_absorb", False)),
        ask_wall_absorb_ratio=feat.get("ask_wall_absorb_ratio"),
        bid_wall_break=bool(feat.get("bid_wall_break", False)),
        bid_wall_break_ratio=feat.get("bid_wall_break_ratio"),
        boll_over=feat["boll_over"],
        spoofing_suspected=feat["spoofing_suspected"],
        big_order_bias=feat["big_order_bias"],
        super_order_bias=feat.get("super_order_bias"),
        super_net_flow_bps=feat.get("super_net_flow_bps"),
        robust_zscore=feat["robust_zscore"],
        volume_pace_ratio=feat.get("volume_pace_ratio"),
        volume_pace_state=feat.get("volume_pace_state"),
        main_rally_guard=bool(feat.get("main_rally_guard", False)),
        main_rally_info=feat.get("main_rally_info"),
        industry_ecology=m.get("industry_ecology"),
        concept_ecology=m.get("concept_ecology"),
        concept_ecology_multi=m.get("concept_ecology_multi"),
        index_context=load_t0_index_context(instrument_profile=m.get("instrument_profile"), ts_code=ts_code),
        ts_code=ts_code,
        thresholds=thresholds,
    )

    z_th = None
    try:
        z_th = float((thresholds or {}).get("z_th"))
    except Exception:
        z_th = None
    trigger_items: list[str] = []
    if bool(feat.get("boll_over", False)):
        trigger_items.append("boll_over")
    try:
        robust_z = float(feat.get("robust_zscore")) if feat.get("robust_zscore") is not None else None
    except Exception:
        robust_z = None
    if robust_z is not None and z_th is not None and robust_z > z_th:
        trigger_items.append("robust_zscore")

    confirm_items: list[str] = []
    if bool(feat.get("v_power_divergence", False)):
        confirm_items.append("v_power_divergence")
    if bool(feat.get("ask_wall_building", False)):
        confirm_items.append("ask_wall_building")
    if bool(feat.get("bid_wall_break", False)):
        confirm_items.append("bid_wall_break")
    if bool(feat.get("real_buy_fading", False)):
        confirm_items.append("real_buy_fading")
    try:
        if feat.get("super_order_bias") is not None and float(feat.get("super_order_bias")) <= -0.20:
            confirm_items.append("super_order_bias")
    except Exception:
        pass
    try:
        if feat.get("super_net_flow_bps") is not None and float(feat.get("super_net_flow_bps")) <= -120.0:
            confirm_items.append("super_net_flow_bps")
    except Exception:
        pass

    details = rev.get("details") if isinstance(rev.get("details"), dict) else {}
    veto = str(details.get("veto") or "")
    has_signal = bool(rev.get("has_signal", False))
    observe_only = bool(details.get("observe_only", False))
    strength = float(rev.get("strength", 0) or 0)
    index_context_gate = details.get("index_context_gate") if isinstance(details.get("index_context_gate"), dict) else {}
    index_context = details.get("index_context") if isinstance(details.get("index_context"), dict) else {}

    if has_signal:
        status = "trigger_observe_only" if observe_only else "triggered"
    elif veto == "trend_surge_absorb_guard":
        status = "surge_guard_veto"
    elif veto == "trend_extension_guard":
        status = "guard_veto"
    elif trigger_items and not confirm_items:
        status = "trigger_no_confirm"
    elif trigger_items and confirm_items:
        status = "trigger_score_filtered"
    elif bool(feat.get("main_rally_guard", False)):
        status = "candidate"
    else:
        status = "idle"

    return {
        "ts_code": ts_code,
        "name": name,
        "price": round(price, 3),
        "pct_chg": round(pct_chg, 3),
        "status": status,
        "has_signal": has_signal,
        "observe_only": observe_only,
        "strength": round(strength, 2),
        "message": str(rev.get("message", "") or ""),
        "veto": veto or None,
        "main_rally_guard": bool(feat.get("main_rally_guard", False)),
        "trend_guard": feat.get("main_rally_info") if isinstance(feat.get("main_rally_info"), dict) else {},
        "trigger_items": trigger_items,
        "confirm_items": confirm_items,
        "bearish_confirm_count": len(confirm_items),
        "threshold": details.get("threshold") if isinstance(details.get("threshold"), dict) else thresholds,
        "index_context_gate": index_context_gate,
        "index_context": index_context,
        "momentum_divergence": dict(vpower_info or {}),
        "market_structure": {
            "tag": ms.get("tag"),
            "big_order_bias": ms.get("big_order_bias"),
            "big_net_flow_bps": ms.get("big_net_flow_bps"),
            "super_order_bias": ms.get("super_order_bias"),
            "super_net_flow_bps": ms.get("super_net_flow_bps"),
            "bid_ask_ratio": ms.get("bid_ask_ratio"),
            "ask_wall_absorb_ratio": ms.get("ask_wall_absorb_ratio"),
            "bid_wall_break_ratio": ms.get("bid_wall_break_ratio"),
            "ask_wall_absorbed": bool(ms.get("ask_wall_absorbed", False)),
            "bid_wall_broken": bool(ms.get("bid_wall_broken", False)),
            "active_buy_fading": bool(ms.get("active_buy_fading", False)),
            "pull_up_not_stable": bool(ms.get("pull_up_not_stable", False)),
            "peak_gain_pct": ms.get("peak_gain_pct"),
            "retrace_from_high_pct": ms.get("retrace_from_high_pct"),
        },
        "feature_snapshot": {
            "trend_up": trend_up,
            "boll_over": bool(feat.get("boll_over", False)),
            "robust_zscore": feat.get("robust_zscore"),
            "bid_ask_ratio": feat.get("bid_ask_ratio"),
            "volume_pace_ratio": feat.get("volume_pace_ratio"),
            "volume_pace_state": feat.get("volume_pace_state"),
            "main_rally_guard": bool(feat.get("main_rally_guard", False)),
            "pct_chg": feat.get("pct_chg"),
        },
    }
def runtime_t0_reverse_diagnostics(
    limit: int = Query(50, ge=1, le=500),
    interesting_only: bool = Query(True),
):
    with _ro_conn_ctx() as conn:
        members_data = _build_member_data(conn, 2)
    provider = get_tick_provider()
    rows: list[dict] = []
    for m in members_data or []:
        try:
            rows.append(_build_t0_reverse_diag_row(m, provider))
        except Exception as e:
            rows.append(
                {
                    "ts_code": str((m or {}).get("ts_code") or ""),
                    "name": str((m or {}).get("name") or ""),
                    "status": "error",
                    "error": str(e),
                }
            )

    total_rows = len(rows)
    summary = {
        "members": total_rows,
        "triggered": 0,
        "observe_only": 0,
        "index_observe_only": 0,
        "index_risk_on_no_distribution": 0,
        "surge_guard_veto": 0,
        "guard_veto": 0,
        "main_rally_guard": 0,
        "trigger_no_confirm": 0,
        "trigger_score_filtered": 0,
        "candidate": 0,
        "off_session_skip": 0,
        "error": 0,
    }
    for row in rows:
        status = str(row.get("status") or "")
        if status == "triggered":
            summary["triggered"] += 1
        elif status == "trigger_observe_only":
            summary["triggered"] += 1
            summary["observe_only"] += 1
        elif status == "surge_guard_veto":
            summary["surge_guard_veto"] += 1
        elif status == "guard_veto":
            summary["guard_veto"] += 1
        elif status == "trigger_no_confirm":
            summary["trigger_no_confirm"] += 1
        elif status == "trigger_score_filtered":
            summary["trigger_score_filtered"] += 1
        elif status == "candidate":
            summary["candidate"] += 1
        elif status == "off_session_skip":
            summary["off_session_skip"] += 1
        elif status == "error":
            summary["error"] += 1
        if bool(row.get("main_rally_guard", False)):
            summary["main_rally_guard"] += 1
        index_gate = row.get("index_context_gate") if isinstance(row.get("index_context_gate"), dict) else {}
        index_reasons = [str(x or "").strip() for x in (index_gate.get("reasons") or [])] if isinstance(index_gate.get("reasons"), list) else []
        if bool(index_gate.get("observe_only")):
            summary["index_observe_only"] += 1
        if "index_risk_on_no_distribution" in index_reasons:
            summary["index_risk_on_no_distribution"] += 1

    if interesting_only:
        rows = [r for r in rows if str(r.get("status") or "") not in {"idle", "off_session_skip"}]
    rows.sort(key=_t0_reverse_diag_sort_key)

    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "interesting_only": bool(interesting_only),
        "summary": summary,
        "rows_total": total_rows,
        "count": len(rows[: int(limit)]),
        "rows": rows[: int(limit)],
    }
def runtime_consistency_check(
    pool_id: int = Query(1, ge=1, le=2),
    limit: int = Query(200, ge=1, le=2000),
):
    """
    Alias of fast/slow consistency diagnostics.
    Output includes:
    - field missing
    - signal type diff
    - trigger condition diff
    - threshold diff
    """
    return runtime_fast_slow_diff(pool_id=pool_id, limit=limit)
def _fast_slow_trend_key(pool_id: int) -> str:
    return f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:fast_slow:trend:pool:{int(pool_id)}"
def _append_fast_slow_trend_snapshot(snap: dict, now_ts: Optional[float] = None) -> None:
    """
    Persist fast/slow mismatch snapshots for trend view.
    Storage priority: Redis (when enabled), with in-memory fallback always kept.
    """
    if not _FAST_SLOW_TREND_ENABLED or not isinstance(snap, dict):
        return
    ts_now = float(now_ts) if isinstance(now_ts, (int, float)) else time.time()
    pool_id = int(snap.get("pool_id", 0) or 0)
    if pool_id <= 0:
        return

    min_interval = max(1.0, float(_FAST_SLOW_TREND_MIN_INTERVAL_SEC))
    with _fast_slow_trend_lock:
        last_ts = float(_fast_slow_trend_last_ts.get(pool_id, 0.0) or 0.0)
        if last_ts > 0 and (ts_now - last_ts) < min_interval:
            return
        _fast_slow_trend_last_ts[pool_id] = ts_now

    point = {
        "pool_id": pool_id,
        "at": int(ts_now),
        "at_iso": datetime.datetime.fromtimestamp(ts_now).isoformat(),
        "mismatch_ratio": round(float(snap.get("mismatch_ratio", 0.0) or 0.0), 6),
        "mismatch": int(snap.get("mismatch", 0) or 0),
        "members": int(snap.get("members", 0) or 0),
        "error": snap.get("error"),
    }

    with _fast_slow_trend_lock:
        dq = _fast_slow_trend_mem.get(pool_id)
        if dq is None:
            dq = deque(maxlen=max(200, int(_FAST_SLOW_TREND_MAX_POINTS)))
            _fast_slow_trend_mem[pool_id] = dq
        dq.append(point)

    cli = _get_runtime_state_redis()
    if cli is None:
        return
    key = _fast_slow_trend_key(pool_id)
    retention_sec = max(3600, int(_FAST_SLOW_TREND_RETENTION_HOURS) * 3600)
    ttl = max(_runtime_state_ttl_sec(), retention_sec + 24 * 3600)
    try:
        raw = json.dumps(point, ensure_ascii=False, separators=(",", ":"))
        pipe = cli.pipeline(transaction=False)
        pipe.zadd(key, {raw: int(ts_now)})
        pipe.zremrangebyscore(key, 0, int(ts_now) - retention_sec)
        pipe.expire(key, ttl)
        pipe.execute()
        # hard cap size (oldest first)
        try:
            zc = int(cli.zcard(key) or 0)
            max_pts = max(200, int(_FAST_SLOW_TREND_MAX_POINTS))
            if zc > max_pts:
                cli.zremrangebyrank(key, 0, zc - max_pts - 1)
        except Exception:
            pass
    except Exception:
        pass
def runtime_fast_slow_trend(
    pool_id: int = Query(1, ge=1, le=2),
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(500, ge=10, le=5000),
):
    if pool_id not in POOLS:
        raise HTTPException(404, f"pool_id {pool_id} not found")
    now_ts = int(time.time())
    since_ts = now_ts - int(hours) * 3600
    lim = max(10, min(5000, int(limit)))

    points: list[dict] = []
    storage = "memory"
    cli = _get_runtime_state_redis()
    if cli is not None:
        key = _fast_slow_trend_key(int(pool_id))
        try:
            raws = cli.zrangebyscore(key, since_ts, now_ts)
            if raws:
                if len(raws) > lim:
                    raws = raws[-lim:]
                for raw in raws:
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict):
                            points.append(obj)
                    except Exception:
                        continue
                storage = "redis"
        except Exception:
            points = []

    # Fallback to in-memory snapshots when redis not available or empty.
    if not points:
        with _fast_slow_trend_lock:
            dq = _fast_slow_trend_mem.get(int(pool_id))
            arr = list(dq) if dq is not None else []
        points = [p for p in arr if int(p.get("at", 0) or 0) >= since_ts]
        if len(points) > lim:
            points = points[-lim:]
        storage = "memory"

    latest = points[-1] if points else None
    return {
        "ok": True,
        "checked_at": datetime.datetime.now().isoformat(),
        "pool_id": int(pool_id),
        "hours": int(hours),
        "count": len(points),
        "latest": latest,
        "storage_source": storage,
        "points": points,
    }
def _get_fast_slow_health_snapshot(pool_id: int) -> dict:
    now_ts = time.time()
    ttl = max(1.0, float(_FAST_SLOW_HEALTH_CACHE_TTL_SEC))
    with _fast_slow_health_lock:
        cached_at = float(_fast_slow_health_cache.get("at", 0.0) or 0.0)
        cached_pool = int(_fast_slow_health_cache.get("pool_id", 0) or 0)
        cached_payload = _fast_slow_health_cache.get("payload")
        if cached_pool == int(pool_id) and cached_payload and (now_ts - cached_at) <= ttl:
            snap = dict(cached_payload)
            snap["cached"] = True
            snap["cache_age_sec"] = round(max(0.0, now_ts - cached_at), 3)
            return snap

    try:
        diff = runtime_fast_slow_diff(pool_id=int(pool_id), limit=2000)
        summary = diff.get("summary", {}) if isinstance(diff, dict) else {}
        payload = {
            "ok": bool(diff.get("ok", False)) if isinstance(diff, dict) else False,
            "pool_id": int(pool_id),
            "members": int(summary.get("members", 0) or 0),
            "mismatch": int(summary.get("mismatch", 0) or 0),
            "mismatch_ratio": float(summary.get("mismatch_ratio", 0.0) or 0.0),
            "checked_at": diff.get("checked_at") if isinstance(diff, dict) else None,
            "error": None,
            "cached": False,
            "cache_age_sec": 0.0,
        }
    except Exception as e:
        payload = {
            "ok": False,
            "pool_id": int(pool_id),
            "members": 0,
            "mismatch": 0,
            "mismatch_ratio": 0.0,
            "checked_at": datetime.datetime.now().isoformat(),
            "error": str(e),
            "cached": False,
            "cache_age_sec": 0.0,
        }

    with _fast_slow_health_lock:
        _fast_slow_health_cache["at"] = now_ts
        _fast_slow_health_cache["pool_id"] = int(pool_id)
        _fast_slow_health_cache["payload"] = dict(payload)
    # record trend only for fresh computations (not cache hit path)
    _append_fast_slow_trend_snapshot(payload, now_ts=now_ts)
    return payload
def _maybe_record_health_alert(payload: dict) -> dict:
    now_ts = time.time()
    level = str(payload.get("level", "green"))
    triggered = False
    snapshot = {}
    with _health_alert_lock:
        st = _health_alert_state
        st["last_level"] = level
        if level == "green":
            st["non_green_streak"] = 0
            st["yellow_streak"] = 0
            st["red_streak"] = 0
        elif level == "yellow":
            st["non_green_streak"] = int(st.get("non_green_streak", 0)) + 1
            st["yellow_streak"] = int(st.get("yellow_streak", 0)) + 1
            st["red_streak"] = 0
        else:
            st["non_green_streak"] = int(st.get("non_green_streak", 0)) + 1
            st["red_streak"] = int(st.get("red_streak", 0)) + 1
            st["yellow_streak"] = 0

        can_alert = (
            _HEALTH_ALERT_ENABLED
            and level in {"yellow", "red"}
            and int(st.get("non_green_streak", 0)) >= max(1, _HEALTH_ALERT_CONSECUTIVE)
            and (now_ts - float(st.get("last_alert_at", 0.0))) >= max(1, _HEALTH_ALERT_COOLDOWN_SEC)
        )
        if can_alert:
            event = {
                "at": datetime.datetime.fromtimestamp(now_ts).isoformat(),
                "level": level,
                "non_green_streak": int(st.get("non_green_streak", 0)),
                "yellow_streak": int(st.get("yellow_streak", 0)),
                "red_streak": int(st.get("red_streak", 0)),
                "profile": payload.get("profile"),
                "market": payload.get("market"),
                "summary": payload.get("summary"),
            }
            events = list(st.get("events", []))
            events.append(event)
            if len(events) > max(10, _HEALTH_ALERT_MAX_EVENTS):
                events = events[-max(10, _HEALTH_ALERT_MAX_EVENTS):]
            st["events"] = events
            st["last_alert_at"] = now_ts
            triggered = True
            try:
                logger.warning(
                    "[HealthAlert] level={} streak={} profile={} summary={}",
                    level,
                    st.get("non_green_streak", 0),
                    payload.get("profile"),
                    payload.get("summary"),
                )
            except Exception:
                pass
        snapshot = {
            "last_level": st.get("last_level"),
            "non_green_streak": int(st.get("non_green_streak", 0)),
            "yellow_streak": int(st.get("yellow_streak", 0)),
            "red_streak": int(st.get("red_streak", 0)),
            "last_alert_at": (
                datetime.datetime.fromtimestamp(float(st.get("last_alert_at", 0.0))).isoformat()
                if float(st.get("last_alert_at", 0.0)) > 0
                else None
            ),
            "events_count": len(st.get("events", [])),
        }
    return {"triggered": triggered, "state": snapshot}
def _build_runtime_health_summary(
    profile_override: Optional[str] = None,
    record_alert: bool = True,
    simulate_market_open: Optional[bool] = None,
) -> dict:
    now = datetime.datetime.now()
    market = _is_market_open()
    is_open = bool(market.get("is_open", False))
    effective_is_open = is_open if simulate_market_open is None else bool(simulate_market_open)
    profile = _resolve_health_profile(market, profile_override)
    th = _health_thresholds(profile)

    metrics = runtime_db_metrics().get("data", {})
    selfcheck = runtime_selfcheck()
    checks = selfcheck.get("checks", []) or []
    ok_all = bool(selfcheck.get("ok", False))

    check_map: dict[str, dict] = {str(c.get("name")): c for c in checks if isinstance(c, dict)}
    pool_cnt = int((check_map.get("realtime_pool_member_count") or {}).get("value") or 0)

    queue_size = int(metrics.get("db_write_queue_size", 0) or 0)
    queue_cap = int(metrics.get("db_write_queue_capacity", 1) or 1)
    queue_ratio = (float(queue_size) / float(queue_cap)) if queue_cap > 0 else 0.0
    queue_warn = queue_ratio >= float(th.get("queue_warn_ratio", 0.5))
    queue_ok = queue_ratio < float(th.get("queue_crit_ratio", 0.8))

    redis_enabled = bool(metrics.get("db_writer_redis_enabled", False))
    redis_ready = bool(metrics.get("db_writer_redis_ready", False))
    redis_ok = (not redis_enabled) or redis_ready
    state_redis_enabled = bool(metrics.get("runtime_state_redis_enabled", False))
    state_redis_ready = bool(metrics.get("runtime_state_redis_ready", False))
    state_redis_ok = (not state_redis_enabled) or state_redis_ready

    latest_signal_age_sec = None
    latest_signal_eval_age_sec = None
    latest_signal_eval_samples = 0
    latest_tick_age_sec = None
    rt_conn = None
    member_codes: list[str] = []
    try:
        rt_conn = _get_conn()
        try:
            r = rt_conn.execute("SELECT MAX(triggered_at) FROM signal_history").fetchone()
            if r and r[0]:
                dt = r[0]
                if hasattr(dt, "timestamp"):
                    latest_signal_age_sec = max(0, int(now.timestamp() - dt.timestamp()))
        except Exception:
            pass
        try:
            r2 = rt_conn.execute("SELECT MAX(ts) FROM tick_history").fetchone()
            if r2 and r2[0]:
                ts = int(r2[0] or 0)
                if ts > 10**12:
                    ts //= 1000
                latest_tick_age_sec = max(0, int(now.timestamp() - ts))
        except Exception:
            pass
        try:
            mrows = rt_conn.execute(
                """
                SELECT DISTINCT ts_code
                FROM monitor_pools
                ORDER BY ts_code
                LIMIT 500
                """
            ).fetchall()
            member_codes = [str(r[0]) for r in (mrows or []) if r and r[0]]
        except Exception:
            member_codes = []
    except Exception:
        pass
    finally:
        if rt_conn is not None:
            try:
                rt_conn.close()
            except Exception:
                pass

    # Signal freshness should reflect whether Layer2 signal engine is still evaluating,
    # not whether a *new* signal was recently triggered.
    if member_codes:
        try:
            provider = get_tick_provider()
        except Exception:
            provider = None
        if provider is not None:
            latest_eval_ts = 0
            sample_cnt = 0
            for code in member_codes:
                try:
                    entry = provider.get_cached_signals(code)
                except Exception:
                    entry = None
                if not isinstance(entry, dict):
                    continue
                ev = entry.get("evaluated_at")
                if ev is None:
                    continue
                try:
                    ev_ts = int(float(ev))
                    if ev_ts > 10**12:
                        ev_ts //= 1000
                    if ev_ts > 0:
                        sample_cnt += 1
                        if ev_ts > latest_eval_ts:
                            latest_eval_ts = ev_ts
                except Exception:
                    continue
            if latest_eval_ts > 0:
                latest_signal_eval_age_sec = max(0, int(now.timestamp() - int(latest_eval_ts)))
                latest_signal_eval_samples = int(sample_cnt)

    # Freshness thresholds: profile-based.
    signal_fresh_age = latest_signal_eval_age_sec if latest_signal_eval_age_sec is not None else latest_signal_age_sec
    signal_fresh_basis = "signal_eval" if latest_signal_eval_age_sec is not None else ("signal_event" if latest_signal_age_sec is not None else "none")
    if effective_is_open:
        tick_ok = latest_tick_age_sec is not None and latest_tick_age_sec <= int(th.get("tick_ok_sec", 30))
        tick_warn = latest_tick_age_sec is not None and int(th.get("tick_ok_sec", 30)) < latest_tick_age_sec <= int(th.get("tick_warn_sec", 120))
        signal_ok = signal_fresh_age is not None and signal_fresh_age <= int(th.get("signal_ok_sec", 300))
        signal_warn = signal_fresh_age is not None and int(th.get("signal_ok_sec", 300)) < signal_fresh_age <= int(th.get("signal_warn_sec", 900))
    else:
        # After close, missing/old tick is expected and should not turn health red.
        tick_ok = True
        tick_warn = latest_tick_age_sec is not None and latest_tick_age_sec > int(th.get("closed_tick_warn_sec", 24 * 3600))
        signal_ok = True  # after close no strict signal freshness requirement
        signal_warn = False

    fast_slow_item = None
    if _FAST_SLOW_HEALTH_ENABLED and int(_FAST_SLOW_HEALTH_POOL_ID) in POOLS:
        snap = _get_fast_slow_health_snapshot(int(_FAST_SLOW_HEALTH_POOL_ID))
        members = int(snap.get("members", 0) or 0)
        ratio = float(snap.get("mismatch_ratio", 0.0) or 0.0)
        mismatch = int(snap.get("mismatch", 0) or 0)
        err = snap.get("error")

        # Health policy:
        # - compute error -> yellow (not red), avoid transient heavy-check false negatives
        # - ratio >= crit -> red
        # - ratio >= warn -> yellow
        # - members too small -> yellow (diagnostic weak)
        if err:
            fs_ok = True
            fs_warn = True
        else:
            fs_ok = ratio < float(_FAST_SLOW_HEALTH_CRIT_RATIO)
            fs_warn = (ratio >= float(_FAST_SLOW_HEALTH_WARN_RATIO) and fs_ok) or (members > 0 and members < 10)

        fast_slow_item = {
            "name": f"fast_slow_consistency_pool{int(_FAST_SLOW_HEALTH_POOL_ID)}",
            "ok": fs_ok,
            "level": _health_level(fs_ok, warn=fs_warn),
            "detail": {
                "pool_id": int(_FAST_SLOW_HEALTH_POOL_ID),
                "members": members,
                "mismatch": mismatch,
                "mismatch_ratio": round(ratio, 6),
                "warn_ratio": float(_FAST_SLOW_HEALTH_WARN_RATIO),
                "crit_ratio": float(_FAST_SLOW_HEALTH_CRIT_RATIO),
                "checked_at": snap.get("checked_at"),
                "cached": bool(snap.get("cached", False)),
                "cache_age_sec": snap.get("cache_age_sec"),
                "error": err,
            },
        }

    items = [
        {
            "name": "db_connectivity",
            "ok": ok_all,
            "level": _health_level(ok_all),
            "detail": "selfcheck",
        },
        {
            "name": "pool_members",
            "ok": pool_cnt > 0,
            "level": _health_level(pool_cnt > 0, warn=(pool_cnt > 0 and pool_cnt < 10)),
            "detail": {"count": pool_cnt},
        },
        {
            "name": "writer_queue",
            "ok": queue_ok,
            "level": _health_level(queue_ok, warn=queue_warn and queue_ok),
            "detail": {"size": queue_size, "capacity": queue_cap, "ratio": round(queue_ratio, 4)},
        },
        {
            "name": "writer_redis",
            "ok": redis_ok,
            "level": _health_level(redis_ok),
            "detail": {"enabled": redis_enabled, "ready": redis_ready},
        },
        {
            "name": "runtime_state_redis",
            "ok": state_redis_ok,
            "level": _health_level(state_redis_ok),
            "detail": {"enabled": state_redis_enabled, "ready": state_redis_ready},
        },
        {
            "name": "tick_freshness",
            "ok": tick_ok,
            "level": _health_level(tick_ok, warn=tick_warn),
            "detail": {
                "age_sec": latest_tick_age_sec,
                "market_open": is_open,
                "effective_market_open": effective_is_open,
            },
        },
        {
            "name": "signal_freshness",
            "ok": signal_ok,
            "level": _health_level(signal_ok, warn=signal_warn),
            "detail": {
                "age_sec": signal_fresh_age,
                "basis": signal_fresh_basis,
                "signal_event_age_sec": latest_signal_age_sec,
                "signal_eval_age_sec": latest_signal_eval_age_sec,
                "signal_eval_samples": latest_signal_eval_samples,
                "market_open": is_open,
                "effective_market_open": effective_is_open,
            },
        },
    ]
    if fast_slow_item is not None:
        items.append(fast_slow_item)

    reds = sum(1 for x in items if x["level"] == "red")
    yellows = sum(1 for x in items if x["level"] == "yellow")
    overall = "red" if reds > 0 else ("yellow" if yellows > 0 else "green")
    thresholds_ext = dict(th)
    if _FAST_SLOW_HEALTH_ENABLED and int(_FAST_SLOW_HEALTH_POOL_ID) in POOLS:
        thresholds_ext.update(
            {
                "fast_slow_pool_id": int(_FAST_SLOW_HEALTH_POOL_ID),
                "fast_slow_warn_ratio": float(_FAST_SLOW_HEALTH_WARN_RATIO),
                "fast_slow_crit_ratio": float(_FAST_SLOW_HEALTH_CRIT_RATIO),
                "fast_slow_cache_ttl_sec": float(_FAST_SLOW_HEALTH_CACHE_TTL_SEC),
            }
        )
    payload = {
        "ok": overall != "red",
        "level": overall,
        "checked_at": now.isoformat(),
        "profile": profile,
        "simulation": {
            "simulate_market_open": simulate_market_open,
            "effective_market_open": effective_is_open,
        },
        "thresholds": thresholds_ext,
        "market": market,
        "summary": {
            "green": sum(1 for x in items if x["level"] == "green"),
            "yellow": yellows,
            "red": reds,
        },
        "items": items,
    }
    if record_alert:
        payload["alert"] = _maybe_record_health_alert(payload)
    return payload
def runtime_health_summary(
    profile: Optional[str] = Query(None, description="override: open|normal|closed"),
    simulate_open: bool = Query(False, description="simulate market open branch for threshold checks"),
):
    p = profile.strip().lower() if isinstance(profile, str) else None
    if p not in (None, "open", "normal", "closed"):
        raise HTTPException(status_code=400, detail="invalid profile, expected open|normal|closed")
    sim = True if (isinstance(simulate_open, bool) and simulate_open) else None
    return _build_runtime_health_summary(profile_override=p, record_alert=True, simulate_market_open=sim)
def runtime_health_pressure():
    return {
        "checked_at": datetime.datetime.now().isoformat(),
        "profiles": {
            "open": _build_runtime_health_summary(
                profile_override="open",
                record_alert=False,
                simulate_market_open=True,
            ),
            "normal": _build_runtime_health_summary(
                profile_override="normal",
                record_alert=False,
                simulate_market_open=True,
            ),
            "closed": _build_runtime_health_summary(
                profile_override="closed",
                record_alert=False,
                simulate_market_open=False,
            ),
        },
    }
def runtime_health_alerts(limit: int = Query(20, ge=1, le=200)):
    lim = int(limit) if isinstance(limit, int) else 20
    lim = max(1, min(200, lim))
    with _health_alert_lock:
        st = dict(_health_alert_state)
        events = list(st.get("events", []))
    events = events[-lim:]
    return {
        "enabled": _HEALTH_ALERT_ENABLED,
        "consecutive": _HEALTH_ALERT_CONSECUTIVE,
        "cooldown_sec": _HEALTH_ALERT_COOLDOWN_SEC,
        "state": {
            "last_level": st.get("last_level", "green"),
            "non_green_streak": int(st.get("non_green_streak", 0)),
            "yellow_streak": int(st.get("yellow_streak", 0)),
            "red_streak": int(st.get("red_streak", 0)),
            "last_alert_at": (
                datetime.datetime.fromtimestamp(float(st.get("last_alert_at", 0.0))).isoformat()
                if float(st.get("last_alert_at", 0.0)) > 0
                else None
            ),
            "events_count": len(st.get("events", [])),
        },
        "events": events,
    }
