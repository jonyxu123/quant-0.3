"""Realtime diagnostics service: selfcheck, replay, diff, health, calibration snapshot."""
from __future__ import annotations

from .common import *  # noqa: F401,F403
from .persistence_service import *  # noqa: F401,F403
from .pool_service import _build_member_data, _evaluate_signals_fast_internal
import backend.realtime_runtime.runtime_jobs as runtime_jobs

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
            "instrument_profile": m.get("instrument_profile"),
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
