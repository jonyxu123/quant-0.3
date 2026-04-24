"""Realtime runtime jobs: writer loops, txn refresh, quality/drift, closed-loop."""
from __future__ import annotations

from .common import *  # noqa: F401,F403
from .persistence_service import *  # noqa: F401,F403

_concept_snapshot_refresher_started = False
_industry_snapshot_refresher_started = False
_CONCEPT_SNAPSHOT_REFRESH_SEC = float(os.getenv("CONCEPT_SNAPSHOT_REFRESH_SEC", "60") or 60.0)
_CONCEPT_SNAPSHOT_OFF_SESSION_SEC = float(os.getenv("CONCEPT_SNAPSHOT_OFF_SESSION_SEC", "300") or 300.0)
_INDUSTRY_SNAPSHOT_REFRESH_SEC = float(os.getenv("INDUSTRY_SNAPSHOT_REFRESH_SEC", "60") or 60.0)
_INDUSTRY_SNAPSHOT_OFF_SESSION_SEC = float(os.getenv("INDUSTRY_SNAPSHOT_OFF_SESSION_SEC", "300") or 300.0)

def _load_latest_vol20_map(ts_codes: list[str]) -> dict[str, float]:
    if not ts_codes:
        return {}
    out: dict[str, float] = {}
    try:
        uniq = sorted({str(x) for x in ts_codes if x})
        if not uniq:
            return out
        placeholders = ",".join(["?"] * len(uniq))
        with _data_ro_conn_ctx() as conn:
            try:
                schema_rows = conn.execute("PRAGMA table_info('stk_factor_pro')").fetchall()
                columns = {str(r[1]).lower() for r in schema_rows}
            except Exception:
                columns = set()
            vol_col = None
            for c in ("vol_20", "vol20", "vol"):
                if c in columns:
                    vol_col = c
                    break
            if not vol_col:
                return out
            rows = conn.execute(
                f"""
                WITH latest AS (
                    SELECT ts_code, MAX(trade_date) AS max_date
                    FROM stk_factor_pro
                    WHERE ts_code IN ({placeholders})
                    GROUP BY ts_code
                )
                SELECT f.ts_code, f.{vol_col} AS vol_20
                FROM stk_factor_pro f
                JOIN latest l ON f.ts_code = l.ts_code AND f.trade_date = l.max_date
                """,
                uniq,
            ).fetchall()
            for ts_code, v in rows:
                try:
                    out[str(ts_code)] = float(v) if v is not None else -1.0
                except Exception:
                    out[str(ts_code)] = -1.0
    except Exception:
        return out
    return out
def _build_threshold_closed_loop_snapshot(window_hours: int) -> dict:
    now_dt = datetime.datetime.now()
    since_dt = now_dt - datetime.timedelta(hours=int(window_hours))
    signal_rows: list[tuple] = []
    quality_rows: list[tuple] = []
    industry_map: dict[str, str] = {}
    instrument_profile_map: dict[str, dict] = {}
    try:
        with _ro_conn_ctx() as conn:
            signal_rows = conn.execute(
                """
                SELECT pool_id, ts_code, signal_type, strength, triggered_at
                FROM signal_history
                WHERE triggered_at >= ?
                  AND signal_type IN ('left_side_buy','right_side_breakout','timing_clear','positive_t','reverse_t')
                """,
                [since_dt],
            ).fetchall()
            quality_rows = conn.execute(
                """
                SELECT ts_code, signal_type, market_phase, direction_correct, trigger_time
                FROM t0_signal_quality
                WHERE trigger_time >= ?
                """,
                [since_dt],
            ).fetchall()
            inds = conn.execute(
                """
                SELECT ts_code, MAX(COALESCE(industry, '')) AS industry
                FROM monitor_pools
                GROUP BY ts_code
                """
            ).fetchall()
            for ts_code, industry in inds:
                industry_map[str(ts_code)] = str(industry or "")
    except Exception as e:
        return {
            "ok": False,
            "checked_at": now_dt.isoformat(),
            "window_hours": int(window_hours),
            "error": str(e),
            "rows": [],
        }

    ts_codes = [str(r[1]) for r in signal_rows] + [str(r[0]) for r in quality_rows]
    uniq_codes = sorted({c for c in ts_codes if c})
    if uniq_codes and infer_instrument_profile is not None:
        try:
            with _data_ro_conn_ctx() as data_conn:
                placeholders = ",".join(["?"] * len(uniq_codes))
                meta_rows = data_conn.execute(
                    f"""
                    SELECT ts_code, name, market, list_date
                    FROM stock_basic
                    WHERE ts_code IN ({placeholders})
                    """,
                    uniq_codes,
                ).fetchall()
            for ts_code, name, market_name, list_date in meta_rows:
                profile = infer_instrument_profile(
                    str(ts_code or ""),
                    name=name,
                    market_name=market_name,
                    list_date=list_date,
                    ts_epoch=int(now_dt.timestamp()),
                )
                instrument_profile_map[str(ts_code)] = profile
        except Exception:
            instrument_profile_map = {}
    vol_map = _load_latest_vol20_map(ts_codes)
    aggr: dict[tuple[str, str, str, str, str, str, str], dict] = {}

    for pool_id, ts_code, signal_type, strength, triggered_at in signal_rows:
        ts_code = str(ts_code or "")
        signal_type = str(signal_type or "")
        tdt = triggered_at if isinstance(triggered_at, datetime.datetime) else now_dt
        phase = _phase_from_dt(tdt)
        industry = str(industry_map.get(ts_code, "") or "*")
        regime = _vol_regime_from_vol20(vol_map.get(ts_code))
        profile = instrument_profile_map.get(ts_code, {})
        board_segment = str(profile.get("board_segment") or "*")
        security_type = str(profile.get("security_type") or "*")
        listing_stage = str(profile.get("listing_stage") or "*")
        key = (signal_type, phase, regime, industry, board_segment, security_type, listing_stage)
        d = aggr.setdefault(
            key,
            {
                "signal_type": signal_type,
                "market_phase": phase,
                "regime": regime,
                "industry": industry,
                "board_segment": board_segment,
                "security_type": security_type,
                "listing_stage": listing_stage,
                "trigger_count": 0,
                "pass_count": 0,
                "hit_count": 0,
                "eval_count": 0,
                "symbols": set(),
            },
        )
        d["trigger_count"] += 1
        d["symbols"].add(ts_code)
        try:
            s_val = float(strength or 0)
        except Exception:
            s_val = 0.0
        pass_th = _CLOSED_LOOP_SCORE_OBS_POOL1 if int(pool_id or 0) == 1 else _CLOSED_LOOP_SCORE_OBS_T0
        if s_val >= pass_th:
            d["pass_count"] += 1

    for ts_code, signal_type, market_phase, direction_correct, _trigger_time in quality_rows:
        ts_code = str(ts_code or "")
        signal_type = str(signal_type or "")
        phase = str(market_phase or "off_session")
        industry = str(industry_map.get(ts_code, "") or "*")
        regime = _vol_regime_from_vol20(vol_map.get(ts_code))
        profile = instrument_profile_map.get(ts_code, {})
        board_segment = str(profile.get("board_segment") or "*")
        security_type = str(profile.get("security_type") or "*")
        listing_stage = str(profile.get("listing_stage") or "*")
        key = (signal_type, phase, regime, industry, board_segment, security_type, listing_stage)
        d = aggr.setdefault(
            key,
            {
                "signal_type": signal_type,
                "market_phase": phase,
                "regime": regime,
                "industry": industry,
                "board_segment": board_segment,
                "security_type": security_type,
                "listing_stage": listing_stage,
                "trigger_count": 0,
                "pass_count": 0,
                "hit_count": 0,
                "eval_count": 0,
                "symbols": set(),
            },
        )
        d["eval_count"] += 1
        if bool(direction_correct):
            d["hit_count"] += 1

    rows: list[dict] = []
    for _, d in aggr.items():
        symbols = max(1, len(d["symbols"]))
        trigger_count = int(d["trigger_count"])
        pass_count = int(d["pass_count"])
        eval_count = int(d["eval_count"])
        hit_count = int(d["hit_count"])
        trigger_rate = float(trigger_count) / float(symbols)
        pass_rate = (float(pass_count) / float(trigger_count)) if trigger_count > 0 else None
        hit_rate = (float(hit_count) / float(eval_count)) if eval_count > 0 else None
        samples = max(trigger_count, eval_count)
        rows.append(
            {
                "signal_type": d["signal_type"],
                "market_phase": d["market_phase"],
                "regime": d["regime"],
                "industry": d["industry"],
                "board_segment": d["board_segment"],
                "security_type": d["security_type"],
                "listing_stage": d["listing_stage"],
                "symbols": symbols,
                "trigger_count": trigger_count,
                "pass_count": pass_count,
                "eval_count": eval_count,
                "hit_count": hit_count,
                "trigger_rate": round(trigger_rate, 4),
                "pass_rate": round(pass_rate, 4) if pass_rate is not None else None,
                "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
                "samples": int(samples),
            }
        )

    rows.sort(key=lambda x: (x["signal_type"], x["market_phase"], x["regime"], x["industry"], x["board_segment"], x["security_type"], x["listing_stage"]))
    return {
        "ok": True,
        "checked_at": now_dt.isoformat(),
        "window_hours": int(window_hours),
        "rows": rows,
    }
def _build_threshold_calibration_rules(snapshot: dict) -> list[dict]:
    rows = snapshot.get("rows", []) if isinstance(snapshot, dict) else []
    if not isinstance(rows, list):
        return []
    min_samples = max(1, int(_CLOSED_LOOP_MIN_SAMPLES))
    trigger_low = float(_CLOSED_LOOP_TARGETS.get("trigger_per_symbol_low", 0.3) or 0.3)
    trigger_high = float(_CLOSED_LOOP_TARGETS.get("trigger_per_symbol_high", 3.0) or 3.0)
    pass_low = float(_CLOSED_LOOP_TARGETS.get("pass_rate_low", 0.35) or 0.35)
    hit_low = float(_CLOSED_LOOP_TARGETS.get("hit_rate_low", 0.48) or 0.48)
    hit_high = float(_CLOSED_LOOP_TARGETS.get("hit_rate_high", 0.62) or 0.62)

    step_pos = float(_CLOSED_LOOP_STEPS.get("positive_t_bias_vwap", 0.1) or 0.1)
    step_rev = float(_CLOSED_LOOP_STEPS.get("reverse_t_z", 0.1) or 0.1)
    step_left = float(_CLOSED_LOOP_STEPS.get("left_near_lower", 0.03) or 0.03)
    step_right = float(_CLOSED_LOOP_STEPS.get("right_eps", 0.0003) or 0.0003)

    rules: list[dict] = []
    for r in rows:
        try:
            samples = int(r.get("samples", 0) or 0)
            if samples < min_samples:
                continue
            sig = str(r.get("signal_type") or "")
            phase = str(r.get("market_phase") or "*")
            regime = str(r.get("regime") or "*")
            industry = str(r.get("industry") or "*")
            board_segment = str(r.get("board_segment") or "*")
            security_type = str(r.get("security_type") or "*")
            listing_stage = str(r.get("listing_stage") or "*")
            trigger_rate = float(r.get("trigger_rate", 0) or 0)
            pass_rate = float(r.get("pass_rate", 0) or 0)
            hit_rate = r.get("hit_rate")
            hit_rate = float(hit_rate) if hit_rate is not None else None
        except Exception:
            continue

        bad = (pass_rate < pass_low) or (trigger_rate > trigger_high) or (hit_rate is not None and hit_rate < hit_low)
        good = (trigger_rate < trigger_low) and (hit_rate is not None and hit_rate > hit_high)
        if not (bad or good):
            continue

        delta: dict = {}
        reason = "tighten" if bad else "loosen"
        if sig == "positive_t":
            delta["bias_vwap_th"] = -step_pos if bad else step_pos
        elif sig == "reverse_t":
            delta["z_th"] = step_rev if bad else -step_rev
        elif sig == "left_side_buy":
            delta["near_lower_th"] = -step_left if bad else step_left
        elif sig == "right_side_breakout":
            eps_delta = step_right if bad else -step_right
            delta["eps_mid"] = eps_delta
            delta["eps_upper"] = eps_delta
        else:
            continue

        rules.append(
            {
                "signal_type": sig,
                "market_phase": phase,
                "regime": regime,
                "industry": industry,
                "board_segment": board_segment,
                "security_type": security_type,
                "listing_stage": listing_stage,
                "delta": delta,
                "reason": reason,
                "metrics": {
                    "samples": samples,
                    "trigger_rate": round(trigger_rate, 4),
                    "pass_rate": round(pass_rate, 4),
                    "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
                },
            }
        )
    return rules
def _write_threshold_calibration_payload(payload: dict) -> bool:
    cli = _get_runtime_state_redis()
    if cli is None:
        return False
    key_latest = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:threshold:calibration:latest"
    key_hist = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:threshold:calibration:history:{_trade_date_from_dt()}"
    ttl = _runtime_state_ttl_sec()
    try:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        pipe = cli.pipeline(transaction=False)
        pipe.set(key_latest, raw, ex=ttl)
        pipe.lpush(key_hist, raw)
        pipe.ltrim(key_hist, 0, 199)
        pipe.expire(key_hist, ttl)
        pipe.execute()
        _metric_inc("threshold_calib_write_total")
        return True
    except Exception:
        _metric_inc("threshold_calib_write_fail_total")
        return False
def _start_threshold_closed_loop(interval_min: float = 15.0) -> None:
    global _threshold_closed_loop_started, _threshold_calibration_last
    if _threshold_closed_loop_started or not _CLOSED_LOOP_ENABLED:
        return
    _threshold_closed_loop_started = True
    sec = max(30.0, float(interval_min) * 60.0)

    def _loop() -> None:
        logger.info(f"[ThresholdCalib] closed-loop started interval={sec:.0f}s")
        while True:
            try:
                snap = _build_threshold_closed_loop_snapshot(_CLOSED_LOOP_WINDOW_HOURS)
                rules = _build_threshold_calibration_rules(snap) if bool(snap.get("ok")) else []
                payload = {
                    "ok": bool(snap.get("ok")),
                    "checked_at": datetime.datetime.now().isoformat(),
                    "window_hours": int(_CLOSED_LOOP_WINDOW_HOURS),
                    "rows": snap.get("rows", []),
                    "rules": rules,
                    "summary": {
                        "row_count": len(snap.get("rows", []) if isinstance(snap.get("rows"), list) else []),
                        "rule_count": len(rules),
                    },
                }
                _threshold_calibration_last.clear()
                _threshold_calibration_last.update(payload)
                _write_threshold_calibration_payload(payload)
            except Exception as e:
                logger.error(f"[ThresholdCalib] closed-loop error: {e}")
            time.sleep(sec)

    threading.Thread(target=_loop, daemon=True, name="threshold-closed-loop").start()
def _start_tick_persistence() -> None:
    global _tick_persist_started
    if _tick_persist_started:
        return
    _tick_persist_started = True
    _start_db_writer()

    def _loop() -> None:
        logger.info(f"[Layer3] tick persistence thread started enabled={_TICK_DB_PERSIST_ENABLED}")
        provider = get_tick_provider()
        gm_persist_queue = None
        if getattr(provider, "name", "") == "gm":
            try:
                from backend.realtime.gm_tick import init_persist_queue
                gm_persist_queue = init_persist_queue(maxsize=100000)
                logger.info("[Layer3] gm persist queue consumer enabled")
            except Exception as e:
                logger.warning(f"[Layer3] gm persist queue init failed, fallback polling: {e}")
        while True:
            try:
                if gm_persist_queue is not None:
                    try:
                        kind, ts_code, item, extra = gm_persist_queue.get(timeout=1.0)
                    except _queue_mod.Empty:
                        continue
                    if not _is_market_open().get("is_open", False):
                        # Drain queue but skip persistence during non-trading periods.
                        continue
                    if kind == "tick":
                        if not _TICK_DB_PERSIST_ENABLED:
                            continue
                        row = _build_tick_row(str(ts_code), str((item or {}).get("name", "") or ""), item or {})
                        _enqueue_db_write("tick_rows", [row])
                    elif kind == "signal":
                        _enqueue_gm_signal_history(str(ts_code), item or {}, extra or [])
                    continue

                if not _is_market_open().get("is_open", False):
                    time.sleep(30.0)
                    continue
                with _ro_conn_ctx() as conn:
                    members = conn.execute("SELECT DISTINCT ts_code, name FROM monitor_pools").fetchall()
                if not members:
                    time.sleep(5.0)
                    continue
                rows = []
                for ts_code, name in members:
                    try:
                        t = provider.get_tick(ts_code)
                        rows.append(_build_tick_row(str(ts_code), str(name or ""), t))
                    except Exception:
                        pass
                if rows and _TICK_DB_PERSIST_ENABLED:
                    _enqueue_db_write("tick_rows", rows)
            except Exception as e:
                logger.warning(f"[Layer3] tick persistence error: {e}")
            time.sleep(0.05 if gm_persist_queue is not None else 5.0)

    threading.Thread(target=_loop, daemon=True, name="tick-persist").start()
def _start_txn_refresher(interval: float = 3.0) -> None:
    global _txn_refresher_started
    if _txn_refresher_started:
        return
    _txn_refresher_started = True

    def _loop() -> None:
        logger.info(f"[Layer2] txn refresher started interval={interval}s")
        while True:
            try:
                if not _is_market_open().get("is_open", False):
                    time.sleep(max(interval, 15.0))
                    continue
                provider = get_tick_provider()
                with _ro_conn_ctx() as conn:
                    rows = conn.execute("SELECT DISTINCT ts_code FROM monitor_pools").fetchall()
                    ts_codes = [r[0] for r in rows]
                if not ts_codes:
                    time.sleep(interval)
                    continue
                mapping = {}
                for code in ts_codes:
                    try:
                        txns = mootdx_client.get_transactions(code, int(_TXN_ANALYZE_COUNT))
                        if txns:
                            mapping[code] = txns
                    except Exception:
                        pass
                if mapping:
                    provider.bulk_update_recent_txns(mapping)
            except Exception as e:
                logger.warning(f"[Layer2] txn refresher error: {e}")
            time.sleep(interval)

    threading.Thread(target=_loop, daemon=True, name="txn-refresher").start()
def _start_concept_snapshot_refresher(interval_sec: float = _CONCEPT_SNAPSHOT_REFRESH_SEC) -> None:
    global _concept_snapshot_refresher_started
    if _concept_snapshot_refresher_started:
        return
    _concept_snapshot_refresher_started = True

    def _loop() -> None:
        sec = max(15.0, float(interval_sec))
        logger.info(f"[Layer2] concept snapshot refresher started interval={sec:.0f}s")
        while True:
            try:
                from . import pool_service

                force_refresh = True
                raw_snapshot_map, error_text, fetched_at = pool_service._fetch_realtime_concept_snapshot_map(
                    force_refresh=force_refresh
                )
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
                        pool_service._register_concept_snapshot_alias(
                            runtime_snapshot_map,
                            concept_name=concept_name,
                            concept_code=snapshot.get("board_code"),
                            snapshot=dict(snapshot),
                        )
                    if runtime_snapshot_map:
                        provider = get_tick_provider()
                        provider.bulk_update_concept_snapshots(runtime_snapshot_map)
                elif error_text:
                    logger.debug(f"[Layer2] concept snapshot refresh skipped: {error_text}")
            except Exception as e:
                logger.warning(f"[Layer2] concept snapshot refresher error: {e}")

            try:
                market_open = bool(_is_market_open().get("is_open", False))
            except Exception:
                market_open = False
            sleep_sec = sec if market_open else max(sec, float(_CONCEPT_SNAPSHOT_OFF_SESSION_SEC))
            time.sleep(sleep_sec)

    threading.Thread(target=_loop, daemon=True, name="concept-snapshot-refresher").start()


def _load_industry_snapshot_alias_inputs(pool_service_module=None) -> list[str]:
    alias_inputs: list[str] = []
    ts_codes: list[str] = []
    try:
        with _ro_conn_ctx() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT ts_code, industry
                FROM monitor_pools
                WHERE ts_code IS NOT NULL AND ts_code <> ''
                """
            ).fetchall()
        for ts_code, industry in rows:
            code = str(ts_code or "").strip()
            if code:
                ts_codes.append(code)
            industry_name = str(industry or "").strip()
            if industry_name:
                alias_inputs.append(industry_name)
    except Exception:
        pass

    if pool_service_module is not None and ts_codes:
        try:
            with _data_ro_conn_ctx() as data_conn:
                em_industry_name_map, _em_industry_code_map = pool_service_module._load_em_industry_member_maps(
                    data_conn,
                    sorted(set(ts_codes)),
                )
            alias_inputs.extend(
                str(name or "").strip()
                for name in em_industry_name_map.values()
                if str(name or "").strip()
            )
        except Exception:
            pass

    try:
        from backend.realtime.gm_runtime import common as gm_common

        runtime_industry_map = getattr(gm_common, "_main_stock_industry", {}) or {}
        if isinstance(runtime_industry_map, dict):
            alias_inputs.extend(
                str(name or "").strip()
                for name in runtime_industry_map.values()
                if str(name or "").strip()
            )
    except Exception:
        pass

    seen: set[str] = set()
    out: list[str] = []
    for item in alias_inputs:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _start_industry_snapshot_refresher(interval_sec: float = _INDUSTRY_SNAPSHOT_REFRESH_SEC) -> None:
    global _industry_snapshot_refresher_started
    if _industry_snapshot_refresher_started:
        return
    _industry_snapshot_refresher_started = True

    def _loop() -> None:
        sec = max(15.0, float(interval_sec))
        logger.info(f"[Layer2] industry snapshot refresher started interval={sec:.0f}s")
        while True:
            try:
                from . import pool_service

                raw_snapshot_map, error_text, _fetched_at = pool_service._fetch_realtime_industry_snapshot_map(
                    force_refresh=True
                )
                if raw_snapshot_map:
                    runtime_snapshot_map: dict[str, dict] = {}
                    seen_names: set[str] = set()
                    alias_inputs = _load_industry_snapshot_alias_inputs(pool_service)
                    pool_service._attach_industry_snapshot_aliases(
                        raw_snapshot_map,
                        alias_inputs,
                    )
                    for snapshot in raw_snapshot_map.values():
                        if not isinstance(snapshot, dict):
                            continue
                        industry_name = str(snapshot.get("industry_name") or "").strip()
                        if not industry_name or industry_name in seen_names:
                            continue
                        seen_names.add(industry_name)
                        pool_service._register_industry_snapshot_alias(
                            runtime_snapshot_map,
                            industry_name=industry_name,
                            snapshot=dict(snapshot),
                        )
                    pool_service._attach_industry_snapshot_aliases(
                        runtime_snapshot_map,
                        alias_inputs,
                    )
                    if runtime_snapshot_map:
                        provider = get_tick_provider()
                        provider.bulk_update_industry_snapshots(runtime_snapshot_map)
                elif error_text:
                    logger.debug(f"[Layer2] industry snapshot refresh skipped: {error_text}")
            except Exception as e:
                logger.warning(f"[Layer2] industry snapshot refresher error: {e}")

            try:
                market_open = bool(_is_market_open().get("is_open", False))
            except Exception:
                market_open = False
            sleep_sec = sec if market_open else max(sec, float(_INDUSTRY_SNAPSHOT_OFF_SESSION_SEC))
            time.sleep(sleep_sec)

    threading.Thread(target=_loop, daemon=True, name="industry-snapshot-refresher").start()
def _start_t0_quality_monitor(interval: float = 1.0) -> None:
    global _quality_monitor_started
    if _quality_monitor_started:
        return
    _quality_monitor_started = True

    def _loop() -> None:
        logger.info(f"[T0 quality] monitor started interval={interval}s")
        pending: list[dict] = []
        horizons = [60, 180, 300]
        while True:
            try:
                if not _is_market_open().get("is_open", False):
                    # Non-trading session: clear stale pending tasks to avoid
                    # cross-session carry-over and unnecessary IO.
                    pending.clear()
                    while True:
                        try:
                            _quality_track_queue.get_nowait()
                        except _queue_mod.Empty:
                            break
                    time.sleep(max(interval, 5.0))
                    continue
                while True:
                    try:
                        task = _quality_track_queue.get_nowait()
                        task["done_horizons"] = set()
                        pending.append(task)
                    except _queue_mod.Empty:
                        break

                now = time.time()
                provider = get_tick_provider()
                keep: list[dict] = []
                rows_to_insert = []

                for task in pending:
                    ts_code = task["ts_code"]
                    signal_type = task["signal_type"]
                    direction = task["direction"]
                    trigger_time = float(task["trigger_time"])
                    trigger_price = float(task["trigger_price"])
                    channel = str(task.get("channel", "pool2_t0") or "pool2_t0")
                    signal_source = str(task.get("signal_source", "runtime") or "runtime")
                    done = task["done_horizons"]

                    for h in horizons:
                        if h in done:
                            continue
                        if now < trigger_time + h:
                            continue
                        try:
                            t = provider.get_tick(ts_code)
                            eval_price = float(t.get("price", 0) or 0)
                            if eval_price > 0:
                                ret = _calc_ret_bps(trigger_price, eval_price, direction)
                                rows_to_insert.append(
                                    (
                                        ts_code,
                                        signal_type,
                                        direction,
                                        datetime.datetime.fromtimestamp(trigger_time),
                                        trigger_price,
                                        int(h),
                                        eval_price,
                                        round(ret, 2),
                                        round(max(ret, 0.0), 2),
                                        round(min(ret, 0.0), 2),
                                        bool(ret > 0),
                                        _phase_of_ts(trigger_time),
                                        channel,
                                        signal_source,
                                    )
                                )
                        except Exception:
                            pass
                        done.add(h)

                    if len(done) < len(horizons) and now < (trigger_time + max(horizons) + 60):
                        keep.append(task)

                pending = keep

                if rows_to_insert:
                    _enqueue_db_write("quality_rows", rows_to_insert)
            except Exception as e:
                logger.error(f"[T0 quality] monitor error: {e}")
            time.sleep(interval)

    threading.Thread(target=_loop, daemon=True, name="t0-quality-monitor").start()
def _start_drift_monitor(check_interval_min: float = 30.0) -> None:
    global _drift_monitor_started
    if _drift_monitor_started:
        return
    _drift_monitor_started = True

    def _severity(score: float) -> str:
        if score >= 3.0:
            return "high"
        if score >= 2.0:
            return "medium"
        return "low"

    def _loop() -> None:
        logger.info(f"[T0 drift] monitor started interval={check_interval_min}m")
        while True:
            try:
                time.sleep(max(1.0, check_interval_min) * 60.0)
                if not _is_market_open().get("is_open", False):
                    continue
                with _ro_conn_ctx() as conn:
                    baseline = conn.execute(
                        """
                        SELECT ret_bps, mfe_bps, mae_bps, direction_correct
                        FROM t0_signal_quality
                        WHERE eval_horizon_sec = 300
                          AND created_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '7 days'
                                              AND CURRENT_TIMESTAMP - INTERVAL '3 days'
                        """
                    ).fetchall()
                    recent = conn.execute(
                        """
                        SELECT ret_bps, mfe_bps, mae_bps, direction_correct
                        FROM t0_signal_quality
                        WHERE eval_horizon_sec = 300
                          AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 day'
                        """
                    ).fetchall()
                    if len(baseline) < 20 or len(recent) < 10:
                        continue

                    b_prec = _safe_mean([1.0 if r[3] else 0.0 for r in baseline]) or 0.0
                    r_prec = _safe_mean([1.0 if r[3] else 0.0 for r in recent]) or 0.0
                    precision_drop = b_prec - r_prec

                    feature_specs = {
                        "ret_bps": ([r[0] for r in baseline], [r[0] for r in recent]),
                        "mfe_bps": ([r[1] for r in baseline], [r[1] for r in recent]),
                        "mae_bps": ([r[2] for r in baseline], [r[2] for r in recent]),
                    }

                    drift_rows = []
                    for feature_name, (b_vals, r_vals) in feature_specs.items():
                        b_mean = _safe_mean(b_vals) or 0.0
                        r_mean = _safe_mean(r_vals) or 0.0
                        b_var = _safe_mean([(x - b_mean) ** 2 for x in b_vals]) or 0.0
                        b_std = (b_var ** 0.5) if b_var > 0 else 0.0
                        z = abs(r_mean - b_mean) / (b_std if b_std > 1e-6 else 1.0)
                        psi = abs(r_mean - b_mean) / (abs(b_mean) + 1.0)
                        drifted = bool(z >= 2.0 or psi >= 0.2)
                        sev = _severity(max(z, psi * 10.0))
                        alerted = bool(drifted and precision_drop >= 0.08)
                        drift_rows.append(
                            (
                                feature_name,
                                float(psi),
                                float(z),
                                0.01 if drifted else 0.5,
                                sev,
                                drifted,
                                float(precision_drop),
                                alerted,
                            )
                        )
                    if drift_rows:
                        _enqueue_db_write("drift_rows", drift_rows)
            except Exception as e:
                logger.error(f"[T0 drift] monitor error: {e}")

    threading.Thread(target=_loop, daemon=True, name="t0-drift-monitor").start()


def is_threshold_closed_loop_started() -> bool:
    return bool(_threshold_closed_loop_started)


def get_threshold_calibration_snapshot_local() -> dict:
    return dict(_threshold_calibration_last) if isinstance(_threshold_calibration_last, dict) else {}
