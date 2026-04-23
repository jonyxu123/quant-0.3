"""Realtime persistence service: schema, pools, queues, DB writes."""
from __future__ import annotations

from .common import *  # noqa: F401,F403

def _metric_inc(name: str, value: float = 1.0) -> None:
    with _metrics_lock:
        _runtime_metrics[name] = float(_runtime_metrics.get(name, 0.0) + value)
def _metric_set(name: str, value: float) -> None:
    with _metrics_lock:
        _runtime_metrics[name] = float(value)
def _metrics_snapshot() -> dict:
    with _metrics_lock:
        return dict(_runtime_metrics)
def _get_db_writer_redis():
    global _db_writer_redis, _db_writer_redis_warn_at, _db_writer_redis_next_retry_at
    if not _DB_WRITER_REDIS_ENABLED or _redis_mod is None:
        return None
    if _db_writer_redis is not None:
        return _db_writer_redis
    now = time.time()
    if now < _db_writer_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_DB_WRITER_REDIS_URL, decode_responses=True)
        cli.ping()
        _db_writer_redis = cli
        logger.info(f"[DB writer] redis enabled key={_DB_WRITER_REDIS_KEY}")
        return _db_writer_redis
    except Exception as e:
        _db_writer_redis_next_retry_at = now + 5.0
        if now - _db_writer_redis_warn_at >= 30.0:
            _db_writer_redis_warn_at = now
            logger.warning(f"[DB writer] redis unavailable, fallback local queue: {e}")
        return None
def _redis_enqueue(kind: str, payload) -> bool:
    cli = _get_db_writer_redis()
    if cli is None:
        return False
    try:
        msg = json.dumps(
            {"kind": kind, "payload": _serialize_write_payload(kind, payload)},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        cli.rpush(_DB_WRITER_REDIS_KEY, msg)
        _metric_inc("db_redis_enqueue_total")
        return True
    except Exception:
        _metric_inc("db_redis_enqueue_fail_total")
        return False
def _redis_dequeue_batch(max_items: int) -> list[tuple[str, object]]:
    cli = _get_db_writer_redis()
    if cli is None:
        return []
    out: list[tuple[str, object]] = []
    for _ in range(max_items):
        try:
            raw = cli.lpop(_DB_WRITER_REDIS_KEY)
        except Exception:
            _metric_inc("db_redis_dequeue_fail_total")
            break
        if not raw:
            break
        try:
            obj = json.loads(raw)
            kind = str(obj.get("kind") or "")
            payload = _deserialize_write_payload(kind, obj.get("payload"))
            if kind:
                out.append((kind, payload))
        except Exception:
            _metric_inc("db_redis_dequeue_fail_total")
            continue
    if out:
        _metric_inc("db_redis_dequeue_total", float(len(out)))
    return out
def _get_runtime_state_redis():
    global _runtime_state_redis, _runtime_state_redis_warn_at, _runtime_state_redis_next_retry_at
    if not _RUNTIME_STATE_REDIS_ENABLED or _redis_mod is None:
        return None
    if _runtime_state_redis is not None:
        return _runtime_state_redis
    now = time.time()
    if now < _runtime_state_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_RUNTIME_STATE_REDIS_URL, decode_responses=True)
        cli.ping()
        _runtime_state_redis = cli
        logger.info(f"[RuntimeState] redis enabled prefix={_RUNTIME_STATE_REDIS_KEY_PREFIX}")
        return _runtime_state_redis
    except Exception as e:
        _runtime_state_redis_next_retry_at = now + 5.0
        if now - _runtime_state_redis_warn_at >= 30.0:
            _runtime_state_redis_warn_at = now
            logger.warning(f"[RuntimeState] redis unavailable: {e}")
        return None
def _runtime_state_write_metrics(metrics: dict) -> None:
    cli = _get_runtime_state_redis()
    if cli is None:
        return
    trade_date = _trade_date_from_dt()
    key = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:metrics:{trade_date}"
    now_iso = datetime.datetime.now().isoformat()
    mapping: dict[str, str] = {"updated_at": now_iso}
    for k, v in (metrics or {}).items():
        if isinstance(v, bool):
            mapping[str(k)] = "1" if v else "0"
        elif isinstance(v, (int, float, str)):
            mapping[str(k)] = str(v)
        else:
            try:
                mapping[str(k)] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                mapping[str(k)] = str(v)
    try:
        pipe = cli.pipeline(transaction=False)
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, _runtime_state_ttl_sec())
        pipe.execute()
        _metric_inc("runtime_state_redis_write_total")
    except Exception:
        _metric_inc("runtime_state_redis_write_fail_total")
def _t0_quality_ingest_redis(rows: list[tuple]) -> None:
    """Persist T0 quality aggregates + sliding windows to Redis by trade_date."""
    cli = _get_runtime_state_redis()
    if cli is None or not rows:
        return
    ttl = _runtime_state_ttl_sec()
    max_win = max(20, int(_RUNTIME_T0_QUALITY_WINDOW_SIZE))
    try:
        pipe = cli.pipeline(transaction=False)
        for r in rows:
            if not r or len(r) < 13:
                continue
            ts_code = str(r[0] or "")
            signal_type = str(r[1] or "")
            horizon = int(r[5] or 0)
            ret_bps = float(r[7] or 0.0)
            mfe_bps = float(r[8] or 0.0)
            mae_bps = float(r[9] or 0.0)
            is_correct = 1 if bool(r[10]) else 0
            trigger_time = r[3] if isinstance(r[3], datetime.datetime) else None
            channel = str((r[12] if len(r) > 12 else "") or "unknown")
            signal_source = str((r[13] if len(r) > 13 else "") or "unknown")
            trade_date = _trade_date_from_dt(trigger_time)

            k_agg = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:t0_quality:agg:{trade_date}"
            k_win = f"{_RUNTIME_STATE_REDIS_KEY_PREFIX}:t0_quality:window:{trade_date}:{signal_type}:{horizon}"

            pipe.hincrby(k_agg, "total", 1)
            pipe.hincrby(k_agg, "correct", is_correct)
            pipe.hincrbyfloat(k_agg, "ret_bps_sum", ret_bps)
            pipe.hincrbyfloat(k_agg, "mfe_bps_sum", mfe_bps)
            pipe.hincrbyfloat(k_agg, "mae_bps_sum", mae_bps)

            pipe.hincrby(k_agg, f"h:{horizon}:total", 1)
            pipe.hincrby(k_agg, f"h:{horizon}:correct", is_correct)
            pipe.hincrby(k_agg, f"s:{signal_type}:total", 1)
            pipe.hincrby(k_agg, f"s:{signal_type}:correct", is_correct)
            pipe.hincrby(k_agg, f"s:{signal_type}:h:{horizon}:total", 1)
            pipe.hincrby(k_agg, f"s:{signal_type}:h:{horizon}:correct", is_correct)
            pipe.hincrby(k_agg, f"c:{channel}:total", 1)
            pipe.hincrby(k_agg, f"c:{channel}:correct", is_correct)
            pipe.hincrby(k_agg, f"src:{signal_source}:total", 1)
            pipe.hincrby(k_agg, f"src:{signal_source}:correct", is_correct)
            pipe.hincrby(k_agg, f"c:{channel}:src:{signal_source}:total", 1)
            pipe.hincrby(k_agg, f"c:{channel}:src:{signal_source}:correct", is_correct)
            pipe.hset(k_agg, mapping={"updated_at": datetime.datetime.now().isoformat()})
            pipe.expire(k_agg, ttl)

            win_payload = json.dumps(
                {
                    "ts_code": ts_code,
                    "signal_type": signal_type,
                    "horizon": horizon,
                    "ret_bps": round(ret_bps, 2),
                    "mfe_bps": round(mfe_bps, 2),
                    "mae_bps": round(mae_bps, 2),
                    "correct": bool(is_correct),
                    "trigger_time": trigger_time.isoformat() if isinstance(trigger_time, datetime.datetime) else None,
                    "channel": channel,
                    "signal_source": signal_source,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            pipe.lpush(k_win, win_payload)
            pipe.ltrim(k_win, 0, max_win - 1)
            pipe.expire(k_win, ttl)
        pipe.execute()
        _metric_inc("t0_quality_redis_write_total", float(len(rows)))
    except Exception:
        _metric_inc("t0_quality_redis_write_fail_total")
def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    with _TABLE_LOCK:
        if _TABLE_READY:
            return
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monitor_pools (
                pool_id INTEGER,
                ts_code VARCHAR,
                name VARCHAR,
                industry VARCHAR,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                note VARCHAR,
                sort_order INTEGER DEFAULT 0,
                PRIMARY KEY (pool_id, ts_code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_history (
                id INTEGER PRIMARY KEY,
                pool_id INTEGER,
                ts_code VARCHAR,
                name VARCHAR,
                signal_type VARCHAR,
                channel VARCHAR,
                signal_source VARCHAR,
                strength DOUBLE,
                message VARCHAR,
                price DOUBLE,
                pct_chg DOUBLE,
                details_json VARCHAR,
                triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS t0_signal_quality (
                id BIGINT,
                ts_code VARCHAR,
                signal_type VARCHAR,
                direction VARCHAR,
                trigger_time TIMESTAMP,
                trigger_price DOUBLE,
                eval_horizon_sec INTEGER,
                eval_price DOUBLE,
                ret_bps DOUBLE,
                mfe_bps DOUBLE,
                mae_bps DOUBLE,
                direction_correct BOOLEAN,
                market_phase VARCHAR,
                channel VARCHAR,
                signal_source VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS t0_feature_drift (
                id BIGINT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                feature_name VARCHAR,
                psi DOUBLE,
                ks_stat DOUBLE,
                ks_p DOUBLE,
                severity VARCHAR,
                drifted BOOLEAN,
                precision_drop DOUBLE,
                alerted BOOLEAN DEFAULT FALSE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tick_history (
                ts_code VARCHAR,
                name VARCHAR,
                price DOUBLE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                pre_close DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                pct_chg DOUBLE,
                ts BIGINT,
                saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS signal_history_id_seq")
        except Exception:
            pass
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS t0_signal_quality_id_seq")
        except Exception:
            pass
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS t0_feature_drift_id_seq")
        except Exception:
            pass
        # Backward-compatible schema migration for existing DBs.
        for ddl in (
            "ALTER TABLE signal_history ADD COLUMN channel VARCHAR",
            "ALTER TABLE signal_history ADD COLUMN signal_source VARCHAR",
            "ALTER TABLE signal_history ADD COLUMN details_json VARCHAR",
            "ALTER TABLE t0_signal_quality ADD COLUMN channel VARCHAR",
            "ALTER TABLE t0_signal_quality ADD COLUMN signal_source VARCHAR",
        ):
            try:
                conn.execute(ddl)
            except Exception:
                pass
        for ddl in (
            "CREATE INDEX IF NOT EXISTS idx_monitor_pools_pool_order ON monitor_pools(pool_id, sort_order, added_at)",
            "CREATE INDEX IF NOT EXISTS idx_signal_history_pool_code_type_ts ON signal_history(pool_id, ts_code, signal_type, triggered_at)",
            "CREATE INDEX IF NOT EXISTS idx_signal_history_triggered_at ON signal_history(triggered_at)",
            "CREATE INDEX IF NOT EXISTS idx_tick_history_code_ts ON tick_history(ts_code, ts)",
            "CREATE INDEX IF NOT EXISTS idx_t0_quality_created_at ON t0_signal_quality(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_t0_quality_code_type_time ON t0_signal_quality(ts_code, signal_type, trigger_time, eval_horizon_sec)",
            "CREATE INDEX IF NOT EXISTS idx_t0_quality_channel_source_time ON t0_signal_quality(channel, signal_source, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_t0_drift_checked_at ON t0_feature_drift(checked_at)",
            "CREATE INDEX IF NOT EXISTS idx_t0_drift_feature_checked ON t0_feature_drift(feature_name, checked_at)",
        ):
            try:
                conn.execute(ddl)
            except Exception:
                pass
        _bootstrap_monitor_pools_from_data_db(conn)
        _TABLE_READY = True
def _bootstrap_monitor_pools_from_data_db(rt_conn: duckdb.DuckDBPyConnection) -> None:
    """One-time bootstrap: copy monitor_pools from cold DB when hot DB is empty."""
    try:
        if os.path.abspath(_REALTIME_DB) == os.path.abspath(_DATA_DB):
            return
    except Exception:
        pass
    try:
        c = rt_conn.execute("SELECT COUNT(*) FROM monitor_pools").fetchone()
        if c and int(c[0] or 0) > 0:
            return
    except Exception:
        return
    data_conn = None
    try:
        data_conn = open_duckdb_conn(_DATA_DB, retries=2, base_sleep_sec=0.05)
        rows = data_conn.execute(
            """
            SELECT pool_id, ts_code, name, industry, added_at, note, sort_order
            FROM monitor_pools
            ORDER BY pool_id, sort_order, added_at
            """
        ).fetchall()
        if not rows:
            return
        rt_conn.executemany(
            """
            INSERT INTO monitor_pools (pool_id, ts_code, name, industry, added_at, note, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        logger.info(f"[RealtimeDB] bootstrapped monitor_pools from data DB rows={len(rows)}")
    except Exception:
        # best effort; keep startup resilient
        return
    finally:
        if data_conn is not None:
            try:
                data_conn.close()
            except Exception:
                pass
def _get_conn() -> duckdb.DuckDBPyConnection:
    conn = open_duckdb_conn(
        _REALTIME_DB,
        retries=3,
        base_sleep_sec=0.08,
        on_retry=lambda _attempt, _exc: _metric_inc("db_lock_open_rw_total"),
    )
    _ensure_table(conn)
    return conn
def _new_ro_conn() -> duckdb.DuckDBPyConnection:
    return open_duckdb_conn(
        _REALTIME_DB,
        retries=3,
        base_sleep_sec=0.05,
        on_retry=lambda _attempt, _exc: _metric_inc("db_lock_open_ro_total"),
    )
def _acquire_ro_conn() -> duckdb.DuckDBPyConnection:
    try:
        conn = _db_read_pool.get_nowait()
        _metric_inc("db_read_pool_hit_total")
        return conn
    except _queue_mod.Empty:
        _metric_inc("db_read_pool_miss_total")
        return _new_ro_conn()
def _release_ro_conn(conn: Optional[duckdb.DuckDBPyConnection]) -> None:
    if conn is None:
        return
    try:
        _db_read_pool.put_nowait(conn)
    except _queue_mod.Full:
        try:
            conn.close()
        except Exception:
            pass
def _new_data_ro_conn() -> duckdb.DuckDBPyConnection:
    return open_duckdb_conn(
        _DATA_DB,
        retries=3,
        base_sleep_sec=0.05,
        on_retry=lambda _attempt, _exc: _metric_inc("db_lock_open_ro_total"),
    )
def _acquire_data_ro_conn() -> duckdb.DuckDBPyConnection:
    try:
        conn = _db_data_read_pool.get_nowait()
        _metric_inc("db_data_read_pool_hit_total")
        return conn
    except _queue_mod.Empty:
        _metric_inc("db_data_read_pool_miss_total")
        return _new_data_ro_conn()
def _release_data_ro_conn(conn: Optional[duckdb.DuckDBPyConnection]) -> None:
    if conn is None:
        return
    try:
        _db_data_read_pool.put_nowait(conn)
    except _queue_mod.Full:
        try:
            conn.close()
        except Exception:
            pass
@contextmanager
def _ro_conn_ctx():
    conn = _acquire_ro_conn()
    try:
        yield conn
    finally:
        _release_ro_conn(conn)


@contextmanager
def _data_ro_conn_ctx():
    conn = _acquire_data_ro_conn()
    try:
        yield conn
    finally:
        _release_data_ro_conn(conn)
def _enqueue_db_write(kind: str, payload) -> None:
    global _db_write_drop_warn_at
    if kind in {"tick_rows", "signal_rows", "quality_rows", "drift_rows"}:
        if not _is_market_open().get("is_open", False):
            _metric_inc("db_write_skip_non_trading_total")
            return
    if _redis_enqueue(kind, payload):
        return
    try:
        _db_write_queue.put_nowait((kind, payload))
    except _queue_mod.Full:
        _metric_inc("db_writer_queue_drop_total")
        now = time.time()
        if now - _db_write_drop_warn_at >= 5.0:
            _db_write_drop_warn_at = now
            logger.warning(f"[DB writer] queue full, dropped kind={kind}")
def _enqueue_quality_track(task: dict) -> None:
    try:
        _quality_track_queue.put_nowait(task)
    except Exception:
        pass
def _collect_signal_rows(pool_id: int, evaluated: list) -> list[dict]:
    rows: list[dict] = []
    now_ts = int(time.time())
    for row in evaluated or []:
        ts_code = str(row.get("ts_code", "") or "")
        if not ts_code:
            continue
        for s in row.get("signals", []) or []:
            if not s.get("has_signal"):
                continue
            sig_type = str(s.get("type") or "")
            if not sig_type:
                continue
            trigger_ts = int(s.get("triggered_at") or now_ts)
            rows.append(
                {
                    "pool_id": int(pool_id),
                    "ts_code": ts_code,
                    "name": str(row.get("name", "") or ""),
                    "signal_type": sig_type,
                    "channel": str(
                        s.get("channel")
                        or (s.get("details") or {}).get("channel")
                        or ("pool1_timing" if int(pool_id) == 1 else "pool2_t0")
                    ),
                    "signal_source": str(
                        s.get("signal_source")
                        or (s.get("details") or {}).get("signal_source")
                        or "runtime"
                    ),
                    "strength": float(s.get("strength", 0) or 0),
                    "message": str(s.get("message", "") or ""),
                    "price": float(row.get("price", 0) or 0),
                    "pct_chg": float(row.get("pct_chg", 0) or 0),
                    "details_json": _compact_history_snapshot(s),
                    "triggered_at": datetime.datetime.fromtimestamp(trigger_ts),
                    "direction": str(s.get("direction", "buy") or "buy"),
                    "trigger_time": float(trigger_ts),
                    "trigger_price": float(s.get("price", row.get("price", 0)) or 0),
                }
            )
    return rows
def _build_tick_row(ts_code: str, name: str, tick: dict) -> tuple:
    now_ts = int(time.time())
    t = tick or {}
    return (
        ts_code,
        name or t.get("name", "") or "",
        float(t.get("price", 0) or 0),
        float(t.get("open", 0) or 0),
        float(t.get("high", 0) or 0),
        float(t.get("low", 0) or 0),
        float(t.get("pre_close", 0) or 0),
        int(t.get("volume", 0) or 0),
        float(t.get("amount", 0) or 0),
        float(t.get("pct_chg", 0) or 0),
        int(t.get("timestamp", now_ts) or now_ts),
    )
def _enqueue_gm_signal_history(ts_code: str, tick_item: dict, fired_signals: list) -> None:
    if not _is_market_open().get("is_open", False):
        return
    if not fired_signals:
        return
    rows_by_pool: dict[int, list[dict]] = {1: [], 2: []}
    for s in fired_signals:
        if not isinstance(s, dict):
            continue
        if not s.get("has_signal", True):
            continue
        if not s.get("is_new", False):
            continue
        sig_type = str(s.get("type") or "")
        if not sig_type:
            continue
        raw_pools = s.get("_pool_ids")
        pools: list[int]
        if isinstance(raw_pools, (list, tuple, set)):
            pools = []
            for pid in raw_pools:
                try:
                    p = int(pid)
                except Exception:
                    continue
                if p in POOL_SIGNAL_TYPES:
                    pools.append(p)
        else:
            pools = [1, 2]
        for pid in pools:
            if sig_type not in POOL_SIGNAL_TYPES.get(pid, set()):
                continue
            rows_by_pool[pid].append(
                {
                    "ts_code": ts_code,
                    "name": str(tick_item.get("name", "") or ""),
                    "price": float(tick_item.get("price", 0) or 0),
                    "pct_chg": float(tick_item.get("pct_chg", 0) or 0),
                    "signals": [s],
                }
            )
    for pid, rows in rows_by_pool.items():
        if rows:
            _save_signal_history(pid, rows)
def _process_db_batch(conn: duckdb.DuckDBPyConnection, batch: list[tuple[str, object]]) -> list[dict]:
    tick_rows: list[tuple] = []
    quality_rows: list[tuple] = []
    drift_rows: list[tuple] = []
    signal_rows: list[dict] = []
    quality_tasks: list[dict] = []

    for kind, payload in batch:
        if kind == "tick_rows":
            tick_rows.extend(payload or [])
        elif kind == "quality_rows":
            quality_rows.extend(payload or [])
        elif kind == "drift_rows":
            drift_rows.extend(payload or [])
        elif kind == "signal_rows":
            signal_rows.extend(payload or [])

    if tick_rows:
        conn.executemany(
            """
            INSERT INTO tick_history
            (ts_code, name, price, open, high, low, pre_close, volume, amount, pct_chg, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tick_rows,
        )

    if signal_rows:
        for r in signal_rows:
            pool_id = int(r.get("pool_id", 0) or 0)
            ts_code = str(r.get("ts_code", "") or "")
            sig_type = str(r.get("signal_type", "") or "")
            triggered_at = r.get("triggered_at")
            if pool_id <= 0 or not ts_code or not sig_type or triggered_at is None:
                continue
            if isinstance(triggered_at, datetime.datetime):
                dedup_since = triggered_at - datetime.timedelta(seconds=_signal_history_dedup_seconds(pool_id, sig_type))
            else:
                dedup_since = datetime.datetime.now() - datetime.timedelta(
                    seconds=_signal_history_dedup_seconds(pool_id, sig_type)
                )
            dup = conn.execute(
                """
                SELECT id FROM signal_history
                WHERE pool_id = ? AND ts_code = ? AND signal_type = ?
                  AND triggered_at > ?
                LIMIT 1
                """,
                [pool_id, ts_code, sig_type, dedup_since],
            ).fetchone()
            if dup:
                continue
            conn.execute(
                """
                INSERT INTO signal_history
                (id, pool_id, ts_code, name, signal_type, channel, signal_source, strength, message, price, pct_chg, details_json, triggered_at)
                VALUES (nextval('signal_history_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    pool_id,
                    ts_code,
                    r.get("name", ""),
                    sig_type,
                    str(r.get("channel", "") or ""),
                    str(r.get("signal_source", "") or ""),
                    float(r.get("strength", 0) or 0),
                    str(r.get("message", "") or ""),
                    float(r.get("price", 0) or 0),
                    float(r.get("pct_chg", 0) or 0),
                    str(r.get("details_json", "") or "") or None,
                    triggered_at,
                ],
            )
            if pool_id == 2:
                quality_tasks.append(
                    {
                        "ts_code": ts_code,
                        "signal_type": sig_type,
                        "direction": str(r.get("direction", "buy") or "buy"),
                        "trigger_time": float(r.get("trigger_time", time.time()) or time.time()),
                        "trigger_price": float(r.get("trigger_price", r.get("price", 0)) or 0),
                        "channel": str(r.get("channel", "pool2_t0") or "pool2_t0"),
                        "signal_source": str(r.get("signal_source", "runtime") or "runtime"),
                    }
                )

    if quality_rows:
        normalized_quality_rows: list[tuple] = []
        for r in quality_rows:
            if not isinstance(r, (list, tuple)):
                continue
            row = list(r)
            # Backward compatibility:
            # old format: 12 columns (without channel/signal_source)
            # new format: 14 columns.
            if len(row) < 12:
                continue
            if len(row) == 12:
                row.extend(["pool2_t0", "runtime"])
            elif len(row) == 13:
                row.append("runtime")
            if len(row) > 14:
                row = row[:14]
            normalized_quality_rows.append(tuple(row))
        if not normalized_quality_rows:
            quality_rows = []
        else:
            quality_rows = normalized_quality_rows
    if quality_rows:
        conn.executemany(
            """
            INSERT INTO t0_signal_quality
            (id, ts_code, signal_type, direction, trigger_time, trigger_price,
             eval_horizon_sec, eval_price, ret_bps, mfe_bps, mae_bps,
             direction_correct, market_phase, channel, signal_source)
            VALUES (nextval('t0_signal_quality_id_seq'),
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            quality_rows,
        )
        _t0_quality_ingest_redis(quality_rows)

    if drift_rows:
        conn.executemany(
            """
            INSERT INTO t0_feature_drift
            (id, feature_name, psi, ks_stat, ks_p, severity, drifted, precision_drop, alerted)
            VALUES (nextval('t0_feature_drift_id_seq'),
                    ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            drift_rows,
        )
    return quality_tasks
def _start_db_writer(flush_interval: float = _DB_WRITER_FLUSH_SEC, max_batch: int = _DB_WRITER_MAX_BATCH) -> None:
    global _db_writer_started
    if _db_writer_started:
        return
    _db_writer_started = True

    def _loop() -> None:
        logger.info(f"[DB writer] started flush={flush_interval}s batch={max_batch}")
        pending: list[tuple[str, object]] = []
        while True:
            try:
                if not pending:
                    pending.extend(_redis_dequeue_batch(max_batch))
                if not pending:
                    kind, payload = _db_write_queue.get(timeout=flush_interval)
                    pending.append((kind, payload))
                while len(pending) < max_batch:
                    need = max_batch - len(pending)
                    if need > 0:
                        pending.extend(_redis_dequeue_batch(need))
                        if len(pending) >= max_batch:
                            break
                    try:
                        pending.append(_db_write_queue.get_nowait())
                    except _queue_mod.Empty:
                        break
                if not pending:
                    continue
                conn = _get_conn()
                try:
                    conn.execute("BEGIN")
                    quality_tasks = _process_db_batch(conn, pending)
                    conn.execute("COMMIT")
                    for task in quality_tasks:
                        _enqueue_quality_track(task)
                    _metric_inc("db_writer_batch_tx_total")
                    pending.clear()
                except Exception:
                    _metric_inc("db_writer_batch_tx_fail_total")
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise
                finally:
                    conn.close()
            except _queue_mod.Empty:
                continue
            except Exception as e:
                if _is_db_lock_error(e):
                    _metric_inc("db_lock_writer_total")
                    logger.warning(f"[DB writer] lock retry: {e}")
                else:
                    logger.error(f"[DB writer] error: {e}")
                time.sleep(0.3)

    threading.Thread(target=_loop, daemon=True, name="db-writer").start()
def _load_tick_history_map(pool_id: int, limit: int = 240) -> dict:
    with _ro_conn_ctx() as conn:
        members = conn.execute(
            "SELECT ts_code FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
            [pool_id],
        ).fetchall()
        out: dict[str, list] = {}
        for (ts_code,) in members:
            rows = conn.execute(
                """
                SELECT ts, price, volume, amount
                FROM (
                    SELECT ts, price, volume, amount
                    FROM tick_history
                    WHERE ts_code = ?
                    ORDER BY ts DESC
                    LIMIT ?
                ) t
                ORDER BY ts ASC
                """,
                [ts_code, int(limit)],
            ).fetchall()
            if rows:
                out[ts_code] = [
                    {"time": int(r[0]), "price": r[1], "volume": r[2], "amount": r[3]}
                    for r in rows
                ]
        return out
def _save_signal_history(pool_id: int, evaluated: list) -> None:
    if not _is_market_open().get("is_open", False):
        return
    try:
        rows = _collect_signal_rows(pool_id, evaluated)
        if rows:
            _enqueue_db_write("signal_rows", rows)
    except Exception as e:
        logger.debug(f"save signal history failed: {e}")


__all__ = [name for name in globals() if not name.startswith("__")]
