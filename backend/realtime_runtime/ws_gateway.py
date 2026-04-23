"""Realtime websocket gateway."""
from __future__ import annotations

from .common import *  # noqa: F401,F403
from .persistence_service import *  # noqa: F401,F403
from .pool_service import _evaluate_signals_fast_internal, _load_indices_snapshot

async def realtime_ws(ws: WebSocket):
    await ws.accept()
    subscribed_pool: Optional[int] = None
    push_task: Optional[asyncio.Task] = None

    async def send_msg(data: dict):
        try:
            await ws.send_text(_nan_safe_json(data))
        except Exception:
            pass

    async def push_loop(pool_id: int):
        last_market = None
        last_tx_push_at = 0.0
        last_tx_full_at = 0.0
        last_tx_fp: dict[str, tuple] = {}
        last_indices_push_at = 0.0
        last_tick_full_at = 0.0
        last_tick_fp: dict[str, tuple] = {}
        last_signal_full_at = 0.0
        last_signal_fp: dict[str, tuple] = {}
        indices_cache: list = []
        members_cache: list[tuple[str, str]] = []
        last_members_load_at = 0.0
        last_lock_warn_at = 0.0
        member_source = "cache_reuse"
        while True:
            try:
                cycle_t0 = time.perf_counter()
                market = _is_market_open()
                if market != last_market:
                    last_market = market
                    await send_msg({"type": "market_status", **market})
                is_open = bool(market.get("is_open", False))

                now = time.time()
                members = members_cache
                should_refresh_members = (not members_cache) or (now - last_members_load_at >= _WS_MEMBER_REFRESH_SEC)
                if should_refresh_members:
                    try:
                        with _ro_conn_ctx() as conn:
                            members = conn.execute(
                                "SELECT ts_code, name FROM monitor_pools WHERE pool_id = ? ORDER BY sort_order, added_at",
                                [pool_id],
                            ).fetchall()
                        members_cache = list(members or [])
                        last_members_load_at = now
                        member_source = "db"
                    except Exception as e:
                        # DB lock fallback: keep streaming with previous in-memory member snapshot.
                        if _is_db_lock_error(e) and members_cache:
                            _metric_inc("ws_member_cache_fallback_total")
                            members = members_cache
                            member_source = "cache_fallback"
                            if now - last_lock_warn_at >= 30.0:
                                logger.warning(f"ws member refresh fallback to cache due DB lock: {e}")
                                last_lock_warn_at = now
                        else:
                            raise
                else:
                    member_source = "cache_reuse"
                if not members:
                    await asyncio.sleep(5 if is_open else 30)
                    continue

                ts_codes = [m[0] for m in members]
                provider = get_tick_provider()
                provider.subscribe_symbols(ts_codes)

                tick_data = {}
                miss_codes: list[str] = []
                tick_t0 = time.perf_counter()
                for code in ts_codes:
                    try:
                        cached_tick = provider.get_cached_tick(code)
                    except Exception:
                        cached_tick = None
                    if cached_tick:
                        tick_data[code] = cached_tick
                    else:
                        miss_codes.append(code)

                if miss_codes:
                    if str(getattr(provider, "name", "")) == "gm":
                        fallback_cap = max(0, int(_WS_GM_MISS_TICK_FALLBACK_MAX))
                        fallback_codes = miss_codes[:fallback_cap]
                        for code in fallback_codes:
                            try:
                                tick_data[code] = provider.get_tick(code)
                            except Exception:
                                tick_data[code] = _empty_tick_payload(code)
                        for code in miss_codes[fallback_cap:]:
                            tick_data[code] = _empty_tick_payload(code)
                    else:
                        for code in miss_codes:
                            try:
                                tick_data[code] = provider.get_tick(code)
                            except Exception:
                                tick_data[code] = _empty_tick_payload(code)
                _metric_set("ws_tick_fetch_ms", round((time.perf_counter() - tick_t0) * 1000.0, 2))
                changed_tick_codes: list[str] = []
                full_snapshot = (not last_tick_fp) or (now - last_tick_full_at >= max(1.0, float(_WS_TICK_FULL_SNAPSHOT_SEC)))
                if full_snapshot:
                    await send_msg({"type": "tick", "data": tick_data, "full_snapshot": True})
                    last_tick_fp = {code: _tick_ws_fingerprint(tk) for code, tk in tick_data.items()}
                    last_tick_full_at = now
                    changed_tick_codes = list(tick_data.keys())
                    _metric_inc("ws_tick_full_push_total")
                    _metric_inc("ws_tick_delta_keys_total", float(len(tick_data)))
                else:
                    # 清理已不在当前池中的旧键，避免指纹表无限增长
                    for old_code in list(last_tick_fp.keys()):
                        if old_code not in tick_data:
                            del last_tick_fp[old_code]
                    delta_tick_data: dict[str, dict] = {}
                    for code, tk in tick_data.items():
                        fp = _tick_ws_fingerprint(tk)
                        if last_tick_fp.get(code) != fp:
                            delta_tick_data[code] = tk
                            last_tick_fp[code] = fp
                    changed_tick_codes = list(delta_tick_data.keys())
                    if delta_tick_data:
                        await send_msg({"type": "tick", "data": delta_tick_data, "full_snapshot": False})
                        _metric_inc("ws_tick_delta_push_total")
                        _metric_inc("ws_tick_delta_keys_total", float(len(delta_tick_data)))

                if _WS_PUSH_INDICES_ENABLED and (now - last_indices_push_at >= _WS_INDICES_INTERVAL_SEC):
                    idx_t0 = time.perf_counter()
                    try:
                        idx = _load_indices_snapshot(force_refresh=False)
                        if isinstance(idx, list) and idx:
                            indices_cache = idx
                    except Exception:
                        pass
                    _metric_set("ws_indices_fetch_ms", round((time.perf_counter() - idx_t0) * 1000.0, 2))
                    last_indices_push_at = now
                    if indices_cache:
                        await send_msg({"type": "indices", "data": indices_cache})
                        _metric_inc("ws_indices_push_total")
                elif (not _WS_PUSH_INDICES_ENABLED) and (now - last_indices_push_at >= _WS_INDICES_INTERVAL_SEC):
                    _metric_inc("ws_indices_skip_total")
                    last_indices_push_at = now

                if is_open and (now - last_tx_push_at >= _WS_TX_INTERVAL_SEC):
                    for old_code in list(last_tx_fp.keys()):
                        if old_code not in ts_codes:
                            del last_tx_fp[old_code]
                    tx_full_snapshot = (not last_tx_fp) or (
                        now - last_tx_full_at >= max(1.0, float(_WS_TX_FULL_SNAPSHOT_SEC))
                    )
                    tx_codes = ts_codes if tx_full_snapshot else changed_tick_codes
                    txn_data = {}
                    for code in tx_codes:
                        try:
                            txns = provider.get_cached_transactions(code)
                            if txns is None:
                                txns = mootdx_client.get_transactions(code, 15)
                            txn_data[code] = txns or []
                        except Exception:
                            pass
                    if tx_full_snapshot:
                        await send_msg({"type": "transactions", "data": txn_data, "full_snapshot": True})
                        last_tx_fp = {code: _txn_row_ws_fingerprint(v) for code, v in txn_data.items()}
                        last_tx_full_at = now
                        _metric_inc("ws_tx_full_push_total")
                        _metric_inc("ws_tx_delta_codes_total", float(len(txn_data)))
                    else:
                        delta_txn_data: dict[str, list] = {}
                        for code, rows in txn_data.items():
                            fp = _txn_row_ws_fingerprint(rows)
                            if last_tx_fp.get(code) != fp:
                                delta_txn_data[code] = rows
                                last_tx_fp[code] = fp
                        if delta_txn_data:
                            await send_msg({"type": "transactions", "data": delta_txn_data, "full_snapshot": False})
                            _metric_inc("ws_tx_delta_push_total")
                            _metric_inc("ws_tx_delta_codes_total", float(len(delta_txn_data)))
                    last_tx_push_at = now

                try:
                    signal_result = _evaluate_signals_fast_internal(pool_id, members, provider, tick_data)
                    signal_full_snapshot = (not last_signal_fp) or (
                        now - last_signal_full_at >= max(1.0, float(_WS_SIGNAL_FULL_SNAPSHOT_SEC))
                    )
                    if signal_full_snapshot:
                        await send_msg(
                            {
                                "type": "signals",
                                "pool_id": pool_id,
                                "data": signal_result,
                                "full_snapshot": True,
                                "meta": {
                                    "member_source": member_source,
                                    "ws_member_cache_fallback_total": int(
                                        _metrics_snapshot().get("ws_member_cache_fallback_total", 0)
                                    ),
                                },
                            }
                        )
                        last_signal_fp = {
                            str(r.get("ts_code") or ""): _signal_row_ws_fingerprint(r)
                            for r in (signal_result or [])
                            if isinstance(r, dict) and str(r.get("ts_code") or "")
                        }
                        last_signal_full_at = now
                        _metric_inc("ws_signal_full_push_total")
                        _metric_inc("ws_signal_delta_rows_total", float(len(signal_result or [])))
                    else:
                        cur_signal_fp: dict[str, tuple] = {}
                        delta_signal_rows: list[dict] = []
                        for r in (signal_result or []):
                            if not isinstance(r, dict):
                                continue
                            code = str(r.get("ts_code") or "")
                            if not code:
                                continue
                            fp = _signal_row_ws_fingerprint(r)
                            cur_signal_fp[code] = fp
                            if last_signal_fp.get(code) != fp:
                                delta_signal_rows.append(r)
                        for old_code in list(last_signal_fp.keys()):
                            if old_code not in cur_signal_fp:
                                del last_signal_fp[old_code]
                        for c, fp in cur_signal_fp.items():
                            last_signal_fp[c] = fp
                        if delta_signal_rows:
                            await send_msg(
                                {
                                    "type": "signals",
                                    "pool_id": pool_id,
                                    "data": delta_signal_rows,
                                    "full_snapshot": False,
                                    "meta": {
                                        "member_source": member_source,
                                        "ws_member_cache_fallback_total": int(
                                            _metrics_snapshot().get("ws_member_cache_fallback_total", 0)
                                        ),
                                    },
                                }
                            )
                            _metric_inc("ws_signal_delta_push_total")
                            _metric_inc("ws_signal_delta_rows_total", float(len(delta_signal_rows)))
                except Exception as e:
                    _metric_inc("ws_signals_fail_total")
                    logger.debug(f"ws signals failed: {e}")

                if is_open:
                    _metric_set("ws_cycle_ms", round((time.perf_counter() - cycle_t0) * 1000.0, 2))
                    await asyncio.sleep(max(0.2, float(_WS_OPEN_INTERVAL_SEC)))
                else:
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                _metric_inc("ws_push_loop_error_total")
                logger.error(f"ws push loop error: {e}")
                await asyncio.sleep(3)

    try:
        await send_msg({"type": "connected"})
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                await send_msg({"type": "error", "message": "invalid json"})
                continue

            action = msg.get("action")
            if action == "ping":
                await send_msg({"type": "pong"})
                continue
            if action == "unsubscribe":
                if push_task and not push_task.done():
                    push_task.cancel()
                    push_task = None
                subscribed_pool = None
                await send_msg({"type": "unsubscribed"})
                continue
            if action == "subscribe":
                pool_id = int(msg.get("pool_id", 0))
                if pool_id not in POOLS:
                    await send_msg({"type": "error", "message": f"pool_id {pool_id} not found"})
                    continue
                if push_task and not push_task.done():
                    push_task.cancel()
                subscribed_pool = pool_id
                push_task = asyncio.create_task(push_loop(pool_id))
                try:
                    history_map = _load_tick_history_map(pool_id, limit=240)
                    await send_msg({"type": "tick_history", "data": history_map})
                except Exception:
                    pass
                await send_msg({"type": "subscribed", "pool_id": pool_id})
                continue
    except WebSocketDisconnect:
        pass
    finally:
        if push_task and not push_task.done():
            push_task.cancel()
        _ = subscribed_pool
