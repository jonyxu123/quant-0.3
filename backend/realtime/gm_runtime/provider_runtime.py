"""gm runtime provider/subscription runtime."""
from __future__ import annotations

import backend.realtime.gm_runtime.common as gm_common
from .common import *  # noqa: F401,F403
from .position_state import *  # noqa: F401,F403
from .observe_service import *  # noqa: F401,F403
from .signal_engine import _compute_signals_for_tick, _postprocess_fired_signals, _start_signal_state_refresh_thread

class _TickSyncManager(BaseManager):
    """跨进程共享 Queue 与 Cache 的 Manager。"""
    pass


_TickSyncManager.register('get_queue', callable=lambda: _main_queue)
_TickSyncManager.register('get_cache', callable=lambda: _main_cache)


def _save_subscribed_codes():
    """保存订阅列表到文件（供 gm 策略脚本 init 读取）。"""
    try:
        with open(_SUBS_FILE, 'w') as f:
            f.write(','.join(_subscribed_codes))
    except Exception as e:
        logger.warning(f"保存订阅列表失败: {e}")


def _load_subscribed_codes():
    """从文件加载订阅列表。"""
    try:
        if os.path.exists(_SUBS_FILE):
            with open(_SUBS_FILE, 'r') as f:
                codes = f.read().strip()
                _subscribed_codes.clear()
                if codes:
                    _subscribed_codes.update(c for c in codes.split(',') if c)
    except Exception:
        _subscribed_codes.clear()


def seed_subscribed_codes(ts_codes: list[str], overwrite: bool = True) -> int:
    """
    预写 gm 订阅文件（用于 gm.run() 启动前注入初始订阅列表）。
    返回写入后的订阅总数。
    """
    cleaned = sorted(
        {
            str(c).strip()
            for c in (ts_codes or [])
            if c is not None and str(c).strip() and "." in str(c)
        }
    )
    if overwrite:
        _subscribed_codes.clear()
        _subscribed_codes.update(cleaned)
    else:
        _subscribed_codes.update(cleaned)
    _save_subscribed_codes()
    return len(_subscribed_codes)


def _start_manager_server():
    """启动 BaseManager TCP server（主进程调用一次）。"""
    global _manager_server, _TICK_MGR_ADDRESS
    if _manager_server is not None:
        return

    mgr = _TickSyncManager(address=_TICK_MGR_ADDRESS, authkey=_TICK_MGR_AUTHKEY)
    _manager_server = mgr.get_server()
    actual_addr = _manager_server.address  # (host, port)
    _TICK_MGR_ADDRESS = actual_addr

    # 通过环境变量传给 gm 子进程，使其连接此 server
    os.environ['GM_TICK_MGR_HOST'] = str(actual_addr[0])
    os.environ['GM_TICK_MGR_PORT'] = str(actual_addr[1])
    os.environ['GM_TICK_MGR_AUTHKEY'] = _TICK_MGR_AUTHKEY.hex()

    t = threading.Thread(target=_manager_server.serve_forever, daemon=True, name='tick-mgr-server')
    t.start()
    logger.info(f"TickManager TCP server: {actual_addr[0]}:{actual_addr[1]}")


def init_persist_queue(maxsize: int = 100000) -> _StdQueue:
    """Initialize Layer3 persistence queue for realtime writer consumption."""
    if gm_common._persist_queue is None:
        gm_common._persist_queue = _StdQueue(maxsize=maxsize)
        logger.info(f"Layer3 persistence queue created (maxsize={maxsize})")
    return gm_common._persist_queue

def _start_consumer_thread():
    """启动消费线程：queue->cache->Layer2 信号计算。"""
    global _consumer_thread_started
    if _consumer_thread_started:
        return
    _consumer_thread_started = True

    def _consume_loop():
        logger.info("tick 消费线程启动（含 Layer2 信号计算）")
        count = 0
        signal_fires = 0
        dropped_invalid_ticks = 0
        while True:
            try:
                item = _main_queue.get(timeout=1.0)
            except _QueueEmpty:
                continue
            except Exception as e:
                logger.error(f"tick 消费异常: {e}")
                continue
            # 批量拉取并按股票合并，只处理每只股票最新一笔，降低高频重算压力。
            raw_batch: list[dict] = [item]
            max_batch = max(1, int(_CONSUMER_BATCH_MAX))
            drain_max = max(max_batch, int(_CONSUMER_DRAIN_MAX))
            backlog_trigger = max(0, int(_CONSUMER_BACKLOG_DRAIN_TRIGGER))
            try:
                backlog_now = int(_main_queue.qsize())
            except Exception:
                backlog_now = 0
            target_batch = max_batch
            if backlog_trigger > 0 and backlog_now >= backlog_trigger:
                # 积压时优先追最新状态，适度扩大单轮拉取上限，减少前端可见延迟。
                target_batch = min(drain_max, max_batch + backlog_now)
            for _ in range(max(0, target_batch - 1)):
                try:
                    raw_batch.append(_main_queue.get_nowait())
                except _QueueEmpty:
                    break
                except Exception:
                    break
            process_batch = raw_batch
            if _CONSUMER_COALESCE_ENABLED and len(raw_batch) >= max(1, int(_CONSUMER_COALESCE_MIN_BATCH)):
                latest_by_code: dict[str, dict] = {}
                for it in raw_batch:
                    if not isinstance(it, dict):
                        continue
                    code = str(it.get('ts_code') or '')
                    if not code:
                        continue
                    latest_by_code[code] = it
                if latest_by_code:
                    process_batch = list(latest_by_code.values())
            _record_consumer_batch_perf(
                len(raw_batch),
                len(process_batch),
                backlog=backlog_now,
                target=target_batch,
            )
            for item in process_batch:
                try:
                    ts_code = item.get('ts_code')
                    if not ts_code:
                        continue
                    try:
                        px = float(item.get('price', 0) or 0)
                    except Exception:
                        px = 0.0
                    if px <= 0:
                        est_px, est_src = _derive_price_from_order_book(item)
                        if est_px > 0:
                            item = dict(item)
                            item['price'] = float(est_px)
                            item['price_source'] = str(item.get('price_source') or f'consumer_{est_src}')
                            pre_close = float(item.get('pre_close', 0) or 0)
                            if pre_close > 0:
                                item['pct_chg'] = (float(est_px) - pre_close) / pre_close * 100
                            px = float(est_px)
                        else:
                            dropped_invalid_ticks += 1
                            if dropped_invalid_ticks <= 3 or dropped_invalid_ticks % 200 == 0:
                                logger.debug(f"[消费线程] 丢弃无效tick {ts_code} price={px} dropped={dropped_invalid_ticks}")
                            continue

                    # 1. 更新 tick cache
                    _main_cache[ts_code] = item
                    count += 1

                    # 1.5 异步写入持久化队列（Layer3）
                    try:
                        if gm_common._persist_queue is not None:
                            gm_common._persist_queue.put_nowait(('tick', ts_code, item, None))
                    except Exception:
                        pass  # 队列满时丢弃，保证消费线程不阻塞
                    # 2. Layer2：若已加载日线指标，则立即计算信号
                    daily = _main_daily_cache.get(ts_code)
                    if daily:
                        now_ts = int(time.time())
                        if not gm_common._is_signal_processing_allowed(now_ts):
                            continue
                        sig_t0 = time.perf_counter()
                        try:
                            all_fired = _compute_signals_for_tick(item, daily, ts_code=ts_code)
                        except Exception as ce:
                            logger.warning(f"信号计算失败 {ts_code}: {ce}")
                            all_fired = []

                        processed = _postprocess_fired_signals(
                            ts_code=ts_code,
                            tick=item,
                            all_fired=all_fired,
                            now=now_ts,
                            persist=True,
                        )
                        sig_ms = (time.perf_counter() - sig_t0) * 1000.0
                        _record_signal_perf(sig_ms)
                        if sig_ms >= (_SIGNAL_PERF_SLOW_MS * 2):
                            logger.debug(f"[信号慢路径] {ts_code} signal_ms={sig_ms:.2f} fired={len(all_fired)}")
                        new_fired = processed.get('signals', [])
                        new_count = int(processed.get('new_count', 0) or 0)
                        if new_count > 0:
                            signal_fires += 1
                            if signal_fires <= 10 or signal_fires % 100 == 0:
                                types = processed.get('new_types', [])
                                logger.info(f"[新信号#{signal_fires}] {ts_code} price={item.get('price')} types={types}")

                    if count <= 3 or count % 500 == 0:
                        msg = f"[消费线程 #{count}] {ts_code} price={item.get('price')} daily_loaded={bool(daily)}"
                        if count % 500 == 0:
                            msg = f"{msg} | {_pool1_observe_summary()}"
                        logger.info(msg)
                except Exception as e:
                    logger.warning(f"tick 消费处理失败: {e}")

    t = threading.Thread(target=_consume_loop, daemon=True, name='tick-consumer')
    t.start()


# ============================================================
# GmTickProvider
# ============================================================
class GmTickProvider(TickProvider):
    """
    掘金量化(gm) Tick Provider（subscribe 订阅模式）。
    由 gm.run() 驱动 on_tick 写入缓存，get_tick() 从内存缓存读取。"""

    @property
    def name(self) -> str:
        return 'gm'

    @property
    def display_name(self) -> str:
        return '掘金量化(gm)'

    def get_tick(self, ts_code: str) -> dict:
        """从主进程内存 cache 读取最新 tick，O(1) 访问。"""
        tick = self.get_cached_tick(ts_code)
        if tick is not None:
            return tick

        # Cache-miss fallback to mootdx is throttled; avoid blocking hot paths.
        if _GM_MISS_FALLBACK_ENABLED:
            now = time.time()
            if _GM_MISS_FALLBACK_WHEN_OPEN or (not _is_cn_trading_time_now()):
                last = float(_main_miss_fallback_at.get(ts_code, 0.0) or 0.0)
                if now - last >= max(0.5, float(_GM_MISS_FALLBACK_TTL_SEC)):
                    _main_miss_fallback_at[ts_code] = now
                    try:
                        mt = mootdx_client.get_tick(ts_code)
                        if float(mt.get('price', 0) or 0) > 0:
                            return mt
                    except Exception:
                        pass
        # gm 缓存无数据时返回空结构，不回退 mock
        return {
            'ts_code': ts_code,
            'name': '',
            'price': 0,
            'open': 0,
            'high': 0,
            'low': 0,
            'pre_close': 0,
            'volume': 0,
            'amount': 0,
            'pct_chg': 0,
            'timestamp': int(time.time()),
            'bids': [(0, 0)] * 5,
            'asks': [(0, 0)] * 5,
            'is_mock': False,
        }

    def get_cached_tick(self, ts_code: str) -> Optional[dict]:
        """读取缓存 tick，不触发网络请求。"""
        tick = _main_cache.get(ts_code)
        if tick is None:
            return None
        return _with_pre_close_fallback(ts_code, tick)

    def subscribe_symbols(self, ts_codes: list[str]):
        """
        更新订阅列表，并将结果持久化到文件供 gm 策略脚本读取。
        """
        new_codes = []
        with _tick_lock:
            for code in ts_codes:
                if code not in _subscribed_codes:
                    _subscribed_codes.add(code)
                    new_codes.append(code)

        if new_codes:
            logger.info(f"gm 订阅列表新增 {len(new_codes)} 只: {new_codes}")
            _save_subscribed_codes()

    def unsubscribe_symbols(self, ts_codes: list[str]):
        """取消订阅。"""
        with _tick_lock:
            for code in ts_codes:
                _subscribed_codes.discard(code)

    # ========================================================
    # Layer2 数据更新入口
    # ========================================================
    def update_daily_factors(self, ts_code: str, factors: dict):
        """更新单只股票的日线因子缓存（供消费线程计算信号使用）。"""
        _main_daily_cache[ts_code] = dict(factors)

    def bulk_update_daily_factors(self, factors_map: dict[str, dict]):
        """批量更新日线因子缓存（用于定时刷新）。"""
        _main_daily_cache.update(factors_map)
        logger.info(f"Layer2 日线因子缓存已更新: {len(factors_map)}")

    def bulk_update_chip_features(self, features_map: dict[str, dict]):
        """批量更新筹码特征缓存（Pool1 筹码加分）。"""
        _main_chip_cache.update(features_map)
        logger.info(f"Layer2 筹码特征缓存已更新: {len(features_map)}")

    def update_stock_pools(self, ts_code: str, pool_ids: set):
        """更新单只股票的池归属（set of pool_id）。"""
        _main_stock_pools[ts_code] = set(pool_ids)

    def bulk_update_stock_pools(self, mapping: dict[str, set]):
        """
        批量更新股票->池映射。mapping: {ts_code: {pool_id, ...}}
        """
        _main_stock_pools.clear()
        for k, v in mapping.items():
            _main_stock_pools[k] = set(v)
        logger.info(f"Layer2 股票池映射已更新: {len(mapping)}")

    def update_stock_industry(self, ts_code: str, industry: str):
        """更新单只股票行业标签（供动态阈值分桶使用）。"""
        _main_stock_industry[ts_code] = str(industry or "")

    def bulk_update_stock_industry(self, mapping: dict[str, str]):
        """批量更新股票行业标签。mapping: {ts_code: industry}"""
        _main_stock_industry.clear()
        for k, v in (mapping or {}).items():
            _main_stock_industry[str(k)] = str(v or "")
        logger.info(f"Layer2 股票行业映射已更新: {len(mapping or {})}")

    def bulk_update_stock_concepts(self, mapping: dict[str, dict | list | tuple | str]):
        """批量更新股票到东方财富概念板块映射。"""
        _main_stock_concepts.clear()
        _main_stock_core_concept.clear()
        for ts_code, payload in (mapping or {}).items():
            code = str(ts_code or "")
            if not code:
                continue
            if isinstance(payload, dict):
                boards = payload.get("concept_boards") if isinstance(payload.get("concept_boards"), list) else payload.get("boards")
                core = payload.get("core_concept_board") or payload.get("core_concept") or payload.get("core")
            elif isinstance(payload, (list, tuple, set)):
                boards = list(payload)
                core = boards[0] if boards else ""
            else:
                boards = [payload] if payload else []
                core = payload
            clean_boards: list[str] = []
            seen: set[str] = set()
            for board in boards or []:
                name = str(board or "").strip()
                if not name or name in seen:
                    continue
                seen.add(name)
                clean_boards.append(name)
            _main_stock_concepts[code] = clean_boards
            _main_stock_core_concept[code] = str(core or (clean_boards[0] if clean_boards else "")).strip()
        logger.info(f"Layer2 东方财富概念映射已更新: {len(mapping or {})}")

    def bulk_update_concept_snapshots(self, mapping: dict[str, dict]):
        """批量更新东方财富概念板块生态快照。"""
        _main_concept_snapshot.clear()
        for concept_name, payload in (mapping or {}).items():
            name = str(concept_name or "").strip()
            if not name:
                continue
            _main_concept_snapshot[name] = dict(payload or {})
        logger.info(f"Layer2 东方财富概念快照已更新: {len(mapping or {})}")

    def update_instrument_profile(self, ts_code: str, profile: dict):
        """更新单只股票的制度画像。"""
        _main_instrument_profile[str(ts_code)] = dict(profile or {})

    def bulk_update_instrument_profiles(self, mapping: dict[str, dict]):
        """批量更新股票制度画像。mapping: {ts_code: {...}}"""
        _main_instrument_profile.clear()
        for k, v in (mapping or {}).items():
            _main_instrument_profile[str(k)] = dict(v or {})
        logger.info(f"Layer2 标的制度画像已更新: {len(mapping or {})}")

    def update_recent_txns(self, ts_code: str, txns: list):
        """更新单只股票的逐笔成交缓存。"""
        if txns:
            _main_recent_txns[ts_code] = list(txns)[-TXN_ANALYZE_COUNT:]

    def bulk_update_recent_txns(self, mapping: dict):
        """批量更新逐笔成交缓存。"""
        for k, v in mapping.items():
            if v:
                _main_recent_txns[k] = list(v)[-TXN_ANALYZE_COUNT:]

    def get_cached_signals(self, ts_code: str) -> Optional[dict]:
        """读取指定 ts_code 的信号缓存（秒级响应）。"""
        return _main_signal_cache.get(ts_code)

    def get_cached_transactions(self, ts_code: str) -> Optional[list]:
        """读取指定 ts_code 的逐笔成交缓存（秒级响应）。"""
        return _main_recent_txns.get(ts_code)

    def get_prev_price(self, ts_code: str) -> Optional[float]:
        """读取上一笔价格（用于慢路径突破跨越确认）。"""
        try:
            v = _main_prev_price.get(ts_code)
            return float(v) if v is not None else None
        except Exception:
            return None

    def get_cached_signals_bulk(self, ts_codes: list[str]) -> list[dict]:
        """批量读取信号缓存，用于盯盘池列表。"""
        result = []
        for code in ts_codes:
            entry = _main_signal_cache.get(code)
            if entry is not None:
                result.append(entry)
        return result

    def get_pool1_observe_stats(self) -> Optional[dict]:
        """读取 Pool1 两阶段统计快照。"""
        return get_pool1_observe_stats()

    def get_pool1_position_state(self, ts_code: str) -> Optional[dict]:
        """读取 Pool1 单票持仓状态快照（observe/holding）。"""
        return get_pool1_position_state(ts_code)

    def get_pool1_position_storage_status(self) -> Optional[dict]:
        """读取 Pool1 持仓状态存储运行态（redis/file 来源）。"""
        return get_pool1_position_storage_status()

    def get_signal_perf_stats(self) -> Optional[dict]:
        """读取 Layer2 信号计算性能快照（毫秒级）。"""
        return get_signal_perf_stats()

    def start(self):
        """启动 gm 后台订阅进程。"""
        if not _GM_AVAILABLE:
            logger.warning("gm 未安装，GmTickProvider 将仅返回空数据")
            return

        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from config import GM_TOKEN

        if not GM_TOKEN:
            logger.warning("GM_TOKEN 未配置，GmTickProvider 将仅返回空数据")
            return

        # 策略脚本存在性检查
        if not os.path.exists(_STRATEGY_FILE):
            logger.error(f"gm strategy script not found: {_STRATEGY_FILE}")
            logger.error("please ensure _gm_strategy.py exists in project root")
            return

        # 1. 启动 BaseManager TCP server（共享 queue + cache）
        _start_manager_server()
        # 2. 启动消费线程（queue -> cache）
        _start_consumer_thread()
        # 2.1 启动状态机时间刷新线程（午休/尾盘）
        _start_signal_state_refresh_thread()
        # 3. 启动 gm 后台进程（通过 env 连接 manager）
        process = multiprocessing.Process(target=self._run_gm, daemon=True, name='gm-subscribe')
        process.start()
        logger.info(f"gm subscribe process started (strategy: {_STRATEGY_FILE})")

    def stop(self):
        """停止 gm 后台进程（当前为请求停止）。"""
        logger.info("gm subscribe process stop requested")

    @staticmethod
    def _run_gm():
        """在独立进程中运行 gm.run()。"""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from config import GM_TOKEN, GM_MODE

        try:
            # 切换到项目根目录，确保 gm.run() 能找到策略脚本
            original_dir = os.getcwd()
            os.chdir(_PROJECT_ROOT)
            logger.info(f"switch cwd to {_PROJECT_ROOT}")

            logger.info(f"gm.run() 启动, mode={GM_MODE}, token={GM_TOKEN[:8]}...")
            run(
                strategy_id='quant_tick_subscriber',
                filename='_gm_strategy.py',
                mode=GM_MODE,
                token=GM_TOKEN,
                backtest_start_time='2026-04-17 08:00:00',
                backtest_end_time='2026-04-17 16:00:00',
                backtest_adjust=ADJUST_PREV,
                backtest_initial_cash=10000000,
                backtest_commission_ratio=0.0001,
                backtest_slippage_ratio=0.0001
            )
        except Exception as e:
            logger.error(f"gm 后台进程异常: {e}")
        finally:
            # 恢复原工作目录
            os.chdir(original_dir)


__all__ = [name for name in globals() if not name.startswith("__")]
