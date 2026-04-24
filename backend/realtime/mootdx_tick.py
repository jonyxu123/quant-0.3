"""
Mootdx tick provider with lightweight in-memory cache.

This provider keeps tick/minute/transaction caches and reuses the shared
signal engine in `backend.realtime.gm_tick` so fast-path APIs stay consistent
between `gm` and `mootdx` modes.
"""
from __future__ import annotations

import threading
import time
from typing import Optional

from loguru import logger

from backend.realtime.mootdx_client import client as mootdx_client
from backend.realtime.tick_provider import TickProvider


class MootdxTickProvider(TickProvider):
    """Mootdx polling provider."""

    POLL_INTERVAL = 1.0
    BAR_INTERVAL = 15.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribed: set[str] = set()

        self._tick_cache: dict[str, dict] = {}
        self._bar_cache: dict[str, list] = {}
        self._bar_ts: dict[str, float] = {}
        self._txn_cache: dict[str, list] = {}

        self._poll_thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()

    @property
    def name(self) -> str:
        return "mootdx"

    @property
    def display_name(self) -> str:
        return "mootdx polling cache"

    def get_tick(self, ts_code: str) -> dict:
        cached = self._tick_cache.get(ts_code)
        if cached:
            return cached
        tick = mootdx_client.get_tick(ts_code)
        self._tick_cache[ts_code] = tick
        return tick

    def subscribe_symbols(self, ts_codes: list[str]):
        with self._lock:
            before = len(self._subscribed)
            self._subscribed.update(ts_codes)
            if len(self._subscribed) != before:
                logger.debug(f"[mootdx] subscriptions updated: {len(self._subscribed)}")

    def unsubscribe_symbols(self, ts_codes: list[str]):
        with self._lock:
            self._subscribed.difference_update(ts_codes)

    def get_cached_signals(self, ts_code: str) -> Optional[dict]:
        try:
            from backend.realtime import gm_tick
            return gm_tick._main_signal_cache.get(ts_code)
        except Exception:
            return None

    def get_cached_transactions(self, ts_code: str) -> Optional[list]:
        return self._txn_cache.get(ts_code)

    def get_cached_tick(self, ts_code: str) -> Optional[dict]:
        return self._tick_cache.get(ts_code)

    def get_cached_bars(self, ts_code: str) -> Optional[list]:
        return self._bar_cache.get(ts_code)

    def bulk_update_recent_txns(self, mapping: dict):
        self._txn_cache.update(mapping)
        try:
            from backend.realtime import gm_tick
            gm_tick._main_recent_txns.update(mapping)
        except Exception:
            pass

    def get_pool1_observe_stats(self) -> Optional[dict]:
        try:
            from backend.realtime import gm_tick
            return gm_tick.get_pool1_observe_stats()
        except Exception:
            return None

    def get_pool1_position_state(self, ts_code: str) -> Optional[dict]:
        try:
            from backend.realtime import gm_tick
            return gm_tick.get_pool1_position_state(ts_code)
        except Exception:
            return None

    def get_pool1_position_storage_status(self) -> Optional[dict]:
        try:
            from backend.realtime import gm_tick
            return gm_tick.get_pool1_position_storage_status()
        except Exception:
            return None

    def bulk_update_daily_factors(self, factors_map: dict):
        try:
            from backend.realtime import gm_tick
            gm_tick._main_daily_cache.update(factors_map)
        except Exception as e:
            logger.debug(f"[mootdx] bulk_update_daily_factors failed: {e}")

    def bulk_update_chip_features(self, features_map: dict):
        try:
            from backend.realtime import gm_tick
            gm_tick._main_chip_cache.update(features_map)
        except Exception as e:
            logger.debug(f"[mootdx] bulk_update_chip_features failed: {e}")

    def bulk_update_stock_pools(self, mapping: dict):
        try:
            from backend.realtime import gm_tick
            for ts_code, pools in mapping.items():
                gm_tick._main_stock_pools[ts_code] = pools
        except Exception as e:
            logger.debug(f"[mootdx] bulk_update_stock_pools failed: {e}")

    def bulk_update_stock_industry(self, mapping: dict):
        try:
            from backend.realtime import gm_tick
            gm_tick._main_stock_industry.clear()
            gm_tick._main_stock_industry_code.clear()
            for ts_code, payload in (mapping or {}).items():
                code = str(ts_code or "")
                if not code:
                    continue
                if isinstance(payload, dict):
                    industry_name = str(payload.get("industry_name") or payload.get("industry") or "").strip()
                    industry_code = str(payload.get("industry_code") or payload.get("code") or "").strip().upper()
                else:
                    industry_name = str(payload or "").strip()
                    industry_code = ""
                gm_tick._main_stock_industry[code] = industry_name
                gm_tick._main_stock_industry_code[code] = industry_code
        except Exception as e:
            logger.debug(f"[mootdx] bulk_update_stock_industry failed: {e}")

    def bulk_update_stock_concepts(self, mapping: dict):
        try:
            from backend.realtime import gm_tick
            gm_tick._main_stock_concepts.clear()
            gm_tick._main_stock_core_concept.clear()
            gm_tick._main_stock_concept_codes.clear()
            gm_tick._main_stock_core_concept_code.clear()
            for ts_code, payload in (mapping or {}).items():
                code = str(ts_code or "")
                if not code:
                    continue
                if isinstance(payload, dict):
                    boards = payload.get("concept_boards") if isinstance(payload.get("concept_boards"), list) else payload.get("boards")
                    board_codes = payload.get("concept_codes") if isinstance(payload.get("concept_codes"), list) else payload.get("board_codes")
                    core = payload.get("core_concept_board") or payload.get("core_concept") or payload.get("core")
                    core_code = payload.get("core_concept_code") or payload.get("core_code")
                elif isinstance(payload, (list, tuple, set)):
                    boards = list(payload)
                    board_codes = []
                    core = boards[0] if boards else ""
                    core_code = ""
                else:
                    boards = [payload] if payload else []
                    board_codes = []
                    core = payload
                    core_code = ""
                clean_boards: list[str] = []
                seen: set[str] = set()
                for board in boards or []:
                    name = str(board or "").strip()
                    if not name or name in seen:
                        continue
                    seen.add(name)
                    clean_boards.append(name)
                clean_codes: list[str] = []
                seen_codes: set[str] = set()
                for raw_code in board_codes or []:
                    board_code = str(raw_code or "").strip().upper()
                    if not board_code or board_code in seen_codes:
                        continue
                    seen_codes.add(board_code)
                    clean_codes.append(board_code)
                gm_tick._main_stock_concepts[code] = clean_boards
                gm_tick._main_stock_core_concept[code] = str(core or (clean_boards[0] if clean_boards else "")).strip()
                gm_tick._main_stock_concept_codes[code] = clean_codes
                gm_tick._main_stock_core_concept_code[code] = str(core_code or (clean_codes[0] if clean_codes else "")).strip().upper()
        except Exception as e:
            logger.debug(f"[mootdx] bulk_update_stock_concepts failed: {e}")

    def bulk_update_concept_snapshots(self, mapping: dict):
        try:
            from backend.realtime import gm_tick
            gm_tick._main_concept_snapshot.clear()
            for concept_name, payload in (mapping or {}).items():
                item = dict(payload or {})
                name = str(item.get("concept_name") or concept_name or "").strip()
                board_code = str(item.get("board_code") or "").strip().upper()
                if not name and not board_code:
                    continue
                if name:
                    item["concept_name"] = name
                    gm_tick._main_concept_snapshot[name] = dict(item)
                if board_code:
                    item["board_code"] = board_code
                    gm_tick._main_concept_snapshot[board_code] = dict(item)
        except Exception as e:
            logger.debug(f"[mootdx] bulk_update_concept_snapshots failed: {e}")

    def bulk_update_industry_snapshots(self, mapping: dict):
        try:
            from backend.realtime import gm_tick
            gm_tick._main_industry_snapshot.clear()
            for industry_key, payload in (mapping or {}).items():
                item = dict(payload or {})
                key = str(industry_key or "").strip()
                name = str(item.get("industry_name") or key or "").strip()
                if not key and not name:
                    continue
                if name:
                    item["industry_name"] = name
                    gm_tick._main_industry_snapshot[name] = dict(item)
                if key:
                    gm_tick._main_industry_snapshot[key] = dict(item)
        except Exception as e:
            logger.debug(f"[mootdx] bulk_update_industry_snapshots failed: {e}")

    def get_prev_price(self, ts_code: str) -> Optional[float]:
        try:
            from backend.realtime import gm_tick
            v = gm_tick._main_prev_price.get(ts_code)
            return float(v) if v is not None else None
        except Exception:
            return None

    def start(self):
        if self._poll_thread and self._poll_thread.is_alive():
            return
        self._stop_evt.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_loop,
            name="mootdx-poller",
            daemon=True,
        )
        self._poll_thread.start()
        logger.info("[mootdx] polling cache thread started")

    def stop(self):
        self._stop_evt.set()
        if self._poll_thread:
            self._poll_thread.join(timeout=5)

    def _poll_loop(self):
        while not self._stop_evt.is_set():
            t0 = time.time()
            try:
                self._poll_once()
            except Exception as e:
                logger.warning(f"[mootdx] poll loop error: {e}")
            elapsed = time.time() - t0
            self._stop_evt.wait(max(0.05, self.POLL_INTERVAL - elapsed))

    def _poll_once(self):
        with self._lock:
            codes = list(self._subscribed)
        if not codes:
            return

        try:
            from backend.realtime import gm_tick
        except Exception as e:
            logger.debug(f"[mootdx] import gm_tick failed: {e}")
            return

        for ts_code, txns in self._txn_cache.items():
            gm_tick._main_recent_txns[ts_code] = txns

        now = time.time()
        ticks_map: dict[str, dict] = {}
        try:
            ticks_map = mootdx_client.get_ticks(codes)
        except Exception as e:
            logger.debug(f"[mootdx] batch get_ticks failed, fallback per code: {e}")

        for ts_code in codes:
            try:
                tick = ticks_map.get(ts_code) or mootdx_client.get_tick(ts_code)
                self._tick_cache[ts_code] = tick

                last_bar_ts = self._bar_ts.get(ts_code, 0.0)
                if now - last_bar_ts > self.BAR_INTERVAL:
                    try:
                        bars = mootdx_client.get_minute_bars(ts_code, 240)
                        if bars:
                            self._bar_cache[ts_code] = bars
                            self._bar_ts[ts_code] = now
                    except Exception:
                        pass

                daily = gm_tick._main_daily_cache.get(ts_code, {})
                pools = gm_tick._main_stock_pools.get(ts_code, set())
                fired: list[dict] = []
                if daily and pools and ((1 in pools) or (2 in pools)) and gm_tick._is_signal_processing_allowed(int(now)):
                    try:
                        fired = gm_tick._compute_signals_for_tick(tick, daily, ts_code=ts_code) or []
                    except Exception as ce:
                        logger.debug(f"[mootdx] compute signal failed {ts_code}: {ce}")

                if gm_tick._is_signal_processing_allowed(int(now)):
                    gm_tick._postprocess_fired_signals(
                        ts_code=ts_code,
                        tick=tick,
                        all_fired=fired,
                        now=int(now),
                        persist=True,
                    )
            except Exception as e:
                logger.debug(f"[mootdx] poll {ts_code} failed: {e}")
