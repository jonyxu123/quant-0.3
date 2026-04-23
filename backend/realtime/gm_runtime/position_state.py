"""Pool1 position state service for gm runtime."""
from __future__ import annotations

from .common import *  # noqa: F401,F403

def _pool1_position_empty(now_ts: Optional[int] = None) -> dict:
    ts = int(now_ts or time.time())
    return {
        'status': 'observe',
        'position_ratio': 0.0,
        'updated_at': ts,
        'last_buy_at': 0,
        'last_buy_price': 0.0,
        'last_buy_type': '',
        'last_sell_at': 0,
        'last_sell_price': 0.0,
        'last_sell_type': '',
        'last_reduce_at': 0,
        'last_reduce_price': 0.0,
        'last_reduce_type': '',
        'last_reduce_ratio': 0.0,
        'reduce_streak': 0,
        'reduce_ratio_cum': 0.0,
        'last_rebuild_at': 0,
        'last_rebuild_price': 0.0,
        'last_rebuild_type': '',
        'last_rebuild_transition': '',
        'last_rebuild_add_ratio': 0.0,
        'last_rebuild_from_partial_count': 0,
        'last_rebuild_from_partial_ratio': 0.0,
        'last_exit_after_partial': False,
        'last_exit_partial_count': 0,
        'last_exit_reduce_ratio_cum': 0.0,
        'left_quick_clear_streak': 0,
        'last_left_quick_clear_at': 0,
        'signal_seq': 0,
    }


def _pool1_hold_days(entry: dict, now_ts: Optional[int] = None) -> float:
    if not isinstance(entry, dict):
        return 0.0
    if str(entry.get('status') or 'observe') != 'holding':
        return 0.0
    last_buy_at = int(entry.get('last_buy_at', 0) or 0)
    if last_buy_at <= 0:
        return 0.0
    ts = int(now_ts or time.time())
    return max(0.0, float(ts - last_buy_at) / 86400.0)


def _pool1_position_redis_key() -> str:
    return f"{_P1_POS_REDIS_KEY_PREFIX}:state"


def _normalize_pool1_position_ratio(status: str, value: object) -> float:
    st = 'holding' if str(status or '').strip().lower() == 'holding' else 'observe'
    if st != 'holding':
        return 0.0
    try:
        ratio = float(value)
    except Exception:
        ratio = 1.0
    if ratio <= 0:
        ratio = 1.0
    return max(0.0, min(1.0, ratio))


def _get_pool1_position_redis():
    global _pool1_position_redis_cli, _pool1_position_redis_warn_at, _pool1_position_redis_next_retry_at
    if not _P1_POS_REDIS_ENABLED or _redis_mod is None:
        return None
    if _pool1_position_redis_cli is not None:
        return _pool1_position_redis_cli
    now = time.time()
    if now < _pool1_position_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_P1_POS_REDIS_URL, decode_responses=True)
        cli.ping()
        _pool1_position_redis_cli = cli
        logger.info(f"Pool1 position Redis enabled: {_P1_POS_REDIS_KEY_PREFIX}")
        return _pool1_position_redis_cli
    except Exception as e:
        _pool1_position_redis_next_retry_at = now + 5.0
        if now - _pool1_position_redis_warn_at >= 30.0:
            _pool1_position_redis_warn_at = now
            logger.warning(f"Pool1 position Redis unavailable, fallback file/memory: {e}")
        return None


def _normalize_pool1_position_entry(item: dict, now_ts: Optional[int] = None) -> dict:
    ent = _pool1_position_empty(now_ts=now_ts)
    if isinstance(item, dict):
        ent.update(
            {
                'status': str(item.get('status') or 'observe'),
                'position_ratio': float(item.get('position_ratio', 0.0) or 0.0),
                'updated_at': int(item.get('updated_at', 0) or 0),
                'last_buy_at': int(item.get('last_buy_at', 0) or 0),
                'last_buy_price': float(item.get('last_buy_price', 0.0) or 0.0),
                'last_buy_type': str(item.get('last_buy_type') or ''),
                'last_sell_at': int(item.get('last_sell_at', 0) or 0),
                'last_sell_price': float(item.get('last_sell_price', 0.0) or 0.0),
                'last_sell_type': str(item.get('last_sell_type') or ''),
                'last_reduce_at': int(item.get('last_reduce_at', 0) or 0),
                'last_reduce_price': float(item.get('last_reduce_price', 0.0) or 0.0),
                'last_reduce_type': str(item.get('last_reduce_type') or ''),
                'last_reduce_ratio': float(item.get('last_reduce_ratio', 0.0) or 0.0),
                'reduce_streak': int(item.get('reduce_streak', 0) or 0),
                'reduce_ratio_cum': float(item.get('reduce_ratio_cum', 0.0) or 0.0),
                'last_rebuild_at': int(item.get('last_rebuild_at', 0) or 0),
                'last_rebuild_price': float(item.get('last_rebuild_price', 0.0) or 0.0),
                'last_rebuild_type': str(item.get('last_rebuild_type') or ''),
                'last_rebuild_transition': str(item.get('last_rebuild_transition') or ''),
                'last_rebuild_add_ratio': float(item.get('last_rebuild_add_ratio', 0.0) or 0.0),
                'last_rebuild_from_partial_count': int(item.get('last_rebuild_from_partial_count', 0) or 0),
                'last_rebuild_from_partial_ratio': float(item.get('last_rebuild_from_partial_ratio', 0.0) or 0.0),
                'last_exit_after_partial': bool(item.get('last_exit_after_partial', False)),
                'last_exit_partial_count': int(item.get('last_exit_partial_count', 0) or 0),
                'last_exit_reduce_ratio_cum': float(item.get('last_exit_reduce_ratio_cum', 0.0) or 0.0),
                'left_quick_clear_streak': int(item.get('left_quick_clear_streak', 0) or 0),
                'last_left_quick_clear_at': int(item.get('last_left_quick_clear_at', 0) or 0),
                'signal_seq': int(item.get('signal_seq', 0) or 0),
            }
        )
    if ent['status'] not in {'observe', 'holding'}:
        ent['status'] = 'observe'
    ent['position_ratio'] = _normalize_pool1_position_ratio(ent['status'], ent.get('position_ratio'))
    return ent


def _pool1_position_read_redis() -> Optional[dict[str, dict]]:
    cli = _get_pool1_position_redis()
    if cli is None:
        return None
    key = _pool1_position_redis_key()
    try:
        raw = cli.hgetall(key) or {}
    except Exception:
        return None
    if not raw:
        return None
    loaded: dict[str, dict] = {}
    for code, text in raw.items():
        if not isinstance(code, str) or not code:
            continue
        if not isinstance(text, str) or not text:
            continue
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        loaded[code] = _normalize_pool1_position_entry(parsed)
    return loaded if loaded else None


def _pool1_position_write_redis_entry(code: str, entry: dict) -> bool:
    cli = _get_pool1_position_redis()
    if cli is None:
        return False
    key = _pool1_position_redis_key()
    try:
        payload = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
        pipe = cli.pipeline(transaction=True)
        pipe.hset(key, code, payload)
        ttl = max(1, int(_P1_POS_REDIS_TTL_DAYS)) * 24 * 3600
        pipe.expire(key, ttl)
        pipe.execute()
        return True
    except Exception:
        return False


def _pool1_position_write_redis_bulk(payload: dict[str, dict]) -> bool:
    cli = _get_pool1_position_redis()
    if cli is None:
        return False
    key = _pool1_position_redis_key()
    try:
        pipe = cli.pipeline(transaction=True)
        pipe.delete(key)
        for code, entry in payload.items():
            if not isinstance(code, str) or not code or not isinstance(entry, dict):
                continue
            text = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
            pipe.hset(key, code, text)
        ttl = max(1, int(_P1_POS_REDIS_TTL_DAYS)) * 24 * 3600
        pipe.expire(key, ttl)
        pipe.execute()
        return True
    except Exception:
        return False


def _load_pool1_position_state() -> None:
    global _pool1_position_state_loaded, _pool1_position_last_source
    if _pool1_position_state_loaded:
        return
    _pool1_position_state_loaded = True
    if not _P1_POS_ENABLED:
        return
    # 优先从 Redis 恢复（跨进程一致性更好）
    redis_loaded = _pool1_position_read_redis()
    if isinstance(redis_loaded, dict) and redis_loaded:
        _pool1_position_state.update(redis_loaded)
        _pool1_position_last_source = 'redis'
        logger.info(f"Pool1 持仓状态已从 Redis 加载: {len(redis_loaded)}")
        return
    if not _P1_POS_FILE_FALLBACK:
        _pool1_position_last_source = 'memory'
        return
    try:
        if not os.path.exists(_P1_POS_FILE):
            return
        with open(_P1_POS_FILE, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return
        loaded: dict[str, dict] = {}
        for code, item in raw.items():
            if not isinstance(code, str) or not code or not isinstance(item, dict):
                continue
            loaded[code] = _normalize_pool1_position_entry(item)
        _pool1_position_state.update(loaded)
        if loaded:
            _pool1_position_last_source = 'file'
            logger.info(f"Pool1 持仓状态已从文件加载: {len(loaded)}")
            # 文件恢复后尽量回写 Redis，供后续进程复用。
            if _P1_POS_REDIS_ENABLED:
                _pool1_position_write_redis_bulk(loaded)
    except Exception as e:
        logger.warning(f"Pool1 持仓状态加载失败: {e}")


def _persist_pool1_position_state(*, changed_code: Optional[str] = None, changed_entry: Optional[dict] = None) -> None:
    global _pool1_position_last_source
    if not _P1_POS_ENABLED:
        return
    if _P1_POS_REDIS_ENABLED:
        redis_ok = False
        if isinstance(changed_code, str) and changed_code and isinstance(changed_entry, dict):
            redis_ok = _pool1_position_write_redis_entry(changed_code, changed_entry)
        else:
            try:
                with _pool1_position_state_lock:
                    payload = {k: dict(v) for k, v in _pool1_position_state.items() if isinstance(v, dict)}
                redis_ok = _pool1_position_write_redis_bulk(payload)
            except Exception:
                redis_ok = False
        if redis_ok:
            _pool1_position_last_source = 'redis'
        elif not _P1_POS_FILE_FALLBACK:
            _pool1_position_last_source = 'memory'
    if not _P1_POS_FILE_FALLBACK:
        return
    try:
        with _pool1_position_state_lock:
            items = list(_pool1_position_state.items())
        if len(items) > _P1_POS_FILE_MAX_ITEMS:
            items.sort(key=lambda kv: int((kv[1] or {}).get('updated_at', 0) or 0), reverse=True)
            items = items[:_P1_POS_FILE_MAX_ITEMS]
        payload = {k: v for k, v in items if isinstance(v, dict)}
        tmp = _P1_POS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
        os.replace(tmp, _P1_POS_FILE)
        if _pool1_position_last_source != 'redis':
            _pool1_position_last_source = 'file'
    except Exception as e:
        logger.debug(f"Pool1 持仓状态持久化失败: {e}")


def _pool1_get_position_state(ts_code: str, now_ts: Optional[int] = None) -> dict:
    _load_pool1_position_state()
    code = str(ts_code or '')
    ts = int(now_ts or time.time())
    with _pool1_position_state_lock:
        ent = _pool1_position_state.get(code)
        if not isinstance(ent, dict):
            ent = _pool1_position_empty(ts)
            _pool1_position_state[code] = ent
        snapshot = dict(ent)
    snapshot['holding_days'] = round(_pool1_hold_days(snapshot, ts), 4)
    return snapshot


def _pool1_set_position_state(
    ts_code: str,
    status: str,
    *,
    now_ts: Optional[int] = None,
    signal_type: str = '',
    signal_price: Optional[float] = None,
    clear_level: str = '',
    reduce_ratio: Optional[float] = None,
    rebuild_ratio: Optional[float] = None,
) -> dict:
    _load_pool1_position_state()
    code = str(ts_code or '')
    ts = int(now_ts or time.time())
    st = 'holding' if str(status or '') == 'holding' else 'observe'
    with _pool1_position_state_lock:
        ent = _pool1_position_state.get(code)
        if not isinstance(ent, dict):
            ent = _pool1_position_empty(ts)
        prev_status = str(ent.get('status') or 'observe')
        prev_last_buy_at = int(ent.get('last_buy_at', 0) or 0)
        prev_last_buy_type = str(ent.get('last_buy_type') or '')
        prev_last_sell_at = int(ent.get('last_sell_at', 0) or 0)
        prev_ratio = _normalize_pool1_position_ratio(str(ent.get('status') or 'observe'), ent.get('position_ratio'))
        prev_reduce_streak = int(ent.get('reduce_streak', 0) or 0)
        prev_reduce_ratio_cum = float(ent.get('reduce_ratio_cum', 0.0) or 0.0)
        prev_left_quick_clear_at = int(ent.get('last_left_quick_clear_at', 0) or 0)
        prev_left_quick_clear_streak = int(ent.get('left_quick_clear_streak', 0) or 0)
        ent['status'] = st
        ent['updated_at'] = ts
        ent['signal_seq'] = int(ent.get('signal_seq', 0) or 0) + 1
        px = float(signal_price or 0.0)
        clear_level_norm = str(clear_level or '').strip().lower()
        try:
            reduce_ratio_val = float(reduce_ratio) if reduce_ratio is not None else 0.0
        except Exception:
            reduce_ratio_val = 0.0
        reduce_ratio_val = max(0.0, min(1.0, reduce_ratio_val))
        try:
            rebuild_ratio_val = float(rebuild_ratio) if rebuild_ratio is not None else 0.0
        except Exception:
            rebuild_ratio_val = 0.0
        rebuild_ratio_val = max(0.0, min(1.0, rebuild_ratio_val))
        is_partial_reduce = (
            st == 'holding'
            and str(signal_type or '') == 'timing_clear'
            and clear_level_norm == 'partial'
            and reduce_ratio_val > 0
        )
        is_inplace_rebuild = (
            st == 'holding'
            and prev_status == 'holding'
            and str(signal_type or '') in {'left_side_buy', 'right_side_breakout'}
            and rebuild_ratio_val > 0
            and prev_ratio < 0.999
        )
        if is_partial_reduce:
            next_ratio = prev_ratio if prev_ratio > 0 else 1.0
            next_ratio = max(0.0, min(1.0, next_ratio * (1.0 - reduce_ratio_val)))
            ent['status'] = 'observe' if next_ratio <= 0.01 else 'holding'
            ent['position_ratio'] = 0.0 if ent['status'] == 'observe' else round(next_ratio, 4)
            ent['last_reduce_at'] = ts
            ent['last_reduce_price'] = px
            ent['last_reduce_type'] = str(signal_type or '')
            ent['last_reduce_ratio'] = round(reduce_ratio_val, 4)
            ent['reduce_streak'] = max(0, prev_reduce_streak) + 1
            ent['reduce_ratio_cum'] = round(max(0.0, min(1.0, prev_reduce_ratio_cum + reduce_ratio_val)), 4)
            if ent['status'] == 'observe':
                ent['last_sell_at'] = ts
                ent['last_sell_price'] = px
                ent['last_sell_type'] = f"{signal_type or 'timing_clear'}:partial_exhausted"
                ent['last_exit_after_partial'] = True
                ent['last_exit_partial_count'] = int(ent.get('reduce_streak', 0) or 0)
                ent['last_exit_reduce_ratio_cum'] = round(float(ent.get('reduce_ratio_cum', 0.0) or 0.0), 4)
                ent['reduce_streak'] = 0
                ent['reduce_ratio_cum'] = 0.0
        elif is_inplace_rebuild:
            missing_ratio = max(0.0, 1.0 - prev_ratio)
            next_ratio = max(0.0, min(1.0, prev_ratio + missing_ratio * rebuild_ratio_val))
            ent['status'] = 'holding'
            ent['position_ratio'] = round(next_ratio, 4)
            ent['last_rebuild_at'] = ts
            ent['last_rebuild_price'] = px
            ent['last_rebuild_type'] = str(signal_type or '')
            ent['last_rebuild_transition'] = 'holding->holding(rebuild)'
            ent['last_rebuild_add_ratio'] = round(rebuild_ratio_val, 4)
            ent['last_rebuild_from_partial_count'] = max(0, prev_reduce_streak)
            ent['last_rebuild_from_partial_ratio'] = round(max(0.0, min(1.0, prev_reduce_ratio_cum)), 4)
            ent['last_exit_after_partial'] = False
            ent['last_exit_partial_count'] = 0
            ent['last_exit_reduce_ratio_cum'] = 0.0
            ent['reduce_streak'] = 0
            ent['reduce_ratio_cum'] = 0.0
        elif st == 'holding':
            ent['position_ratio'] = 1.0
            ent['last_buy_at'] = ts
            ent['last_buy_price'] = px
            ent['last_buy_type'] = str(signal_type or '')
            rebuild_after_partial = (
                prev_status == 'observe'
                and (prev_last_sell_at <= 0 or ts > prev_last_sell_at)
                and bool(ent.get('last_exit_after_partial', False))
            )
            if rebuild_after_partial:
                ent['last_rebuild_at'] = ts
                ent['last_rebuild_price'] = px
                ent['last_rebuild_type'] = str(signal_type or '')
                ent['last_rebuild_transition'] = 'observe->holding(rebuild_after_partial)'
                ent['last_rebuild_add_ratio'] = 1.0
                ent['last_rebuild_from_partial_count'] = int(ent.get('last_exit_partial_count', 0) or 0)
                ent['last_rebuild_from_partial_ratio'] = round(float(ent.get('last_exit_reduce_ratio_cum', 0.0) or 0.0), 4)
            else:
                ent['last_rebuild_at'] = 0
                ent['last_rebuild_price'] = 0.0
                ent['last_rebuild_type'] = ''
                ent['last_rebuild_transition'] = ''
                ent['last_rebuild_add_ratio'] = 0.0
                ent['last_rebuild_from_partial_count'] = 0
                ent['last_rebuild_from_partial_ratio'] = 0.0
            ent['last_exit_after_partial'] = False
            ent['last_exit_partial_count'] = 0
            ent['last_exit_reduce_ratio_cum'] = 0.0
            ent['reduce_streak'] = 0
            ent['reduce_ratio_cum'] = 0.0
        else:
            ent['position_ratio'] = 0.0
            ent['last_sell_at'] = ts
            ent['last_sell_price'] = px
            ent['last_sell_type'] = str(signal_type or '')
            ent['last_exit_after_partial'] = prev_reduce_streak > 0 or prev_reduce_ratio_cum > 0
            ent['last_exit_partial_count'] = max(0, prev_reduce_streak)
            ent['last_exit_reduce_ratio_cum'] = round(max(0.0, min(1.0, prev_reduce_ratio_cum)), 4)
            ent['reduce_streak'] = 0
            ent['reduce_ratio_cum'] = 0.0
            hold_days = max(0.0, float(ts - prev_last_buy_at) / 86400.0) if prev_last_buy_at > 0 else 0.0
            quick_clear_days = 3.0
            try:
                from config import POOL1_SIGNAL_CONFIG as _P1_SIG_CFG
                quick_clear_days = float((((_P1_SIG_CFG or {}).get('left_streak_guard') or {}).get('quick_clear_max_hold_days', 3.0)) or 3.0)
            except Exception:
                quick_clear_days = 3.0
            cooldown_hours = 72.0
            try:
                from config import POOL1_SIGNAL_CONFIG as _P1_SIG_CFG
                cooldown_hours = float((((_P1_SIG_CFG or {}).get('left_streak_guard') or {}).get('cooldown_hours', 72.0)) or 72.0)
            except Exception:
                cooldown_hours = 72.0
            if (
                str(signal_type or '') == 'timing_clear'
                and prev_last_buy_type == 'left_side_buy'
                and prev_last_buy_at > 0
                and hold_days <= quick_clear_days
            ):
                if prev_left_quick_clear_at > 0 and (ts - prev_left_quick_clear_at) <= int(cooldown_hours * 3600):
                    ent['left_quick_clear_streak'] = max(1, prev_left_quick_clear_streak) + 1
                else:
                    ent['left_quick_clear_streak'] = 1
                ent['last_left_quick_clear_at'] = ts
            elif prev_last_buy_type == 'left_side_buy' and prev_last_buy_at > 0 and hold_days > quick_clear_days:
                ent['left_quick_clear_streak'] = 0
        _pool1_position_state[code] = ent
        persisted = dict(ent)
        out = dict(ent)
    out['holding_days'] = round(_pool1_hold_days(out, ts), 4)
    out['position_ratio'] = round(float(out.get('position_ratio', 0.0) or 0.0), 4)
    _persist_pool1_position_state(changed_code=code, changed_entry=persisted)
    return out


def get_pool1_position_state(ts_code: str) -> Optional[dict]:
    try:
        return _pool1_get_position_state(ts_code)
    except Exception:
        return None


def get_pool1_position_storage_status() -> dict:
    try:
        redis_ready = bool(_get_pool1_position_redis() is not None) if _P1_POS_REDIS_ENABLED else False
    except Exception:
        redis_ready = False
    return {
        'enabled': bool(_P1_POS_ENABLED),
        'redis_enabled': bool(_P1_POS_REDIS_ENABLED),
        'redis_ready': bool(redis_ready),
        'file_fallback': bool(_P1_POS_FILE_FALLBACK),
        'source': str(_pool1_position_last_source or 'memory'),
    }


__all__ = [name for name in globals() if not name.startswith("__")]
