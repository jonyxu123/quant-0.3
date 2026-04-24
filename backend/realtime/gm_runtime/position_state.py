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
        'last_reduce_source': '',
        'last_reduce_avwap_tier': '',
        'last_reduce_avwap_reason': '',
        'reduce_streak': 0,
        'reduce_ratio_cum': 0.0,
        'last_rebuild_at': 0,
        'last_rebuild_price': 0.0,
        'last_rebuild_type': '',
        'last_rebuild_transition': '',
        'last_rebuild_add_ratio': 0.0,
        'last_rebuild_add_ratio_base': 0.0,
        'last_rebuild_add_ratio_bias': 0.0,
        'last_rebuild_from_partial_count': 0,
        'last_rebuild_from_partial_ratio': 0.0,
        'last_rebuild_from_partial_source': '',
        'last_rebuild_from_partial_avwap_tier': '',
        'last_rebuild_from_partial_avwap_reason': '',
        'last_exit_after_partial': False,
        'last_exit_partial_count': 0,
        'last_exit_reduce_ratio_cum': 0.0,
        'last_exit_reduce_source': '',
        'last_exit_reduce_avwap_tier': '',
        'last_exit_reduce_avwap_reason': '',
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
                'last_reduce_source': str(item.get('last_reduce_source') or ''),
                'last_reduce_avwap_tier': str(item.get('last_reduce_avwap_tier') or ''),
                'last_reduce_avwap_reason': str(item.get('last_reduce_avwap_reason') or ''),
                'reduce_streak': int(item.get('reduce_streak', 0) or 0),
                'reduce_ratio_cum': float(item.get('reduce_ratio_cum', 0.0) or 0.0),
                'last_rebuild_at': int(item.get('last_rebuild_at', 0) or 0),
                'last_rebuild_price': float(item.get('last_rebuild_price', 0.0) or 0.0),
                'last_rebuild_type': str(item.get('last_rebuild_type') or ''),
                'last_rebuild_transition': str(item.get('last_rebuild_transition') or ''),
                'last_rebuild_add_ratio': float(item.get('last_rebuild_add_ratio', 0.0) or 0.0),
                'last_rebuild_add_ratio_base': float(item.get('last_rebuild_add_ratio_base', 0.0) or 0.0),
                'last_rebuild_add_ratio_bias': float(item.get('last_rebuild_add_ratio_bias', 0.0) or 0.0),
                'last_rebuild_from_partial_count': int(item.get('last_rebuild_from_partial_count', 0) or 0),
                'last_rebuild_from_partial_ratio': float(item.get('last_rebuild_from_partial_ratio', 0.0) or 0.0),
                'last_rebuild_from_partial_source': str(item.get('last_rebuild_from_partial_source') or ''),
                'last_rebuild_from_partial_avwap_tier': str(item.get('last_rebuild_from_partial_avwap_tier') or ''),
                'last_rebuild_from_partial_avwap_reason': str(item.get('last_rebuild_from_partial_avwap_reason') or ''),
                'last_exit_after_partial': bool(item.get('last_exit_after_partial', False)),
                'last_exit_partial_count': int(item.get('last_exit_partial_count', 0) or 0),
                'last_exit_reduce_ratio_cum': float(item.get('last_exit_reduce_ratio_cum', 0.0) or 0.0),
                'last_exit_reduce_source': str(item.get('last_exit_reduce_source') or ''),
                'last_exit_reduce_avwap_tier': str(item.get('last_exit_reduce_avwap_tier') or ''),
                'last_exit_reduce_avwap_reason': str(item.get('last_exit_reduce_avwap_reason') or ''),
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
    reduce_meta: Optional[dict] = None,
    rebuild_meta: Optional[dict] = None,
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
        reduce_meta = reduce_meta if isinstance(reduce_meta, dict) else {}
        rebuild_meta = rebuild_meta if isinstance(rebuild_meta, dict) else {}
        reduce_source = str(reduce_meta.get('source') or '').strip()
        reduce_avwap_tier = str(reduce_meta.get('avwap_tier') or '').strip().lower()
        reduce_avwap_reason = str(reduce_meta.get('avwap_reason') or '').strip()
        rebuild_add_ratio_base = float(rebuild_meta.get('base_add_ratio', 0.0) or 0.0)
        rebuild_add_ratio_bias = float(rebuild_meta.get('add_ratio_bias', 0.0) or 0.0)
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
            ent['last_reduce_source'] = reduce_source
            ent['last_reduce_avwap_tier'] = reduce_avwap_tier
            ent['last_reduce_avwap_reason'] = reduce_avwap_reason
            ent['reduce_streak'] = max(0, prev_reduce_streak) + 1
            ent['reduce_ratio_cum'] = round(max(0.0, min(1.0, prev_reduce_ratio_cum + reduce_ratio_val)), 4)
            if ent['status'] == 'observe':
                ent['last_sell_at'] = ts
                ent['last_sell_price'] = px
                ent['last_sell_type'] = f"{signal_type or 'timing_clear'}:partial_exhausted"
                ent['last_exit_after_partial'] = True
                ent['last_exit_partial_count'] = int(ent.get('reduce_streak', 0) or 0)
                ent['last_exit_reduce_ratio_cum'] = round(float(ent.get('reduce_ratio_cum', 0.0) or 0.0), 4)
                ent['last_exit_reduce_source'] = str(ent.get('last_reduce_source') or '')
                ent['last_exit_reduce_avwap_tier'] = str(ent.get('last_reduce_avwap_tier') or '')
                ent['last_exit_reduce_avwap_reason'] = str(ent.get('last_reduce_avwap_reason') or '')
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
            ent['last_rebuild_add_ratio_base'] = round(rebuild_add_ratio_base, 4)
            ent['last_rebuild_add_ratio_bias'] = round(rebuild_add_ratio_bias, 4)
            ent['last_rebuild_from_partial_count'] = max(0, prev_reduce_streak)
            ent['last_rebuild_from_partial_ratio'] = round(max(0.0, min(1.0, prev_reduce_ratio_cum)), 4)
            ent['last_rebuild_from_partial_source'] = str(ent.get('last_reduce_source') or '')
            ent['last_rebuild_from_partial_avwap_tier'] = str(ent.get('last_reduce_avwap_tier') or '')
            ent['last_rebuild_from_partial_avwap_reason'] = str(ent.get('last_reduce_avwap_reason') or '')
            ent['last_exit_after_partial'] = False
            ent['last_exit_partial_count'] = 0
            ent['last_exit_reduce_ratio_cum'] = 0.0
            ent['last_exit_reduce_source'] = ''
            ent['last_exit_reduce_avwap_tier'] = ''
            ent['last_exit_reduce_avwap_reason'] = ''
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
                ent['last_rebuild_add_ratio_base'] = round(rebuild_add_ratio_base, 4)
                ent['last_rebuild_add_ratio_bias'] = round(rebuild_add_ratio_bias, 4)
                ent['last_rebuild_from_partial_count'] = int(ent.get('last_exit_partial_count', 0) or 0)
                ent['last_rebuild_from_partial_ratio'] = round(float(ent.get('last_exit_reduce_ratio_cum', 0.0) or 0.0), 4)
                ent['last_rebuild_from_partial_source'] = str(ent.get('last_exit_reduce_source') or '')
                ent['last_rebuild_from_partial_avwap_tier'] = str(ent.get('last_exit_reduce_avwap_tier') or '')
                ent['last_rebuild_from_partial_avwap_reason'] = str(ent.get('last_exit_reduce_avwap_reason') or '')
            else:
                ent['last_rebuild_at'] = 0
                ent['last_rebuild_price'] = 0.0
                ent['last_rebuild_type'] = ''
                ent['last_rebuild_transition'] = ''
                ent['last_rebuild_add_ratio'] = 0.0
                ent['last_rebuild_add_ratio_base'] = 0.0
                ent['last_rebuild_add_ratio_bias'] = 0.0
                ent['last_rebuild_from_partial_count'] = 0
                ent['last_rebuild_from_partial_ratio'] = 0.0
                ent['last_rebuild_from_partial_source'] = ''
                ent['last_rebuild_from_partial_avwap_tier'] = ''
                ent['last_rebuild_from_partial_avwap_reason'] = ''
                ent['last_reduce_source'] = ''
                ent['last_reduce_avwap_tier'] = ''
                ent['last_reduce_avwap_reason'] = ''
            ent['last_exit_after_partial'] = False
            ent['last_exit_partial_count'] = 0
            ent['last_exit_reduce_ratio_cum'] = 0.0
            ent['last_exit_reduce_source'] = ''
            ent['last_exit_reduce_avwap_tier'] = ''
            ent['last_exit_reduce_avwap_reason'] = ''
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
            ent['last_exit_reduce_source'] = str(ent.get('last_reduce_source') or '')
            ent['last_exit_reduce_avwap_tier'] = str(ent.get('last_reduce_avwap_tier') or '')
            ent['last_exit_reduce_avwap_reason'] = str(ent.get('last_reduce_avwap_reason') or '')
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


def _pool2_trade_date(now_ts: Optional[int] = None) -> str:
    try:
        dt = _dt.datetime.fromtimestamp(int(now_ts or time.time()))
    except Exception:
        dt = _dt.datetime.now()
    return dt.strftime('%Y-%m-%d')


def _pool2_t0_inventory_empty(now_ts: Optional[int] = None) -> dict:
    ts = int(now_ts or time.time())
    trade_date = _pool2_trade_date(ts)
    return {
        'trade_date': trade_date,
        'updated_at': ts,
        'overnight_base_qty': 0,
        'tradable_t_qty': 0,
        'reserve_qty': 0,
        'today_t_count': 0,
        'today_positive_t_qty': 0,
        'today_reverse_t_qty': 0,
        'cash_available_for_t': 0.0,
        'inventory_anchor_cost': 0.0,
        'last_signal_at': 0,
        'last_signal_type': '',
        'last_signal_price': 0.0,
        'last_action_qty': 0,
        'last_action_mode': '',
        'last_action_role': '',
        'today_action_log': [],
        'last_reset_at': ts,
        'signal_seq': 0,
    }


def _normalize_pool2_qty(value: object) -> int:
    try:
        qty = int(float(value or 0))
    except Exception:
        qty = 0
    return max(0, qty)


def _normalize_pool2_cash(value: object) -> float:
    try:
        cash = float(value or 0.0)
    except Exception:
        cash = 0.0
    return round(max(0.0, cash), 4)


def _normalize_pool2_cost(value: object) -> float:
    try:
        cost = float(value or 0.0)
    except Exception:
        cost = 0.0
    return round(max(0.0, cost), 4)


def _normalize_pool2_action_log(value: object, trade_date: str, limit: int = 20) -> list[dict]:
    if not isinstance(value, list):
        return []
    normalized: list[dict] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        item_trade_date = str(item.get('trade_date') or trade_date or '')
        if trade_date and item_trade_date and item_trade_date != trade_date:
            continue
        try:
            at = int(item.get('at', 0) or 0)
        except Exception:
            at = 0
        try:
            seq = int(item.get('signal_seq', 0) or 0)
        except Exception:
            seq = 0
        normalized.append(
            {
                'at': at,
                'at_iso': str(item.get('at_iso') or ''),
                'trade_date': item_trade_date,
                'signal_type': str(item.get('signal_type') or ''),
                'action_mode': str(item.get('action_mode') or ''),
                'action_role': str(item.get('action_role') or ''),
                'price': _normalize_pool2_cost(item.get('price')),
                'qty': _normalize_pool2_qty(item.get('qty')),
                'remaining_tradable_t_qty': _normalize_pool2_qty(item.get('remaining_tradable_t_qty')),
                'today_t_count': max(0, int(float(item.get('today_t_count') or 0))),
                'signal_seq': seq,
            }
        )
    normalized.sort(key=lambda x: (int(x.get('at', 0) or 0), int(x.get('signal_seq', 0) or 0)))
    return normalized[-max(1, int(limit or 20)):]


def _normalize_pool2_inventory_entry(item: dict, now_ts: Optional[int] = None) -> dict:
    ts = int(now_ts or time.time())
    ent = _pool2_t0_inventory_empty(now_ts=ts)
    if isinstance(item, dict):
        ent.update(
            {
                'trade_date': str(item.get('trade_date') or ent['trade_date']),
                'updated_at': int(item.get('updated_at', ts) or ts),
                'overnight_base_qty': _normalize_pool2_qty(item.get('overnight_base_qty')),
                'tradable_t_qty': _normalize_pool2_qty(item.get('tradable_t_qty')),
                'reserve_qty': _normalize_pool2_qty(item.get('reserve_qty')),
                'today_t_count': int(item.get('today_t_count', 0) or 0),
                'today_positive_t_qty': _normalize_pool2_qty(item.get('today_positive_t_qty')),
                'today_reverse_t_qty': _normalize_pool2_qty(item.get('today_reverse_t_qty')),
                'cash_available_for_t': _normalize_pool2_cash(item.get('cash_available_for_t')),
                'inventory_anchor_cost': _normalize_pool2_cost(item.get('inventory_anchor_cost')),
                'last_signal_at': int(item.get('last_signal_at', 0) or 0),
                'last_signal_type': str(item.get('last_signal_type') or ''),
                'last_signal_price': _normalize_pool2_cost(item.get('last_signal_price')),
                'last_action_qty': _normalize_pool2_qty(item.get('last_action_qty')),
                'last_action_mode': str(item.get('last_action_mode') or ''),
                'last_action_role': str(item.get('last_action_role') or ''),
                'last_reset_at': int(item.get('last_reset_at', ts) or ts),
                'signal_seq': int(item.get('signal_seq', 0) or 0),
            }
        )
        ent['today_action_log'] = _normalize_pool2_action_log(
            item.get('today_action_log'),
            str(ent.get('trade_date') or ''),
        )
    base_qty = _normalize_pool2_qty(ent.get('overnight_base_qty'))
    reserve_qty = min(base_qty, _normalize_pool2_qty(ent.get('reserve_qty')))
    max_tradable = max(0, base_qty - reserve_qty)
    trade_date = _pool2_trade_date(ts)
    if str(ent.get('trade_date') or '') != trade_date:
        ent['trade_date'] = trade_date
        ent['today_t_count'] = 0
        ent['today_positive_t_qty'] = 0
        ent['today_reverse_t_qty'] = 0
        ent['tradable_t_qty'] = max_tradable
        ent['today_action_log'] = []
        ent['last_reset_at'] = ts
    else:
        ent['tradable_t_qty'] = min(max_tradable, _normalize_pool2_qty(ent.get('tradable_t_qty')))
    ent['overnight_base_qty'] = base_qty
    ent['reserve_qty'] = reserve_qty
    ent['cash_available_for_t'] = _normalize_pool2_cash(ent.get('cash_available_for_t'))
    ent['inventory_anchor_cost'] = _normalize_pool2_cost(ent.get('inventory_anchor_cost'))
    ent['today_t_count'] = max(0, int(ent.get('today_t_count', 0) or 0))
    ent['today_action_log'] = _normalize_pool2_action_log(ent.get('today_action_log'), str(ent.get('trade_date') or ''))
    return ent


def _pool2_t0_inventory_redis_key() -> str:
    return f"{_T0_INV_REDIS_KEY_PREFIX}:state"


def _get_pool2_t0_inventory_redis():
    global _pool2_t0_inventory_redis_cli, _pool2_t0_inventory_redis_warn_at, _pool2_t0_inventory_redis_next_retry_at
    if not _T0_INV_REDIS_ENABLED or _redis_mod is None:
        return None
    if _pool2_t0_inventory_redis_cli is not None:
        return _pool2_t0_inventory_redis_cli
    now = time.time()
    if now < _pool2_t0_inventory_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_T0_INV_REDIS_URL, decode_responses=True)
        cli.ping()
        _pool2_t0_inventory_redis_cli = cli
        logger.info(f"Pool2 T0 inventory Redis enabled: {_T0_INV_REDIS_KEY_PREFIX}")
        return _pool2_t0_inventory_redis_cli
    except Exception as e:
        _pool2_t0_inventory_redis_next_retry_at = now + 5.0
        if now - _pool2_t0_inventory_redis_warn_at >= 30.0:
            _pool2_t0_inventory_redis_warn_at = now
            logger.warning(f"Pool2 T0 inventory Redis unavailable, fallback file/memory: {e}")
        return None


def _pool2_t0_inventory_read_redis() -> Optional[dict[str, dict]]:
    cli = _get_pool2_t0_inventory_redis()
    if cli is None:
        return None
    key = _pool2_t0_inventory_redis_key()
    try:
        raw = cli.hgetall(key) or {}
    except Exception:
        return None
    if not raw:
        return None
    loaded: dict[str, dict] = {}
    for code, text in raw.items():
        if not isinstance(code, str) or not code or not isinstance(text, str) or not text:
            continue
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        loaded[code] = _normalize_pool2_inventory_entry(parsed)
    return loaded if loaded else None


def _pool2_t0_inventory_write_redis_entry(code: str, entry: dict) -> bool:
    cli = _get_pool2_t0_inventory_redis()
    if cli is None:
        return False
    key = _pool2_t0_inventory_redis_key()
    try:
        payload = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
        pipe = cli.pipeline(transaction=True)
        pipe.hset(key, code, payload)
        ttl = max(1, int(_T0_INV_REDIS_TTL_DAYS)) * 24 * 3600
        pipe.expire(key, ttl)
        pipe.execute()
        return True
    except Exception:
        return False


def _pool2_t0_inventory_write_redis_bulk(payload: dict[str, dict]) -> bool:
    cli = _get_pool2_t0_inventory_redis()
    if cli is None:
        return False
    key = _pool2_t0_inventory_redis_key()
    try:
        pipe = cli.pipeline(transaction=True)
        pipe.delete(key)
        for code, entry in payload.items():
            if not isinstance(code, str) or not code or not isinstance(entry, dict):
                continue
            text = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
            pipe.hset(key, code, text)
        ttl = max(1, int(_T0_INV_REDIS_TTL_DAYS)) * 24 * 3600
        pipe.expire(key, ttl)
        pipe.execute()
        return True
    except Exception:
        return False


def _load_pool2_t0_inventory_state() -> None:
    global _pool2_t0_inventory_state_loaded, _pool2_t0_inventory_last_source
    if _pool2_t0_inventory_state_loaded:
        return
    _pool2_t0_inventory_state_loaded = True
    if not _T0_INV_ENABLED:
        return
    redis_loaded = _pool2_t0_inventory_read_redis()
    if isinstance(redis_loaded, dict) and redis_loaded:
        _pool2_t0_inventory_state.update(redis_loaded)
        _pool2_t0_inventory_last_source = 'redis'
        logger.info(f"Pool2 T0 inventory state loaded from Redis: {len(redis_loaded)}")
        return
    if not _T0_INV_FILE_FALLBACK:
        _pool2_t0_inventory_last_source = 'memory'
        return
    try:
        if not os.path.exists(_T0_INV_FILE):
            return
        with open(_T0_INV_FILE, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return
        loaded: dict[str, dict] = {}
        for code, item in raw.items():
            if not isinstance(code, str) or not code or not isinstance(item, dict):
                continue
            loaded[code] = _normalize_pool2_inventory_entry(item)
        _pool2_t0_inventory_state.update(loaded)
        if loaded:
            _pool2_t0_inventory_last_source = 'file'
            logger.info(f"Pool2 T0 inventory state loaded from file: {len(loaded)}")
            if _T0_INV_REDIS_ENABLED:
                _pool2_t0_inventory_write_redis_bulk(loaded)
    except Exception as e:
        logger.warning(f"Pool2 T0 inventory state load failed: {e}")


def _persist_pool2_t0_inventory_state(*, changed_code: Optional[str] = None, changed_entry: Optional[dict] = None) -> None:
    global _pool2_t0_inventory_last_source
    if not _T0_INV_ENABLED:
        return
    if _T0_INV_REDIS_ENABLED:
        redis_ok = False
        if changed_code and isinstance(changed_entry, dict):
            redis_ok = _pool2_t0_inventory_write_redis_entry(changed_code, changed_entry)
        else:
            with _pool2_t0_inventory_state_lock:
                payload = {k: dict(v) for k, v in _pool2_t0_inventory_state.items() if isinstance(v, dict)}
            redis_ok = _pool2_t0_inventory_write_redis_bulk(payload)
        if redis_ok:
            _pool2_t0_inventory_last_source = 'redis'
        elif not _T0_INV_FILE_FALLBACK:
            _pool2_t0_inventory_last_source = 'memory'
    if not _T0_INV_FILE_FALLBACK:
        return
    try:
        with _pool2_t0_inventory_state_lock:
            items = list(_pool2_t0_inventory_state.items())
        if len(items) > _T0_INV_FILE_MAX_ITEMS:
            items.sort(key=lambda kv: int((kv[1] or {}).get('updated_at', 0) or 0), reverse=True)
            items = items[:_T0_INV_FILE_MAX_ITEMS]
        payload = {code: dict(entry) for code, entry in items if isinstance(entry, dict)}
        tmp = _T0_INV_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
        os.replace(tmp, _T0_INV_FILE)
        if _pool2_t0_inventory_last_source != 'redis':
            _pool2_t0_inventory_last_source = 'file'
    except Exception:
        pass


def _pool2_get_t0_inventory_state(ts_code: str, now_ts: Optional[int] = None) -> dict:
    _load_pool2_t0_inventory_state()
    code = str(ts_code or '')
    ts = int(now_ts or time.time())
    with _pool2_t0_inventory_state_lock:
        ent = _pool2_t0_inventory_state.get(code)
        if not isinstance(ent, dict):
            ent = _pool2_t0_inventory_empty(ts)
            _pool2_t0_inventory_state[code] = ent
        normalized = _normalize_pool2_inventory_entry(ent, now_ts=ts)
        if normalized != ent:
            _pool2_t0_inventory_state[code] = normalized
        snapshot = dict(normalized)
    snapshot['remaining_tradable_t_qty'] = int(snapshot.get('tradable_t_qty', 0) or 0)
    snapshot['max_t_count_per_day'] = int(_T0_INV_MAX_T_COUNT_PER_DAY)
    snapshot['lot_size'] = int(_T0_INV_LOT_SIZE)
    snapshot['state_ready'] = bool(
        int(snapshot.get('overnight_base_qty', 0) or 0) > 0
        or float(snapshot.get('cash_available_for_t', 0.0) or 0.0) > 0
    )
    return snapshot


def _pool2_apply_t0_signal(
    ts_code: str,
    *,
    signal_type: str,
    signal_price: Optional[float] = None,
    action_qty: Optional[int] = None,
    now_ts: Optional[int] = None,
) -> tuple[dict, dict]:
    _load_pool2_t0_inventory_state()
    code = str(ts_code or '')
    ts = int(now_ts or time.time())
    sig_type = str(signal_type or '').strip()
    px = _normalize_pool2_cost(signal_price)
    qty = _normalize_pool2_qty(action_qty)
    with _pool2_t0_inventory_state_lock:
        ent = _pool2_t0_inventory_state.get(code)
        if not isinstance(ent, dict):
            ent = _pool2_t0_inventory_empty(ts)
        ent = _normalize_pool2_inventory_entry(ent, now_ts=ts)
        before = dict(ent)
        if sig_type in {'positive_t', 'reverse_t'} and qty > 0:
            remaining = _normalize_pool2_qty(ent.get('tradable_t_qty'))
            applied_qty = min(remaining, qty)
            if applied_qty > 0:
                ent['tradable_t_qty'] = max(0, remaining - applied_qty)
                ent['today_t_count'] = max(0, int(ent.get('today_t_count', 0) or 0)) + 1
                if sig_type == 'positive_t':
                    ent['today_positive_t_qty'] = _normalize_pool2_qty(ent.get('today_positive_t_qty')) + applied_qty
                    ent['last_action_mode'] = 'buy_then_sell_old'
                    ent['last_action_role'] = 'lower_intraday_cost'
                    base_qty = max(1, _normalize_pool2_qty(ent.get('overnight_base_qty')))
                    anchor_cost = _normalize_pool2_cost(ent.get('inventory_anchor_cost'))
                    if anchor_cost > 0 and px > 0 and px < anchor_cost:
                        improve = (anchor_cost - px) * (applied_qty / float(base_qty))
                        ent['inventory_anchor_cost'] = round(max(0.0, anchor_cost - improve), 4)
                else:
                    ent['today_reverse_t_qty'] = _normalize_pool2_qty(ent.get('today_reverse_t_qty')) + applied_qty
                    ent['last_action_mode'] = 'sell_old_then_buy_back'
                    ent['last_action_role'] = 'lock_profit_rebuy'
                ent['last_signal_at'] = ts
                ent['last_signal_type'] = sig_type
                ent['last_signal_price'] = px
                ent['last_action_qty'] = applied_qty
                ent['updated_at'] = ts
                ent['signal_seq'] = int(ent.get('signal_seq', 0) or 0) + 1
                action_log = list(ent.get('today_action_log') or [])
                action_log.append(
                    {
                        'at': ts,
                        'at_iso': _dt.datetime.fromtimestamp(ts).isoformat(),
                        'trade_date': str(ent.get('trade_date') or _pool2_trade_date(ts)),
                        'signal_type': sig_type,
                        'action_mode': str(ent.get('last_action_mode') or ''),
                        'action_role': str(ent.get('last_action_role') or ''),
                        'price': px,
                        'qty': applied_qty,
                        'remaining_tradable_t_qty': _normalize_pool2_qty(ent.get('tradable_t_qty')),
                        'today_t_count': int(ent.get('today_t_count', 0) or 0),
                        'signal_seq': int(ent.get('signal_seq', 0) or 0),
                    }
                )
                ent['today_action_log'] = _normalize_pool2_action_log(
                    action_log,
                    str(ent.get('trade_date') or _pool2_trade_date(ts)),
                )
        _pool2_t0_inventory_state[code] = ent
        after = dict(ent)
        persisted = dict(ent)
    _persist_pool2_t0_inventory_state(changed_code=code, changed_entry=persisted)
    before['remaining_tradable_t_qty'] = int(before.get('tradable_t_qty', 0) or 0)
    before['max_t_count_per_day'] = int(_T0_INV_MAX_T_COUNT_PER_DAY)
    before['lot_size'] = int(_T0_INV_LOT_SIZE)
    after['remaining_tradable_t_qty'] = int(after.get('tradable_t_qty', 0) or 0)
    after['max_t_count_per_day'] = int(_T0_INV_MAX_T_COUNT_PER_DAY)
    after['lot_size'] = int(_T0_INV_LOT_SIZE)
    return before, after


def _pool2_update_t0_inventory_state(ts_code: str, updates: Optional[dict] = None, now_ts: Optional[int] = None) -> dict:
    _load_pool2_t0_inventory_state()
    code = str(ts_code or '').strip().upper()
    if not code:
        raise ValueError('ts_code is required')
    ts = int(now_ts or time.time())
    payload = dict(updates or {})
    reset_today = bool(payload.pop('reset_today', False))
    with _pool2_t0_inventory_state_lock:
        ent = _pool2_t0_inventory_state.get(code)
        if not isinstance(ent, dict):
            ent = _pool2_t0_inventory_empty(ts)
        ent = _normalize_pool2_inventory_entry(ent, now_ts=ts)

        if 'overnight_base_qty' in payload:
            ent['overnight_base_qty'] = _normalize_pool2_qty(payload.get('overnight_base_qty'))
        if 'reserve_qty' in payload:
            ent['reserve_qty'] = _normalize_pool2_qty(payload.get('reserve_qty'))
        if 'tradable_t_qty' in payload:
            ent['tradable_t_qty'] = _normalize_pool2_qty(payload.get('tradable_t_qty'))
        if 'cash_available_for_t' in payload:
            ent['cash_available_for_t'] = _normalize_pool2_cash(payload.get('cash_available_for_t'))
        if 'inventory_anchor_cost' in payload:
            ent['inventory_anchor_cost'] = _normalize_pool2_cost(payload.get('inventory_anchor_cost'))
        if 'today_t_count' in payload:
            ent['today_t_count'] = max(0, int(float(payload.get('today_t_count') or 0)))
        if 'today_positive_t_qty' in payload:
            ent['today_positive_t_qty'] = _normalize_pool2_qty(payload.get('today_positive_t_qty'))
        if 'today_reverse_t_qty' in payload:
            ent['today_reverse_t_qty'] = _normalize_pool2_qty(payload.get('today_reverse_t_qty'))

        ent = _normalize_pool2_inventory_entry(ent, now_ts=ts)
        if reset_today:
            base_qty = _normalize_pool2_qty(ent.get('overnight_base_qty'))
            reserve_qty = min(base_qty, _normalize_pool2_qty(ent.get('reserve_qty')))
            ent['today_t_count'] = 0
            ent['today_positive_t_qty'] = 0
            ent['today_reverse_t_qty'] = 0
            ent['tradable_t_qty'] = max(0, base_qty - reserve_qty)
            ent['today_action_log'] = []
            ent['last_reset_at'] = ts

        ent['updated_at'] = ts
        _pool2_t0_inventory_state[code] = ent
        snapshot = dict(ent)
    _persist_pool2_t0_inventory_state(changed_code=code, changed_entry=snapshot)
    snapshot['remaining_tradable_t_qty'] = int(snapshot.get('tradable_t_qty', 0) or 0)
    snapshot['max_t_count_per_day'] = int(_T0_INV_MAX_T_COUNT_PER_DAY)
    snapshot['lot_size'] = int(_T0_INV_LOT_SIZE)
    snapshot['state_ready'] = bool(
        int(snapshot.get('overnight_base_qty', 0) or 0) > 0
        or float(snapshot.get('cash_available_for_t', 0.0) or 0.0) > 0
    )
    return snapshot


def get_pool2_t0_inventory_state(ts_code: str) -> Optional[dict]:
    try:
        return _pool2_get_t0_inventory_state(ts_code)
    except Exception:
        return None


def get_pool2_t0_inventory_storage_status() -> dict:
    try:
        redis_ready = bool(_get_pool2_t0_inventory_redis() is not None) if _T0_INV_REDIS_ENABLED else False
    except Exception:
        redis_ready = False
    return {
        'enabled': bool(_T0_INV_ENABLED),
        'redis_enabled': bool(_T0_INV_REDIS_ENABLED),
        'redis_ready': bool(redis_ready),
        'file_fallback': bool(_T0_INV_FILE_FALLBACK),
        'source': str(_pool2_t0_inventory_last_source or 'memory'),
    }


def update_pool2_t0_inventory_state(ts_code: str, updates: Optional[dict] = None) -> Optional[dict]:
    try:
        return _pool2_update_t0_inventory_state(ts_code, updates=updates)
    except Exception:
        return None


__all__ = [name for name in globals() if not name.startswith("__")]
