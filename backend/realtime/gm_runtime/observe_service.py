"""Pool1 observe statistics service for gm runtime."""
from __future__ import annotations

from .common import *  # noqa: F401,F403

def _pool1_observe_trade_date() -> str:
    """当前统计交易日（本地日期口径）。"""
    return _dt.datetime.now().strftime('%Y-%m-%d')


def _pool1_observe_redis_key(trade_date: str) -> str:
    return f"{_P1_OBS_REDIS_KEY_PREFIX}:{trade_date}"


def _get_pool1_observe_redis():
    global _pool1_observe_redis_cli, _pool1_observe_redis_warn_at, _pool1_observe_redis_next_retry_at
    if not _P1_OBS_REDIS_ENABLED or _redis_mod is None:
        return None
    if _pool1_observe_redis_cli is not None:
        return _pool1_observe_redis_cli
    now = time.time()
    if now < _pool1_observe_redis_next_retry_at:
        return None
    try:
        cli = _redis_mod.Redis.from_url(_P1_OBS_REDIS_URL, decode_responses=True)
        cli.ping()
        _pool1_observe_redis_cli = cli
        logger.info(f"Pool1 observe Redis enabled: {_P1_OBS_REDIS_KEY_PREFIX}")
        return _pool1_observe_redis_cli
    except Exception as e:
        _pool1_observe_redis_next_retry_at = now + 5.0
        if now - _pool1_observe_redis_warn_at >= 30.0:
            _pool1_observe_redis_warn_at = now
            logger.warning(f"Pool1 observe Redis unavailable, fallback memory: {e}")
        return None


def _pool1_observe_read_redis(trade_date: str) -> Optional[dict]:
    cli = _get_pool1_observe_redis()
    if cli is None:
        return None
    key = _pool1_observe_redis_key(trade_date)
    try:
        raw = cli.hgetall(key) or {}
    except Exception:
        return None
    if not raw:
        return None
    reject_counts: dict[str, int] = {}
    threshold_observe: dict = {}
    detail_observe: dict = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not k.startswith('reject:'):
            continue
        rk = k.split(':', 1)[1]
        try:
            rv = int(v or 0)
        except Exception:
            rv = 0
        if rk:
            reject_counts[rk] = rv
    try:
        th_raw = raw.get('threshold_observe_json')
        if isinstance(th_raw, str) and th_raw.strip():
            parsed = json.loads(th_raw)
            if isinstance(parsed, dict):
                threshold_observe = parsed
    except Exception:
        threshold_observe = {}
    try:
        detail_raw = raw.get('detail_observe_json')
        if isinstance(detail_raw, str) and detail_raw.strip():
            parsed = json.loads(detail_raw)
            if isinstance(parsed, dict):
                detail_observe = parsed
    except Exception:
        detail_observe = {}
    return {
        'trade_date': raw.get('trade_date') or trade_date,
        'updated_at': int(raw.get('updated_at') or 0),
        'screen_total': int(raw.get('screen_total') or 0),
        'screen_pass': int(raw.get('screen_pass') or 0),
        'stage2_triggered': int(raw.get('stage2_triggered') or 0),
        'reject_counts': reject_counts,
        'reject_by_signal_type': detail_observe.get('reject_by_signal_type') if isinstance(detail_observe, dict) else {},
        'reject_by_industry': detail_observe.get('reject_by_industry') if isinstance(detail_observe, dict) else {},
        'reject_by_regime': detail_observe.get('reject_by_regime') if isinstance(detail_observe, dict) else {},
        'reject_by_board_segment': detail_observe.get('reject_by_board_segment') if isinstance(detail_observe, dict) else {},
        'reject_by_security_type': detail_observe.get('reject_by_security_type') if isinstance(detail_observe, dict) else {},
        'reject_by_listing_stage': detail_observe.get('reject_by_listing_stage') if isinstance(detail_observe, dict) else {},
        'threshold_observe': threshold_observe if isinstance(threshold_observe, dict) else {},
    }


def _normalize_reject_reason(reason: str) -> str:
    txt = str(reason or '').strip().lower().replace(' ', '_').replace('-', '_')
    safe = ''.join(ch for ch in txt if (ch.isalnum() or ch == '_'))
    return safe[:48] if safe else ''


def _normalize_bucket(value: str, default: str = 'unknown', max_len: int = 48) -> str:
    txt = str(value or '').strip()
    if not txt:
        return default
    txt = txt.replace(':', '_').replace('|', '/')
    return txt[:max_len]


def _count_inc(counter: dict, key: str, n: int = 1) -> None:
    counter[key] = int(counter.get(key, 0) or 0) + int(n)


def _nested_count_inc(counter: dict, k1: str, k2: str, n: int = 1) -> None:
    sub = counter.setdefault(k1, {})
    if not isinstance(sub, dict):
        sub = {}
        counter[k1] = sub
    sub[k2] = int(sub.get(k2, 0) or 0) + int(n)


def _sorted_count_map(counter: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    if not isinstance(counter, dict):
        return out
    items = []
    for k, v in counter.items():
        try:
            iv = int(v or 0)
        except Exception:
            iv = 0
        if iv > 0:
            items.append((str(k), iv))
    items.sort(key=lambda kv: int(kv[1]), reverse=True)
    for k, v in items:
        out[k] = int(v)
    return out


def _sorted_nested_count_map(counter: dict) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    if not isinstance(counter, dict):
        return out
    for k, sub in counter.items():
        sub_out = _sorted_count_map(sub if isinstance(sub, dict) else {})
        if sub_out:
            out[str(k)] = sub_out
    return out


def _pool1_threshold_bucket(th: Optional[dict]) -> str:
    if not isinstance(th, dict):
        return 'unknown|unknown|unknown'
    phase = _normalize_bucket(str(th.get('market_phase') or 'unknown'), default='unknown', max_len=24)
    regime = _normalize_bucket(str(th.get('regime') or 'unknown'), default='unknown', max_len=24)
    industry = _normalize_bucket(str(th.get('industry') or 'unknown'), default='unknown', max_len=32)
    return f'{phase}|{regime}|{industry}'


def _pool1_threshold_source(th: Optional[dict]) -> str:
    if not isinstance(th, dict):
        return 'unknown'
    cali = th.get('calibration')
    if isinstance(cali, dict) and bool(cali.get('applied')):
        return 'runtime_calibration'
    return 'dynamic_profile'


def _pool1_threshold_snapshot(th: Optional[dict]) -> dict:
    if not isinstance(th, dict):
        return {}
    out: dict[str, object] = {}
    for k in (
        'near_lower_th',
        'rsi_oversold',
        'eps_mid',
        'eps_upper',
        'breakout_max_offset_pct',
    ):
        if k in th:
            v = th.get(k)
            if isinstance(v, float):
                out[k] = round(v, 6)
            else:
                out[k] = v
    lf = th.get('limit_filter')
    if isinstance(lf, dict):
        out['limit_filter'] = {
            'distance_pct': lf.get('distance_pct'),
            'threshold_pct': lf.get('threshold_pct'),
            'action': lf.get('action'),
            'blocked': bool(lf.get('blocked')),
            'observer_only': bool(lf.get('observer_only')),
        }
    return out


def _pool1_threshold_blocked_reason(
    th: Optional[dict],
    signal_reject_reasons: Optional[list[str]] = None,
) -> str:
    reasons = [
        _normalize_reject_reason(x)
        for x in (signal_reject_reasons or [])
        if _normalize_reject_reason(x)
    ]
    for r in reasons:
        if r in {'limit_magnet_block', 'threshold_blocked'}:
            return r
    if not isinstance(th, dict):
        return ''
    if not bool(th.get('blocked')):
        return ''
    explicit_reason = str(th.get('blocked_reason') or '').strip()
    if explicit_reason:
        return explicit_reason
    lf = th.get('limit_filter')
    if isinstance(lf, dict):
        act = str(lf.get('action') or '')
        if act == 'blocked':
            if bool(lf.get('limit_magnet')):
                return 'limit_magnet_block'
            return 'threshold_blocked'
    return 'threshold_blocked'


def _record_pool1_threshold_observe(
    threshold_info: Optional[dict[str, Optional[dict]]] = None,
    signal_rejects: Optional[dict[str, list[str]]] = None,
) -> None:
    by_sig = _pool1_observe.setdefault('threshold_observe', _pool1_threshold_observe_default())
    if not isinstance(by_sig, dict):
        by_sig = _pool1_threshold_observe_default()
        _pool1_observe['threshold_observe'] = by_sig
    now_iso = _dt.datetime.now().isoformat()
    for sig_type in _POOL1_SIGNAL_TYPES:
        th = None
        if isinstance(threshold_info, dict):
            th = threshold_info.get(sig_type)
        if not isinstance(th, dict):
            continue
        bucket = _pool1_threshold_bucket(th)
        version = str(th.get('threshold_version') or 'unknown')
        source = _pool1_threshold_source(th)
        blocked = bool(th.get('blocked'))
        observer_only = bool(th.get('observer_only'))
        blocked_reason = _pool1_threshold_blocked_reason(
            th,
            signal_reject_reasons=(signal_rejects or {}).get(sig_type) if isinstance(signal_rejects, dict) else None,
        )
        entry = by_sig.setdefault(
            sig_type,
            {
                'total': 0,
                'blocked': 0,
                'observer_only': 0,
                'by_bucket': {},
                'blocked_by_bucket': {},
                'observer_by_bucket': {},
                'by_version': {},
                'by_source': {},
                'blocked_reasons': {},
                'current': {},
            },
        )
        entry['total'] = int(entry.get('total', 0) or 0) + 1
        if blocked:
            entry['blocked'] = int(entry.get('blocked', 0) or 0) + 1
        if observer_only:
            entry['observer_only'] = int(entry.get('observer_only', 0) or 0) + 1
        _count_inc(entry.setdefault('by_bucket', {}), bucket, 1)
        _count_inc(entry.setdefault('by_version', {}), version, 1)
        _count_inc(entry.setdefault('by_source', {}), source, 1)
        if blocked:
            _count_inc(entry.setdefault('blocked_by_bucket', {}), bucket, 1)
        if observer_only:
            _count_inc(entry.setdefault('observer_by_bucket', {}), bucket, 1)
        if blocked_reason:
            _count_inc(entry.setdefault('blocked_reasons', {}), blocked_reason, 1)
        entry['current'] = {
            'signal_type': sig_type,
            'bucket': bucket,
            'threshold_version': version,
            'threshold_source': source,
            'blocked': blocked,
            'observer_only': observer_only,
            'blocked_reason': blocked_reason,
            'snapshot': _pool1_threshold_snapshot(th),
            'updated_at': int(time.time()),
            'updated_at_iso': now_iso,
        }


def _pool1_observe_window_snapshot(now_ts: Optional[int] = None) -> dict:
    ts_now = int(now_ts if isinstance(now_ts, int) else time.time())
    events = list(_pool1_observe_recent_events)
    windows_out: dict[str, dict] = {}
    for win_key, sec in _POOL1_OBS_WINDOWS_SEC.items():
        lower = ts_now - int(sec)
        win_events = [e for e in events if int(e.get('ts', 0) or 0) >= lower]
        reject_counts: dict[str, int] = {}
        by_signal_type: dict[str, dict[str, int]] = {k: {} for k in _POOL1_SIGNAL_TYPES}
        by_industry: dict[str, dict[str, int]] = {}
        by_regime: dict[str, dict[str, int]] = {}
        by_board_segment: dict[str, dict[str, int]] = {}
        by_security_type: dict[str, dict[str, int]] = {}
        by_listing_stage: dict[str, dict[str, int]] = {}
        for ev in win_events:
            reason = str(ev.get('reason') or '')
            if not reason:
                continue
            sig_type = str(ev.get('signal_type') or '')
            industry = _normalize_bucket(ev.get('industry') or '')
            regime = _normalize_bucket(ev.get('regime') or '')
            board_segment = _normalize_bucket(ev.get('board_segment') or '')
            security_type = _normalize_bucket(ev.get('security_type') or '')
            listing_stage = _normalize_bucket(ev.get('listing_stage') or '')
            if sig_type == 'all':
                _count_inc(reject_counts, reason, 1)
                _nested_count_inc(by_industry, industry, reason, 1)
                _nested_count_inc(by_regime, regime, reason, 1)
                _nested_count_inc(by_board_segment, board_segment, reason, 1)
                _nested_count_inc(by_security_type, security_type, reason, 1)
                _nested_count_inc(by_listing_stage, listing_stage, reason, 1)
            elif sig_type in _POOL1_SIGNAL_TYPES:
                _nested_count_inc(by_signal_type, sig_type, reason, 1)
        reject_counts_out = _sorted_count_map(reject_counts)
        top_rejects = sorted(reject_counts_out.items(), key=lambda kv: int(kv[1]), reverse=True)
        windows_out[win_key] = {
            'seconds': int(sec),
            'event_count': len(win_events),
            'reject_counts': reject_counts_out,
            'top_rejects': [{'reason': k, 'count': int(v)} for k, v in top_rejects if int(v) > 0][:6],
            'reject_by_signal_type': _sorted_nested_count_map(by_signal_type),
            'reject_by_industry': _sorted_nested_count_map(by_industry),
            'reject_by_regime': _sorted_nested_count_map(by_regime),
            'reject_by_board_segment': _sorted_nested_count_map(by_board_segment),
            'reject_by_security_type': _sorted_nested_count_map(by_security_type),
            'reject_by_listing_stage': _sorted_nested_count_map(by_listing_stage),
        }
    return windows_out


def _pool1_observe_write_redis(
    trade_date: str,
    stage1_pass: bool,
    stage2_triggered: bool,
    ts_now: int,
    reject_reasons: Optional[list[str]] = None,
    threshold_observe: Optional[dict] = None,
    detail_observe: Optional[dict] = None,
) -> bool:
    cli = _get_pool1_observe_redis()
    if cli is None:
        return False
    key = _pool1_observe_redis_key(trade_date)
    reasons = sorted({_normalize_reject_reason(r) for r in (reject_reasons or []) if _normalize_reject_reason(r)})
    try:
        pipe = cli.pipeline(transaction=True)
        pipe.hincrby(key, 'screen_total', 1)
        if stage1_pass:
            pipe.hincrby(key, 'screen_pass', 1)
            if stage2_triggered:
                pipe.hincrby(key, 'stage2_triggered', 1)
        for rr in reasons:
            pipe.hincrby(key, f'reject:{rr}', 1)
        if isinstance(detail_observe, dict) and detail_observe:
            try:
                pipe.hset(
                    key,
                    'detail_observe_json',
                    json.dumps(detail_observe, ensure_ascii=False, separators=(',', ':')),
                )
            except Exception:
                pass
        if isinstance(threshold_observe, dict) and threshold_observe:
            try:
                pipe.hset(
                    key,
                    'threshold_observe_json',
                    json.dumps(threshold_observe, ensure_ascii=False, separators=(',', ':')),
                )
            except Exception:
                pass
        pipe.hset(key, mapping={'trade_date': trade_date, 'updated_at': int(ts_now)})
        ttl = max(1, int(_P1_OBS_REDIS_TTL_DAYS)) * 24 * 3600
        pipe.expire(key, ttl)
        pipe.execute()
        return True
    except Exception:
        return False


def _ensure_pool1_observe_day() -> None:
    """若跨日则重置统计，保证按交易日聚合。"""
    global _pool1_observe_last_source
    today = _pool1_observe_trade_date()
    if _pool1_observe.get('trade_date') == today:
        return
    _pool1_observe['trade_date'] = today
    _pool1_observe['updated_at'] = int(time.time())
    _pool1_observe['screen_total'] = 0
    _pool1_observe['screen_pass'] = 0
    _pool1_observe['stage2_triggered'] = 0
    _pool1_observe['reject_counts'] = {}
    _pool1_observe['reject_by_signal_type'] = {k: {} for k in _POOL1_SIGNAL_TYPES}
    _pool1_observe['reject_by_industry'] = {}
    _pool1_observe['reject_by_regime'] = {}
    _pool1_observe['reject_by_board_segment'] = {}
    _pool1_observe['reject_by_security_type'] = {}
    _pool1_observe['reject_by_listing_stage'] = {}
    _pool1_observe['threshold_observe'] = _pool1_threshold_observe_default()
    _pool1_observe_recent_events.clear()
    snap = _pool1_observe_read_redis(today)
    if snap:
        _pool1_observe.update(snap)
        _pool1_observe_last_source = 'redis'
    else:
        _pool1_observe_last_source = 'memory'


def _record_pool1_observe(
    stage1_pass: bool,
    stage2_triggered: bool,
    reject_reasons: Optional[list[str]] = None,
    signal_rejects: Optional[dict[str, list[str]]] = None,
    threshold_info: Optional[dict[str, Optional[dict]]] = None,
    industry: Optional[str] = None,
    regime: Optional[str] = None,
    board_segment: Optional[str] = None,
    security_type: Optional[str] = None,
    listing_stage: Optional[str] = None,
) -> None:
    global _pool1_observe_last_source, _pool1_observe_redis_last_flush_at, _pool1_observe_redis_last_threshold_flush_at
    if not _is_cn_trading_time_now():
        return
    _ensure_pool1_observe_day()
    _pool1_observe['screen_total'] += 1
    if stage1_pass:
        _pool1_observe['screen_pass'] += 1
        if stage2_triggered:
            _pool1_observe['stage2_triggered'] += 1
    rej_map = _pool1_observe.setdefault('reject_counts', {})
    reasons = sorted({_normalize_reject_reason(r) for r in (reject_reasons or []) if _normalize_reject_reason(r)})
    for rr in reasons:
        rej_map[rr] = int(rej_map.get(rr, 0) or 0) + 1
    # 按信号类型拆分
    by_sig = _pool1_observe.setdefault('reject_by_signal_type', {k: {} for k in _POOL1_SIGNAL_TYPES})
    sig_rejects_norm: dict[str, list[str]] = {}
    for sig_type in _POOL1_SIGNAL_TYPES:
        sub = []
        if isinstance(signal_rejects, dict):
            raw_sub = signal_rejects.get(sig_type)
            if isinstance(raw_sub, list):
                sub = [_normalize_reject_reason(x) for x in raw_sub if _normalize_reject_reason(x)]
        sub = sorted(set(sub))
        sig_rejects_norm[sig_type] = sub
        if sig_type not in by_sig or not isinstance(by_sig.get(sig_type), dict):
            by_sig[sig_type] = {}
        for rr in sub:
            _count_inc(by_sig[sig_type], rr, 1)
    # 按行业 / regime 拆分（按总 reject_reasons 口径）
    ind_bucket = _normalize_bucket(industry or '')
    reg_bucket = _normalize_bucket(regime or '')
    board_bucket = _normalize_bucket(board_segment or '')
    sec_bucket = _normalize_bucket(security_type or '')
    stage_bucket = _normalize_bucket(listing_stage or '')
    by_ind = _pool1_observe.setdefault('reject_by_industry', {})
    by_reg = _pool1_observe.setdefault('reject_by_regime', {})
    by_board = _pool1_observe.setdefault('reject_by_board_segment', {})
    by_sec = _pool1_observe.setdefault('reject_by_security_type', {})
    by_stage = _pool1_observe.setdefault('reject_by_listing_stage', {})
    for rr in reasons:
        _nested_count_inc(by_ind, ind_bucket, rr, 1)
        _nested_count_inc(by_reg, reg_bucket, rr, 1)
        _nested_count_inc(by_board, board_bucket, rr, 1)
        _nested_count_inc(by_sec, sec_bucket, rr, 1)
        _nested_count_inc(by_stage, stage_bucket, rr, 1)
    _record_pool1_threshold_observe(threshold_info=threshold_info, signal_rejects=sig_rejects_norm)
    ts_now = int(time.time())
    _pool1_observe['updated_at'] = ts_now
    # 记录近窗事件：all 口径 + 按信号类型口径
    for rr in reasons:
        _pool1_observe_recent_events.append({
            'ts': ts_now,
            'signal_type': 'all',
            'reason': rr,
            'industry': ind_bucket,
            'regime': reg_bucket,
            'board_segment': board_bucket,
            'security_type': sec_bucket,
            'listing_stage': stage_bucket,
        })
    for sig_type in _POOL1_SIGNAL_TYPES:
        for rr in sig_rejects_norm.get(sig_type, []):
            _pool1_observe_recent_events.append({
                'ts': ts_now,
                'signal_type': sig_type,
                'reason': rr,
                'industry': ind_bucket,
                'regime': reg_bucket,
                'board_segment': board_bucket,
                'security_type': sec_bucket,
                'listing_stage': stage_bucket,
            })
    # Redis 落库节流：内存实时累计，按时间窗口刷盘，避免每 tick IO 阻塞。
    now_f = float(time.time())
    need_flush = (now_f - float(_pool1_observe_redis_last_flush_at)) >= max(0.2, float(_P1_OBS_REDIS_FLUSH_SEC))
    # 有 stage2 命中时优先刷盘，保证实盘观察延迟可控。
    if stage2_triggered:
        need_flush = True
    if need_flush:
        include_threshold = (
            now_f - float(_pool1_observe_redis_last_threshold_flush_at)
        ) >= max(0.5, float(_P1_OBS_REDIS_THRESHOLD_FLUSH_SEC))
        threshold_payload = _pool1_observe.get('threshold_observe') if include_threshold else None
        detail_payload = {
            'reject_by_signal_type': _pool1_observe.get('reject_by_signal_type') or {},
            'reject_by_industry': _pool1_observe.get('reject_by_industry') or {},
            'reject_by_regime': _pool1_observe.get('reject_by_regime') or {},
            'reject_by_board_segment': _pool1_observe.get('reject_by_board_segment') or {},
            'reject_by_security_type': _pool1_observe.get('reject_by_security_type') or {},
            'reject_by_listing_stage': _pool1_observe.get('reject_by_listing_stage') or {},
        }
        if _pool1_observe_write_redis(
            _pool1_observe.get('trade_date') or _pool1_observe_trade_date(),
            stage1_pass,
            stage2_triggered,
            ts_now,
            reject_reasons=reasons,
            threshold_observe=threshold_payload,
            detail_observe=detail_payload,
        ):
            _pool1_observe_last_source = 'redis'
            _pool1_observe_redis_last_flush_at = now_f
            if include_threshold:
                _pool1_observe_redis_last_threshold_flush_at = now_f
        else:
            _pool1_observe_last_source = 'memory'


def _pool1_observe_summary() -> str:
    _ensure_pool1_observe_day()
    total = int(_pool1_observe.get('screen_total', 0) or 0)
    passed = int(_pool1_observe.get('screen_pass', 0) or 0)
    triggered = int(_pool1_observe.get('stage2_triggered', 0) or 0)
    if total <= 0:
        return "pool1无样本"
    pass_rate = (passed / total) * 100
    trigger_rate = (triggered / passed) * 100 if passed > 0 else 0.0
    reject_counts = _pool1_observe.get('reject_counts') or {}
    top_reason = ''
    if isinstance(reject_counts, dict) and reject_counts:
        try:
            rk, rv = sorted(reject_counts.items(), key=lambda kv: int(kv[1]), reverse=True)[0]
            if int(rv) > 0:
                top_reason = f" 主要拦截:{rk}({int(rv)})"
        except Exception:
            top_reason = ''
    return (
        f"pool1通过率 {pass_rate:.1f}%({passed}/{total}) "
        f"二阶段触发率 {trigger_rate:.1f}%({triggered}/{passed}){top_reason}"
    )


def get_pool1_observe_stats() -> dict:
    """返回 Pool1 两阶段统计快照（轻量接口，无 IO）。"""
    global _pool1_observe_last_source
    _ensure_pool1_observe_day()
    snap = _pool1_observe_read_redis(_pool1_observe.get('trade_date') or _pool1_observe_trade_date())
    if snap and int(snap.get('updated_at', 0) or 0) >= int(_pool1_observe.get('updated_at', 0) or 0):
        _pool1_observe.update(snap)
        _pool1_observe_last_source = 'redis'
    total = int(_pool1_observe.get('screen_total', 0) or 0)
    passed = int(_pool1_observe.get('screen_pass', 0) or 0)
    triggered = int(_pool1_observe.get('stage2_triggered', 0) or 0)
    reject_counts = _pool1_observe.get('reject_counts') or {}
    reject_by_signal_type = _pool1_observe.get('reject_by_signal_type') or {}
    reject_by_industry = _pool1_observe.get('reject_by_industry') or {}
    reject_by_regime = _pool1_observe.get('reject_by_regime') or {}
    reject_by_board_segment = _pool1_observe.get('reject_by_board_segment') or {}
    reject_by_security_type = _pool1_observe.get('reject_by_security_type') or {}
    reject_by_listing_stage = _pool1_observe.get('reject_by_listing_stage') or {}
    updated_at = int(_pool1_observe.get('updated_at', 0) or 0)
    pass_rate = (passed / total) if total > 0 else 0.0
    trigger_rate = (triggered / passed) if passed > 0 else 0.0
    reject_counts_out = _sorted_count_map(reject_counts)
    for k in _POOL1_REJECT_DEFAULTS:
        reject_counts_out.setdefault(k, 0)
    reject_by_signal_type_out = _sorted_nested_count_map(reject_by_signal_type)
    for sig_type in _POOL1_SIGNAL_TYPES:
        reject_by_signal_type_out.setdefault(sig_type, {})
    reject_by_industry_out = _sorted_nested_count_map(reject_by_industry)
    reject_by_regime_out = _sorted_nested_count_map(reject_by_regime)
    reject_by_board_segment_out = _sorted_nested_count_map(reject_by_board_segment)
    reject_by_security_type_out = _sorted_nested_count_map(reject_by_security_type)
    reject_by_listing_stage_out = _sorted_nested_count_map(reject_by_listing_stage)
    threshold_raw = _pool1_observe.get('threshold_observe') or {}
    threshold_out: dict[str, dict] = {}
    for sig_type in _POOL1_SIGNAL_TYPES:
        e = threshold_raw.get(sig_type) if isinstance(threshold_raw, dict) else {}
        total_th = int((e or {}).get('total', 0) or 0)
        blocked_th = int((e or {}).get('blocked', 0) or 0)
        observer_th = int((e or {}).get('observer_only', 0) or 0)
        by_bucket = _sorted_count_map((e or {}).get('by_bucket') if isinstance(e, dict) else {})
        blocked_by_bucket = _sorted_count_map((e or {}).get('blocked_by_bucket') if isinstance(e, dict) else {})
        observer_by_bucket = _sorted_count_map((e or {}).get('observer_by_bucket') if isinstance(e, dict) else {})
        by_version = _sorted_count_map((e or {}).get('by_version') if isinstance(e, dict) else {})
        by_source = _sorted_count_map((e or {}).get('by_source') if isinstance(e, dict) else {})
        blocked_reasons = _sorted_count_map((e or {}).get('blocked_reasons') if isinstance(e, dict) else {})
        current = (e or {}).get('current') if isinstance(e, dict) else {}
        top_bucket = next(iter(by_bucket.keys()), '')
        threshold_out[sig_type] = {
            'total': total_th,
            'blocked': blocked_th,
            'observer_only': observer_th,
            'blocked_ratio': round((blocked_th / total_th), 4) if total_th > 0 else 0.0,
            'observer_ratio': round((observer_th / total_th), 4) if total_th > 0 else 0.0,
            'by_bucket': by_bucket,
            'blocked_by_bucket': blocked_by_bucket,
            'observer_by_bucket': observer_by_bucket,
            'by_version': by_version,
            'by_source': by_source,
            'blocked_reasons': blocked_reasons,
            'top_bucket': top_bucket,
            'current': current if isinstance(current, dict) else {},
        }
    reject_windows = _pool1_observe_window_snapshot(now_ts=updated_at if updated_at > 0 else None)
    top_rejects = sorted(reject_counts_out.items(), key=lambda kv: int(kv[1]), reverse=True)
    redis_enabled = bool(_P1_OBS_REDIS_ENABLED)
    redis_ready = bool(_get_pool1_observe_redis() is not None) if redis_enabled else False
    expected_source = 'redis' if redis_enabled else 'memory'
    actual_source = str(_pool1_observe_last_source or 'memory')
    if not redis_enabled:
        storage_verified = (actual_source == 'memory')
        storage_degraded = False
        storage_note = 'redis_disabled'
    elif not redis_ready:
        storage_verified = (actual_source == 'memory')
        storage_degraded = True
        storage_note = 'redis_unavailable_fallback'
    elif total <= 0:
        storage_verified = True
        storage_degraded = False
        storage_note = 'redis_ready_no_samples'
    else:
        storage_verified = (actual_source == 'redis')
        storage_degraded = not storage_verified
        storage_note = 'ok' if storage_verified else 'redis_ready_but_memory_source'
    return {
        'trade_date': _pool1_observe.get('trade_date') or _pool1_observe_trade_date(),
        'updated_at': updated_at,
        'updated_at_iso': _dt.datetime.fromtimestamp(updated_at).isoformat() if updated_at > 0 else None,
        'screen_total': total,
        'screen_pass': passed,
        'stage2_triggered': triggered,
        'pass_rate': round(pass_rate, 4),
        'trigger_rate': round(trigger_rate, 4),
        'reject_counts': reject_counts_out,
        'reject_by_signal_type': reject_by_signal_type_out,
        'reject_by_industry': reject_by_industry_out,
        'reject_by_regime': reject_by_regime_out,
        'reject_by_board_segment': reject_by_board_segment_out,
        'reject_by_security_type': reject_by_security_type_out,
        'reject_by_listing_stage': reject_by_listing_stage_out,
        'threshold_observe': threshold_out,
        'reject_windows': reject_windows,
        'top_rejects': [{'reason': k, 'count': int(v)} for k, v in top_rejects if int(v) > 0][:6],
        'summary': _pool1_observe_summary(),
        'storage_source': actual_source,
        'storage_expected': expected_source,
        'storage_verified': bool(storage_verified),
        'storage_degraded': bool(storage_degraded),
        'storage_note': storage_note,
        'redis_enabled': redis_enabled,
        'redis_ready': redis_ready,
    }


__all__ = [name for name in globals() if not name.startswith("__")]
