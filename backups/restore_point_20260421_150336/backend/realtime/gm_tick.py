"""
掘金量化(gm) Tick Provider(subscribe 订阅模式 - 多进程版(
工作原理(  1. 项目根目录的静)策略脚本文件?_gm_strategy.py 作为 gm.run() 的入口?  2. 在独立进程中运 gm.run(filename='_gm_strategy.py')(启动掘金行情服务?  3. gm 框架执行策略脚本中的 init() ->?subscribe() 订阅 tick
  4. gm 框架持续调用策略脚本中的 on_tick() ->?将 tick 数据写入文件缓存
  5. GmTickProvider.get_tick() 从文件缓存取最新数据?
娉ㄦ剰(  - gm.run() 必须在主线程运行(使?signal 妯″潡(  - 使用 multiprocessing.Process 在独立进程运行,每个进程有自己的主线程?  - 使用文件通信(pickle/txt)实现价进程数据共享
  - gm 使用 'SHSE.600519' 格式的 symbol(需要转换?
掘金 subscribe API 文档:
  https://www.myquant.cn/docs2/sdk/python/API介绍/数据订阅.html#subscribe-行情订阅
"""
from __future__ import annotations

import datetime as _dt
import multiprocessing
import secrets
import statistics
import threading
import time
import random
import os
from collections import deque
from multiprocessing.managers import BaseManager
from queue import Queue as _StdQueue, Empty as _QueueEmpty
from typing import Optional
from loguru import logger

from backend.realtime.tick_provider import TickProvider
from backend.realtime.mootdx_client import client as mootdx_client

try:
    from config import POOL1_SIGNAL_CONFIG as _P1_CFG
except Exception:
    _P1_CFG = {}

try:
    from backend.realtime.threshold_engine import (
        build_signal_context as _build_signal_context,
        get_thresholds as _get_thresholds,
    )
except Exception:
    _build_signal_context = None
    _get_thresholds = None

# 尝试导入 gm
try:
    from gm.api import *  # type: ignore
    _GM_AVAILABLE = True
except ImportError:
    _GM_AVAILABLE = False
    logger.warning("gm 未安装,掘金 Tick Provider 将仅返回 mock 数据，请安装 pip install gm")


# ============================================================
# 工具函数
# ============================================================
def _ts_code_to_gm_symbol(ts_code: str) -> str:
    """将'600519.SH' 转换为掘金格式'SHSE.600519'"""
    code, suffix = ts_code.split('.')
    if suffix.upper() == 'SH':
        return f'SHSE.{code}'
    elif suffix.upper() == 'SZ':
        return f'SZSE.{code}'
    elif suffix.upper() == 'BJ':
        return f'BJSE.{code}'
    return f'{suffix.upper()}.{code}'


def _gm_symbol_to_ts_code(symbol: str) -> str:
    """将掘金格式'SHSE.600519' 转换为'600519.SH'"""
    exchange, code = symbol.split('.')
    if exchange == 'SHSE':
        return f'{code}.SH'
    elif exchange == 'SZSE':
        return f'{code}.SZ'
    elif exchange == 'BJSE':
        return f'{code}.BJ'
    return f'{code}.{exchange}'


def _mock_tick(ts_code: str) -> dict:
    """生成 mock tick 数据"""
    base = 10 + random.uniform(-2, 2)
    return {
        'ts_code': ts_code, 'name': 'MOCK', 'price': round(base, 2),
        'open': round(base * 0.99, 2), 'high': round(base * 1.02, 2),
        'low': round(base * 0.97, 2), 'pre_close': round(base * 0.98, 2),
        'volume': random.randint(10000, 1000000),
        'amount': random.uniform(1e6, 1e8),
        'pct_chg': round(random.uniform(-3, 3), 2),
        'timestamp': int(time.time()),
        'bids': [(round(base - i * 0.01, 2), random.randint(100, 5000)) for i in range(1, 6)],
        'asks': [(round(base + i * 0.01, 2), random.randint(100, 5000)) for i in range(1, 6)],
        'is_mock': True,
    }


# ============================================================
# 信″类衰减鐘舵)佹満工具函数
# ============================================================
def _get_market_phase(ts_epoch: int) -> str:
    """根据 epoch 根据epoch判断当前交易时段：open/morning/afternoon/close"""
    dt = _dt.datetime.fromtimestamp(ts_epoch)
    hm = dt.hour * 60 + dt.minute
    if 570 <= hm < 600:    # 09:30鈥?0:00
        return 'open'
    elif 600 <= hm < 690:  # 10:00鈥?1:30
        return 'morning'
    elif 780 <= hm < 870:  # 13:00鈥?4:30
        return 'afternoon'
    elif 870 <= hm < 900:  # 14:30鈥?5:00
        return 'close'
    return 'morning'       # 默


def _trading_minutes_elapsed(start_ts: int, end_ts: int) -> float:
    """据$撴嬭や信间戳棿癸逛箣椂寸殑浜ゆ槗鍒嗛挓上帮术鑷姩岀宠繃級堜紤 11:30鈥?3:00("""
    if end_ts <= start_ts:
        return 0.0
    elapsed = (end_ts - start_ts) / 60.0
    start_dt = _dt.datetime.fromtimestamp(start_ts)
    end_dt = _dt.datetime.fromtimestamp(end_ts)
    start_hm = start_dt.hour * 60 + start_dt.minute
    end_hm = end_dt.hour * 60 + end_dt.minute
    # 价午休(11:30鈥?3:00 = 90 分钟非交易时间)
    noon_start, noon_end = 690, 780
    overlap = 0.0
    if start_hm < noon_end and end_hm > noon_start:
        overlap = min(end_hm, noon_end) - max(start_hm, noon_start)
        overlap = max(0.0, overlap)
    return max(0.0, elapsed - overlap)


def _phase_decay_lambda(phase: str, signal_type: str) -> float:
    """返回侀囧畾间舵鍜屼俊墽风被鍨嬪搴旂殑衰减鐜?lambda(忚瘡浜ゆ槗鍒嗛挓("""
    try:
        from config import T0_SIGNAL_CONFIG
        lam = T0_SIGNAL_CONFIG['decay'].get(phase, 0.10)
    except Exception:
        lam = 0.10
    # reverse_t 閸︺劌绱戦惄?灏剧面间舵衰减鏇村揩(噪音更大）
    if signal_type == 'reverse_t' and phase in ('open', 'close'):
        lam *= 1.3
    return lam


def _pool1_half_life_lambda(signal_type: str) -> Optional[float]:
    """Pool1 独立半"期转 lambda(忚寜浜ゆ槗鍒嗛挓(夈)?"""
    half_life = _P1_HALF_LIFE_CFG.get(signal_type)
    if half_life is None:
        return None
    try:
        hl = float(half_life)
        if hl <= 0:
            return None
        import math
        return math.log(2) / hl
    except Exception:
        return None


def _decay_factor(signal_type: str, signal_ts: int, now_ts: int) -> float:
    """
    计算 [0,1] 閻ㄥ嫯"返忕郴上帮价忚囩敤鍒嗘侀囨算衰减。    factor = exp(-lambda * trading_minutes_elapsed)
    """
    import math
    trading_min = _trading_minutes_elapsed(signal_ts, now_ts)
    p1_lambda = _pool1_half_life_lambda(signal_type)
    if p1_lambda is not None:
        return math.exp(-p1_lambda * trading_min)
    phase = _get_market_phase(now_ts)
    lam = _phase_decay_lambda(phase, signal_type)
    return math.exp(-lam * trading_min)


def _dedup_seconds_for_signal(signal_type: str, pools: set) -> int:
    """按信号类型返回去重窗口;Pool1 信″类浼樺厛分荤嫭绔嬮厤缃)?"""
    if signal_type in ('left_side_buy', 'right_side_breakout'):
        return int(_P1_DEDUP_CFG.get(signal_type, 300) or 300)
    dedup_seconds = 300
    for pool_id in pools:
        dedup_seconds = min(dedup_seconds, SIGNAL_DEDUP_SECONDS_POOL.get(pool_id, 300))
    return dedup_seconds


def _pool1_resonance_60m_proxy(daily: dict, price: float, prev_price: Optional[float]) -> bool:
    """P2 计勭暀(0m 閸兘鲸灏熸禒g悊(忚棤鍒嗛挓绾?0m鍥級瓙间控殑大婚噺斿归技(夈)?"""
    cfg = _P1_CFG.get('resonance_60m', {}) if isinstance(_P1_CFG, dict) else {}
    if not cfg.get('enabled', False):
        return True
    ma5 = float(daily.get('ma5', 0) or 0)
    ma10 = float(daily.get('ma10', 0) or 0)
    boll_mid = float(daily.get('boll_mid', 0) or 0)
    if ma5 <= ma10 or boll_mid <= 0 or prev_price is None or prev_price <= 0:
        return False
    return prev_price <= boll_mid and price > boll_mid


def _pool1_resonance_60m(ts_code: str, daily: dict, price: float, prev_price: Optional[float], now_ts: Optional[float] = None) -> tuple[bool, dict]:
    """
    Pool1 60m 共振(鍫濇彥鐠虹窞(嶈涚窗
      1) 浼樺厛璧颁綆计戠紦瀛橈术合嶄綆偣橀閾捐矾 IO(      2) 缓存过期后再?1 鍒嗛挓绾胯仛日級椼楃湡屾?60m 共振
      3) 据$撴嶈辫触下)回代理)級忕帆(保证链价?    """
    if not _P1_RESONANCE_ENABLED:
        return True, {'enabled': False, 'source': 'disabled'}

    now_ts = float(now_ts if now_ts is not None else time.time())
    refresh_sec = max(30, int(_P1_RESONANCE_REFRESH_SEC))
    cache = _pool1_resonance_cache.get(ts_code)
    if cache:
        age = now_ts - float(cache.get('updated_at', 0) or 0)
        if age < refresh_sec:
            info = dict(cache.get('info') or {})
            info['source'] = 'cache'
            info['cache_age_sec'] = round(age, 1)
            return bool(cache.get('value', False)), info

    try:
        from backend.realtime import signals as sig
        bars_1m = mootdx_client.get_minute_bars(ts_code, 240)
        if bars_1m:
            val, info = sig.compute_pool1_resonance_60m(bars_1m, fallback=False)
            info = dict(info or {})
            info['source'] = '1m_aggregate'
            _pool1_resonance_cache[ts_code] = {
                'value': bool(val),
                'info': info,
                'updated_at': now_ts,
            }
            return bool(val), info
    except Exception as e:
        logger.debug(f"pool1 60m共振算失败 {ts_code}: {e}")

    # 嶉滃号:卟时,嘶卮呒
    proxy_val = _pool1_resonance_60m_proxy(daily, price, prev_price)
    info = {'enabled': True, 'source': 'proxy_fallback', 'reason': 'minute_bars_unavailable'}
    _pool1_resonance_cache[ts_code] = {
        'value': bool(proxy_val),
        'info': info,
        'updated_at': now_ts,
    }
    return bool(proxy_val), info


def _update_signal_state(ts_code: str, signal: dict, now: int) -> dict:
    """
    更新价栧垱寤轰俊墽风殑衰减鐘舵)侊价返回合信鍒浜嗙姸鎬佸瓧娈电殑信″类鍓湰。    信号生命周期:active ->?decaying ->?expired
    """
    try:
        from config import T0_SIGNAL_CONFIG
        decay_cfg = T0_SIGNAL_CONFIG['decay']
        hard_expiry_min = decay_cfg.get('hard_expiry_min', 12)
        min_floor = decay_cfg.get('min_floor', 0.15)
    except Exception:
        hard_expiry_min, min_floor = 12, 0.15

    signal_type = signal.get('type', '')
    states = _main_signal_state_cache.setdefault(ts_code, {})

    initial_strength = signal.get('strength', 50)
    triggered_at = signal.get('triggered_at', now)
    is_new = signal.get('is_new', False)

    if signal_type not in states or is_new:
        # 麓:'状态
        states[signal_type] = {
            'state': 'active',
            'triggered_at': triggered_at,
            'trigger_price': signal.get('price', 0),
            'initial_strength': initial_strength,
            'current_strength': float(initial_strength),
            'last_update_at': now,
            'expire_reason': '',
        }

    st = states[signal_type]
    trading_min = _trading_minutes_elapsed(st['triggered_at'], now)
    factor = _decay_factor(signal_type, st['triggered_at'], now)
    current_strength = st['initial_strength'] * factor
    st['current_strength'] = round(current_strength, 1)
    st['last_update_at'] = now

    # 状态迁
    if st['state'] != 'expired':
        if trading_min >= hard_expiry_min:
            st['state'] = 'expired'
            st['expire_reason'] = 'timeout'
        elif current_strength < min_floor * st['initial_strength']:
            st['state'] = 'expired'
            st['expire_reason'] = 'weak'
        elif current_strength < 0.6 * st['initial_strength']:
            st['state'] = 'decaying'

    s = dict(signal)
    s['state'] = st['state']
    s['age_sec'] = int(now - st['triggered_at'])
    s['current_strength'] = st['current_strength']
    s['expire_reason'] = st['expire_reason']
    return s


def _expire_reversed_signal_state(ts_code: str, signal_type: str) -> None:
    """墽急鏂信″类触﹀傚间讹价强度埗灏嗗绔格俊墽风姸鎬佹爣嬭?expired(reversed("""
    states = _main_signal_state_cache.get(ts_code, {})
    if signal_type in states and states[signal_type]['state'] != 'expired':
        states[signal_type]['state'] = 'expired'
        states[signal_type]['expire_reason'] = 'reversed'


# ============================================================
# 项目跾
# ============================================================
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_STRATEGY_FILE = os.path.join(_PROJECT_ROOT, '_gm_strategy.py')  # 静态)策略脚本文件文牸?
# ============================================================
# 三层结构(轨1低ワ栵(氬唴瀛橀槦鍒?+ 嶉变韩字典
# ------------------------------------------------------------
# Layer 1(生产)? gm 咏): on_tick ->?shared_queue.put()
# Layer 2(消费线程? 主进程): queue.get() ->?shared_cache[ts] = tick
# Layer 3(級墽? FastAPI( GmTickProvider.get_tick() ->?cache[ts]
#
# 通过 BaseManager TCP server 实现价进?Queue 鍜?dict。# 嬭昏繘绋嬫寔本夌湡屾炲璞★术_main_queue / _main_cache),
# 子进程通过 TCP 代理连接访问。# ============================================================
_tick_lock = threading.Lock()  # 贪叱全
_subscribed_codes: set[str] = set()  # 讯 ts_code 闆嗗悎
_gm_context = None

# 订阅列表牸嶇敤文件下布俊(堜綆计戯价椼)級曞彲闈战栵
_SUBS_FILE = os.path.join(_PROJECT_ROOT, '.gm_tick_subs.txt')

_main_queue: _StdQueue = _StdQueue(maxsize=50000)   # (ts_code, tick_dict) 队列
_main_cache: dict[str, dict] = {}                   # ts_code → tick dict

_main_daily_cache: dict[str, dict] = {}             # ts_code -> 日线指标
_main_chip_cache: dict[str, dict] = {}              # ts_code ->?筹码特征 {chip_concentration_pct, winner_rate, ...}
_main_signal_cache: dict[str, dict] = {}            # ts_code ->?{'signals': [...], 'evaluated_at': int, 'price': float, 'pct_chg': float}
_main_stock_pools: dict[str, set] = {}              # ts_code -> {pool_id, ...}
# 信号去重(ts_code, signal_type) ->?上触发 timestamp
_main_signal_last_fire: dict[tuple, int] = {}
SIGNAL_DEDUP_SECONDS_POOL: dict[int, int] = {
    1: 300,  # 姹?(分钟去重窗口
    2: 120,  # 姹?(分钟去重窗口(T+0机会轞即)）
}
REVERSE_SIGNAL_PAIRS = {
    'positive_t': 'reverse_t',
    'reverse_t': 'positive_t',
    'left_side_buy': 'right_side_breakout',
    'right_side_breakout': 'left_side_buy',
}

_P1_DEDUP_CFG = _P1_CFG.get('dedup_sec', {}) if isinstance(_P1_CFG, dict) else {}
_P1_HALF_LIFE_CFG = _P1_CFG.get('decay_half_life_min', {}) if isinstance(_P1_CFG, dict) else {}
_P1_RESONANCE_CFG = _P1_CFG.get('resonance_60m', {}) if isinstance(_P1_CFG, dict) else {}
_P1_RESONANCE_ENABLED = bool(_P1_RESONANCE_CFG.get('enabled', False))
_P1_RESONANCE_REFRESH_SEC = int(_P1_RESONANCE_CFG.get('refresh_sec', 120) or 120)

# 每个池旂殑信″类型标识闆嗗悎(时 vs T+0 池)
POOL_SIGNAL_TYPES: dict[int, set] = {
    1: {'left_side_buy', 'right_side_breakout'},   # 时:战略建仓
    2: {'positive_t', 'reverse_t'},                # T+0 池:日内套利
}

# Layer 2 T+0 池:tick 紧状)侊术每忓褰褔懖$銈ㄦ稉)嬭綋间ョ疮据″鐡褔崗闈╃礆
# _intraday_state[ts_code] = {
#   'date': 'YYYY-MM-DD',
#   'cum_volume': int, 'cum_amount': float,    # 绱忚忛 ->?VWAP
#   'last_volume': int,                          # 用于椼?螖volume
#   'price_window': deque[(ts, price, vol)],   # 本)斿?N 分钟 tick 用于 Z-score / 閼椂瞼
#   'gub5_small_sells_5min': deque[ts],        # 灏忓卖出时间戳滚?5 分钟 ->?GUB5 趋势
# }
_intraday_state: dict[str, dict] = {}
INTRADAY_WINDOW_SECONDS = 20 * 60                    # 20 分钟价格滚动窗口
GUB5_WINDOW_SECONDS = 5 * 60                         # 5 分钟 GUB5 滚动窗口

# Layer 2: recent transactions cache shared by Pool1/Pool2.
# _main_recent_txns[ts_code] = [{time, price, volume, direction}, ...]
_main_recent_txns: dict[str, list] = {}

# Large-order threshold (hand count) for transaction structure analysis.
BIG_ORDER_THRESHOLD = 500
# Number of recent transactions kept for analysis.
TXN_ANALYZE_COUNT = 50

# Layer 2 信″类衰减鐘舵)缓?# ts_code -> { signal_type -> {
#   'state':            'active' | 'decaying' | 'expired',
#   'triggered_at':     int,       # 妫f牗鐟欙箑褰傞惃?epoch 绉?#   'trigger_price':    float,
#   'initial_strength': float,
#   'current_strength': float,
#   'last_update_at':   int,
#   'expire_reason':    str,       # '' | 'timeout' | 'reversed' | 'weak'
# }}
_main_signal_state_cache: dict[str, dict] = {}
_main_prev_price: dict[str, float] = {}
_pool1_resonance_cache: dict[str, dict] = {}

# Layer 3 志没(消费线程 ->强构 writer)?# 元素格接(kind, ts_code, tick_dict, signal_list)
_persist_queue: Optional[_StdQueue] = None
_TICK_MGR_AUTHKEY: bytes = secrets.token_bytes(16)
_TICK_MGR_ADDRESS: tuple = ('127.0.0.1', 0)          # 0 = 自动分配端口

_manager_server = None
_consumer_thread_started = False

# Pool1(时(夊彲触构祴撴熻(统)墽e緞
_pool1_observe = {
    'trade_date': '',
    'updated_at': 0,
    'screen_total': 0,
    'screen_pass': 0,
    'stage2_triggered': 0,
}


def _pool1_observe_trade_date() -> str:
    """褰撳墠撴熻浜ゆ槗间ワ术本湴间ユ湡墽e緞(夈)?"""
    return _dt.datetime.now().strftime('%Y-%m-%d')


def _ensure_pool1_observe_day() -> None:
    """鑻ヨ法间ュ垯忚嶇疆撴熻(保证口径按交易日聚合)?"""
    today = _pool1_observe_trade_date()
    if _pool1_observe.get('trade_date') == today:
        return
    _pool1_observe['trade_date'] = today
    _pool1_observe['updated_at'] = int(time.time())
    _pool1_observe['screen_total'] = 0
    _pool1_observe['screen_pass'] = 0
    _pool1_observe['stage2_triggered'] = 0


def _record_pool1_observe(stage1_pass: bool, stage2_triggered: bool) -> None:
    _ensure_pool1_observe_day()
    _pool1_observe['screen_total'] += 1
    if stage1_pass:
        _pool1_observe['screen_pass'] += 1
        if stage2_triggered:
            _pool1_observe['stage2_triggered'] += 1
    _pool1_observe['updated_at'] = int(time.time())


def _pool1_observe_summary() -> str:
    _ensure_pool1_observe_day()
    total = int(_pool1_observe.get('screen_total', 0) or 0)
    passed = int(_pool1_observe.get('screen_pass', 0) or 0)
    triggered = int(_pool1_observe.get('stage2_triggered', 0) or 0)
    if total <= 0:
        return "pool1无样本"
    pass_rate = (passed / total) * 100
    trigger_rate = (triggered / passed) * 100 if passed > 0 else 0.0
    return (
        f"pool1通过率={pass_rate:.1f}%({passed}/{total}) "
        f"浜伩樁娈佃袝墽戠巼={trigger_rate:.1f}%({triggered}/{passed})"
    )


def get_pool1_observe_stats() -> dict:
    """返回 Pool1 两阶段统计快照(轻量接口,无 IO(夈)?"""
    _ensure_pool1_observe_day()
    total = int(_pool1_observe.get('screen_total', 0) or 0)
    passed = int(_pool1_observe.get('screen_pass', 0) or 0)
    triggered = int(_pool1_observe.get('stage2_triggered', 0) or 0)
    updated_at = int(_pool1_observe.get('updated_at', 0) or 0)
    pass_rate = (passed / total) if total > 0 else 0.0
    trigger_rate = (triggered / passed) if passed > 0 else 0.0
    return {
        'trade_date': _pool1_observe.get('trade_date') or _pool1_observe_trade_date(),
        'updated_at': updated_at,
        'updated_at_iso': _dt.datetime.fromtimestamp(updated_at).isoformat() if updated_at > 0 else None,
        'screen_total': total,
        'screen_pass': passed,
        'stage2_triggered': triggered,
        'pass_rate': round(pass_rate, 4),
        'trigger_rate': round(trigger_rate, 4),
        'summary': _pool1_observe_summary(),
    }


class _TickSyncManager(BaseManager):
    """价进程共槦鍒楀拰瀛楀吀鐨?Manager"""
    pass


_TickSyncManager.register('get_queue', callable=lambda: _main_queue)
_TickSyncManager.register('get_cache', callable=lambda: _main_cache)


def _save_subscribed_codes():
    """信濆瓨订阅列表鍒版文牸讹术渚?gm 策略脚本 init 读取("""
    try:
        with open(_SUBS_FILE, 'w') as f:
            f.write(','.join(_subscribed_codes))
    except Exception as e:
        logger.warning(f"保存订阅列表失败: {e}")


def _load_subscribed_codes():
    """从文件加载阅列?"""
    global _subscribed_codes
    try:
        if os.path.exists(_SUBS_FILE):
            with open(_SUBS_FILE, 'r') as f:
                codes = f.read().strip()
                if codes:
                    _subscribed_codes = set(codes.split(','))
    except Exception:
        _subscribed_codes = set()


def _start_manager_server():
    """閸氬袟 BaseManager TCP server(堜富斿涚▼璋冪敤嬭)娆★栵"""
    global _manager_server, _TICK_MGR_ADDRESS
    if _manager_server is not None:
        return

    mgr = _TickSyncManager(address=_TICK_MGR_ADDRESS, authkey=_TICK_MGR_AUTHKEY)
    _manager_server = mgr.get_server()
    actual_addr = _manager_server.address  # (host, port)
    _TICK_MGR_ADDRESS = actual_addr

    # 通过鐜墽橀噺浼犵粰 gm 瀛愯繘绋嬶价据╁叾斿炴帴低?server
    os.environ['GM_TICK_MGR_HOST'] = str(actual_addr[0])
    os.environ['GM_TICK_MGR_PORT'] = str(actual_addr[1])
    os.environ['GM_TICK_MGR_AUTHKEY'] = _TICK_MGR_AUTHKEY.hex()

    t = threading.Thread(target=_manager_server.serve_forever, daemon=True, name='tick-mgr-server')
    t.start()
    logger.info(f"TickManager TCP server: {actual_addr[0]}:{actual_addr[1]}")


def init_persist_queue(maxsize: int = 100000) -> _StdQueue:
    """
    鍒濆鍖?Layer 3 侀佷箙鍖栭槦鍒椼)鍌滄暠 realtime_monitor._start_tick_persistence() 璋冪敤。    返回队列瀵硅薄(傚介柈?writer 线程从中 get。    """
    global _persist_queue
    if _persist_queue is None:
        _persist_queue = _StdQueue(maxsize=maxsize)
        logger.info(f"Layer3 志没已创建 (maxsize={maxsize})")
    return _persist_queue


def _update_intraday_state(ts_code: str, tick: dict) -> dict:
    """更新日内绱鐘舵)侊术VWAP、佷林归肩獥墽c)丟UB5),返回 state 用于信″类据$撴"""
    today = _dt.datetime.now().strftime('%Y-%m-%d')
    state = _intraday_state.get(ts_code)
    if state is None or state.get('date') != today:
        state = {
            'date': today,
            'cum_volume': 0,
            'cum_amount': 0.0,
            'last_volume': 0,
            'price_window': deque(),   # (ts, price, delta_volume)
            'gub5_window': deque(),    # (ts, small_sell_flag: 0/1)
        }
        _intraday_state[ts_code] = state

    now = int(tick.get('timestamp') or time.time())
    price = float(tick.get('price', 0) or 0)
    cum_volume = int(tick.get('volume', 0) or 0)
    cum_amount = float(tick.get('amount', 0) or 0)

    # 计算增量成交
    last_v = state['last_volume']
    delta_v = cum_volume - last_v if cum_volume >= last_v else cum_volume  # 当日重置
    state['last_volume'] = cum_volume
    state['cum_volume'] = cum_volume
    state['cum_amount'] = cum_amount

    # 滚动价格窗口
    pw = state['price_window']
    pw.append((now, price, delta_v))
    while pw and now - pw[0][0] > INTRADAY_WINDOW_SECONDS:
        pw.popleft()

    # GUB5:简化为"本价架氦忚兘緝灏?< 500 鎵?= 50000 鑲?且价格低于上)嬭?tick"娴f粈璐熺亸忓宕熼崡归褍毉閺急洩
    gw = state['gub5_window']
    small_sell = 0
    if delta_v > 0 and delta_v < 50000 and len(pw) >= 2 and pw[-1][1] < pw[-2][1]:
        small_sell = 1
    gw.append((now, small_sell))
    while gw and now - gw[0][0] > GUB5_WINDOW_SECONDS:
        gw.popleft()

    return state


def _compute_vwap(state: dict) -> Optional[float]:
    """日内 VWAP = cum_amount / cum_volume"""
    if state['cum_volume'] > 0:
        return state['cum_amount'] / state['cum_volume']
    return None


def _compute_gub5_trend(state: dict) -> str:
    """GUB5 趋势(up' | 'flat' | 'down'(根捜?5 分钟小单卖出占比"""
    gw = state['gub5_window']
    if len(gw) < 10:
        return 'flat'
    ratio = sum(f for _, f in gw) / len(gw)
    if ratio > 0.4:
        return 'up'
    elif ratio > 0.2:
        return 'flat'
    else:
        return 'down'


def _compute_gub5_transition(state: dict) -> str:
    """
    GUB5 轐)测,用于层)?    板忓棛鐛ラ崣e半与后半对比(岃繑鍥炴柟日戝彉鍖栧瓧绗︿覆(      'up->flat' | 'up->down' | 'flat->down' | 'flat' | 'up' | 'down' | ...
    """
    gw = state['gub5_window']
    if len(gw) < 20:
        return _compute_gub5_trend(state)
    half = len(gw) // 2
    items = list(gw)
    def _ratio(seg): return sum(f for _, f in seg) / len(seg) if seg else 0
    def _label(r): return 'up' if r > 0.4 else ('flat' if r > 0.2 else 'down')
    r_prev = _ratio(items[:half])
    r_curr = _ratio(items[half:])
    prev_label = _label(r_prev)
    curr_label = _label(r_curr)
    if prev_label == curr_label:
        return curr_label
    return f'{prev_label}->{curr_label}'


def _compute_bid_ask_ratio(tick: dict) -> Optional[float]:
    """
    五档买卖量比:sum(bid_vol 1-5) / sum(ask_vol 1-5)
    > 1.15 触嗕负樻扮面鎵挎帴。    """
    bids = tick.get('bids') or []
    asks = tick.get('asks') or []
    bid_vol = sum(v for _, v in bids[:5]) if bids else 0
    ask_vol = sum(v for _, v in asks[:5]) if asks else 0
    if ask_vol <= 0:
        return None
    return round(bid_vol / ask_vol, 3)


def _compute_price_zscore(state: dict) -> Optional[float]:
    """当前价格相 20 鍒嗛挓佺楀彛鐨?Z-Score"""
    pw = state['price_window']
    if len(pw) < 10:
        return None
    prices = [p for _, p, _ in pw]
    try:
        m = statistics.mean(prices[:-1])     # 懦前
        s = statistics.stdev(prices[:-1])
        if s == 0:
            return None
        return (prices[-1] - m) / s
    except Exception:
        return None


def _pool1_daily_screening(daily: dict) -> tuple[bool, int, dict]:
    """Pool1 笸阶(氭棩绾价瓫下栵术椂ㄦ + 鍔級垎("""
    ma5 = float(daily.get('ma5', 0) or 0)
    ma10 = float(daily.get('ma10', 0) or 0)
    ma20 = float(daily.get('ma20', 0) or 0)
    rsi6_val = daily.get('rsi6')
    rsi6 = float(rsi6_val) if rsi6_val is not None else None
    volume_ratio_val = daily.get('volume_ratio')
    volume_ratio = float(volume_ratio_val) if volume_ratio_val is not None else None

    trend_ok = ma5 > ma10 > ma20 > 0
    momentum_ok = (rsi6 is not None) and (35 <= rsi6 <= 70)
    volume_ok = (volume_ratio is not None) and (volume_ratio >= 1.0)

    passed = trend_ok or momentum_ok
    bonus = 0
    if trend_ok:
        bonus += 10
    if momentum_ok:
        bonus += 10
    if volume_ok:
        bonus += 5

    return passed, min(25, bonus), {
        'trend_ok': trend_ok,
        'momentum_ok': momentum_ok,
        'volume_ok': volume_ok,
        'bonus': min(25, bonus),
    }


def _compute_signals_for_tick(tick: dict, daily: dict, ts_code: str = '') -> list[dict]:
    """
    Layer 2 信″类据$撴 閳ユ柡)?侀夎偂绁ㄦ墍傚炴略鍒嗘祦。
    司$銈ㄩ幍)鍦ㄦ略据$撴鐎电懓簲信″类(      - 姹?1(择时): left_side_buy + right_side_breakout(基?tick + 日线 BOLL/RSI/MA
      - 姹?2(T+0( positive_t + reverse_t(基?tick + 日内紧窗口(圴WAP/GUB5/Z-score(
    嬭)参票若同时在两丱(要垯嬭ゅ信″类閮界撴。    """
    from backend.realtime import signals as sig

    price = float(tick.get('price', 0) or 0)
    pct_chg = float(tick.get('pct_chg', 0) or 0)
    boll_upper = float(daily.get('boll_upper', 0) or 0)
    boll_mid = float(daily.get('boll_mid', 0) or 0)
    boll_lower = float(daily.get('boll_lower', 0) or 0)
    rsi6 = daily.get('rsi6')
    volume_ratio = daily.get('volume_ratio')
    prev_price = _main_prev_price.get(ts_code)

    if price <= 0 or boll_upper <= 0:
        return []

    # 魏渭爻虿患藕
    pools = _main_stock_pools.get(ts_code, set())
    if not pools:
        return []

    # 嬭标姏琛屼负鍒嗘瀽(两丱共用(嶈涚窗下愮瑪 + 五档
    txns = _main_recent_txns.get(ts_code)
    ms = _analyze_market_structure(tick, txns)
    bid_ask_ratio = _compute_bid_ask_ratio(tick)
    up_limit = None
    down_limit = None
    pre_close = float(tick.get('pre_close', 0) or 0)
    if pre_close > 0:
        up_limit = pre_close * 1.1
        down_limit = pre_close * 0.9

    daily_with_limits = dict(daily)
    if up_limit is not None:
        daily_with_limits['up_limit'] = up_limit
    if down_limit is not None:
        daily_with_limits['down_limit'] = down_limit

    def _build_ctx(pool_id: int, intraday: Optional[dict] = None) -> Optional[dict]:
        if _build_signal_context is None:
            return None
        try:
            ctx = _build_signal_context(
                ts_code=ts_code,
                pool_id=pool_id,
                tick=tick,
                daily=daily_with_limits,
                intraday=intraday or {},
                market={
                    'vol_20': daily.get('vol_20'),
                    'atr_14': daily.get('atr_14'),
                },
            )
            # Pool1 limit-magnet distance now follows dynamic threshold engine config.
            return ctx
        except Exception as e:
            logger.debug(f"build_signal_context 强常 {ts_code}/{pool_id}: {e}")
            return None

    def _get_th(signal_type: str, ctx: Optional[dict]) -> Optional[dict]:
        if _get_thresholds is None or ctx is None:
            return None
        try:
            return _get_thresholds(signal_type, ctx)
        except Exception as e:
            logger.debug(f"get_thresholds 强常 {ts_code}/{signal_type}: {e}")
            return None

    fired: list[dict] = []

    # ===== 姹?1(氭瀚ㄩ弮?鈥?战略建仓 =====
    if 1 in pools:
        stage1_pass, stage1_bonus, stage1_info = _pool1_daily_screening(daily)
        stage2_triggered = False
        p1_ctx = _build_ctx(pool_id=1)
        p1_left_th = _get_th('left_side_buy', p1_ctx)
        p1_right_th = _get_th('right_side_breakout', p1_ctx)

        # Pool1 椂冭埖2(氱有冩壒寰佽瘎鍒嗭术板樻强强)嶉虫池鍒讹价牸呭鍒鍒嗕笉鏀硅袝墽戯栵
        chip_feat = dict(_main_chip_cache.get(ts_code, {}))
        chip_feat.update({
            'chip_concentration_pct': daily.get('chip_concentration_pct'),
            'winner_rate': daily.get('winner_rate'),
        })
        chip_bonus, chip_info = sig.compute_pool1_chip_bonus(chip_feat)
        resonance_60m, resonance_60m_info = _pool1_resonance_60m(ts_code, daily, price, prev_price)

        if stage1_pass:
            left = sig.detect_left_side_buy(
                price=price, boll_upper=boll_upper, boll_mid=boll_mid, boll_lower=boll_lower,
                rsi6=rsi6, pct_chg=pct_chg,
                bid_ask_ratio=bid_ask_ratio,
                lure_long=ms.get('lure_long', False),
                wash_trade=ms.get('wash_trade', False),
                up_limit=up_limit, down_limit=down_limit,
                resonance_60m=resonance_60m,
                thresholds=p1_left_th,
            )
            if left.get('has_signal'):
                left['strength'] = min(100, int(left.get('strength', 0)) + stage1_bonus + chip_bonus)
                left = sig.finalize_pool1_signal(left, thresholds=p1_left_th)
            if left.get('has_signal'):
                left.setdefault('details', {})['pool1_stage'] = {
                    'daily_screening': stage1_info,
                    'stage2_triggered': True,
                    'chip': chip_info,
                    'resonance_60m': bool(resonance_60m),
                    'resonance_60m_info': resonance_60m_info,
                }
                fired.append(left)
                stage2_triggered = True

            right = sig.detect_right_side_breakout(
                price=price, boll_upper=boll_upper, boll_mid=boll_mid, boll_lower=boll_lower,
                volume_ratio=volume_ratio,
                ma5=daily.get('ma5'), ma10=daily.get('ma10'), rsi6=rsi6,
                prev_price=prev_price,
                bid_ask_ratio=bid_ask_ratio,
                lure_long=ms.get('lure_long', False),
                wash_trade=ms.get('wash_trade', False),
                up_limit=up_limit, down_limit=down_limit,
                resonance_60m=resonance_60m,
                thresholds=p1_right_th,
            )
            if right.get('has_signal'):
                right['strength'] = min(100, int(right.get('strength', 0)) + stage1_bonus + chip_bonus)
                right = sig.finalize_pool1_signal(right, thresholds=p1_right_th)
            if right.get('has_signal'):
                right.setdefault('details', {})['pool1_stage'] = {
                    'daily_screening': stage1_info,
                    'stage2_triggered': True,
                    'chip': chip_info,
                    'resonance_60m': bool(resonance_60m),
                    'resonance_60m_info': resonance_60m_info,
                }
                fired.append(right)
                stage2_triggered = True

        _record_pool1_observe(stage1_pass=stage1_pass, stage2_triggered=stage2_triggered)

    # ===== 姹?2:T+0 鈥?日内套利 =====
    if 2 in pools and ts_code:
        state = _update_intraday_state(ts_code, tick)
        vwap = _compute_vwap(state)
        gub5 = _compute_gub5_trend(state)
        gub5_transition = _compute_gub5_transition(state)
        bid_ask_ratio = _compute_bid_ask_ratio(tick)

        pw = state.get('price_window')
        intraday_prices = [p for _, p, _ in pw] if pw else None
        v_power_divergence = _detect_v_power_divergence(state)

        wash_trade = ms.get('wash_trade', False)
        lure_short = ms.get('lure_short', False)
        big_order_bias = ms.get('big_order_bias', 0.0)
        real_buy_fading = (ms.get('tag') == 'real_buy' and big_order_bias < 0.1)
        wash_trade_severe = wash_trade and abs(big_order_bias) < 0.05

        try:
            feat = sig.build_t0_features(
                tick_price=price,
                boll_lower=boll_lower,
                boll_upper=boll_upper,
                vwap=vwap,
                intraday_prices=intraday_prices,
                gub5_trend=gub5,
                gub5_transition=gub5_transition,
                bid_ask_ratio=bid_ask_ratio,
                lure_short=lure_short,
                wash_trade=wash_trade,
                spread_abnormal=False,
                v_power_divergence=v_power_divergence,
                ask_wall_building=ms.get('lure_long', False),
                real_buy_fading=real_buy_fading,
                wash_trade_severe=wash_trade_severe,
                liquidity_drain=False,
                big_order_bias=big_order_bias,
            )
        except Exception as e:
            logger.debug(f"build_t0_features 强常 {ts_code}: {e}")
            feat = None

        if feat is not None:
            t0_ctx = _build_ctx(
                pool_id=2,
                intraday={
                    'vwap': vwap,
                    'bias_vwap': feat.get('bias_vwap'),
                    'robust_zscore': feat.get('robust_zscore'),
                    'gub5_ratio': None,
                    'gub5_transition': gub5_transition,
                },
            )
            t0_pos_th = _get_th('positive_t', t0_ctx)
            t0_rev_th = _get_th('reverse_t', t0_ctx)
            try:
                pos_t = sig.detect_positive_t(
                    tick_price=feat['tick_price'],
                    boll_lower=feat['boll_lower'],
                    vwap=feat['vwap'],
                    gub5_trend=feat['gub5_trend'],
                    gub5_transition=feat['gub5_transition'],
                    bid_ask_ratio=feat['bid_ask_ratio'],
                    lure_short=feat['lure_short'],
                    wash_trade=feat['wash_trade'],
                    spread_abnormal=feat['spread_abnormal'],
                    boll_break=feat['boll_break'],
                    ask_wall_absorb=feat['ask_wall_absorb'],
                    spoofing_suspected=feat['spoofing_suspected'],
                    bias_vwap=feat['bias_vwap'],
                    ts_code=ts_code,
                    thresholds=t0_pos_th,
                )
                if pos_t.get('has_signal'):
                    fired.append(pos_t)
            except Exception as e:
                logger.debug(f"positive_t 计算强常 {ts_code}: {e}")

            try:
                rev_t = sig.detect_reverse_t(
                    tick_price=feat['tick_price'],
                    boll_upper=feat['boll_upper'],
                    intraday_prices=intraday_prices,
                    v_power_divergence=feat['v_power_divergence'],
                    ask_wall_building=feat['ask_wall_building'],
                    real_buy_fading=feat['real_buy_fading'],
                    wash_trade_severe=feat['wash_trade_severe'],
                    liquidity_drain=feat['liquidity_drain'],
                    boll_over=feat['boll_over'],
                    spoofing_suspected=feat['spoofing_suspected'],
                    big_order_bias=feat['big_order_bias'],
                    robust_zscore=feat['robust_zscore'],
                    ts_code=ts_code,
                    thresholds=t0_rev_th,
                )
                if rev_t and rev_t.get('has_signal'):
                    fired.append(rev_t)
            except Exception as e:
                logger.debug(f"reverse_t 计算强常 {ts_code}: {e}")

    # 搴旂敤嬭标姏琛屼负璋涓暣(买入信?+strength_delta(傚藉礌閸戣桨信婇崣?-strength_delta
    # 閸氬本妞傞幎?market_structure 嵌入 details(供前展示
    for s in fired:
        direction = s.get('direction', 'buy')
        delta = ms['strength_delta'] if direction == 'buy' else -ms['strength_delta']
        original = s.get('strength', 0)
        s['strength'] = max(0, min(100, original + delta))
        s.setdefault('details', {})['market_structure'] = {
            'tag': ms['tag'],
            'big_order_bias': round(ms['big_order_bias'], 3),
            'bid_ask_ratio': round(ms['bid_ask_ratio'], 2),
            'lure_long': ms['lure_long'],
            'lure_short': ms['lure_short'],
            'wash_trade': ms['wash_trade'],
            'strength_delta': delta,
        }
        # 在 message 末尾追为签(牸呴潪 normal)
        if ms['tag'] != 'normal':
            tag_map = {
                'lure_long': '斩',
                'lure_short': '诱空回补',
                'wash': '缘',
                'real_buy': '嬭标姏娴佸樻',
                'real_sell': '嬭标姏流出',
            }
            s['message'] = s.get('message', '') + f" [{tag_map.get(ms['tag'], ms['tag'])}]"

    if ts_code and price > 0:
        _main_prev_price[ts_code] = price

    return fired


def _analyze_market_structure(tick: dict, txns: Optional[list]) -> dict:
    """
    基于)鏉╂垿)笔成交 + 五档监樺彛撴煎悎鍒嗘瀽嬭标姏鎰忓浘。
    分嗗埆(      - lure_long(級嶈氾栵(大单买入占优,但盘e压大(傚肩幆閺急吋妲楅崶癸舵儰
      - lure_short(級绌猴栵(大单卖出占优,但盘d压大(傚肩幆閺急吋妲楅崣宥瀵剨
      - wash_trade(鍫濋崐鎺炵礆(大单买卖交替且价格不动(要埗下犳垚浜ら噺鍋囪薄
      - real_buy / real_sell(大单真实流?流出

    返回(      {
        'big_buy_vol', 'big_sell_vol', 'big_order_bias' ->[-1, 1],
        'bid_vol', 'ask_vol', 'bid_ask_ratio',
        'lure_long', 'lure_short', 'wash_trade',
        'tag': 'normal'|'lure_long'|'lure_short'|'wash'|'real_buy'|'real_sell',
        'strength_delta': int ->[-30, +30]  # 瀵?买入信号"鐨信鍒鍒?      }
    """
    result = {
        'big_buy_vol': 0, 'big_sell_vol': 0, 'big_order_bias': 0.0,
        'bid_vol': 0, 'ask_vol': 0, 'bid_ask_ratio': 1.0,
        'lure_long': False, 'lure_short': False, 'wash_trade': False,
        'tag': 'normal', 'strength_delta': 0,
    }

    # 1. 五档盘口压力
    bids = tick.get('bids') or []
    asks = tick.get('asks') or []
    bid_vol = sum(int(v or 0) for _, v in bids[:5])
    ask_vol = sum(int(v or 0) for _, v in asks[:5])
    result['bid_vol'] = bid_vol
    result['ask_vol'] = ask_vol
    result['bid_ask_ratio'] = (bid_vol / ask_vol) if ask_vol > 0 else 1.0

    # 2. 逐笔大单分析
    if not txns:
        return result
    recent = txns[-TXN_ANALYZE_COUNT:]
    big_buy_vol = 0
    big_sell_vol = 0
    big_buy_cnt = 0
    big_sell_cnt = 0
    big_buy_prices: list[float] = []
    big_sell_prices: list[float] = []
    all_prices: list[float] = []
    total_vol = 0

    for t in recent:
        v = int(t.get('volume', 0) or 0)
        p = float(t.get('price', 0) or 0)
        d = int(t.get('direction', 0) or 0)  # 0=buy, 1=sell
        total_vol += v
        all_prices.append(p)
        if v >= BIG_ORDER_THRESHOLD:
            if d == 0:
                big_buy_vol += v
                big_buy_cnt += 1
                big_buy_prices.append(p)
            else:
                big_sell_vol += v
                big_sell_cnt += 1
                big_sell_prices.append(p)

    total_big = big_buy_vol + big_sell_vol
    result['big_buy_vol'] = big_buy_vol
    result['big_sell_vol'] = big_sell_vol
    result['big_order_bias'] = (big_buy_vol - big_sell_vol) / total_big if total_big > 0 else 0.0

    # 3. 鐠囧崬/诱空/鐎电懓)判?    # 鐠囧崬(大单买占优 (bias > 0.3)(但盘口卖压?(bid_ask_ratio < 0.7)
    if result['big_order_bias'] > 0.3 and result['bid_ask_ratio'] < 0.7:
        result['lure_long'] = True
        result['tag'] = 'lure_long'
        result['strength_delta'] = -25   # 瀵逛堜嶉ヤ俊墽牸墸鍒?
    # 诱空:大单卖占优 (bias < -0.3)(但盘口买压?(bid_ask_ratio > 1.5)
    elif result['big_order_bias'] < -0.3 and result['bid_ask_ratio'] > 1.5:
        result['lure_short'] = True
        result['tag'] = 'lure_short'
        result['strength_delta'] = +25   # 对买入信号鍔級垎(抄底更可靠)

    # 鐎电懓):大单买卖次数相近 + 牸牸鎺侀箙灏?+ 大单成交量占总量比高
    elif big_buy_cnt >= 3 and big_sell_cnt >= 3:
        cnt_ratio = min(big_buy_cnt, big_sell_cnt) / max(big_buy_cnt, big_sell_cnt)
        if all_prices and total_vol > 0:
            price_range = (max(all_prices) - min(all_prices)) / max(all_prices)
            big_vol_ratio = total_big / total_vol
            if cnt_ratio > 0.7 and price_range < 0.003 and big_vol_ratio > 0.4:
                result['wash_trade'] = True
                result['tag'] = 'wash'
                result['strength_delta'] = -20   # 鐎电懓)为,)有信号降?
    # 鐪湪整嶈褍崟娴佸樻/流出(未识为斩/诱空/缘时)
    if result['tag'] == 'normal':
        if result['big_order_bias'] > 0.3 and result['bid_ask_ratio'] >= 1.0:
            result['tag'] = 'real_buy'
            result['strength_delta'] = +15
        elif result['big_order_bias'] < -0.3 and result['bid_ask_ratio'] <= 1.0:
            result['tag'] = 'real_sell'
            result['strength_delta'] = -15

    return result


def _detect_v_power_divergence(state: dict) -> Optional[bool]:
    """
    忚个林閼椂瞼妫)娴嬶术反扵 点)(    本)斿?20 嬭佺嬭价牸牸鎺鍒涙柊偣樹絾鍔ㄨ嬭(坧rice*delta_volume(夊噺强便)?    """
    pw = state.get('price_window')
    if not pw or len(pw) < 20:
        return False
    recent = list(pw)[-20:]
    prices = [p for _, p, _ in recent]
    vpower = [p * dv for _, p, dv in recent]
    price_new_high = prices[-1] == max(prices)
    if len(vpower) < 5:
        return False
    vpower_recent_avg = sum(vpower[-5:]) / 5
    return price_new_high and vpower[-1] < vpower_recent_avg


def _start_consumer_thread():
    """閸氬袟消费线程(从 queue 墽?tick ->?更新 cache ->?计算信号(Layer 2("""
    global _consumer_thread_started
    if _consumer_thread_started:
        return
    _consumer_thread_started = True

    def _consume_loop():
        logger.info("tick 消费线程启动(含 Layer2 信″类据$撴)")
        count = 0
        signal_fires = 0
        while True:
            try:
                item = _main_queue.get(timeout=1.0)
            except _QueueEmpty:
                continue
            except Exception as e:
                logger.error(f"tick 消费强常: {e}")
                continue
            try:
                ts_code = item.get('ts_code')
                if not ts_code:
                    continue

                # 1. 閺囧瓨鏌婇張)鏂?tick cache
                _main_cache[ts_code] = item
                count += 1

                # 1.5 鎺ㄩ)佸埌持久化队列(牸标姟4:强步落盘)
                try:
                    if _persist_queue is not None:
                        _persist_queue.put_nowait(('tick', ts_code, item, None))
                except Exception:
                    pass  # 队列婊′涪强冿价信濇寔消费线程嬭噸樆濉?
                # 2. Layer 2:若有日线指标,立即计算信号
                daily = _main_daily_cache.get(ts_code)
                if daily:
                    try:
                        all_fired = _compute_signals_for_tick(item, daily, ts_code=ts_code)
                    except Exception as ce:
                        logger.warning(f"信″类据$撴失败 {ts_code}: {ce}")
                        all_fired = []

                    # 信号去重:按池类型浣价敤不同窗口,址支藕
                    now = int(time.time())
                    new_fired = []
                    # 偏信缓左茬紦瀛樹俊墽风殑瀛楀吀(屼函闈炴柊触﹀傚间跺鐢术信濇寔触﹀傚间提林归煎浐屾氾栵
                    cached_entry = _main_signal_cache.get(ts_code)
                    cached_signals_by_type = {}
                    if cached_entry:
                        for cs in cached_entry.get('signals', []):
                            cached_signals_by_type[cs.get('type')] = cs

                    for s in all_fired:
                        signal_type = s.get('type')
                        key = (ts_code, signal_type)
                        last_ts = _main_signal_last_fire.get(key, 0)

                        pools = _main_stock_pools.get(ts_code, set())
                        dedup_seconds = _dedup_seconds_for_signal(signal_type, pools)

                        # 妫)鏌ユ槸日﹁Е墽戝弽日归俊墽凤术绔嬪嵆忚嶇疆嬭婁竴嬭俊墽风殑据℃杩鍣ㄥ拰衰减鐘舵)渚婄礆
                        reverse_type = REVERSE_SIGNAL_PAIRS.get(signal_type)
                        if reverse_type:
                            reverse_key = (ts_code, reverse_type)
                            if reverse_key in _main_signal_last_fire:
                                del _main_signal_last_fire[reverse_key]
                                cached_signals_by_type.pop(reverse_type, None)
                                _expire_reversed_signal_state(ts_code, reverse_type)

                        if now - last_ts >= dedup_seconds:
                            # 鏂拌袝墽戯跌浣价敤褰撳墠信″类瀵硅薄(包吽前触发价格)
                            _main_signal_last_fire[key] = now
                            s['is_new'] = True
                            new_fired.append(s)
                        else:
                            # 非麓:没械原'藕哦,执鄄
                            original = cached_signals_by_type.get(signal_type)
                            if original is not None:
                                original['is_new'] = False
                                new_fired.append(original)
                            else:
                                # 缓存嬭病本嶈涚礉浣价敤褰撳墠瀵硅薄浣嗘爣据颁负闈炴柊
                                s['is_new'] = False
                                new_fired.append(s)

                    # 对每丿号调用"返忕姸鎬佹満(附?state/age_sec/current_strength/expire_reason
                    new_fired = [_update_signal_state(ts_code, s, now) for s in new_fired]

                    # 牸呭姝棣栧抚价栨湁新信号杩更新缓存,执劭诰
                    cached = _main_signal_cache.get(ts_code)
                    if cached is None:
                        _main_signal_cache[ts_code] = {
                            'ts_code': ts_code,
                            'name': item.get('name', ts_code),
                            'price': item.get('price', 0),
                            'pct_chg': item.get('pct_chg', 0),
                            'signals': new_fired,
                            'evaluated_at': now,
                        }
                    else:
                        # 墽洿新信号垪琛ㄥ拰分勪及间戳棿(傚肩瑝閺囧瓨鏌婃禒閿嬬壐(堜繚侀佽袝墽控杩鐨勪林归硷栵
                        cached['signals'] = new_fired
                        cached['evaluated_at'] = now
                        cached['pct_chg'] = item.get('pct_chg', 0)
                    new_count = sum(1 for s in new_fired if s.get('is_new'))
                    if new_count > 0:
                        signal_fires += 1
                        if signal_fires <= 10 or signal_fires % 100 == 0:
                            types = [s.get('type') for s in new_fired if s.get('is_new')]
                            logger.info(f"[新信号#{signal_fires}] {ts_code} price={item.get('price')} types={types}")
                        # 灏嗚偂绁ㄦ墍傚?pool_id 合信鍒鍒版瘡嬭?signal(屼函侀佷箙鍖?writer 浣价敤
                        stock_pools = _main_stock_pools.get(ts_code, set())
                        for s in new_fired:
                            s['_pool_ids'] = list(stock_pools)
                        # 推送到志没
                        try:
                            if _persist_queue is not None:
                                _persist_queue.put_nowait(('signal', ts_code, item, new_fired))
                        except Exception:
                            pass

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
    掘金量化(gm) Tick Provider(subscribe 订阅模式(
    鍦ㄥ悗墽扮嚎绋建繍琛?gm.run()(伩)氳繃策略鑴氭湰鐨?on_tick 鍥炶皟缓存本)鏂?tick。    get_tick() 牸庢膩鍧楃骇 _tick_cache 读取。    """

    @property
    def name(self) -> str:
        return 'gm'

    @property
    def display_name(self) -> str:
        return '掘金量化(gm)'

    def get_tick(self, ts_code: str) -> dict:
        """从主进程内存 cache 读取)鏂?tick,O(1) 直接访问"""
        tick = _main_cache.get(ts_code)
        if tick is not None:
            return tick
        # gm 间犳算鎹杩返回绌烘算鎹价嬭噸檷绾褍埌 mock
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

    def subscribe_symbols(self, ts_codes: list[str]):
        """
        据㈤槄鑲$エ鍒楄〃。        更新订阅列表鍒版文牸讹价渚?gm 策略鑴氭湰读取。        """
        new_codes = []
        with _tick_lock:
            for code in ts_codes:
                if code not in _subscribed_codes:
                    _subscribed_codes.add(code)
                    new_codes.append(code)

        if new_codes:
            logger.info(f"gm 订阅列表鏂樻 {len(new_codes)} 墽? {new_codes}")
            _save_subscribed_codes()

    def unsubscribe_symbols(self, ts_codes: list[str]):
        """取消订阅"""
        with _tick_lock:
            for code in ts_codes:
                _subscribed_codes.discard(code)

    # ========================================================
    # Layer 2 鐎电懓鎺ュ彛
    # ========================================================
    def update_daily_factors(self, ts_code: str, factors: dict):
        """
        更新鏌愬褰褔懖$エ鐨勬棩绾挎寚归囩紦瀛橈术渚涙秷璐圭嚎绋建椼置俊墽牸杩浣价敤(夈)?        factors 应包吼boll_upper, boll_mid, boll_lower, rsi6, ma5, ma10, volume_ratio
        """
        _main_daily_cache[ts_code] = dict(factors)

    def bulk_update_daily_factors(self, factors_map: dict[str, dict]):
        """鎵癸噺更新日线侀囨爣(启?屾氭湡鍒牸柊间控敤("""
        _main_daily_cache.update(factors_map)
        logger.info(f"Layer2 日线因子缓存已更新: {len(factors_map)}")

    def bulk_update_chip_features(self, features_map: dict[str, dict]):
        """批量更新筹码特征缓存(Pool1 椂冭埖2计勭暀(夈)?"""
        _main_chip_cache.update(features_map)
        logger.info(f"Layer2 筹码特征缓存已更新: {len(features_map)}")

    def update_stock_pools(self, ts_code: str, pool_ids: set):
        """单只股票的池癸属(set of pool_id("""
        _main_stock_pools[ts_code] = set(pool_ids)

    def bulk_update_stock_pools(self, mapping: dict[str, set]):
        """
        鎵癸噺据剧疆鑲$エ->池映射(岃监栧接更新。        mapping: {ts_code: {pool_id, ...}}
        """
        _main_stock_pools.clear()
        for k, v in mapping.items():
            _main_stock_pools[k] = set(v)
        logger.info(f"Layer2 股票池映射已更新: {len(mapping)}")

    def update_recent_txns(self, ts_code: str, txns: list):
        """更新鏌愬褰褔懖$銈ㄩ張)鏉╂垿)笔成交(轨敱嶈栭儴鍒牸柊绾价▼璋冪敤("""
        if txns:
            _main_recent_txns[ts_code] = list(txns)[-TXN_ANALYZE_COUNT:]

    def bulk_update_recent_txns(self, mapping: dict):
        """批量更新逐笔缓存"""
        for k, v in mapping.items():
            if v:
                _main_recent_txns[k] = list(v)[-TXN_ANALYZE_COUNT:]

    def get_cached_signals(self, ts_code: str) -> Optional[dict]:
        """读取 ts_code 本)鏂扮紦瀛偣殑信″类分勪及撴灉灉(秒级响应("""
        return _main_signal_cache.get(ts_code)

    def get_cached_transactions(self, ts_code: str) -> Optional[list]:
        """读取 ts_code 本)斿戠紦瀛偣殑下笔成交数据(秒级响应("""
        return _main_recent_txns.get(ts_code)

    def get_cached_signals_bulk(self, ts_codes: list[str]) -> list[dict]:
        """批量读取信号缓存,用于盯盘池列表"""
        result = []
        for code in ts_codes:
            entry = _main_signal_cache.get(code)
            if entry is not None:
                result.append(entry)
        return result

    def get_pool1_observe_stats(self) -> Optional[dict]:
        """读取 Pool1 嬭ら樁娈靛彲触构祴撴熻(級交忚忓揩鐓褝栵。"""
        return get_pool1_observe_stats()

    def start(self):
        """閸氬袟 gm 后台订阅进程"""
        if not _GM_AVAILABLE:
            logger.warning("gm 朮装,GmTickProvider 将仅返回 mock 数据")
            return

        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from config import GM_TOKEN

        if not GM_TOKEN:
            logger.warning("GM_TOKEN 閺堝帳缂冪礉GmTickProvider 将仅返回 mock 数据")
            return

        # 越疟欠
        if not os.path.exists(_STRATEGY_FILE):
            logger.error(f"gm strategy script not found: {_STRATEGY_FILE}")
            logger.error("please ensure _gm_strategy.py exists in project root")
            return

        # 1. 启动 BaseManager TCP server(嶉变韩 queue + cache)
        _start_manager_server()
        # 2. 叱(queue -> cache)
        _start_consumer_thread()
        # 3. 启动 gm 咏(通过 env 斿炴帴 manager)
        process = multiprocessing.Process(target=self._run_gm, daemon=True, name='gm-subscribe')
        process.start()
        logger.info(f"gm subscribe process started (strategy: {_STRATEGY_FILE})")

    def stop(self):
        """閸嬫粍 gm 后台进程"""
        logger.info("gm subscribe process stop requested")

    @staticmethod
    def _run_gm():
        """在独立进程中运 gm.run()(忚斿涚▼本夎嚜左辩殑嬭荤嚎绋嬶栵"""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from config import GM_TOKEN, GM_MODE

        try:
            # 谢目目录,确 gm.run() 能找到策略脚本
            original_dir = os.getcwd()
            os.chdir(_PROJECT_ROOT)
            logger.info(f"switch cwd to {_PROJECT_ROOT}")

            logger.info(f"gm.run() 閸氬袟, mode={GM_MODE}, token={GM_TOKEN[:8]}...")
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
            logger.error(f"gm 后台进▼强鍒父下)返? {e}")
        finally:
            # 指原目录
            os.chdir(original_dir)


