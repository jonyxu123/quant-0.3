# coding=utf-8
"""
gm 策略脚本（静态文件）- 供 gm.run() 执行

三层架构（Layer 1：数据捕获层 / 生产者）：
  - init(): 从 .gm_tick_subs.txt 读取订阅列表并 subscribe tick
  - on_tick(): 收到 tick → 只做最小解析 → 写入跨进程 Queue（微秒级）
  - 不做计算、不直接写数据库，保持回调极速返回

与主进程通信（通过 multiprocessing.managers.BaseManager TCP）：
  - 环境变量 GM_TICK_MGR_HOST / PORT / AUTHKEY 提供连接信息
  - 主进程暴露 get_queue() 和 get_cache() 两个共享对象
  - 订阅列表仍用 .gm_tick_subs.txt（低频，启动时一次性读取）
"""
from __future__ import print_function, absolute_import
from gm.api import *
from multiprocessing.managers import BaseManager
import time, os, sys

# 获取项目根目录
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = _current_dir

_SUBS_FILE = os.path.join(_project_root, '.gm_tick_subs.txt')

_subscribed_codes = set()


class _TickSyncManager(BaseManager):
    """连接主进程的 Manager"""
    pass


# 子进程侧 register（无 callable，仅代理）
_TickSyncManager.register('get_queue')
_TickSyncManager.register('get_cache')

_remote_queue = None   # 懒加载，缓存 queue 代理


def _connect_manager():
    """连接主进程的 BaseManager，返回共享 queue 代理"""
    global _remote_queue
    if _remote_queue is not None:
        return _remote_queue
    host = os.environ.get('GM_TICK_MGR_HOST')
    port = os.environ.get('GM_TICK_MGR_PORT')
    authkey_hex = os.environ.get('GM_TICK_MGR_AUTHKEY')
    if not (host and port and authkey_hex):
        print("[gm策略] 未找到 TickManager 环境变量，无法连接主进程")
        return None
    try:
        mgr = _TickSyncManager(address=(host, int(port)), authkey=bytes.fromhex(authkey_hex))
        mgr.connect()
        _remote_queue = mgr.get_queue()
        print(f"[gm策略] 已连接 TickManager {host}:{port}")
        return _remote_queue
    except Exception as e:
        print(f"[gm策略] 连接 TickManager 失败: {e}")
        return None


def _load_state():
    """从文件加载订阅列表"""
    global _subscribed_codes
    try:
        if os.path.exists(_SUBS_FILE):
            with open(_SUBS_FILE, 'r') as f:
                content = f.read().strip()
                if content:
                    _subscribed_codes = set(content.split(','))
    except Exception:
        pass

def _ts_code_to_gm_symbol(ts_code):
    code, suffix = ts_code.split('.')
    if suffix.upper() == 'SH':
        return 'SHSE.' + code
    elif suffix.upper() == 'SZ':
        return 'SZSE.' + code
    elif suffix.upper() == 'BJ':
        return 'BJSE.' + code
    return suffix.upper() + '.' + code

def _gm_symbol_to_ts_code(symbol):
    exchange, code = symbol.split('.')
    if exchange == 'SHSE':
        return code + '.SH'
    elif exchange == 'SZSE':
        return code + '.SZ'
    elif exchange == 'BJSE':
        return code + '.BJ'
    return code + '.' + exchange


def init(context):
    """gm 初始化回调：订阅所有股票的 tick"""
    _load_state()
    # 预连接主进程 Manager（失败不阻断，on_tick 会重试）
    _connect_manager()

    codes = list(_subscribed_codes)
    if not codes:
        print("gm init: 订阅列表为空，等待后续动态订阅")
        return
    gm_symbols = [_ts_code_to_gm_symbol(c) for c in codes]
    symbol_str = ','.join(gm_symbols)
    print(f"gm init: 订阅 {len(gm_symbols)} 只股票 tick: {symbol_str}")
    subscribe(symbols=symbol_str, frequency='tick', count=1, unsubscribe_previous=True)


_tick_count = 0  # 调试用：统计 on_tick 调用次数


def _tf(obj, key, default=0):
    """安全取字段：兼容 dict 和对象（gm Tick 是对象，非 dict）"""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def on_tick(context, tick):
    """gm tick 回调：缓存最新 tick 数据

    注意：tick 是 gm 的 Tick 对象（或 dict，版本不同），用 _tf 兼容访问。
    """
    global _tick_count
    _tick_count += 1
    symbol = _tf(tick, 'symbol', '') or ''
    price_dbg = _tf(tick, 'price', 0)
    if _tick_count <= 3 or _tick_count % 100 == 0:
        print(f"[on_tick #{_tick_count}] symbol={symbol} price={price_dbg}")
    try:
        ts_code = _gm_symbol_to_ts_code(symbol)

        # 解析五档盘口
        bids = []
        asks = []
        quotes = _tf(tick, 'quotes', []) or []
        if quotes and len(quotes) > 0:
            for q in quotes[:5]:
                bids.append((float(_tf(q, 'bid_p', 0) or 0), int(_tf(q, 'bid_v', 0) or 0)))
                asks.append((float(_tf(q, 'ask_p', 0) or 0), int(_tf(q, 'ask_v', 0) or 0)))
        if not bids:
            bids = [(0, 0)] * 5
            asks = [(0, 0)] * 5

        price = float(_tf(tick, 'price', 0) or 0)
        cum_volume = int(_tf(tick, 'cum_volume', 0) or 0)
        cum_amount = float(_tf(tick, 'cum_amount', 0) or 0)
        open_price = float(_tf(tick, 'open', 0) or 0)
        high = float(_tf(tick, 'high', 0) or 0)
        low = float(_tf(tick, 'low', 0) or 0)

        # 尝试获取 pre_close
        pre_close = 0.0
        pct_chg = 0.0
        try:
            from gm.api import current
            # current 获取最新快照包含 pre_close
            snap = current(symbols=symbol, fields='pre_close')
            if snap and len(snap) > 0:
                pre_close = float(_tf(snap[0], 'pre_close', 0) or 0)
        except Exception:
            pass
        if pre_close > 0:
            pct_chg = (price - pre_close) / pre_close * 100

        tick_dict = {
            'ts_code': ts_code,
            'name': ts_code,
            'price': price,
            'open': open_price,
            'high': high,
            'low': low,
            'pre_close': pre_close,
            'volume': cum_volume,
            'amount': cum_amount,
            'pct_chg': pct_chg,
            'timestamp': int(time.time()),
            'bids': bids,
            'asks': asks,
        }

        # Layer 1: 只负责 put 到跨进程 queue（微秒级，不阻塞 gm 回调线程）
        q = _connect_manager()
        if q is not None:
            try:
                q.put(tick_dict, block=False)
            except Exception as qe:
                # 队列满或连接异常，丢弃本次 tick 避免阻塞（只告警前几次）
                if _tick_count <= 5:
                    print(f"[gm策略] queue.put 失败(#{_tick_count}): {qe}")

    except Exception as e:
        if _tick_count <= 5:
            import traceback
            print(f"gm on_tick 处理失败: {e}")
            traceback.print_exc()


if __name__ == '__main__':
    """
    策略入口 - 独立运行时使用。
    通常由 backend/realtime/gm_tick.py 的 _run_gm() 通过 gm.run(filename='_gm_strategy.py') 启动。

    独立调试用法:
      python _gm_strategy.py
    """
    import sys
    sys.path.insert(0, _project_root)
    try:
        from config import GM_TOKEN, GM_MODE
    except ImportError:
        print("无法导入 config，请确保 _gm_strategy.py 位于项目根目录")
        sys.exit(1)

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
        backtest_slippage_ratio=0.0001,
    )
