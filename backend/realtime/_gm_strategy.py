# coding=utf-8
"""gm 策略脚本 - 由 gm_tick.py 动态生成，供 gm.run() 执行"""
from __future__ import print_function, absolute_import
from gm.api import *
import time, threading

# 导入共享状态（与 GmTickProvider 通信）
from backend.realtime.gm_tick import (
    _tick_cache, _tick_lock, _subscribed_codes, _gm_symbol_to_ts_code,
)
from backend.realtime.gm_tick import _ts_code_to_gm_symbol
from loguru import logger


def init(context):
    """gm 初始化回调：订阅所有股票的 tick"""
    unsubscribe(symbols='*', frequency='tick')
    with _tick_lock:
        codes = list(_subscribed_codes)
    if not codes:
        logger.warning("gm init: 订阅列表为空，等待后续动态订阅")
        return
    gm_symbols = [_ts_code_to_gm_symbol(c) for c in codes]
    symbol_str = ','.join(gm_symbols)
    logger.info(f"gm init: 订阅 {len(gm_symbols)} 只股票 tick: {symbol_str}")
    subscribe(symbols=symbol_str, frequency='tick', count=1,unsubscribe_previous=True)


def on_tick(context, tick):
    """gm tick 回调：缓存最新 tick 数据"""
    try:
        symbol = tick.get('symbol', '')
        ts_code = _gm_symbol_to_ts_code(symbol)

        # 解析五档盘口
        bids = []
        asks = []
        quotes = tick.get('quotes', [])
        if quotes and len(quotes) > 0:
            for q in quotes[:5]:
                bids.append((float(q.get('bid_p', 0) or 0), int(q.get('bid_v', 0) or 0)))
                asks.append((float(q.get('ask_p', 0) or 0), int(q.get('ask_v', 0) or 0)))
        if not bids:
            bids = [(0, 0)] * 5
            asks = [(0, 0)] * 5

        price = float(tick.get('price', 0) or 0)
        cum_volume = int(tick.get('cum_volume', 0) or 0)
        cum_amount = float(tick.get('cum_amount', 0) or 0)
        open_price = float(tick.get('open', 0) or 0)
        high = float(tick.get('high', 0) or 0)
        low = float(tick.get('low', 0) or 0)

        # 尝试获取 pre_close
        pre_close = 0.0
        pct_chg = 0.0
        try:
            data = context.data(symbol=symbol, frequency='1d', count=2, fields='pre_close')
            if data is not None and len(data) > 0:
                pre_close = float(data.iloc[-1].get('pre_close', 0) or 0)
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

        with _tick_lock:
            _tick_cache[ts_code] = tick_dict

    except Exception as e:
        logger.error(f"gm on_tick 处理失败: {e}")
