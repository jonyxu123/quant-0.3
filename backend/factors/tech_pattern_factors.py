""""技术形态因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:
    """"注册因子到全局注册"""

    return {

        'TP_BB_BREAKOUT': {

            'dimension': 'TP_',

            'name': '布林带突',

            'direction': 'positive',

            'compute_func': compute_tp_bb_breakout

        },

        'TP_BB_SQUEEZE': {

            'dimension': 'TP_',

            'name': '布林带收',

            'direction': 'positive',

            'compute_func': compute_tp_bb_squeeze

        },

        'TP_MACD_DIV_TOP': {

            'dimension': 'TP_',

            'name': 'MACD顶背',

            'direction': 'negative',

            'compute_func': compute_tp_macd_div_top

        },

        'TP_MACD_DIV_BOT': {

            'dimension': 'TP_',

            'name': 'MACD底背',

            'direction': 'positive',

            'compute_func': compute_tp_macd_div_bot

        },

        'TP_ENGULFING': {

            'dimension': 'TP_',

            'name': '吞没形',

            'direction': 'positive',

            'compute_func': compute_tp_engulfing

        },

        'TP_HAMMER_STAR': {

            'dimension': 'TP_',

            'name': '锤子/星线',

            'direction': 'positive',

            'compute_func': compute_tp_hammer_star

        },

        'TP_DOJI_STAR': {

            'dimension': 'TP_',

            'name': '十字',

            'direction': 'positive',

            'compute_func': compute_tp_doji_star

        },

        'TP_MA_CONVERGE': {

            'dimension': 'TP_',

            'name': '均线收敛',

            'direction': 'positive',

            'compute_func': compute_tp_ma_converge

        },

        'TP_CHANNEL_BREAK': {

            'dimension': 'TP_',

            'name': '通道突破',

            'direction': 'positive',

            'compute_func': compute_tp_channel_break

        },

        'TP_GAP_UP': {

            'dimension': 'TP_',

            'name': '跳空高开',

            'direction': 'positive',

            'compute_func': compute_tp_gap_up

        },

        'TP_VOL_COMPRESS': {

            'dimension': 'TP_',

            'name': '成交量压',

            'direction': 'positive',

            'compute_func': compute_tp_vol_compress

        },

        'TP_VCP_PATTERN': {

            'dimension': 'TP_',

            'name': 'VCP形',

            'direction': 'positive',

            'compute_func': compute_tp_vcp_pattern

        },

        'TP_FIB_RETRACEMENT': {

            'dimension': 'TP_',

            'name': '斐波那契回撤',

            'direction': 'positive',

            'compute_func': compute_tp_fib_retracement

        },

        'TP_THREE_WHITE': {

            'dimension': 'TP_',

            'name': '三白',

            'direction': 'positive',

            'compute_func': compute_tp_three_white

        },

    }

def compute(trade_date: str, conn, stock_pool: list = None) -> pd.DataFrame:

    """"计算该维度全部因子"""

    try:

        if stock_pool is None:

            stock_pool_df = conn.execute(

                "SELECT ts_code FROM stock_basic WHERE list_date IS NOT NULL"

            ).fetchdf()

            stock_pool = stock_pool_df['ts_code'].tolist()

        if not stock_pool:

            return pd.DataFrame()

        pool_str = "','".join(stock_pool)

        # 从daily取行情数        
        # 
        daily_df = conn.execute(f""""

            SELECT ts_code, trade_date, open, high, low, close, vol, amount, pct_chg, pre_close

            FROM daily

            WHERE trade_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, trade_date DESC

        """).fetchdf()

        # 从stk_factor_pro取技术指        
        try:

            stk_df = conn.execute(f""""

                SELECT ts_code, trade_date, boll_upper, boll_mid, boll_lower,

                       macd_dif, macd_dea, macd_hist,

                       ma_qfq_5, ma_qfq_10, ma_qfq_20, ma_qfq_60,

                       atr, volume_ratio

                FROM stk_factor_pro

                WHERE trade_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ts_code, trade_date DESC

            """).fetchdf()

        except Exception:

            stk_df = pd.DataFrame()

        # 构建当日数据

        today_daily = daily_df[daily_df['trade_date'] == trade_date].copy()

        today_daily = today_daily.set_index('ts_code') if not today_daily.empty else pd.DataFrame()

        today_stk = stk_df[stk_df['trade_date'] == trade_date].copy() if not stk_df.empty else pd.DataFrame()

        today_stk = today_stk.set_index('ts_code') if not today_stk.empty else pd.DataFrame()

        # 合并数据

        df = today_daily.copy() if not today_daily.empty else pd.DataFrame(index=stock_pool)

        if not today_stk.empty:

            common_idx = df.index.intersection(today_stk.index)

            for col in today_stk.columns:

                if col != 'trade_date':

                    df.loc[common_idx, col] = today_stk.loc[common_idx, col]

        # 构建历史数据字典

        price_dict = {}

        for ts_code, group in daily_df.groupby('ts_code'):

            price_dict[ts_code] = group.sort_values('trade_date', ascending=False)

        stk_dict = {}

        if not stk_df.empty:

            for ts_code, group in stk_df.groupby('ts_code'):

                stk_dict[ts_code] = group.sort_values('trade_date', ascending=False)

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['TP_BB_BREAKOUT'] = compute_tp_bb_breakout(df)

        result['TP_BB_SQUEEZE'] = compute_tp_bb_squeeze(df)

        result['TP_MACD_DIV_TOP'] = compute_tp_macd_div_top(df, stk_dict=stk_dict)

        result['TP_MACD_DIV_BOT'] = compute_tp_macd_div_bot(df, stk_dict=stk_dict)

        result['TP_ENGULFING'] = compute_tp_engulfing(df, price_dict=price_dict)

        result['TP_HAMMER_STAR'] = compute_tp_hammer_star(df, price_dict=price_dict)

        result['TP_DOJI_STAR'] = compute_tp_doji_star(df, price_dict=price_dict)

        result['TP_MA_CONVERGE'] = compute_tp_ma_converge(df)

        result['TP_CHANNEL_BREAK'] = compute_tp_channel_break(df, price_dict=price_dict)

        result['TP_GAP_UP'] = compute_tp_gap_up(df)

        result['TP_VOL_COMPRESS'] = compute_tp_vol_compress(df)

        result['TP_VCP_PATTERN'] = compute_tp_vcp_pattern(df, price_dict=price_dict)

        result['TP_FIB_RETRACEMENT'] = compute_tp_fib_retracement(df, price_dict=price_dict)

        result['TP_THREE_WHITE'] = compute_tp_three_white(df, price_dict=price_dict)

        return result

    except Exception as e:

        logger.error(f"计算技术形态因子失败：{e}")

        return pd.DataFrame()

def compute_tp_bb_breakout(df, **kwargs):

    """"TP_BB_BREAKOUT = 布林带突close > boll_upper)"""

    try:

        close = df['close'] if 'close' in df.columns else pd.Series(np.nan, index=df.index)

        upper = df['boll_upper'] if 'boll_upper' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(close > upper, 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_bb_squeeze(df, **kwargs):

    """"TP_BB_SQUEEZE = 布林带收缩 (带宽/中轨 < 阈值)"""

    try:

        upper = df['boll_upper'] if 'boll_upper' in df.columns else pd.Series(np.nan, index=df.index)

        lower = df['boll_lower'] if 'boll_lower' in df.columns else pd.Series(np.nan, index=df.index)

        mid = df['boll_mid'] if 'boll_mid' in df.columns else pd.Series(np.nan, index=df.index)

        bandwidth = np.where(mid > 0, (upper - lower) / mid, np.nan)

        return np.where(bandwidth < 0.05, 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_macd_div_top(df, stk_dict=None, **kwargs):

    """"TP_MACD_DIV_TOP = MACD顶背简
    """

    try:

        dif = df['macd_dif'] if 'macd_dif' in df.columns else pd.Series(np.nan, index=df.index)

        dea = df['macd_dea'] if 'macd_dea' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where((dif < dea) & (dif > 0), 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_macd_div_bot(df, stk_dict=None, **kwargs):

    """"TP_MACD_DIV_BOT = MACD底背简
    """

    try:

        dif = df['macd_dif'] if 'macd_dif' in df.columns else pd.Series(np.nan, index=df.index)

        dea = df['macd_dea'] if 'macd_dea' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where((dif > dea) & (dif < 0), 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_engulfing(df, price_dict=None, **kwargs):

    """"TP_ENGULFING = 吞没形态"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if price_dict:

            for ts_code in df.index:

                if ts_code in price_dict and len(price_dict[ts_code]) >= 2:

                    p = price_dict[ts_code]

                    c0, o0 = p.iloc[0]['close'], p.iloc[0]['open']

                    c1, o1 = p.iloc[1]['close'], p.iloc[1]['open']

                    # 看涨吞没

                    if c0 > o0 and c1 < o1 and c0 > o1 and o0 < c1:

                        result.loc[ts_code] = 1

                    # 看跌吞没

                    elif c0 < o0 and c1 > o1 and c0 < o1 and o0 > c1:

                        result.loc[ts_code] = -1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_hammer_star(df, price_dict=None, **kwargs):

    """"TP_HAMMER_STAR = 锤子/星线形态"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if price_dict:

            for ts_code in df.index:

                if ts_code in price_dict and len(price_dict[ts_code]) >= 1:

                    p = price_dict[ts_code].iloc[0]

                    body = abs(p['close'] - p['open'])

                    lower_shadow = min(p['close'], p['open']) - p['low']

                    upper_shadow = p['high'] - max(p['close'], p['open'])

                    if body > 0 and lower_shadow > 2 * body and upper_shadow < body:

                        result.loc[ts_code] = 1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_doji_star(df, price_dict=None, **kwargs):

    """"TP_DOJI_STAR = 十字"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if price_dict:

            for ts_code in df.index:

                if ts_code in price_dict and len(price_dict[ts_code]) >= 1:

                    p = price_dict[ts_code].iloc[0]

                    body = abs(p['close'] - p['open'])

                    total_range = p['high'] - p['low']

                    if total_range > 0 and body / total_range < 0.1:

                        result.loc[ts_code] = 1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_ma_converge(df, **kwargs):

    """"TP_MA_CONVERGE = 均线收敛(MA5/MA10/MA20接近)"""

    try:

        ma5 = df['ma_qfq_5'] if 'ma_qfq_5' in df.columns else pd.Series(np.nan, index=df.index)

        ma10 = df['ma_qfq_10'] if 'ma_qfq_10' in df.columns else pd.Series(np.nan, index=df.index)

        ma20 = df['ma_qfq_20'] if 'ma_qfq_20' in df.columns else pd.Series(np.nan, index=df.index)

        spread = np.maximum(np.abs(ma5 - ma10), np.abs(ma10 - ma20))

        return np.where(ma20 > 0, spread / ma20, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_channel_break(df, price_dict=None, **kwargs):

    """"TP_CHANNEL_BREAK = 通道突破(20日最高价突破)"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if price_dict:

            for ts_code in df.index:

                if ts_code in price_dict and len(price_dict[ts_code]) >= 20:

                    p = price_dict[ts_code]

                    high_20 = p.iloc[1:21]['high'].max()

                    if p.iloc[0]['close'] > high_20:

                        result.loc[ts_code] = 1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_gap_up(df, **kwargs):

    """"TP_GAP_UP = 跳空高开"""

    try:

        open_p = df['open'] if 'open' in df.columns else pd.Series(np.nan, index=df.index)

        pre_close = df['pre_close'] if 'pre_close' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where((pre_close > 0) & (open_p > pre_close * 1.02), 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_vol_compress(df, **kwargs):

    """"TP_VOL_COMPRESS = 成交量压缩（ATR收窄）"""

    try:

        atr = df['atr'] if 'atr' in df.columns else pd.Series(np.nan, index=df.index)

        close = df['close'] if 'close' in df.columns else pd.Series(np.nan, index=df.index)

        atr_ratio = np.where(close > 0, atr / close, np.nan)

        return np.where(atr_ratio < 0.02, 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_vcp_pattern(df, price_dict=None, **kwargs):

    """"TP_VCP_PATTERN = VCP形态（简化版）
    """

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if price_dict:

            for ts_code in df.index:

                if ts_code in price_dict and len(price_dict[ts_code]) >= 60:

                    p = price_dict[ts_code]

                    # 简化版：20日波动率 < 60日波动率的一半
                    vol_20 = p.iloc[:20]['pct_chg'].std()

                    vol_60 = p.iloc[:60]['pct_chg'].std()

                    if vol_60 > 0 and vol_20 < vol_60 * 0.5:

                        result.loc[ts_code] = 1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_fib_retracement(df, price_dict=None, **kwargs):

    """"TP_FIB_RETRACEMENT = 斐波那契回撤（简化版）
    """

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict:

            for ts_code in df.index:

                if ts_code in price_dict and len(price_dict[ts_code]) >= 60:

                    p = price_dict[ts_code]

                    high = p.iloc[:60]['high'].max()

                    low = p.iloc[:60]['low'].min()

                    cur = p.iloc[0]['close']

                    if high > low:

                        retrace = (high - cur) / (high - low)

                        result.loc[ts_code] = retrace

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tp_three_white(df, price_dict=None, **kwargs):

    """"TP_THREE_WHITE = 三白兵"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if price_dict:

            for ts_code in df.index:

                if ts_code in price_dict and len(price_dict[ts_code]) >= 3:

                    p = price_dict[ts_code]

                    if (p.iloc[0]['close'] > p.iloc[0]['open'] and

                        p.iloc[1]['close'] > p.iloc[1]['open'] and

                        p.iloc[2]['close'] > p.iloc[2]['open'] and

                        p.iloc[0]['close'] > p.iloc[1]['close'] and

                        p.iloc[1]['close'] > p.iloc[2]['close']):

                        result.loc[ts_code] = 1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

