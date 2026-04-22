""""增强技术因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'TECH_ADX': {

            'dimension': 'TECH_',

            'name': 'ADX趋势强度',

            'direction': 'positive',

            'compute_func': compute_tech_adx

        },

        'TECH_CCI': {

            'dimension': 'TECH_',

            'name': 'CCI商品通道指数',

            'direction': 'bidirectional',

            'compute_func': compute_tech_cci

        },

        'TECH_OBV': {

            'dimension': 'TECH_',

            'name': 'OBV能量',

            'direction': 'positive',

            'compute_func': compute_tech_obv

        },

        'TECH_ATR_RATIO': {

            'dimension': 'TECH_',

            'name': 'ATR比率',

            'direction': 'negative',

            'compute_func': compute_tech_atr_ratio

        },

        'TECH_ICHIMOKU': {

            'dimension': 'TECH_',

            'name': '一目均衡表',

            'direction': 'positive',

            'compute_func': compute_tech_ichimoku

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

        # 从 stk_factor_pro 取技术因子数据
        try:

            stk_df = conn.execute(f""""

                SELECT ts_code, trade_date, close, high, low, open,

                       atr_bfq, cci_bfq, obv_bfq, dmi_adx_bfq

                FROM stk_factor_pro

                WHERE trade_date = '{trade_date}'

                  AND ts_code IN ('{pool_str}')

            """).fetchdf()

            stk_df = stk_df.set_index('ts_code') if not stk_df.empty else pd.DataFrame()

        except Exception:

            stk_df = pd.DataFrame()

        # 从 daily 取行情数据用于一目均衡表计算

        daily_df = conn.execute(f""""

            SELECT ts_code, trade_date, open, high, low, close, vol

            FROM daily

            WHERE trade_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, trade_date DESC

        """).fetchdf()

        # 合并数据

        df = stk_df.copy() if not stk_df.empty else pd.DataFrame(index=stock_pool)

        # 构建历史数据字典

        price_dict = {}

        for ts_code, group in daily_df.groupby('ts_code'):

            price_dict[ts_code] = group.sort_values('trade_date', ascending=False)

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['TECH_ADX'] = compute_tech_adx(df)

        result['TECH_CCI'] = compute_tech_cci(df)

        result['TECH_OBV'] = compute_tech_obv(df)

        result['TECH_ATR_RATIO'] = compute_tech_atr_ratio(df)

        result['TECH_ICHIMOKU'] = compute_tech_ichimoku(df, price_dict=price_dict)

        return result

    except Exception as e:

        logger.error(f"计算增强技术因子失败：{e}")

        return pd.DataFrame()

def compute_tech_adx(df, **kwargs):

    """"TECH_ADX = ADX 趋势强度 (直接从 stk_factor_pro 取 dmi_adx_bfq)
    """

    try:

        return df['dmi_adx_bfq'].values if 'dmi_adx_bfq' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tech_cci(df, **kwargs):

    """"TECH_CCI = CCI 商品通道指数 (直接从 stk_factor_pro 取 cci_bfq)
    """

    try:

        return df['cci_bfq'].values if 'cci_bfq' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tech_obv(df, **kwargs):

    """"TECH_OBV = OBV 能量 (直接从 stk_factor_pro 取 obv_bfq)
    """

    try:

        return df['obv_bfq'].values if 'obv_bfq' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tech_atr_ratio(df, **kwargs):

    """"TECH_ATR_RATIO = ATR/收盘"""

    try:

        atr = df['atr_bfq'] if 'atr_bfq' in df.columns else pd.Series(np.nan, index=df.index)

        close = df['close'] if 'close' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(close > 0, atr / close, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_tech_ichimoku(df, price_dict=None, **kwargs):

    """"TECH_ICHIMOKU = 一目均衡表(需本地计算)"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict:

            for ts_code in df.index:

                if ts_code in price_dict and len(price_dict[ts_code]) >= 52:

                    p = price_dict[ts_code].sort_values('trade_date', ascending=True)

                    # 转换线 (9 日)

                    high_9 = p.tail(9)['high'].max()

                    low_9 = p.tail(9)['low'].min()

                    tenkan = (high_9 + low_9) / 2

                    # 基准线 (26 日)

                    high_26 = p.tail(26)['high'].max()

                    low_26 = p.tail(26)['low'].min()

                    kijun = (high_26 + low_26) / 2

                    # 先行带 A

                    span_a = (tenkan + kijun) / 2

                    # 先行带 B(52 日)

                    high_52 = p.tail(52)['high'].max()

                    low_52 = p.tail(52)['low'].min()

                    span_b = (high_52 + low_52) / 2

                    # 信号：价格在云层上方=1, 下方=-1, 中间=0

                    cur_close = p.iloc[-1]['close']

                    if cur_close > max(span_a, span_b):

                        result.loc[ts_code] = 1

                    elif cur_close < min(span_a, span_b):

                        result.loc[ts_code] = -1

                    else:

                        result.loc[ts_code] = 0

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

