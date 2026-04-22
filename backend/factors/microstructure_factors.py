""""微观结构因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'MICRO_BID_ASK_SPREAD': {

            'dimension': 'MICRO_',

            'name': '买卖价差',

            'direction': 'negative',

            'compute_func': compute_micro_bid_ask_spread

        },

        'MICRO_PRICE_IMPACT': {

            'dimension': 'MICRO_',

            'name': '价格冲击',

            'direction': 'negative',

            'compute_func': compute_micro_price_impact

        },

        'MICRO_ORDER_IMBALANCE': {

            'dimension': 'MICRO_',

            'name': '订单不平',

            'direction': 'bidirectional',

            'compute_func': compute_micro_order_imbalance

        },

        'MICRO_TICK_QUALITY': {

            'dimension': 'MICRO_',

            'name': '逐笔质量',

            'direction': 'positive',

            'compute_func': compute_micro_tick_quality

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

        df = pd.DataFrame(index=stock_pool)

        # 微观结构因子数据需AKShare

        result = pd.DataFrame(index=df.index)

        result['MICRO_BID_ASK_SPREAD'] = compute_micro_bid_ask_spread(df)

        result['MICRO_PRICE_IMPACT'] = compute_micro_price_impact(df)

        result['MICRO_ORDER_IMBALANCE'] = compute_micro_order_imbalance(df)

        result['MICRO_TICK_QUALITY'] = compute_micro_tick_quality(df)

        return result

    except Exception as e:

        logger.error(f"计算微观结构因子失败: {e}")

        return pd.DataFrame()

def compute_micro_bid_ask_spread(df, **kwargs):

    """"MICRO_BID_ASK_SPREAD = 买卖价差(需AKShare)"""

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_micro_price_impact(df, **kwargs):

    """"MICRO_PRICE_IMPACT = 价格冲击(需AKShare)"""

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_micro_order_imbalance(df, **kwargs):

    """"MICRO_ORDER_IMBALANCE = 订单不平需AKShare)"""

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_micro_tick_quality(df, **kwargs):

    """"MICRO_TICK_QUALITY = 逐笔质量(需AKShare)"""

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

