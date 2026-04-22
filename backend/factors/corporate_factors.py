""""公司行为因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'CORP_INSIDER_BUY': {

            'dimension': 'CORP_',

            'name': '内部人买',

            'direction': 'positive',

            'compute_func': compute_corp_insider_buy

        },

        'CORP_REPURCHASE': {

            'dimension': 'CORP_',

            'name': '回购',

            'direction': 'positive',

            'compute_func': compute_corp_repurchase

        },

        'CORP_NAME_CHANGE': {

            'dimension': 'CORP_',

            'name': '更名事件',

            'direction': 'bidirectional',

            'compute_func': compute_corp_name_change

        },

        'CORP_SUSPEND': {

            'dimension': 'CORP_',

            'name': '停牌事件',

            'direction': 'negative',

            'compute_func': compute_corp_suspend

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

        # 从stk_holdertrade取内部人交易

        try:

            ht_df = conn.execute(f""""

                SELECT ts_code, ann_date, buy_amount, sell_amount

                FROM stk_holdertrade

                WHERE ann_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ts_code, ann_date DESC

            """).fetchdf()

        except Exception:

            ht_df = pd.DataFrame()

        # 构建数据

        df = pd.DataFrame(index=stock_pool)

        insider_stats = {}

        if not ht_df.empty:

            for ts_code, group in ht_df.groupby('ts_code'):

                recent = group.head(5)

                buy = recent['buy_amount'].sum() if 'buy_amount' in recent.columns else 0

                sell = recent['sell_amount'].sum() if 'sell_amount' in recent.columns else 0

                insider_stats[ts_code] = {'net_buy': buy - sell}

        result = pd.DataFrame(index=df.index)

        result['CORP_INSIDER_BUY'] = compute_corp_insider_buy(df, insider_stats=insider_stats)

        result['CORP_REPURCHASE'] = compute_corp_repurchase(df)

        result['CORP_NAME_CHANGE'] = compute_corp_name_change(df)

        result['CORP_SUSPEND'] = compute_corp_suspend(df)

        return result

    except Exception as e:

        logger.error(f"计算公司行为因子失败: {e}")

        return pd.DataFrame()

def compute_corp_insider_buy(df, insider_stats=None, **kwargs):

    """"CORP_INSIDER_BUY = 内部人买"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if insider_stats:

            for ts_code in df.index:

                if ts_code in insider_stats:

                    result.loc[ts_code] = insider_stats[ts_code]['net_buy']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_corp_repurchase(df, **kwargs):

    """"CORP_REPURCHASE = 回购(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_corp_name_change(df, **kwargs):

    """"CORP_NAME_CHANGE = 更名事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_corp_suspend(df, **kwargs):

    """"CORP_SUSPEND = 停牌事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

