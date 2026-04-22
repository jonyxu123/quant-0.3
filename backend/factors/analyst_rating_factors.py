"""分析师评级因子模"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    return {

        'ANALYST_BUY_R': {

            'dimension': 'ANALYST_',

            'name': '买入评级占比',

            'direction': 'positive',

            'compute_func': compute_analyst_buy_r

        },

        'ANALYST_AVG_SCORE': {

            'dimension': 'ANALYST_',

            'name': '平均评级分数',

            'direction': 'positive',

            'compute_func': compute_analyst_avg_score

        },

        'ANALYST_UPGRADE_1M': {

            'dimension': 'ANALYST_',

            'name': '1月上调次',

            'direction': 'positive',

            'compute_func': compute_analyst_upgrade_1m

        },

        'ANALYST_CONSENSUS_CHG': {

            'dimension': 'ANALYST_',

            'name': '一致预期变',

            'direction': 'positive',

            'compute_func': compute_analyst_consensus_chg

        },

    }

def compute(trade_date: str, conn, stock_pool: list = None) -> pd.DataFrame:


    try:

        if stock_pool is None:

            stock_pool_df = conn.execute(

                "SELECT ts_code FROM stock_basic WHERE list_date IS NOT NULL"

            ).fetchdf()

            stock_pool = stock_pool_df['ts_code'].tolist()

        if not stock_pool:

            return pd.DataFrame()

        df = pd.DataFrame(index=stock_pool)

        # 分析师评级数据需AKShare

        result = pd.DataFrame(index=df.index)

        result['ANALYST_BUY_R'] = compute_analyst_buy_r(df)

        result['ANALYST_AVG_SCORE'] = compute_analyst_avg_score(df)

        result['ANALYST_UPGRADE_1M'] = compute_analyst_upgrade_1m(df)

        result['ANALYST_CONSENSUS_CHG'] = compute_analyst_consensus_chg(df)

        return result

    except Exception as e:

        logger.error(f"计算分析师评级因子失 {e}")

        return pd.DataFrame()

def compute_analyst_buy_r(df, **kwargs):


    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_analyst_avg_score(df, **kwargs):


    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_analyst_upgrade_1m(df, **kwargs):


    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_analyst_consensus_chg(df, **kwargs):


    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

