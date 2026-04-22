""""动量-情绪复合因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'MS_MOM_SENT': {

            'dimension': 'MS_',

            'name': '动量情绪复合',

            'direction': 'positive',

            'compute_func': compute_ms_mom_sent

        },

        'MS_TURNOVER_MOM': {

            'dimension': 'MS_',

            'name': '换手动量复合',

            'direction': 'positive',

            'compute_func': compute_ms_turnover_mom

        },

        'MS_CROWDING_MOM': {

            'dimension': 'MS_',

            'name': '拥挤动量复合',

            'direction': 'negative',

            'compute_func': compute_ms_crowding_mom

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

        # 依赖M_和SENT_因子，需要先计算

        from backend.factors.momentum_factors import compute as momentum_compute

        from backend.factors.sentiment_factors import compute as sentiment_compute

        m_df = momentum_compute(trade_date, conn, stock_pool)

        sent_df = sentiment_compute(trade_date, conn, stock_pool)

        # 合并所有基础因子

        base_df = pd.DataFrame(index=stock_pool)

        for src in [m_df, sent_df]:

            if not src.empty:

                common_idx = base_df.index.intersection(src.index)

                for col in src.columns:

                    base_df.loc[common_idx, col] = src.loc[common_idx, col]

        # 标准化各因子

        def zscore(s):

            if s.std() > 0:

                return (s - s.mean()) / s.std()

            return s * 0

        # 计算各因子
        result = pd.DataFrame(index=base_df.index)

        result['MS_MOM_SENT'] = compute_ms_mom_sent(base_df, zscore=zscore)

        result['MS_TURNOVER_MOM'] = compute_ms_turnover_mom(base_df, zscore=zscore)

        result['MS_CROWDING_MOM'] = compute_ms_crowding_mom(base_df, zscore=zscore)

        return result

    except Exception as e:

        logger.error(f"计算动量-情绪复合因子失败: {e}")

        return pd.DataFrame()

def compute_ms_mom_sent(df, zscore=None, **kwargs):

    """"MS_MOM_SENT = 动量情绪复合 = z(MOM_6M) + z(TURNOVER_Z)"""

    try:

        if zscore is None:

            return pd.Series(np.nan, index=df.index)

        components = []

        for col in ['M_MOM_6M', 'SENT_TURNOVER_Z']:

            if col in df.columns:

                components.append(zscore(df[col].astype(float)))

        if components:

            return sum(components)

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ms_turnover_mom(df, zscore=None, **kwargs):

    """"MS_TURNOVER_MOM = 换手动量复合 = z(TURNOVER) + z(MOM_3M)"""

    try:

        if zscore is None:

            return pd.Series(np.nan, index=df.index)

        components = []

        for col in ['SENT_TURNOVER_Z', 'M_MOM_3M']:

            if col in df.columns:

                components.append(zscore(df[col].astype(float)))

        if components:

            return sum(components)

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_ms_crowding_mom(df, zscore=None, **kwargs):

    """"MS_CROWDING_MOM = 拥挤动量复合 = z(CROWDING) - z(MOM_6M)"""

    try:

        if zscore is None:

            return pd.Series(np.nan, index=df.index)

        crowding = zscore(df['SENT_CROWDING'].astype(float)) if 'SENT_CROWDING' in df.columns else None

        mom = zscore(df['M_MOM_6M'].astype(float)) if 'M_MOM_6M' in df.columns else None

        if crowding is not None and mom is not None:

            return crowding - mom

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

