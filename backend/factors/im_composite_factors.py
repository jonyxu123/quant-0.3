""""机构-动量复合因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'IM_INST_MOM': {

            'dimension': 'IM_',

            'name': '机构动量复合',

            'direction': 'positive',

            'compute_func': compute_im_inst_mom

        },

        'IM_NORTH_MOM': {

            'dimension': 'IM_',

            'name': '北向动量复合',

            'direction': 'positive',

            'compute_func': compute_im_north_mom

        },

        'IM_FLOW_MOM': {

            'dimension': 'IM_',

            'name': '资金流动量复',

            'direction': 'positive',

            'compute_func': compute_im_flow_mom

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

        # 依赖I_和MF_因子，需要先计算

        from backend.factors.institution_factors import compute as institution_compute

        from backend.factors.money_flow_factors import compute as moneyflow_compute

        from backend.factors.momentum_factors import compute as momentum_compute

        i_df = institution_compute(trade_date, conn, stock_pool)

        mf_df = moneyflow_compute(trade_date, conn, stock_pool)

        m_df = momentum_compute(trade_date, conn, stock_pool)

        # 合并所有基础因子

        base_df = pd.DataFrame(index=stock_pool)

        for src in [i_df, mf_df, m_df]:

            if not src.empty:

                common_idx = base_df.index.intersection(src.index)

                for col in src.columns:

                    base_df.loc[common_idx, col] = src.loc[common_idx, col]

        # 标准化各因子

        def zscore(s):

            if s.std() > 0:

                return (s - s.mean()) / s.std()

            return s * 0

        result = pd.DataFrame(index=base_df.index)

        result['IM_INST_MOM'] = compute_im_inst_mom(base_df, zscore=zscore)

        result['IM_NORTH_MOM'] = compute_im_north_mom(base_df, zscore=zscore)

        result['IM_FLOW_MOM'] = compute_im_flow_mom(base_df, zscore=zscore)

        return result

    except Exception as e:

        logger.error(f"计算机构-动量复合因子失败: {e}")

        return pd.DataFrame()

def compute_im_inst_mom(df, zscore=None, **kwargs):

    """"IM_INST_MOM = 机构动量复合 = z(FUND_HOLD_R) + z(MOM_6M)"""

    try:

        if zscore is None:

            return pd.Series(np.nan, index=df.index)

        components = []

        for col in ['I_FUND_HOLD_R', 'M_MOM_6M']:

            if col in df.columns:

                components.append(zscore(df[col].astype(float)))

        if components:

            return sum(components)

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_im_north_mom(df, zscore=None, **kwargs):

    """"IM_NORTH_MOM = 北向动量复合 = z(NORTH_HOLD_R) + z(MOM_3M)"""

    try:

        if zscore is None:

            return pd.Series(np.nan, index=df.index)

        components = []

        for col in ['I_NORTH_HOLD_R', 'M_MOM_3M']:

            if col in df.columns:

                components.append(zscore(df[col].astype(float)))

        if components:

            return sum(components)

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_im_flow_mom(df, zscore=None, **kwargs):

    """"IM_FLOW_MOM = 资金流动量复= z(MAIN_NET_IN) + z(MOM_3M)"""

    try:

        if zscore is None:

            return pd.Series(np.nan, index=df.index)

        components = []

        for col in ['MF_MAIN_NET_IN', 'M_MOM_3M']:

            if col in df.columns:

                components.append(zscore(df[col].astype(float)))

        if components:

            return sum(components)

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

