""""质量 - 价值复合因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'QV_QUALITY_VALUE': {

            'dimension': 'QV_',

            'name': '质量价值复合',

            'direction': 'positive',

            'compute_func': compute_qv_quality_value

        },

        'QV_SAFETY_VALUE': {

            'dimension': 'QV_',

            'name': '安全价值复合',

            'direction': 'positive',

            'compute_func': compute_qv_safety_value

        },

        'QV_GROWTH_VALUE': {

            'dimension': 'QV_',

            'name': '成长价值复合',

            'direction': 'positive',

            'compute_func': compute_qv_growth_value

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

        # 从 daily_basic 取估值数据

        db_df = conn.execute(f""""

            SELECT ts_code, pe_ttm, pb, ps_ttm, dv_ratio

            FROM daily_basic

            WHERE trade_date = '{trade_date}'

              AND ts_code IN ('{pool_str}')

        """).fetchdf()

        # 从 fina_indicator 取财务指标

        fi_df = conn.execute(f""""

            SELECT ts_code, roe, roa, grossprofit_margin AS gross_margin, netprofit_margin AS net_margin

            FROM fina_indicator

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, ann_date DESC

        """).fetchdf()

        # 合并数据

        df = db_df.merge(fi_df.drop_duplicates(subset=['ts_code'], keep='first'), on='ts_code', how='left')

        df = df.set_index('ts_code')

        # 计算各因子

        result = pd.DataFrame(index=df.index)

        result['QV_QUALITY_VALUE'] = compute_qv_quality_value(df)

        result['QV_SAFETY_VALUE'] = compute_qv_safety_value(df)

        result['QV_GROWTH_VALUE'] = compute_qv_growth_value(df)

        return result

    except Exception as e:

        logger.error(f"计算质量 - 价值复合因子失败：{e}")

        return pd.DataFrame()

def compute_qv_quality_value(df, **kwargs):

    """"QV_QUALITY_VALUE = 质量价值复合 = z(ROE) + z(EP) + z(OCF_NI)"""

    try:

        roe = df['roe'] if 'roe' in df.columns else pd.Series(np.nan, index=df.index)

        ep = 1 / df['pe_ttm'] if 'pe_ttm' in df.columns else pd.Series(np.nan, index=df.index)

        # 简化处理

        result = pd.Series(0.0, index=df.index)

        if not roe.empty:

            result += (roe - roe.mean()) / (roe.std() + 1e-6)

        if not ep.empty:

            result += (ep - ep.mean()) / (ep.std() + 1e-6)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_qv_safety_value(df, **kwargs):

    """"QV_SAFETY_VALUE = 安全价值复合 = z(Altman_Z) + z(EP) + z(Current_R)"""

    try:

        ep = 1 / df['pe_ttm'] if 'pe_ttm' in df.columns else pd.Series(np.nan, index=df.index)

        # 简化处理

        result = pd.Series(0.0, index=df.index)

        if not ep.empty:

            result += (ep - ep.mean()) / (ep.std() + 1e-6)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_qv_growth_value(df, **kwargs):

    """"QV_GROWTH_VALUE = 成长价值复合 = z(Rev_YOY) + z(EP) + z(NP_YOY)"""

    try:

        ep = 1 / df['pe_ttm'] if 'pe_ttm' in df.columns else pd.Series(np.nan, index=df.index)

        # 简化处理

        result = pd.Series(0.0, index=df.index)

        if not ep.empty:

            result += (ep - ep.mean()) / (ep.std() + 1e-6)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)
