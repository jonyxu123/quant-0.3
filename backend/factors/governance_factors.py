""""治理因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'GOV_INDEPENDENT_R': {

            'dimension': 'GOV_',

            'name': '独立董事比例',

            'direction': 'positive',

            'compute_func': compute_gov_independent_r

        },

        'GOV_DUAL_ROLE': {

            'dimension': 'GOV_',

            'name': '两职合一',

            'direction': 'negative',

            'compute_func': compute_gov_dual_role

        },

        'GOV_VIOLATION': {

            'dimension': 'GOV_',

            'name': '违规事件',

            'direction': 'negative',

            'compute_func': compute_gov_violation

        },

        'GOV_ESG_SCORE': {

            'dimension': 'GOV_',

            'name': 'ESG评分',

            'direction': 'positive',

            'compute_func': compute_gov_esg_score

        },

        'GOV_AUDIT_OPINION': {

            'dimension': 'GOV_',

            'name': '审计意见',

            'direction': 'positive',

            'compute_func': compute_gov_audit_opinion

        },

        'GOV_INSIDER_TRADE': {

            'dimension': 'GOV_',

            'name': '内部人交',

            'direction': 'negative',

            'compute_func': compute_gov_insider_trade

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

        # 从fina_indicator取审计意见等

        try:

            fi_df = conn.execute(f""""

                SELECT ts_code, audit_opinion

                FROM fina_indicator

                WHERE ann_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ann_date DESC

            """).fetchdf()

            fi_df = fi_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        except Exception:

            fi_df = pd.DataFrame()

        # 合并数据

        df = fi_df.copy() if not fi_df.empty else pd.DataFrame(index=stock_pool)

        result = pd.DataFrame(index=df.index)

        result['GOV_INDEPENDENT_R'] = compute_gov_independent_r(df)

        result['GOV_DUAL_ROLE'] = compute_gov_dual_role(df)

        result['GOV_VIOLATION'] = compute_gov_violation(df)

        result['GOV_ESG_SCORE'] = compute_gov_esg_score(df)

        result['GOV_AUDIT_OPINION'] = compute_gov_audit_opinion(df)

        result['GOV_INSIDER_TRADE'] = compute_gov_insider_trade(df)

        return result

    except Exception as e:

        logger.error(f"计算治理因子失败: {e}")

        return pd.DataFrame()

def compute_gov_independent_r(df, **kwargs):

    """"GOV_INDEPENDENT_R = 独立董事比例(需AKShare)"""

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_gov_dual_role(df, **kwargs):

    """"GOV_DUAL_ROLE = 两职合一(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_gov_violation(df, **kwargs):

    """"GOV_VIOLATION = 违规事件(需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_gov_esg_score(df, **kwargs):

    """"GOV_ESG_SCORE = ESG评分(需AKShare)"""

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_gov_audit_opinion(df, **kwargs):

    """"GOV_AUDIT_OPINION = 审计意见"""

    try:

        audit = df['audit_opinion'] if 'audit_opinion' in df.columns else pd.Series(np.nan, index=df.index)

        # 标准无保留意1, 其他=0

        return np.where(audit == '标准无保留意', 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_gov_insider_trade(df, **kwargs):

    """"GOV_INSIDER_TRADE = 内部人交需AKShare)"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

