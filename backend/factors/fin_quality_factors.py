""""财务质量因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'FQ_ACCRUAL_STABILITY': {

            'dimension': 'FQ_',

            'name': '应计利润稳定',

            'direction': 'positive',

            'compute_func': compute_fq_accrual_stability

        },

        'FQ_RESTATEMENT': {

            'dimension': 'FQ_',

            'name': '财务重述',

            'direction': 'negative',

            'compute_func': compute_fq_restatement

        },

        'FQ_AUDIT_DELAY': {

            'dimension': 'FQ_',

            'name': '审计延迟',

            'direction': 'negative',

            'compute_func': compute_fq_audit_delay

        },

        'FQ_RELATED_PARTY_R': {

            'dimension': 'FQ_',

            'name': '关联交易比例',

            'direction': 'negative',

            'compute_func': compute_fq_related_party_r

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

        # 从income取利润数据        
        inc_df = conn.execute(f""""

            SELECT ts_code, n_income_attr_p, revenue, ann_date

            FROM income

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, ann_date DESC

        """).fetchdf()

        # 从cashflow取现金流

        cf_df = conn.execute(f""""

            SELECT ts_code, n_cashflow_act, ann_date

            FROM cashflow

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, ann_date DESC

        """).fetchdf()

        # 合并数据

        inc_latest = inc_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not inc_df.empty else pd.DataFrame()

        cf_latest = cf_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not cf_df.empty else pd.DataFrame()

        df = inc_latest.copy() if not inc_latest.empty else pd.DataFrame(index=stock_pool)

        if not cf_latest.empty:

            common_idx = df.index.intersection(cf_latest.index)

            for col in cf_latest.columns:

                if col != 'ann_date':

                    df.loc[common_idx, col] = cf_latest.loc[common_idx, col]

        # 计算应计利润稳定多期)

        accrual_stats = _compute_accrual_stability(inc_df, cf_df, df.index)

        result = pd.DataFrame(index=df.index)

        result['FQ_ACCRUAL_STABILITY'] = compute_fq_accrual_stability(df, accrual_stats=accrual_stats)

        result['FQ_RESTATEMENT'] = compute_fq_restatement(df)

        result['FQ_AUDIT_DELAY'] = compute_fq_audit_delay(df)

        result['FQ_RELATED_PARTY_R'] = compute_fq_related_party_r(df)

        return result

    except Exception as e:

        logger.error(f"计算财务质量因子失败: {e}")

        return pd.DataFrame()

def _compute_accrual_stability(inc_df: pd.DataFrame, cf_df: pd.DataFrame, index) -> dict:

    """"计算应计利润稳定"""

    stats = {}

    try:

        if inc_df.empty or cf_df.empty:

            return stats

        inc_latest = inc_df.drop_duplicates(subset='ts_code', keep='first')

        cf_latest = cf_df.drop_duplicates(subset='ts_code', keep='first')

        merged = inc_latest.merge(cf_latest, on='ts_code', suffixes=('_inc', '_cf'))

        for _, row in merged.iterrows():

            ts_code = row['ts_code']

            np_val = row.get('n_income_attr_p', np.nan)

            ocf_val = row.get('n_cashflow_act', np.nan)

            rev_val = row.get('revenue', np.nan)

            if np.isfinite(np_val) and np.isfinite(ocf_val) and np.isfinite(rev_val) and rev_val != 0:

                accrual = (np_val - ocf_val) / abs(rev_val)

                stats[ts_code] = {'accrual': accrual}

    except Exception as e:

        logger.warning(f"计算应计利润稳定性失 {e}")

    return stats

def compute_fq_accrual_stability(df, accrual_stats=None, **kwargs):

    """"FQ_ACCRUAL_STABILITY = 应计利润稳定"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if accrual_stats:

            for ts_code in df.index:

                if ts_code in accrual_stats:

                    # 稳定= 1 - |应计利润率|

                    accrual = abs(accrual_stats[ts_code]['accrual'])

                    result.loc[ts_code] = max(0, 1 - accrual)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_fq_restatement(df, **kwargs):

    """"FQ_RESTATEMENT = 财务重述(简
    """

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_fq_audit_delay(df, **kwargs):

    """"FQ_AUDIT_DELAY = 审计延迟(简
    """

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_fq_related_party_r(df, **kwargs):

    """"FQ_RELATED_PARTY_R = 关联交易比例(简
    """

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

