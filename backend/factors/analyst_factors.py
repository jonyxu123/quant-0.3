""""分析师预期因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'E_FEP': {

            'dimension': 'E_',

            'name': '预期盈利收益',

            'direction': 'positive',

            'compute_func': compute_e_fep

        },

        'E_SUE': {

            'dimension': 'E_',

            'name': '标准化未预期盈利',

            'direction': 'positive',

            'compute_func': compute_e_sue

        },

        'E_REVISION_1M': {

            'dimension': 'E_',

            'name': '1月盈利修',

            'direction': 'positive',

            'compute_func': compute_e_revision_1m

        },

        'E_REVISION_3M': {

            'dimension': 'E_',

            'name': '3月盈利修',

            'direction': 'positive',

            'compute_func': compute_e_revision_3m

        },

        'E_DISPERSION': {

            'dimension': 'E_',

            'name': '预测离散',

            'direction': 'negative',

            'compute_func': compute_e_dispersion

        },

        'E_COVERAGE': {

            'dimension': 'E_',

            'name': '分析师覆盖数',

            'direction': 'positive',

            'compute_func': compute_e_coverage

        },

        'E_SURPRISE': {

            'dimension': 'E_',

            'name': '业绩惊喜',

            'direction': 'positive',

            'compute_func': compute_e_surprise

        },

        'E_LONG_TERM_G': {

            'dimension': 'E_',

            'name': '长期增长预期',

            'direction': 'positive',

            'compute_func': compute_e_long_term_g

        },

        'E_EGRLF': {

            'dimension': 'E_',

            'name': '长期预期增长',

            'direction': 'positive',

            'compute_func': compute_e_egrlf

        },

        'E_NP_YOY_CONSENSUS': {

            'dimension': 'E_',

            'name': '一致预期净利润同比',

            'direction': 'positive',

            'compute_func': compute_e_np_yoy_consensus

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

        pool_str = "','".join(stock_pool)

        # 从forecast取分析师预测数据
        # 注意：forecast表字段为net_profit_min/net_profit_max，不是predict_net_profit
        try:
            fc_df = conn.execute(f"""
                SELECT ts_code, end_date, net_profit_min, net_profit_max, type, p_change_min, p_change_max
                FROM forecast
                WHERE ann_date <= '{trade_date}'
                  AND ts_code IN ('{pool_str}')
                ORDER BY ts_code, ann_date DESC
            """).fetchdf()
        except Exception:
            fc_df = pd.DataFrame()

        # 从income取实际利润
        inc_df = conn.execute(f"""

            SELECT ts_code, n_income_attr_p, total_profit

            FROM income

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ann_date DESC

        """).fetchdf()

        inc_df = inc_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not inc_df.empty else pd.DataFrame()

        # 取 1和3个月前的历史forecast（用于修正幅度）
        from datetime import datetime, timedelta
        try:
            date_1m = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
            date_3m = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=90)).strftime('%Y%m%d')
            hist_fc_1m = conn.execute(f"""
                SELECT ts_code, net_profit_min, net_profit_max
                FROM forecast
                WHERE ann_date <= '{date_1m}'
                  AND ts_code IN ('{pool_str}')
                ORDER BY ts_code, ann_date DESC
            """).fetchdf()
            hist_fc_3m = conn.execute(f"""
                SELECT ts_code, net_profit_min, net_profit_max
                FROM forecast
                WHERE ann_date <= '{date_3m}'
                  AND ts_code IN ('{pool_str}')
                ORDER BY ts_code, ann_date DESC
            """).fetchdf()
        except Exception:
            hist_fc_1m = pd.DataFrame()
            hist_fc_3m = pd.DataFrame()

        # 从daily_basic取市        
        db_df = conn.execute(f"""

            SELECT ts_code, total_mv

            FROM daily_basic

            WHERE trade_date = '{trade_date}'

              AND ts_code IN ('{pool_str}')

        """).fetchdf()

        db_df = db_df.set_index('ts_code') if not db_df.empty else pd.DataFrame()

        # 合并数据

        df = db_df.copy() if not db_df.empty else pd.DataFrame(index=stock_pool)

        for src in [inc_df]:

            if not src.empty:

                common_idx = df.index.intersection(src.index)

                for col in src.columns:

                    df.loc[common_idx, col] = src.loc[common_idx, col]

        # 计算分析师一致预期
        consensus = _compute_consensus(fc_df, hist_fc_1m, hist_fc_3m, df.index)

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['E_FEP'] = compute_e_fep(df, consensus=consensus)

        result['E_SUE'] = compute_e_sue(df, consensus=consensus)

        result['E_REVISION_1M'] = compute_e_revision_1m(df, consensus=consensus)

        result['E_REVISION_3M'] = compute_e_revision_3m(df, consensus=consensus)

        result['E_DISPERSION'] = compute_e_dispersion(df, consensus=consensus)

        result['E_COVERAGE'] = compute_e_coverage(df, consensus=consensus)

        result['E_SURPRISE'] = compute_e_surprise(df, consensus=consensus)

        result['E_LONG_TERM_G'] = compute_e_long_term_g(df, consensus=consensus)

        result['E_EGRLF'] = compute_e_egrlf(df, consensus=consensus)

        result['E_NP_YOY_CONSENSUS'] = compute_e_np_yoy_consensus(df, consensus=consensus)

        return result

    except Exception as e:

        logger.error(f"计算分析师预期因子失败：{e}")

        return pd.DataFrame()

def _compute_consensus(fc_df: pd.DataFrame, hist_fc_1m: pd.DataFrame,
                       hist_fc_3m: pd.DataFrame, index) -> dict:

    """计算分析师一致预期

    使用net_profit_min和net_profit_max的平均值作为预测净利润
    """

    consensus = {}

    try:

        if fc_df.empty:
            return consensus

        # 计算当前一致预期（使用min/max的平均值）
        def calc_mean_prediction(group):
            """计算预测净利润均值 = (min + max) / 2"""
            means = []
            for _, row in group.iterrows():
                min_val = row.get('net_profit_min', np.nan)
                max_val = row.get('net_profit_max', np.nan)
                if pd.notna(min_val) and pd.notna(max_val):
                    means.append((min_val + max_val) / 2)
                elif pd.notna(min_val):
                    means.append(min_val)
                elif pd.notna(max_val):
                    means.append(max_val)
            return means

        # 历史均值（用于修正幅度）
        hist_mean_1m = {}
        if not hist_fc_1m.empty:
            for ts_code, grp in hist_fc_1m.groupby('ts_code'):
                means = calc_mean_prediction(grp)
                if len(means) > 0:
                    hist_mean_1m[ts_code] = np.mean(means)

        hist_mean_3m = {}
        if not hist_fc_3m.empty:
            for ts_code, grp in hist_fc_3m.groupby('ts_code'):
                means = calc_mean_prediction(grp)
                if len(means) > 0:
                    hist_mean_3m[ts_code] = np.mean(means)

        for ts_code, group in fc_df.groupby('ts_code'):
            means = calc_mean_prediction(group)

            if len(means) > 0:
                consensus[ts_code] = {
                    'mean_np': np.mean(means),
                    'std_np': np.std(means) if len(means) > 1 else 0,
                    'count': len(means),
                    'hist_mean_1m': hist_mean_1m.get(ts_code, np.nan),
                    'hist_mean_3m': hist_mean_3m.get(ts_code, np.nan),
                }

    except Exception as e:

        logger.warning(f"计算一致预期失败：{e}")

    return consensus

def compute_e_fep(df, consensus=None, **kwargs):

    """E_FEP = 预期盈利/总市值"""

    try:

        result = pd.Series(np.nan, index=df.index)

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus and 'mean_np' in consensus[ts_code]:

                    mv = total_mv.loc[ts_code] if ts_code in total_mv.index else np.nan

                    if np.isfinite(mv) and mv > 0:

                        result.loc[ts_code] = consensus[ts_code]['mean_np'] / (mv * 10000)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_sue(df, consensus=None, **kwargs):

    """E_SUE = 标准化未预期盈利"""

    try:

        result = pd.Series(np.nan, index=df.index)

        netprofit = df['n_income_attr_p'] if 'n_income_attr_p' in df.columns else pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus and 'mean_np' in consensus[ts_code]:

                    actual = netprofit.loc[ts_code] if ts_code in netprofit.index else np.nan

                    expected = consensus[ts_code]['mean_np']

                    std = consensus[ts_code].get('std_np', np.nan)

                    if np.isfinite(actual) and np.isfinite(std) and std > 0:

                        result.loc[ts_code] = (actual - expected) / std

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_revision_1m(df, consensus=None, **kwargs):

    """E_REVISION_1M = 1月内一致预期修正幅度 = (mean_now - mean_1m_ago) / |mean_1m_ago|"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus:

                    cur = consensus[ts_code].get('mean_np', np.nan)

                    hist = consensus[ts_code].get('hist_mean_1m', np.nan)

                    if np.isfinite(cur) and np.isfinite(hist) and hist != 0:

                        result.loc[ts_code] = (cur - hist) / abs(hist)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_revision_3m(df, consensus=None, **kwargs):

    """E_REVISION_3M = 3月内一致预期修正幅度 = (mean_now - mean_3m_ago) / |mean_3m_ago|"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus:

                    cur = consensus[ts_code].get('mean_np', np.nan)

                    hist = consensus[ts_code].get('hist_mean_3m', np.nan)

                    if np.isfinite(cur) and np.isfinite(hist) and hist != 0:

                        result.loc[ts_code] = (cur - hist) / abs(hist)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_dispersion(df, consensus=None, **kwargs):

    """E_DISPERSION = 预测离散"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus and 'std_np' in consensus[ts_code]:

                    mean_np = consensus[ts_code].get('mean_np', np.nan)

                    std_np = consensus[ts_code]['std_np']

                    if np.isfinite(mean_np) and mean_np != 0:

                        result.loc[ts_code] = std_np / abs(mean_np)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_coverage(df, consensus=None, **kwargs):

    """E_COVERAGE = 分析师覆盖数"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus:

                    result.loc[ts_code] = consensus[ts_code].get('count', 0)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_surprise(df, consensus=None, **kwargs):

    """E_SURPRISE = 相对惊喜度 = (实际 - 预测均值) / |预测均值|（SUE用std归一化，此处用预测值本身归一化）"""

    try:

        result = pd.Series(np.nan, index=df.index)

        actual_np = df['n_income_attr_p'] if 'n_income_attr_p' in df.columns else pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus and 'mean_np' in consensus[ts_code]:

                    expected = consensus[ts_code]['mean_np']

                    actual = actual_np.loc[ts_code] if ts_code in actual_np.index else np.nan

                    if np.isfinite(actual) and np.isfinite(expected) and expected != 0:

                        result.loc[ts_code] = (actual - expected) / abs(expected)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_long_term_g(df, consensus=None, **kwargs):

    """E_LONG_TERM_G = 预期益盈收益率与当前盖度的复合： FEP * ln(1 + coverage)"""

    try:

        result = pd.Series(np.nan, index=df.index)

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus:

                    mean_np = consensus[ts_code].get('mean_np', np.nan)

                    count = consensus[ts_code].get('count', 0)

                    mv = total_mv.loc[ts_code] if ts_code in total_mv.index else np.nan

                    if np.isfinite(mean_np) and np.isfinite(mv) and mv > 0 and count > 0:

                        fep = mean_np / (mv * 10000)

                        result.loc[ts_code] = fep * np.log1p(count)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_egrlf(df, consensus=None, **kwargs):

    """E_EGRLF = 覆盖度加权盈利收益率 = FEP * sqrt(coverage)"""

    try:

        result = pd.Series(np.nan, index=df.index)

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus:

                    mean_np = consensus[ts_code].get('mean_np', np.nan)

                    count = consensus[ts_code].get('count', 0)

                    mv = total_mv.loc[ts_code] if ts_code in total_mv.index else np.nan

                    if np.isfinite(mean_np) and np.isfinite(mv) and mv > 0 and count > 0:

                        fep = mean_np / (mv * 10000)

                        result.loc[ts_code] = fep * np.sqrt(count)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_e_np_yoy_consensus(df, consensus=None, **kwargs):

    """E_NP_YOY_CONSENSUS = 预测净利润 / 实际净利润 - 1（预期增长幅）"""

    try:

        result = pd.Series(np.nan, index=df.index)

        actual_np = df['n_income_attr_p'] if 'n_income_attr_p' in df.columns else pd.Series(np.nan, index=df.index)

        if consensus:

            for ts_code in df.index:

                if ts_code in consensus:

                    mean_np = consensus[ts_code].get('mean_np', np.nan)

                    actual = actual_np.loc[ts_code] if ts_code in actual_np.index else np.nan

                    if np.isfinite(mean_np) and np.isfinite(actual) and actual != 0:

                        result.loc[ts_code] = (mean_np - actual) / abs(actual)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

