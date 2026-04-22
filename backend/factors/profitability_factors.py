""""盈利能力因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'P_ROE': {

            'dimension': 'P_',

            'name': '净资产收益',

            'direction': 'positive',

            'compute_func': compute_p_roe

        },

        'P_ROA': {

            'dimension': 'P_',

            'name': '总资产收益率',

            'direction': 'positive',

            'compute_func': compute_p_roa

        },

        'P_GPM': {

            'dimension': 'P_',

            'name': '毛利',

            'direction': 'positive',

            'compute_func': compute_p_gpm

        },

        'P_NPM': {

            'dimension': 'P_',

            'name': '净利率',

            'direction': 'positive',

            'compute_func': compute_p_npm

        },

        'P_OCFM': {

            'dimension': 'P_',

            'name': '经营现金流利润率',

            'direction': 'positive',

            'compute_func': compute_p_ocfm

        },

        'P_ACCRUAL': {

            'dimension': 'P_',

            'name': '应计利润',

            'direction': 'negative',

            'compute_func': compute_p_accrual

        },

        'P_ROIC': {

            'dimension': 'P_',

            'name': '投入资本回报',

            'direction': 'positive',

            'compute_func': compute_p_roic

        },

        'P_EBITDA_M': {

            'dimension': 'P_',

            'name': 'EBITDA利润',

            'direction': 'positive',

            'compute_func': compute_p_ebitda_m

        },

        'P_EXPENSE_R': {

            'dimension': 'P_',

            'name': '费用',

            'direction': 'negative',

            'compute_func': compute_p_expense_r

        },

        'P_CFO_NI': {

            'dimension': 'P_',

            'name': '现金流净利润',

            'direction': 'positive',

            'compute_func': compute_p_cfo_ni

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

        # 从 fina_indicator 取财务指标
        fi_df = conn.execute(f""""

            SELECT ts_code, roe, roa_dp, grossprofit_margin, netprofit_margin,

                   q_ocf_to_sales, debt_to_assets, current_ratio, quick_ratio

            FROM fina_indicator

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ann_date DESC

        """).fetchdf()

        fi_df = fi_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 从 income 取利润数据
        inc_df = conn.execute(f""""

            SELECT ts_code, total_profit, n_income_attr_p, revenue, total_cogs,

                   sell_exp, admin_exp, fin_exp, int_exp

            FROM income

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ann_date DESC

        """).fetchdf()

        inc_df = inc_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 从 balancesheet 取资产负债数据（用于 ROIC）
        bal_df = conn.execute(f""""

            SELECT ts_code, total_assets, total_cur_liab

            FROM balancesheet

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ann_date DESC

        """).fetchdf()

        bal_df = bal_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 从cashflow取现金流数据

        cf_df = conn.execute(f""""

            SELECT ts_code, n_cashflow_act

            FROM cashflow

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ann_date DESC

        """).fetchdf()

        cf_df = cf_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 合并数据

        df = fi_df.copy()

        for src in [inc_df, cf_df, bal_df]:

            common_idx = df.index.intersection(src.index)

            for col in src.columns:

                df.loc[common_idx, col] = src.loc[common_idx, col]

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['P_ROE'] = compute_p_roe(df)

        result['P_ROA'] = compute_p_roa(df)

        result['P_GPM'] = compute_p_gpm(df)

        result['P_NPM'] = compute_p_npm(df)

        result['P_OCFM'] = compute_p_ocfm(df)

        result['P_ACCRUAL'] = compute_p_accrual(df)

        result['P_ROIC'] = compute_p_roic(df)

        result['P_EBITDA_M'] = compute_p_ebitda_m(df)

        result['P_EXPENSE_R'] = compute_p_expense_r(df)

        result['P_CFO_NI'] = compute_p_cfo_ni(df)

        return result

    except Exception as e:

        logger.error(f"计算盈利能力因子失败: {e}")

        return pd.DataFrame()

def compute_p_roe(df, **kwargs):

    """"P_ROE = ROE"""

    try:

        return df['roe'].values if 'roe' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_roa(df, **kwargs):

    """"P_ROA = ROA"""

    try:

        return df['roa_dp'].values if 'roa_dp' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_gpm(df, **kwargs):

    """"P_GPM = 毛利率"""

    try:

        return df['grossprofit_margin'].values if 'grossprofit_margin' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_npm(df, **kwargs):

    """"P_NPM = 净利率"""

    try:

        return df['netprofit_margin'].values if 'netprofit_margin' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_ocfm(df, **kwargs):

    """"P_OCFM = 经营现金营业收入"""

    try:

        return df['q_ocf_to_sales'].values if 'q_ocf_to_sales' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_accrual(df, **kwargs):

    """"P_ACCRUAL = (净利润 - 经营现金) / 营业收入"""

    try:

        netprofit = df['n_income_attr_p'] if 'n_income_attr_p' in df.columns else pd.Series(np.nan, index=df.index)

        n_cf_act = df['n_cashflow_act'] if 'n_cashflow_act' in df.columns else pd.Series(np.nan, index=df.index)

        revenue = df['revenue'] if 'revenue' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(revenue != 0, (netprofit - n_cf_act) / revenue.abs(), np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_roic(df, **kwargs):

    """"P_ROIC = EBIT / 投入资本，投入资本 = 总资产 - 流动负债"""

    try:

        total_profit = df['total_profit'] if 'total_profit' in df.columns else pd.Series(np.nan, index=df.index)

        int_exp = df['int_exp'] if 'int_exp' in df.columns else pd.Series(np.nan, index=df.index)

        total_assets = df['total_assets'] if 'total_assets' in df.columns else pd.Series(np.nan, index=df.index)

        total_cur_liab = df['total_cur_liab'] if 'total_cur_liab' in df.columns else pd.Series(np.nan, index=df.index)

        ebit = total_profit + int_exp.fillna(0)

        invested_capital = total_assets - total_cur_liab

        return np.where((invested_capital > 0) & np.isfinite(invested_capital),
                        ebit / invested_capital, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_ebitda_m(df, **kwargs):

    """"P_EBITDA_M = EBIT利润率 = EBIT / 营业收入（EBIT = 利润总额 + 利息支出）"""

    try:

        total_profit = df['total_profit'] if 'total_profit' in df.columns else pd.Series(np.nan, index=df.index)

        int_exp = df['int_exp'] if 'int_exp' in df.columns else pd.Series(np.nan, index=df.index)

        revenue = df['revenue'] if 'revenue' in df.columns else pd.Series(np.nan, index=df.index)

        ebit = total_profit + int_exp.fillna(0)

        return np.where((revenue != 0) & np.isfinite(revenue), ebit / revenue, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_expense_r(df, **kwargs):

    """"P_EXPENSE_R = (销售费管理费用+财务费用) / 营业收入"""

    try:

        sell_exp = df['sell_exp'] if 'sell_exp' in df.columns else pd.Series(0, index=df.index)

        admin_exp = df['admin_exp'] if 'admin_exp' in df.columns else pd.Series(0, index=df.index)

        fin_exp = df['fin_exp'] if 'fin_exp' in df.columns else pd.Series(0, index=df.index)

        revenue = df['revenue'] if 'revenue' in df.columns else pd.Series(np.nan, index=df.index)

        total_expense = sell_exp + admin_exp + fin_exp

        return np.where(revenue != 0, total_expense / revenue, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_p_cfo_ni(df, **kwargs):

    """"P_CFO_NI = 经营现金/ 净利润"""

    try:

        n_cf_act = df['n_cashflow_act'] if 'n_cashflow_act' in df.columns else pd.Series(np.nan, index=df.index)

        netprofit = df['n_income_attr_p'] if 'n_income_attr_p' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(netprofit != 0, n_cf_act / netprofit, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

