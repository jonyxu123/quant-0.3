""""质量因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:
    """"注册因子到全局注册"""

    return {

        'Q_ACCRUAL': {

            'dimension': 'Q_',

            'name': '应计利润质量',

            'direction': 'negative',

            'compute_func': compute_q_accrual

        },

        'Q_STABILITY': {

            'dimension': 'Q_',

            'name': '盈利稳定性',

            'direction': 'positive',

            'compute_func': compute_q_stability

        },

        'Q_OCF_NI': {

            'dimension': 'Q_',

            'name': '现金流质量',

            'direction': 'positive',

            'compute_func': compute_q_ocf_ni

        },

        'Q_ALTMAN_Z': {

            'dimension': 'Q_',

            'name': 'Altman Z分数',

            'direction': 'positive',

            'compute_func': compute_q_altman_z

        },

        'Q_PETTI_Z': {

            'dimension': 'Q_',

            'name': 'Altman Z"分数',

            'direction': 'positive',

            'compute_func': compute_q_petti_z

        },

        'Q_CURRENT_R': {

            'dimension': 'Q_',

            'name': '流动比率',

            'direction': 'positive',

            'compute_func': compute_q_current_r

        },

        'Q_QUICK_R': {

            'dimension': 'Q_',

            'name': '速动比率',

            'direction': 'positive',

            'compute_func': compute_q_quick_r

        },

        'Q_DEBT_R': {

            'dimension': 'Q_',

            'name': '资产负债率',

            'direction': 'negative',

            'compute_func': compute_q_debt_r

        },

        'Q_ICR': {

            'dimension': 'Q_',

            'name': '利息覆盖倍数',

            'direction': 'positive',

            'compute_func': compute_q_icr

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

        # 从fina_indicator取财务指标（多期，用于ROE稳定性计算）
        fi_multi_df = conn.execute(f""""

            SELECT ts_code, ann_date, roe, current_ratio, quick_ratio, debt_to_assets,

                   q_ocf_to_sales, netprofit_margin

            FROM fina_indicator

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, ann_date DESC

        """).fetchdf()

        # 计算近8期ROE标准差，用于Q_STABILITY
        roe_std_dict = {}
        for ts_code, grp in fi_multi_df.groupby('ts_code'):
            roe_vals = pd.to_numeric(grp['roe'], errors='coerce').dropna().values[:8]
            if len(roe_vals) >= 3:
                roe_std_dict[ts_code] = float(np.std(roe_vals))

        fi_df = fi_multi_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 从income取利润数据        
        inc_df = conn.execute(f""""

            SELECT ts_code, n_income_attr_p, total_profit, revenue, int_exp

            FROM income

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ann_date DESC

        """).fetchdf()

        inc_df = inc_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 从cashflow取现金流

        cf_df = conn.execute(f""""

            SELECT ts_code, n_cashflow_act

            FROM cashflow

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ann_date DESC

        """).fetchdf()

        cf_df = cf_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 从balance取资产负债        
        bal_df = conn.execute(f""""

            SELECT ts_code, total_assets, total_liab, total_hldr_eqy_exc_min_int,

                   total_cur_assets, total_cur_liab, undistr_porfit

            FROM balancesheet

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ann_date DESC

        """).fetchdf()

        bal_df = bal_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 从daily取当日收盘价格（用于 Altman-Z 等）

        daily_df = conn.execute(f""""

            SELECT ts_code, close

            FROM daily

            WHERE trade_date = '{trade_date}'

              AND ts_code IN ('{pool_str}')

        """).fetchdf()

        daily_df = daily_df.set_index('ts_code') if not daily_df.empty else pd.DataFrame()

        # 从 daily_basic 取总市值（用于 Altman-Z X4 = 市值/总负债）
        db_df = conn.execute(f""""
            SELECT ts_code, total_mv
            FROM daily_basic
            WHERE trade_date = '{trade_date}'
              AND ts_code IN ('{pool_str}')
        """).fetchdf()
        db_df = db_df.set_index('ts_code') if not db_df.empty else pd.DataFrame()

        # 合并数据

        df = fi_df.copy()

        for src in [inc_df, cf_df, bal_df, daily_df, db_df]:

            if not src.empty:

                common_idx = df.index.intersection(src.index)

                for col in src.columns:

                    df.loc[common_idx, col] = src.loc[common_idx, col]

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['Q_ACCRUAL'] = compute_q_accrual(df)

        result['Q_STABILITY'] = compute_q_stability(df, roe_std_dict=roe_std_dict)

        result['Q_OCF_NI'] = compute_q_ocf_ni(df)

        result['Q_ALTMAN_Z'] = compute_q_altman_z(df)

        result['Q_PETTI_Z'] = compute_q_petti_z(df)

        result['Q_CURRENT_R'] = compute_q_current_r(df)

        result['Q_QUICK_R'] = compute_q_quick_r(df)

        result['Q_DEBT_R'] = compute_q_debt_r(df)

        result['Q_ICR'] = compute_q_icr(df)

        return result

    except Exception as e:

        logger.error(f"计算质量因子失败: {e}")

        return pd.DataFrame()

def compute_q_accrual(df, **kwargs):


    try:

        netprofit = df['n_income_attr_p'] if 'n_income_attr_p' in df.columns else pd.Series(np.nan, index=df.index)

        n_cf_act = df['n_cashflow_act'] if 'n_cashflow_act' in df.columns else pd.Series(np.nan, index=df.index)

        total_assets = df['total_assets'] if 'total_assets' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(total_assets != 0, (netprofit - n_cf_act) / total_assets, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_q_stability(df, roe_std_dict=None, **kwargs):

    """"Q_STABILITY = ROE 稳定性 = 1 / (ROE 近8期标准差 + 1e-6)；ROE 越稳定得分越高"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if roe_std_dict:

            for ts_code in df.index:

                std = roe_std_dict.get(ts_code)

                if std is not None and np.isfinite(std):

                    result.loc[ts_code] = 1.0 / (std + 1e-6)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_q_ocf_ni(df, **kwargs):

    try:

        n_cf_act = df['n_cashflow_act'] if 'n_cashflow_act' in df.columns else pd.Series(np.nan, index=df.index)

        netprofit = df['n_income_attr_p'] if 'n_income_attr_p' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(netprofit != 0, n_cf_act / netprofit, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_q_altman_z(df, **kwargs):

    try:

        total_assets = df['total_assets'] if 'total_assets' in df.columns else pd.Series(np.nan, index=df.index)

        total_liab = df['total_liab'] if 'total_liab' in df.columns else pd.Series(np.nan, index=df.index)

        total_cur_assets = df['total_cur_assets'] if 'total_cur_assets' in df.columns else pd.Series(np.nan, index=df.index)

        total_cur_liab = df['total_cur_liab'] if 'total_cur_liab' in df.columns else pd.Series(np.nan, index=df.index)

        undistr_porfit = df['undistr_porfit'] if 'undistr_porfit' in df.columns else pd.Series(np.nan, index=df.index)

        total_profit = df['total_profit'] if 'total_profit' in df.columns else pd.Series(np.nan, index=df.index)

        int_exp = df['int_exp'] if 'int_exp' in df.columns else pd.Series(np.nan, index=df.index)

        revenue = df['revenue'] if 'revenue' in df.columns else pd.Series(np.nan, index=df.index)

        close = df['close'] if 'close' in df.columns else pd.Series(np.nan, index=df.index)

        eq = df['total_hldr_eqy_exc_min_int'] if 'total_hldr_eqy_exc_min_int' in df.columns else pd.Series(np.nan, index=df.index)

        ebit = total_profit + int_exp.fillna(0)

        # Z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5
        # X3 = EBIT/总资产（标准Altman Z模型）

        x1 = np.where(total_assets != 0, (total_cur_assets - total_cur_liab) / total_assets, np.nan)

        x2 = np.where(total_assets != 0, undistr_porfit / total_assets, np.nan)

        x3 = np.where(total_assets != 0, ebit / total_assets, np.nan)

        # 标准 Altman Z (1968 上市公司版): X4 = 股权市值 / 总负债账面值
        # total_mv 单位为万元，转为元后与 total_liab(元) 对齐
        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)
        mv_in_yuan = total_mv * 10000
        x4 = np.where((total_liab != 0) & np.isfinite(mv_in_yuan), mv_in_yuan / total_liab, np.nan)

        x5 = np.where(total_assets != 0, revenue / total_assets, np.nan)

        return 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_q_petti_z(df, **kwargs):


    try:

        total_assets = df['total_assets'] if 'total_assets' in df.columns else pd.Series(np.nan, index=df.index)

        total_liab = df['total_liab'] if 'total_liab' in df.columns else pd.Series(np.nan, index=df.index)

        total_cur_assets = df['total_cur_assets'] if 'total_cur_assets' in df.columns else pd.Series(np.nan, index=df.index)

        total_cur_liab = df['total_cur_liab'] if 'total_cur_liab' in df.columns else pd.Series(np.nan, index=df.index)

        undistr_porfit = df['undistr_porfit'] if 'undistr_porfit' in df.columns else pd.Series(np.nan, index=df.index)

        total_profit = df['total_profit'] if 'total_profit' in df.columns else pd.Series(np.nan, index=df.index)

        int_exp = df['int_exp'] if 'int_exp' in df.columns else pd.Series(np.nan, index=df.index)

        ebit = total_profit + int_exp.fillna(0)

        # Petti Z = 6.56*X1 + 3.26*X2 + 6.72*X3 + 1.05*X4
        # X1=营运资本/总资产, X2=留存收益/总资产, X3=EBIT/总资产, X4=净资产/总负债

        x1 = np.where(total_assets != 0, (total_cur_assets - total_cur_liab) / total_assets, np.nan)

        x2 = np.where(total_assets != 0, undistr_porfit / total_assets, np.nan)

        x3 = np.where(total_assets != 0, ebit / total_assets, np.nan)

        x4 = np.where(total_liab != 0, (total_assets - total_liab) / total_liab, np.nan)

        return 6.56 * x1 + 3.26 * x2 + 6.72 * x3 + 1.05 * x4

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_q_current_r(df, **kwargs):

    try:

        return df['current_ratio'].values if 'current_ratio' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_q_quick_r(df, **kwargs):


    try:

        return df['quick_ratio'].values if 'quick_ratio' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_q_debt_r(df, **kwargs):


    try:

        return df['debt_to_assets'].values if 'debt_to_assets' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_q_icr(df, **kwargs):


    try:

        total_profit = df['total_profit'] if 'total_profit' in df.columns else pd.Series(np.nan, index=df.index)

        int_exp = df['int_exp'] if 'int_exp' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(int_exp != 0, total_profit / int_exp.abs(), np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)


