""""价值因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'V_EP': {

            'dimension': 'V_',

            'name': '盈利收益',

            'direction': 'positive',

            'compute_func': compute_v_ep

        },

        'V_BP': {

            'dimension': 'V_',

            'name': '账面市值比',

            'direction': 'positive',

            'compute_func': compute_v_bp

        },

        'V_FCFY': {

            'dimension': 'V_',

            'name': '自由现金流收益率',

            'direction': 'positive',

            'compute_func': compute_v_fcfy

        },

        'V_OCFY': {

            'dimension': 'V_',

            'name': '经营现金流收益率',

            'direction': 'positive',

            'compute_func': compute_v_ocy

        },

        'V_SG': {

            'dimension': 'V_',

            'name': 'PEG因子',

            'direction': 'negative',

            'compute_func': compute_v_sg

        },

        'V_DY': {

            'dimension': 'V_',

            'name': '股息',

            'direction': 'positive',

            'compute_func': compute_v_dy

        },

        'V_EV_EBITDA': {

            'dimension': 'V_',

            'name': 'EV/EBITDA',

            'direction': 'negative',

            'compute_func': compute_v_ev_ebitda

        },

        'V_NCPS': {

            'dimension': 'V_',

            'name': '每股净资产',

            'direction': 'positive',

            'compute_func': compute_v_ncps

        },

        'V_PCF': {

            'dimension': 'V_',

            'name': '市现',

            'direction': 'negative',

            'compute_func': compute_v_pcf

        },

        'V_PE_TTM': {

            'dimension': 'V_',

            'name': 'PE_TTM',

            'direction': 'negative',

            'compute_func': compute_v_pe_ttm

        },

        'V_PB': {

            'dimension': 'V_',

            'name': 'PB',

            'direction': 'negative',

            'compute_func': compute_v_pb

        },

        'V_PS': {

            'dimension': 'V_',

            'name': 'PS_TTM',

            'direction': 'negative',

            'compute_func': compute_v_ps

        },

    }

def compute(trade_date: str, conn, stock_pool: list = None, prefetch_cache: dict = None) -> pd.DataFrame:

    """"计算该维度全部因子"""

    try:

        # 获取股票池
        if stock_pool is None:

            stock_pool_df = conn.execute(

                "SELECT ts_code FROM stock_basic WHERE list_date IS NOT NULL"

            ).fetchdf()

            stock_pool = stock_pool_df['ts_code'].tolist()

        if not stock_pool:

            return pd.DataFrame()

        pool_str = "','".join(stock_pool)
        cache = prefetch_cache or {}

        # 1. 从 daily_basic 取估值数据（优先用缓存）
        if 'daily_basic' in cache and not cache['daily_basic'].empty:
            _db = cache['daily_basic']
            db_df = _db[_db['ts_code'].isin(stock_pool)][
                [c for c in ['ts_code','pe_ttm','pb','ps_ttm','dv_ttm','dv_ratio','total_mv','total_share','turnover_rate'] if c in _db.columns]
            ].set_index('ts_code')
        else:
            db_df = conn.execute(f""""
                SELECT ts_code, pe_ttm, pb, ps_ttm, dv_ttm, dv_ratio, total_mv, total_share, turnover_rate
                FROM daily_basic
                WHERE trade_date = '{trade_date}'
                  AND ts_code IN ('{pool_str}')
            """).fetchdf().set_index('ts_code')

        # 2. 从cashflow取现金流数据（含折旧摊销，用于真实 EBITDA）
        _cf_cols = ['ts_code','n_cashflow_act','n_cashflow_inv_act','c_pay_acq_const_fiolta',
                    'depr_fa_coga_dpba','amort_intang_assets','lt_amort_deferred_exp']
        if 'cashflow' in cache and not cache['cashflow'].empty:
            _cf = cache['cashflow']
            cf_df = _cf[_cf['ts_code'].isin(stock_pool)][
                [c for c in _cf_cols if c in _cf.columns]
            ].drop_duplicates(subset='ts_code').set_index('ts_code')
        else:
            cf_df = conn.execute(f""""
                SELECT ts_code, n_cashflow_act, n_cashflow_inv_act, c_pay_acq_const_fiolta,
                       depr_fa_coga_dpba, amort_intang_assets, lt_amort_deferred_exp
                FROM cashflow
                WHERE ann_date <= '{trade_date}'
                  AND ts_code IN ('{pool_str}')
                ORDER BY ann_date DESC
            """).fetchdf().drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 3. 从 balance 取资产负债数据（优先用缓存）
        if 'balancesheet' in cache and not cache['balancesheet'].empty:
            _bal = cache['balancesheet']
            bal_df = _bal[_bal['ts_code'].isin(stock_pool)][
                [c for c in ['ts_code','total_hldr_eqy_exc_min_int','total_liab','total_assets'] if c in _bal.columns]
            ].drop_duplicates(subset='ts_code').set_index('ts_code')
        else:
            bal_df = conn.execute(f""""
                SELECT ts_code, total_hldr_eqy_exc_min_int, total_liab, total_assets
                FROM balancesheet
                WHERE ann_date <= '{trade_date}'
                  AND ts_code IN ('{pool_str}')
                ORDER BY ann_date DESC
            """).fetchdf().drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 4. 从 income 取利润数据（优先用缓存）
        if 'income' in cache and not cache['income'].empty:
            _inc = cache['income']
            inc_df = _inc[_inc['ts_code'].isin(stock_pool)][
                [c for c in ['ts_code','total_profit','revenue','total_cogs','total_revenue','int_exp'] if c in _inc.columns]
            ].drop_duplicates(subset='ts_code').set_index('ts_code')
        else:
            inc_df = conn.execute(f""""
                SELECT ts_code, total_profit, revenue, total_cogs, total_revenue, int_exp
                FROM income
                WHERE ann_date <= '{trade_date}'
                  AND ts_code IN ('{pool_str}')
                ORDER BY ann_date DESC
            """).fetchdf().drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 5. 从 fina_indicator 取 netprofit_yoy（优先用缓存）
        try:
            if 'fina_indicator' in cache and not cache['fina_indicator'].empty:
                _fi = cache['fina_indicator']
                fi_df = _fi[_fi['ts_code'].isin(stock_pool)][
                    [c for c in ['ts_code','netprofit_yoy'] if c in _fi.columns]
                ].drop_duplicates(subset='ts_code').set_index('ts_code')
            else:
                fi_df = conn.execute(f""""
                    SELECT ts_code, netprofit_yoy
                    FROM fina_indicator
                    WHERE ann_date <= '{trade_date}'
                      AND ts_code IN ('{pool_str}')
                    ORDER BY ann_date DESC
                """).fetchdf().drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')
        except Exception:
            fi_df = pd.DataFrame()

        # 合并数据

        df = db_df.copy()

        for src in [cf_df, bal_df, inc_df, fi_df]:

            common_idx = df.index.intersection(src.index)

            for col in src.columns:

                df.loc[common_idx, col] = src.loc[common_idx, col]

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['V_EP'] = compute_v_ep(df)

        result['V_BP'] = compute_v_bp(df)

        result['V_FCFY'] = compute_v_fcfy(df)

        result['V_OCFY'] = compute_v_ocy(df)

        result['V_SG'] = compute_v_sg(df)

        result['V_DY'] = compute_v_dy(df)

        result['V_EV_EBITDA'] = compute_v_ev_ebitda(df)

        result['V_NCPS'] = compute_v_ncps(df)

        result['V_PCF'] = compute_v_pcf(df)

        result['V_PE_TTM'] = compute_v_pe_ttm(df)

        result['V_PB'] = compute_v_pb(df)

        result['V_PS'] = compute_v_ps(df)

        return result

    except Exception as e:

        logger.error(f"计算价值因子失败：{e}")

        return pd.DataFrame()

def compute_v_ep(df, **kwargs):

    """"V_EP = 1 / pe_ttm"""

    try:

        pe = df['pe_ttm'] if 'pe_ttm' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where((pe > 0) & np.isfinite(pe), 1.0 / pe, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_bp(df, **kwargs):

    """"V_BP = 1 / pb"""

    try:

        pb = df['pb'] if 'pb' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where((pb > 0) & np.isfinite(pb), 1.0 / pb, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_fcfy(df, **kwargs):

    """"V_FCFY = 自由现金流收益率"""

    try:

        n_cf_act = df['n_cashflow_act'] if 'n_cashflow_act' in df.columns else pd.Series(np.nan, index=df.index)

        n_cf_inv = df['n_cashflow_inv_act'] if 'n_cashflow_inv_act' in df.columns else pd.Series(np.nan, index=df.index)

        c_pay = df['c_pay_acq_const_fiolta'] if 'c_pay_acq_const_fiolta' in df.columns else pd.Series(np.nan, index=df.index)

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        # FCF = 经营现金- 资本支出

        capex = np.where(np.isnan(c_pay), -n_cf_inv, c_pay)

        fcf = n_cf_act - capex

        total_mv_scaled = total_mv * 10000
        return np.where(total_mv_scaled != 0, fcf / total_mv_scaled, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_ocy(df, **kwargs):


    try:

        n_cf_act = df['n_cashflow_act'] if 'n_cashflow_act' in df.columns else pd.Series(np.nan, index=df.index)

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        total_mv_scaled = total_mv * 10000

        return np.where(total_mv_scaled != 0, n_cf_act / total_mv_scaled, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_sg(df, **kwargs):

    """"V_SG = PEG = pe_ttm / (netprofit_yoy/100)；高成长低估值则 PEG 小（负向因子）"""

    try:

        pe = df['pe_ttm'] if 'pe_ttm' in df.columns else pd.Series(np.nan, index=df.index)

        # netprofit_yoy 单位为%，标准 PEG = PE / (年化净利润增速%)
        np_yoy = df['netprofit_yoy'] if 'netprofit_yoy' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where((pe > 0) & (np_yoy > 0) & np.isfinite(np_yoy), pe / np_yoy, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_dy(df, **kwargs):

    """"V_DY = 股息率，直接用 daily_basic 的 dv_ratio（单位%）"""

    try:

        if 'dv_ratio' in df.columns:

            dv_ratio = df['dv_ratio']

            return np.where(np.isfinite(dv_ratio), dv_ratio / 100.0, np.nan)

        # 降级： dv_ttm 是每股股息(元)，除以 pb*每股净资(不可靠)，直接返回 NaN
        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_ev_ebitda(df, **kwargs):

    """"V_EV_EBITDA = EV / EBITDA
    EV = 总市值 + 总负债（省略少数股东权益、现金等价物，简化估算）
    EBITDA = 利润总额 + 利息支出 + 固定资产折旧 + 无形资产摊销 + 长期待摊费用摊销
    """
    try:

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        total_liab = df['total_liab'] if 'total_liab' in df.columns else pd.Series(np.nan, index=df.index)

        total_profit = df['total_profit'] if 'total_profit' in df.columns else pd.Series(np.nan, index=df.index)

        int_exp = df['int_exp'] if 'int_exp' in df.columns else pd.Series(np.nan, index=df.index)

        # 折旧摊销（来自 cashflow 表，缺失时赋 0）
        depr = df['depr_fa_coga_dpba'].fillna(0) if 'depr_fa_coga_dpba' in df.columns else 0
        amort_intang = df['amort_intang_assets'].fillna(0) if 'amort_intang_assets' in df.columns else 0
        amort_lt = df['lt_amort_deferred_exp'].fillna(0) if 'lt_amort_deferred_exp' in df.columns else 0

        # EV = 总市值(万元)*10000 + 总负债(元)
        ev = total_mv * 10000 + total_liab

        # 真实 EBITDA = EBIT + D&A
        ebit = total_profit + int_exp.fillna(0)
        ebitda = ebit + depr + amort_intang + amort_lt

        return np.where((np.isfinite(ev)) & (np.isfinite(ebitda)) & (ebitda > 0), ev / ebitda, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_ncps(df, **kwargs):

    """"V_NCPS = 每股净资产 = 归母权益(元) / 总股本(股)
    Tushare: total_hldr_eqy_exc_min_int 单位元；total_share 单位万股
    ∴ NCPS(元/股) = eq / (total_share * 10000)
    """
    try:

        eq = df['total_hldr_eqy_exc_min_int'] if 'total_hldr_eqy_exc_min_int' in df.columns else pd.Series(np.nan, index=df.index)

        total_share = df['total_share'] if 'total_share' in df.columns else pd.Series(np.nan, index=df.index)

        # total_share 转为股（*10000）
        ncps = np.where(
            (np.isfinite(total_share)) & (total_share > 0),
            eq / (total_share * 10000),
            np.nan,
        )

        return ncps

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_pcf(df, **kwargs):

    """"V_PCF = 市现率 = 总市值 / 经营现金"""

    try:

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        n_cf_act = df['n_cashflow_act'] if 'n_cashflow_act' in df.columns else pd.Series(np.nan, index=df.index)

        total_mv_scaled = total_mv * 10000

        return np.where(n_cf_act != 0, total_mv_scaled / n_cf_act, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_pe_ttm(df, **kwargs):

    """"V_PE_TTM = pe_ttm"""

    try:

        return df['pe_ttm'].values if 'pe_ttm' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_pb(df, **kwargs):

    """"V_PB = pb"""

    try:

        return df['pb'].values if 'pb' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_v_ps(df, **kwargs):

    """"V_PS = ps_ttm"""

    try:

        return df['ps_ttm'].values if 'ps_ttm' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

