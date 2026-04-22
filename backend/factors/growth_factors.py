""""成长因子模块"""

import pandas as pd
import numpy as np
from loguru import logger

def register() -> dict:
    """"注册因子到全局注册"""
    return {
        'G_REV_YOY': {
            'dimension': 'G_',
            'name': '营收同比增长',
            'direction': 'positive',
            'compute_func': compute_g_rev_yoy
        },
        'G_REV_QOQ': {
            'dimension': 'G_',
            'name': '营收环比增长',
            'direction': 'positive',
            'compute_func': compute_g_rev_qoq
        },
        'G_NP_YOY': {
            'dimension': 'G_',
            'name': '净利润同比增长',
            'direction': 'positive',
            'compute_func': compute_g_np_yoy
        },
        'G_NI_YOY': {  # 别名，与策略命名保持一致
            'dimension': 'G_',
            'name': '净利润同比增长(策略别名)',
            'direction': 'positive',
            'compute_func': compute_g_np_yoy
        },
        'G_NP_QOQ': {
            'dimension': 'G_',
            'name': '净利润环比增长',
            'direction': 'positive',
            'compute_func': compute_g_np_qoq
        },
        'G_OCF_YOY': {
            'dimension': 'G_',
            'name': '经营现金流同比增长率',
            'direction': 'positive',
            'compute_func': compute_g_ocf_yoy
        },
        'G_EPS_G_3Y': {
            'dimension': 'G_',
            'name': 'EPS 三年增长',
            'direction': 'positive',
            'compute_func': compute_g_eps_g_3y
        },
        'G_REV_CAGR_3Y': {
            'dimension': 'G_',
            'name': '营收三年 CAGR',
            'direction': 'positive',
            'compute_func': compute_g_rev_cagr_3y
        },
        'G_NP_CAGR_3Y': {
            'dimension': 'G_',
            'name': '净利润三年 CAGR',
            'direction': 'positive',
            'compute_func': compute_g_np_cagr_3y
        },
        'G_OCF_CAGR_3Y': {
            'dimension': 'G_',
            'name': '经营现金流三年 CAGR',
            'direction': 'positive',
            'compute_func': compute_g_ocf_cagr_3y
        },
        'G_ROE_DELTA': {
            'dimension': 'G_',
            'name': 'ROE 变化',
            'direction': 'positive',
            'compute_func': compute_g_roe_delta
        },
        'G_GPM_DELTA': {
            'dimension': 'G_',
            'name': '毛利率变化率',
            'direction': 'positive',
            'compute_func': compute_g_gpm_delta
        },
        'G_SGR': {
            'dimension': 'G_',
            'name': '可持续增长率',
            'direction': 'positive',
            'compute_func': compute_g_sgr
        },
        'G_EGRLF': {
            'dimension': 'G_',
            'name': '长期预期增长',
            'direction': 'positive',
            'compute_func': compute_g_egrlf
        },
        'G_ACC_NS': {
            'dimension': 'G_',
            'name': '应计项目非稳定性',
            'direction': 'negative',
            'compute_func': compute_g_acc_ns
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
        
        # 从 fina_indicator 取成长指标（多期，用于 DELTA 和 SGR）
        fi_df = conn.execute(f""""
            SELECT ts_code, ann_date, end_date, roe, grossprofit_margin, or_yoy, netprofit_yoy,
                   eps, dt_eps, ocf_yoy, q_sales_yoy, dt_netprofit_yoy
            FROM fina_indicator
            WHERE ann_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
            ORDER BY ts_code, ann_date DESC
        """).fetchdf()
        fi_latest = fi_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 从 income 取营收和净利润（多期：用于 CAGR，只取年报）
        inc_df = conn.execute(f""""
            SELECT ts_code, revenue, n_income_attr_p, total_profit, ann_date, end_date
            FROM income
            WHERE ann_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
            ORDER BY ts_code, ann_date DESC
        """).fetchdf()
        # 只保留年报（end_date 以 1231 结尾）用于 CAGR 计算
        if 'end_date' in inc_df.columns:
            inc_annual = inc_df[inc_df['end_date'].str.endswith('1231', na=False)].copy()
        else:
            inc_annual = inc_df.copy()

        # 从 cashflow 取现金流数据（多期）
        cf_df = conn.execute(f""""
            SELECT ts_code, n_cashflow_act, ann_date, end_date
            FROM cashflow
            WHERE ann_date <= '{trade_date}'
              AND ts_code IN ('{pool_str}')
            ORDER BY ts_code, ann_date DESC
        """).fetchdf()
        # 只保留年报用于 CAGR 计算
        if 'end_date' in cf_df.columns:
            cf_annual = cf_df[cf_df['end_date'].str.endswith('1231', na=False)].copy()
        else:
            cf_annual = cf_df.copy()

        # 取最近一期 income / cashflow
        inc_latest = inc_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')
        cf_latest  = cf_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code')

        # 合并最新数据为主 df
        df = fi_latest.copy()
        for src in [inc_latest, cf_latest]:
            common_idx = df.index.intersection(src.index)
            for col in src.columns:
                if col not in ('ann_date', 'end_date'):
                    df.loc[common_idx, col] = src.loc[common_idx, col]

        # 计算多期衍生数据（CAGR、DELTA）；CAGR 系列只用年报数据
        cagr_data = _compute_cagr_data(inc_annual, cf_annual, fi_df, trade_date)
        
        # 计算各因子
        result = pd.DataFrame(index=df.index)
        result['G_REV_YOY'] = compute_g_rev_yoy(df)
        result['G_REV_QOQ'] = compute_g_rev_qoq(df, cagr_data)
        result['G_NP_YOY'] = compute_g_np_yoy(df)
        result['G_NP_QOQ'] = compute_g_np_qoq(df, cagr_data)
        result['G_OCF_YOY'] = compute_g_ocf_yoy(df)
        result['G_EPS_G_3Y'] = compute_g_eps_g_3y(df, cagr_data)
        result['G_REV_CAGR_3Y'] = compute_g_rev_cagr_3y(df, cagr_data)
        result['G_NP_CAGR_3Y'] = compute_g_np_cagr_3y(df, cagr_data)
        result['G_OCF_CAGR_3Y'] = compute_g_ocf_cagr_3y(df, cagr_data)
        result['G_ROE_DELTA'] = compute_g_roe_delta(df, cagr_data)
        result['G_GPM_DELTA'] = compute_g_gpm_delta(df, cagr_data)
        result['G_SGR'] = compute_g_sgr(df)
        result['G_EGRLF'] = compute_g_egrlf(df)
        result['G_ACC_NS'] = compute_g_acc_ns(df)
        
        return result
    except Exception as e:
        logger.error(f"计算成长因子失败：{e}")
        return pd.DataFrame()

def _safe_cagr(cur, base, n=3):
    """"安全计算 CAGR；base/cur 可能为负，返回 NaN"""
    try:
        if base > 0 and cur > 0:
            return (cur / base) ** (1.0 / n) - 1
    except Exception:
        pass
    return np.nan


def _compute_cagr_data(inc_df: pd.DataFrame, cf_df: pd.DataFrame,
                       fi_df: pd.DataFrame, trade_date: str) -> dict:
    """"计算多期衍生指标：3年CAGR、环比QOQ、ROE/GPM两期差值"""
    data = {}
    try:
        # ── 3年 CAGR（income 年报，已在外部过滤为 end_date 1231）──
        if not inc_df.empty:
            inc_copy = inc_df.copy()
            inc_copy['year'] = inc_copy['end_date'].str[:4] if 'end_date' in inc_copy.columns else inc_copy['ann_date'].str[:4]
            for ts_code, grp in inc_copy.groupby('ts_code'):
                years = sorted(grp['year'].unique(), reverse=True)
                if ts_code not in data:
                    data[ts_code] = {}
                if len(years) >= 4:
                    r0 = grp[grp['year'] == years[0]]['revenue'].values[0]
                    r3 = grp[grp['year'] == years[3]]['revenue'].values[0]
                    n0 = grp[grp['year'] == years[0]]['n_income_attr_p'].values[0]
                    n3 = grp[grp['year'] == years[3]]['n_income_attr_p'].values[0]
                    data[ts_code]['rev_cagr'] = _safe_cagr(r0, r3)
                    data[ts_code]['np_cagr']  = _safe_cagr(n0, n3)

        # ── OCF 3年 CAGR（cashflow 年报，已在外部过滤）──
        if not cf_df.empty:
            cf_copy = cf_df.copy()
            cf_copy['year'] = cf_copy['end_date'].str[:4] if 'end_date' in cf_copy.columns else cf_copy['ann_date'].str[:4]
            for ts_code, grp in cf_copy.groupby('ts_code'):
                years = sorted(grp['year'].unique(), reverse=True)
                if ts_code not in data:
                    data[ts_code] = {}
                if len(years) >= 4:
                    c0 = grp[grp['year'] == years[0]]['n_cashflow_act'].values[0]
                    c3 = grp[grp['year'] == years[3]]['n_cashflow_act'].values[0]
                    data[ts_code]['ocf_cagr'] = _safe_cagr(c0, c3)

        # ── 历史 EPS 3年 CAGR（fina_indicator，过滤年报 end_date 1231）──
        if not fi_df.empty:
            fi_copy = fi_df.copy()
            if 'end_date' in fi_copy.columns:
                fi_copy = fi_copy[fi_copy['end_date'].str.endswith('1231', na=False)]
            fi_copy['year'] = fi_copy['end_date'].str[:4] if 'end_date' in fi_copy.columns else fi_copy['ann_date'].str[:4]
            for ts_code, grp in fi_copy.groupby('ts_code'):
                years = sorted(grp['year'].unique(), reverse=True)
                if ts_code not in data:
                    data[ts_code] = {}
                # EPS 3年 CAGR
                if len(years) >= 4:
                    e0 = grp[grp['year'] == years[0]]['eps'].values[0]
                    e3 = grp[grp['year'] == years[3]]['eps'].values[0]
                    data[ts_code]['eps_cagr'] = _safe_cagr(e0, e3)
                # ROE 两期差值（最新 - 上一年）
                if len(years) >= 2:
                    roe0 = grp[grp['year'] == years[0]]['roe'].values[0]
                    roe1 = grp[grp['year'] == years[1]]['roe'].values[0]
                    try:
                        data[ts_code]['roe_delta'] = float(roe0) - float(roe1)
                    except Exception:
                        data[ts_code]['roe_delta'] = np.nan
                    # GPM 两期差值
                    gpm0 = grp[grp['year'] == years[0]]['grossprofit_margin'].values[0]
                    gpm1 = grp[grp['year'] == years[1]]['grossprofit_margin'].values[0]
                    try:
                        data[ts_code]['gpm_delta'] = float(gpm0) - float(gpm1)
                    except Exception:
                        data[ts_code]['gpm_delta'] = np.nan

        # QOQ 环比：不再用累计值差（income累计口径），数据已由 fina_indicator.q_sales_yoy / q_profit_yoy 提供

    except Exception as e:
        logger.warning(f"计算多期成长数据失败：{e}")
    return data

def compute_g_rev_yoy(df, **kwargs):
    """"G_REV_YOY = 营收同比增长"""
    try:
        return df['or_yoy'].values if 'or_yoy' in df.columns else pd.Series(np.nan, index=df.index)
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_rev_qoq(df, cagr_data=None, **kwargs):
    """"G_REV_QOQ = 营收单季同比增长（来自 fina_indicator.q_sales_yoy）"""
    try:
        return df['q_sales_yoy'].values if 'q_sales_yoy' in df.columns else pd.Series(np.nan, index=df.index)
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_np_yoy(df, **kwargs):
    """"G_NP_YOY = 净利润同比增长"""
    try:
        return df['netprofit_yoy'].values if 'netprofit_yoy' in df.columns else pd.Series(np.nan, index=df.index)
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_np_qoq(df, cagr_data=None, **kwargs):
    """"G_NP_QOQ = 净利润单季同比增长（来自 fina_indicator.dt_netprofit_yoy）"""
    try:
        return df['dt_netprofit_yoy'].values if 'dt_netprofit_yoy' in df.columns else pd.Series(np.nan, index=df.index)
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_ocf_yoy(df, **kwargs):
    """"G_OCF_YOY = 经营现金流同比增长率"""
    try:
        return df['ocf_yoy'].values if 'ocf_yoy' in df.columns else pd.Series(np.nan, index=df.index)
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_eps_g_3y(df, cagr_data=None, **kwargs):
    """"G_EPS_G_3Y = EPS 三年复合增长率 (CAGR)"""
    try:
        result = pd.Series(np.nan, index=df.index)
        if cagr_data:
            for ts_code in df.index:
                val = cagr_data.get(ts_code, {}).get('eps_cagr')
                if val is not None:
                    result.loc[ts_code] = val
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_rev_cagr_3y(df, cagr_data=None, **kwargs):
    """"G_REV_CAGR_3Y = 营收三年 CAGR"""
    try:
        result = pd.Series(np.nan, index=df.index)
        if cagr_data:
            for ts_code, val in cagr_data.items():
                if ts_code in result.index and 'rev_cagr' in val:
                    result.loc[ts_code] = val['rev_cagr']
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_np_cagr_3y(df, cagr_data=None, **kwargs):
    """"G_NP_CAGR_3Y = 净利润三年 CAGR"""
    try:
        result = pd.Series(np.nan, index=df.index)
        if cagr_data:
            for ts_code, val in cagr_data.items():
                if ts_code in result.index and 'np_cagr' in val:
                    result.loc[ts_code] = val['np_cagr']
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_ocf_cagr_3y(df, cagr_data=None, **kwargs):
    """"G_OCF_CAGR_3Y = 经营现金流三年 CAGR"""
    try:
        result = pd.Series(np.nan, index=df.index)
        if cagr_data:
            for ts_code in df.index:
                val = cagr_data.get(ts_code, {}).get('ocf_cagr')
                if val is not None:
                    result.loc[ts_code] = val
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_roe_delta(df, cagr_data=None, **kwargs):
    """"G_ROE_DELTA = ROE 同比变化（本期 - 上一年同期）"""
    try:
        result = pd.Series(np.nan, index=df.index)
        if cagr_data:
            for ts_code in df.index:
                val = cagr_data.get(ts_code, {}).get('roe_delta')
                if val is not None and np.isfinite(val):
                    result.loc[ts_code] = val
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_gpm_delta(df, cagr_data=None, **kwargs):
    """"G_GPM_DELTA = 毛利率同比变化（本期 - 上一年同期）"""
    try:
        result = pd.Series(np.nan, index=df.index)
        if cagr_data:
            for ts_code in df.index:
                val = cagr_data.get(ts_code, {}).get('gpm_delta')
                if val is not None and np.isfinite(val):
                    result.loc[ts_code] = val
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_sgr(df, **kwargs):
    """"G_SGR = 可持续增长率 = ROE * (1 - 分红率)
       div_payoutratio 单位为%，需/100；缺失时假设留存比例 0.6
    """
    try:
        roe = df['roe'] if 'roe' in df.columns else pd.Series(np.nan, index=df.index)
        if 'div_payoutratio' in df.columns:
            payout = df['div_payoutratio'] / 100.0
            # 异常值保护：分红率应在 [0, 1]
            payout = payout.clip(0, 1)
            retention = 1.0 - payout
        else:
            retention = pd.Series(0.6, index=df.index)
        return np.where(np.isfinite(roe) & np.isfinite(retention), roe * retention, np.nan)
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_egrlf(df, **kwargs):
    """"G_EGRLF = 长期预期增长 (简化用营收增长和利润增长均值)"""
    try:
        or_yoy = df['or_yoy'] if 'or_yoy' in df.columns else pd.Series(np.nan, index=df.index)
        np_yoy = df['netprofit_yoy'] if 'netprofit_yoy' in df.columns else pd.Series(np.nan, index=df.index)
        return (or_yoy + np_yoy) / 2
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_g_acc_ns(df, **kwargs):
    """"G_ACC_NS = 应计项目 = (净利润 - 经营现金流) / 总资产；越大代表盈利质量越差"""
    try:
        netprofit = df['n_income_attr_p'] if 'n_income_attr_p' in df.columns else pd.Series(np.nan, index=df.index)
        n_cf_act  = df['n_cashflow_act']  if 'n_cashflow_act'  in df.columns else pd.Series(np.nan, index=df.index)
        total_assets = df['total_assets'] if 'total_assets' in df.columns else pd.Series(np.nan, index=df.index)
        return np.where((total_assets != 0) & np.isfinite(total_assets),
                        np.abs(netprofit - n_cf_act) / total_assets, np.nan)
    except Exception:
        return pd.Series(np.nan, index=df.index)
