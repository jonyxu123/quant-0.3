""""筹码因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'C_CONCENTRATION': {

            'dimension': 'C_',

            'name': '筹码集中',

            'direction': 'bidirectional',

            'compute_func': compute_c_concentration

        },

        'C_PROFIT_R': {

            'dimension': 'C_',

            'name': '获利盘比',

            'direction': 'positive',

            'compute_func': compute_c_profit_r

        },

        'C_AVG_COST': {

            'dimension': 'C_',

            'name': '平均成本',

            'direction': 'bidirectional',

            'compute_func': compute_c_avg_cost

        },

        'C_LOCKUP_PRESSURE': {

            'dimension': 'C_',

            'name': '解禁压力',

            'direction': 'negative',

            'compute_func': compute_c_lockup_pressure

        },

        'C_SHAREHOLDER_CHG': {

            'dimension': 'C_',

            'name': '股东户数变化',

            'direction': 'negative',

            'compute_func': compute_c_shareholder_chg

        },

        'C_TOP10_R': {

            'dimension': 'C_',

            'name': '0大股东占',

            'direction': 'bidirectional',

            'compute_func': compute_c_top10_r

        },

        'C_FLOAT_R': {

            'dimension': 'C_',

            'name': '流通比',

            'direction': 'bidirectional',

            'compute_func': compute_c_float_r

        },

        'C_INSIDER_R': {

            'dimension': 'C_',

            'name': '内部人持股比',

            'direction': 'bidirectional',

            'compute_func': compute_c_insider_r

        },

        'C_STABILITY': {

            'dimension': 'C_',

            'name': '筹码稳定',

            'direction': 'positive',

            'compute_func': compute_c_stability

        },

        'C_DECENTRAL': {

            'dimension': 'C_',

            'name': '筹码分散',

            'direction': 'negative',

            'compute_func': compute_c_decentral

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

        # 从cyq_perf取筹码分       
        try:

            cyq_df = conn.execute(f""""

                SELECT ts_code, trade_date, weight_avg, winner_rate,

                       cost_5pct, cost_15pct, cost_50pct, cost_85pct, cost_95pct

                FROM cyq_perf

                WHERE trade_date = '{trade_date}'

                  AND ts_code IN ('{pool_str}')

            """).fetchdf()

            cyq_df = cyq_df.set_index('ts_code') if not cyq_df.empty else pd.DataFrame()

        except Exception:

            cyq_df = pd.DataFrame()

        try:

            top10_df = conn.execute(f""""

                SELECT ts_code, end_date, hold_ratio

                FROM top10_holders

                WHERE ann_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ts_code, end_date DESC

            """).fetchdf()

        except Exception:

            top10_df = pd.DataFrame()

        # 从stk_holdertrade取内部人交易数据（用于 C_INSIDER_R）
        try:
            insider_df = conn.execute(f""""
                SELECT ts_code, ann_date, after_ratio, in_de
                FROM stk_holdertrade
                WHERE ann_date <= '{trade_date}'
                  AND ts_code IN ('{pool_str}')
                  AND in_de = 'IN'
                ORDER BY ts_code, ann_date DESC
            """).fetchdf()
            # 取每只股票的最新内部人持股比例
            if not insider_df.empty and 'after_ratio' in insider_df.columns:
                insider_df['after_ratio'] = pd.to_numeric(insider_df['after_ratio'], errors='coerce')
        except Exception:
            insider_df = pd.DataFrame()

        # 从daily_basic取流通市        
        db_df = conn.execute(f""""

            SELECT ts_code, total_mv, circ_mv

            FROM daily_basic

            WHERE trade_date = '{trade_date}'

              AND ts_code IN ('{pool_str}')

        """).fetchdf()

        db_df = db_df.set_index('ts_code') if not db_df.empty else pd.DataFrame()

        # 合并数据

        df = db_df.copy() if not db_df.empty else pd.DataFrame(index=stock_pool)

        if not cyq_df.empty:

            common_idx = df.index.intersection(cyq_df.index)

            for col in cyq_df.columns:

                if col != 'trade_date':

                    df.loc[common_idx, col] = cyq_df.loc[common_idx, col]

        top10_stats = _compute_top10_stats(top10_df, df.index)
        insider_stats = _compute_insider_stats(insider_df, df.index)

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['C_CONCENTRATION'] = compute_c_concentration(df)

        result['C_PROFIT_R'] = compute_c_profit_r(df)

        result['C_AVG_COST'] = compute_c_avg_cost(df)

        result['C_LOCKUP_PRESSURE'] = compute_c_lockup_pressure(df)

        result['C_SHAREHOLDER_CHG'] = compute_c_shareholder_chg(df)

        result['C_TOP10_R'] = compute_c_top10_r(df, top10_stats=top10_stats)

        result['C_FLOAT_R'] = compute_c_float_r(df)

        result['C_INSIDER_R'] = compute_c_insider_r(df, insider_stats=insider_stats)

        result['C_STABILITY'] = compute_c_stability(df)

        result['C_DECENTRAL'] = compute_c_decentral(df)

        return result

    except Exception as e:

        logger.error(f"计算筹码因子失败: {e}")

        return pd.DataFrame()

def _compute_top10_stats(top10_df: pd.DataFrame, index) -> dict:

    """"计算0大股东统计"""

    stats = {}

    try:

        if top10_df.empty:

            return stats

        for ts_code, group in top10_df.groupby('ts_code'):

            latest_end = group['end_date'].iloc[0]

            latest = group[group['end_date'] == latest_end]

            stats[ts_code] = {

                'top10_ratio': latest['hold_ratio'].sum() if 'hold_ratio' in latest.columns else 0,

            }

    except Exception as e:

        logger.warning(f"计算0大股东统计失 {e}")

    return stats

def compute_c_concentration(df, **kwargs):

    """"C_CONCENTRATION = 筹码集中度 = (cost_85pct - cost_15pct) / (cost_95pct - cost_5pct)

    值越小表示筹码越集中
    """

    try:

        c5 = df['cost_5pct'] if 'cost_5pct' in df.columns else pd.Series(np.nan, index=df.index)

        c15 = df['cost_15pct'] if 'cost_15pct' in df.columns else pd.Series(np.nan, index=df.index)

        c85 = df['cost_85pct'] if 'cost_85pct' in df.columns else pd.Series(np.nan, index=df.index)

        c95 = df['cost_95pct'] if 'cost_95pct' in df.columns else pd.Series(np.nan, index=df.index)

        spread_70 = c85 - c15  # 70%筹码区间

        spread_90 = c95 - c5   # 90%筹码区间

        return np.where((spread_90 > 0) & np.isfinite(spread_90), spread_70 / spread_90, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_c_profit_r(df, **kwargs):

    """"C_PROFIT_R = 获利盘比 (来自 cyq_perf.winner_rate，单位%)"""

    try:

        wr = df['winner_rate'] if 'winner_rate' in df.columns else pd.Series(np.nan, index=df.index)

        return wr / 100.0  # 转为 0-1 小数

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_c_avg_cost(df, **kwargs):

    """"C_AVG_COST = 平均成本 (来自 cyq_perf.weight_avg)"""

    try:

        return df['weight_avg'].values if 'weight_avg' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_c_lockup_pressure(df, **kwargs):

    """"C_LOCKUP_PRESSURE = 解禁压力(简
    """

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_c_shareholder_chg(df, **kwargs):

    """"C_SHAREHOLDER_CHG = 股东户数变化(简
    """

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_c_top10_r(df, top10_stats=None, **kwargs):

    # C_TOP10_R0大股东占

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if top10_stats:

            for ts_code in df.index:

                if ts_code in top10_stats:

                    result.loc[ts_code] = top10_stats[ts_code]['top10_ratio']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_c_float_r(df, **kwargs):

    # C_FLOAT_R流通比简

    try:

        circ_mv = df['circ_mv'] if 'circ_mv' in df.columns else pd.Series(np.nan, index=df.index)

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(total_mv > 0, circ_mv / total_mv, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_c_insider_r(df, insider_stats=None, **kwargs):

    """"C_INSIDER_R = 内部人持股比（来自 stk_holdertrade 内部人增持后持股比例）"""

    try:
        result = pd.Series(np.nan, index=df.index)
        if insider_stats:
            for ts_code in df.index:
                if ts_code in insider_stats:
                    result.loc[ts_code] = insider_stats[ts_code]
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def _compute_insider_stats(insider_df, index) -> dict:
    """"计算内部人持股统计：汇总所有内部人持股比例"""
    stats = {}
    try:
        if insider_df.empty:
            return stats
        for ts_code, group in insider_df.groupby('ts_code'):
            if ts_code in index:
                # 取最新一条记录的 after_ratio 作为该股票的内部人持股比例
                latest_ratio = group['after_ratio'].iloc[0] if 'after_ratio' in group.columns else np.nan
                if np.isfinite(latest_ratio):
                    stats[ts_code] = latest_ratio / 100.0  # 转为0-1
    except Exception as e:
        logger.warning(f"计算内部人持股统计失败: {e}")
    return stats

def compute_c_stability(df, **kwargs):

    """"C_STABILITY = 筹码稳定性：获利盘比例越接近50%（筹码均衡），稳定性越高"""

    try:

        winner_r = df['winner_rate'] if 'winner_rate' in df.columns else pd.Series(np.nan, index=df.index)

        profit_r = winner_r / 100.0  # 转为0-1

        return np.where(np.isfinite(profit_r), 1 - np.abs(profit_r - 0.5) * 2, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_c_decentral(df, **kwargs):

    """"C_DECENTRAL = 筹码分散度：与集中度反向，集中度越高分散度越低

    使用 (cost_95pct - cost_5pct) / weight_avg 作为分散度指标
    """

    try:

        c5 = df['cost_5pct'] if 'cost_5pct' in df.columns else pd.Series(np.nan, index=df.index)

        c95 = df['cost_95pct'] if 'cost_95pct' in df.columns else pd.Series(np.nan, index=df.index)

        avg = df['weight_avg'] if 'weight_avg' in df.columns else pd.Series(np.nan, index=df.index)

        spread_90 = c95 - c5

        return np.where((avg > 0) & np.isfinite(spread_90), spread_90 / avg, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

