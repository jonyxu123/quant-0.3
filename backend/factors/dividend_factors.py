"""分红因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:


    return {

        'DIV_YIELD': {

            'dimension': 'DIV_',

            'name': '股息',

            'direction': 'positive',

            'compute_func': compute_div_yield

        },

        'DIV_PAYOUT_R': {

            'dimension': 'DIV_',

            'name': '分红',

            'direction': 'positive',

            'compute_func': compute_div_payout_r

        },

        'DIV_CONTINUITY': {

            'dimension': 'DIV_',

            'name': '分红连续',

            'direction': 'positive',

            'compute_func': compute_div_continuity

        },

        'DIV_GROWTH': {

            'dimension': 'DIV_',

            'name': '分红增长',

            'direction': 'positive',

            'compute_func': compute_div_growth

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

        # 从daily_basic取股息率数据

        db_df = conn.execute(f"""

            SELECT ts_code, dv_ttm, total_mv

            FROM daily_basic

            WHERE trade_date = '{trade_date}'

              AND ts_code IN ('{pool_str}')

        """).fetchdf()

        db_df = db_df.set_index('ts_code') if not db_df.empty else pd.DataFrame()

        # 从income取净利润

        inc_df = conn.execute(f"""

            SELECT ts_code, n_income_attr_p

            FROM income

            WHERE ann_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, ann_date DESC

        """).fetchdf()

        inc_df = inc_df.drop_duplicates(subset='ts_code', keep='first').set_index('ts_code') if not inc_df.empty else pd.DataFrame()

        # 从dividend取历史分红数据（用于DIV_GROWTH）
        # 获取最近2年和去年同期数据
        trade_year = int(trade_date[:4])
        prev_year = trade_year - 1
        div_df = conn.execute(f"""
            SELECT ts_code, end_date, cash_div
            FROM dividend
            WHERE ts_code IN ('{pool_str}')
              AND end_date >= '{prev_year}0101'
              AND end_date <= '{trade_date}'
              AND cash_div > 0
            ORDER BY ts_code, end_date DESC
        """).fetchdf()
        if not div_df.empty:
            div_df['cash_div'] = pd.to_numeric(div_df['cash_div'], errors='coerce')

        # 计算分红增长统计
        div_growth_stats = {}
        if not div_df.empty:
            for ts_code, group in div_df.groupby('ts_code'):
                # 获取最近一年的分红
                latest = group.iloc[0]
                # 获取去年同期的分红（找end_date在去年且cash_div>0的记录）
                prev_year_date = f"{prev_year}{latest['end_date'][4:]}"  # 替换年份
                prev_divs = group[group['end_date'].str.startswith(str(prev_year))]
                if not prev_divs.empty:
                    prev_cash_div = prev_divs['cash_div'].iloc[0]
                    latest_cash_div = latest['cash_div']
                    if prev_cash_div > 0 and pd.notna(latest_cash_div):
                        growth = (latest_cash_div - prev_cash_div) / prev_cash_div
                        div_growth_stats[ts_code] = growth

        # 合并数据
        df = db_df.copy() if not db_df.empty else pd.DataFrame(index=stock_pool)

        if not inc_df.empty:
            common_idx = df.index.intersection(inc_df.index)
            for col in inc_df.columns:
                df.loc[common_idx, col] = inc_df.loc[common_idx, col]

        result = pd.DataFrame(index=df.index)

        result['DIV_YIELD'] = compute_div_yield(df)

        result['DIV_PAYOUT_R'] = compute_div_payout_r(df)

        result['DIV_CONTINUITY'] = compute_div_continuity(df)

        result['DIV_GROWTH'] = compute_div_growth(df, div_growth_stats=div_growth_stats)

        return result

    except Exception as e:

        logger.error(f"计算分红因子失败: {e}")

        return pd.DataFrame()

def compute_div_yield(df, **kwargs):

    """DIV_YIELD = 股息= dv_ttm / total_mv"""

    try:

        dv_ttm = df['dv_ttm'] if 'dv_ttm' in df.columns else pd.Series(np.nan, index=df.index)

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(total_mv > 0, dv_ttm / total_mv, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_div_payout_r(df, **kwargs):

    """DIV_PAYOUT_R = 分红简化用dv_ttm近似)"""

    try:

        dv_ttm = df['dv_ttm'] if 'dv_ttm' in df.columns else pd.Series(np.nan, index=df.index)

        return dv_ttm.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_div_continuity(df, **kwargs):

    """DIV_CONTINUITY = 分红连续简
    """

    try:

        dv_ttm = df['dv_ttm'] if 'dv_ttm' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(dv_ttm > 0, 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_div_growth(df, div_growth_stats=None, **kwargs):

    """DIV_GROWTH = 分红同比增长率

    使用 dividend 表的 cash_div 计算：
    (今年每股分红 - 去年每股分红) / 去年每股分红
    """

    try:
        result = pd.Series(np.nan, index=df.index)
        if div_growth_stats:
            for ts_code in df.index:
                if ts_code in div_growth_stats:
                    result.loc[ts_code] = div_growth_stats[ts_code]
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

