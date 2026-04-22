""""流动性因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'S_LN_FLOAT': {

            'dimension': 'S_',

            'name': '对数流通市',

            'direction': 'bidirectional',

            'compute_func': compute_s_ln_float

        },

        'S_AMT_20_Z': {

            'dimension': 'S_',

            'name': '20日成交额Z分数',

            'direction': 'bidirectional',

            'compute_func': compute_s_amt_20_z

        },

        'S_TURNOVER': {

            'dimension': 'S_',

            'name': '换手',

            'direction': 'bidirectional',

            'compute_func': compute_s_turnover

        },

        'S_ILLIQUIDITY': {

            'dimension': 'S_',

            'name': 'Amihud非流动',

            'direction': 'negative',

            'compute_func': compute_s_illiquidity

        },

        'S_VOL_PRICE_IMPACT': {

            'dimension': 'S_',

            'name': '成交量价格冲',

            'direction': 'negative',

            'compute_func': compute_s_vol_price_impact

        },

        'S_SPREAD': {

            'dimension': 'S_',

            'name': '买卖价差',

            'direction': 'negative',

            'compute_func': compute_s_spread

        },

        'S_MCAP': {

            'dimension': 'S_',

            'name': '总市',

            'direction': 'bidirectional',

            'compute_func': compute_s_mcap

        },

        'S_FREE_FLOAT_R': {

            'dimension': 'S_',

            'name': '自由流通比',

            'direction': 'bidirectional',

            'compute_func': compute_s_free_float_r

        },

        'S_LN_MCAP': {

            'dimension': 'S_',

            'name': '对数总市',

            'direction': 'bidirectional',

            'compute_func': compute_s_ln_mcap

        },

        'S_AMIHUD': {

            'dimension': 'S_',

            'name': 'Amihud非流动性指',

            'direction': 'negative',

            'compute_func': compute_s_amihud

        },

        'L_MAX_DRAWDOWN_1Y': {

            'dimension': 'L_',

            'name': '一年最大回撤',

            'direction': 'negative',

            'compute_func': compute_l_max_drawdown_1y

        },

        'L_VOL_1Y': {

            'dimension': 'L_',

            'name': '一年波动率',

            'direction': 'negative',

            'compute_func': compute_l_vol_1y

        },

        'L_SKEWNESS': {

            'dimension': 'L_',

            'name': '收益率偏度',

            'direction': 'bidirectional',

            'compute_func': compute_l_skewness

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

        # 从daily_basic取估值数据        
        db_df = conn.execute(f""""

            SELECT ts_code, total_mv, circ_mv, turnover_rate, turnover_rate_f

            FROM daily_basic

            WHERE trade_date = '{trade_date}'

              AND ts_code IN ('{pool_str}')

        """).fetchdf()

        db_df = db_df.set_index('ts_code') if not db_df.empty else pd.DataFrame()

        # 从daily取行情数0

        daily_df = conn.execute(f""""

            SELECT ts_code, trade_date, close, vol, amount, pct_chg

            FROM daily

            WHERE trade_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, trade_date DESC

        """).fetchdf()

        # 从moneyflow取资金流数据

        try:

            mf_df = conn.execute(f""""

                SELECT ts_code, buy_sm_amount, sell_sm_amount, buy_lg_amount, sell_lg_amount

                FROM moneyflow

                WHERE trade_date = '{trade_date}'

                  AND ts_code IN ('{pool_str}')

            """).fetchdf()

            mf_df = mf_df.set_index('ts_code') if not mf_df.empty else pd.DataFrame()

        except Exception:

            mf_df = pd.DataFrame()

        # 合并数据

        df = db_df.copy() if not db_df.empty else pd.DataFrame(index=stock_pool)

        if not mf_df.empty:

            common_idx = df.index.intersection(mf_df.index)

            for col in mf_df.columns:

                df.loc[common_idx, col] = mf_df.loc[common_idx, col]

        # 将当日行情数据合并进 df
        today_daily = daily_df[daily_df['trade_date'] == trade_date].set_index('ts_code')
        for col in ['amount', 'vol', 'pct_chg', 'close']:
            if col in today_daily.columns:
                common_idx = df.index.intersection(today_daily.index)
                df.loc[common_idx, col] = today_daily.loc[common_idx, col]

        # 计算20日成交额统计

        amt_20_dict = {}

        for ts_code, group in daily_df.groupby('ts_code'):

            recent = group.head(20)

            amt_20_dict[ts_code] = {

                'mean_amt': recent['amount'].mean(),

                'std_amt': recent['amount'].std(),

                'current_amt': group.iloc[0]['amount'] if len(group) > 0 else np.nan,

                'illiquidity': (recent['pct_chg'].abs() / recent['amount']).mean() if recent['amount'].sum() > 0 else np.nan

            }

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['S_LN_FLOAT'] = compute_s_ln_float(df)

        result['S_AMT_20_Z'] = compute_s_amt_20_z(df, amt_20_dict=amt_20_dict)

        result['S_TURNOVER'] = compute_s_turnover(df)

        result['S_ILLIQUIDITY'] = compute_s_illiquidity(df, amt_20_dict=amt_20_dict)

        result['S_VOL_PRICE_IMPACT'] = compute_s_vol_price_impact(df)

        # 构建历史价格字典（Roll价差 + L_回撤/波动/偏度）
        price_hist_dict = {}
        for ts_code, grp in daily_df.groupby('ts_code'):
            closes = grp.sort_values('trade_date', ascending=False)['close'].values
            if len(closes) >= 3:
                price_hist_dict[ts_code] = closes

        result['S_SPREAD'] = compute_s_spread(df, price_hist_dict=price_hist_dict)

        result['L_MAX_DRAWDOWN_1Y'] = compute_l_max_drawdown_1y(df, price_hist_dict=price_hist_dict)

        result['L_VOL_1Y'] = compute_l_vol_1y(df, price_hist_dict=price_hist_dict)

        result['L_SKEWNESS'] = compute_l_skewness(df, price_hist_dict=price_hist_dict)

        result['S_MCAP'] = compute_s_mcap(df)

        result['S_FREE_FLOAT_R'] = compute_s_free_float_r(df)

        result['S_LN_MCAP'] = compute_s_ln_mcap(df)

        result['S_AMIHUD'] = compute_s_amihud(df, amt_20_dict=amt_20_dict)

        return result

    except Exception as e:

        logger.error(f"计算流动性因子失败：{e}")

        return pd.DataFrame()

def compute_s_ln_float(df, **kwargs):

    """"S_LN_FLOAT = ln(流通市
    """

    try:

        circ_mv = df['circ_mv'] if 'circ_mv' in df.columns else pd.Series(np.nan, index=df.index)

        return np.log(circ_mv.where(circ_mv > 0))

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_amt_20_z(df, amt_20_dict=None, **kwargs):

    """"S_AMT_20_Z = 20日成交额Z分数"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if amt_20_dict:

            for ts_code in df.index:

                if ts_code in amt_20_dict:

                    d = amt_20_dict[ts_code]

                    if d['std_amt'] > 0:

                        result.loc[ts_code] = (d['current_amt'] - d['mean_amt']) / d['std_amt']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_turnover(df, **kwargs):

    """"S_TURNOVER = 换手"""

    try:

        return df['turnover_rate'].values if 'turnover_rate' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_illiquidity(df, amt_20_dict=None, **kwargs):

    """"S_ILLIQUIDITY = Amihud非流动"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if amt_20_dict:

            for ts_code in df.index:

                if ts_code in amt_20_dict:

                    result.loc[ts_code] = amt_20_dict[ts_code]['illiquidity']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_vol_price_impact(df, **kwargs):

    """"S_VOL_PRICE_IMPACT = 成交量价格冲简"""

    try:

        turnover = df['turnover_rate'] if 'turnover_rate' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(turnover > 0, 1.0 / turnover, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_spread(df, price_hist_dict=None, **kwargs):

    """"S_SPREAD = Roll (1984) 隐含价差 = 2*sqrt(max(-cov(r_t, r_{t-1}), 0))"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_hist_dict:

            for ts_code in df.index:

                prices = price_hist_dict.get(ts_code)

                if prices is not None and len(prices) >= 10:

                    p = prices[:20][::-1]  # 升序，最多20日

                    rets = np.diff(p) / np.where(p[:-1] > 0, p[:-1], np.nan)

                    rets = rets[np.isfinite(rets)]

                    if len(rets) >= 4:

                        cov = np.cov(rets[1:], rets[:-1])[0, 1]

                        result.loc[ts_code] = 2.0 * np.sqrt(max(-cov, 0.0))

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_mcap(df, **kwargs):

    """"S_MCAP = 总市"""

    try:

        return df['total_mv'].values if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_free_float_r(df, **kwargs):

    """"S_FREE_FLOAT_R = 自由流通比"""

    try:

        circ_mv = df['circ_mv'] if 'circ_mv' in df.columns else pd.Series(np.nan, index=df.index)

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(total_mv > 0, circ_mv / total_mv, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_ln_mcap(df, **kwargs):

    """"S_LN_MCAP = ln(总市"""

    try:

        total_mv = df['total_mv'] if 'total_mv' in df.columns else pd.Series(np.nan, index=df.index)

        return np.log(total_mv.where(total_mv > 0))

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_s_amihud(df, amt_20_dict=None, **kwargs):

    """"S_AMIHUD = Amihud (2002) 非流动性指标 = mean(|pct_chg| / amount) × 10^6"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if amt_20_dict:

            for ts_code in df.index:

                if ts_code in amt_20_dict:

                    result.loc[ts_code] = amt_20_dict[ts_code].get('illiquidity', np.nan)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_l_max_drawdown_1y(df, price_hist_dict=None, **kwargs):

    """"近250日最大回撤"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_hist_dict:

            for ts_code in df.index:

                prices = price_hist_dict.get(ts_code)

                if prices is not None and len(prices) >= 2:

                    p = prices[:250][::-1]

                    cummax = np.maximum.accumulate(p)

                    dd = (cummax - p) / np.where(cummax > 0, cummax, np.nan)

                    result.loc[ts_code] = float(np.nanmax(dd))

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_l_vol_1y(df, price_hist_dict=None, **kwargs):

    """"近250日收益率年化波动率"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_hist_dict:

            for ts_code in df.index:

                prices = price_hist_dict.get(ts_code)

                if prices is not None and len(prices) >= 20:

                    p = prices[:250][::-1]

                    rets = np.diff(p) / np.where(p[:-1] > 0, p[:-1], np.nan)

                    result.loc[ts_code] = float(np.nanstd(rets) * np.sqrt(250))

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_l_skewness(df, price_hist_dict=None, **kwargs):

    """"近250日日收益率偏度"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_hist_dict:

            for ts_code in df.index:

                prices = price_hist_dict.get(ts_code)

                if prices is not None and len(prices) >= 20:

                    p = prices[:250][::-1]

                    rets = np.diff(p) / np.where(p[:-1] > 0, p[:-1], np.nan)

                    rets = rets[np.isfinite(rets)]

                    if len(rets) >= 4:

                        mu, sigma = rets.mean(), rets.std()

                        if sigma > 0:

                            result.loc[ts_code] = float(np.mean(((rets - mu) / sigma) ** 3))

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

