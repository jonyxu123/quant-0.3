""""波动率因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'VOL_20D': {

            'dimension': 'VOL_',

            'name': '20日波动率',

            'direction': 'negative',

            'compute_func': compute_vol_20d

        },

        'VOL_60D': {

            'dimension': 'VOL_',

            'name': '60日波动率',

            'direction': 'negative',

            'compute_func': compute_vol_60d

        },

        'VOL_ANNUALIZED': {

            'dimension': 'VOL_',

            'name': '年化波动率',

            'direction': 'negative',

            'compute_func': compute_vol_annualized

        },

        'VOL_IDIOSYNCRATIC': {

            'dimension': 'VOL_',

            'name': '特质波动率',

            'direction': 'negative',

            'compute_func': compute_vol_idiosyncratic

        },

        'VOL_DOWNSIDE': {

            'dimension': 'VOL_',

            'name': '下行波动率',

            'direction': 'negative',

            'compute_func': compute_vol_downside

        },

        'VOL_UPSIDE': {

            'dimension': 'VOL_',

            'name': '上行波动率',

            'direction': 'positive',

            'compute_func': compute_vol_upside

        },

        'VOL_OMEGA': {

            'dimension': 'VOL_',

            'name': 'Omega比率',

            'direction': 'positive',

            'compute_func': compute_vol_omega

        },

        'VOL_BETA': {

            'dimension': 'VOL_',

            'name': 'Beta系数',

            'direction': 'bidirectional',

            'compute_func': compute_vol_beta

        },

        'VOL_MAX_DRAWDOWN': {

            'dimension': 'VOL_',

            'name': '最大回撤',

            'direction': 'negative',

            'compute_func': compute_vol_max_drawdown

        },

        'VOL_VOL_OF_VOL': {

            'dimension': 'VOL_',

            'name': '波动率的波动',

            'direction': 'negative',

            'compute_func': compute_vol_vol_of_vol

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

        # 从daily取行情数50个交易日)

        daily_df = conn.execute(f""""

            SELECT ts_code, trade_date, close, pct_chg

            FROM daily

            WHERE trade_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, trade_date DESC

        """).fetchdf()

        # 从index_daily取指数数用于Beta/IVOL)

        try:

            idx_df = conn.execute(f""""

                SELECT ts_code, trade_date, close, pct_chg

                FROM index_daily

                WHERE trade_date <= '{trade_date}'

                  AND ts_code = '000300.SH'

                ORDER BY trade_date DESC

            """).fetchdf()

        except Exception:

            idx_df = pd.DataFrame()

        # 构建数据

        df = pd.DataFrame(index=stock_pool)

        # 计算波动率统计
        vol_stats = _compute_vol_stats(daily_df, idx_df, df.index)

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['VOL_20D'] = compute_vol_20d(df, vol_stats=vol_stats)

        result['VOL_60D'] = compute_vol_60d(df, vol_stats=vol_stats)

        result['VOL_ANNUALIZED'] = compute_vol_annualized(df, vol_stats=vol_stats)

        result['VOL_IDIOSYNCRATIC'] = compute_vol_idiosyncratic(df, vol_stats=vol_stats)

        result['VOL_DOWNSIDE'] = compute_vol_downside(df, vol_stats=vol_stats)

        result['VOL_UPSIDE'] = compute_vol_upside(df, vol_stats=vol_stats)

        result['VOL_OMEGA'] = compute_vol_omega(df, vol_stats=vol_stats)

        result['VOL_BETA'] = compute_vol_beta(df, vol_stats=vol_stats)

        result['VOL_MAX_DRAWDOWN'] = compute_vol_max_drawdown(df, vol_stats=vol_stats)

        result['VOL_VOL_OF_VOL'] = compute_vol_vol_of_vol(df, vol_stats=vol_stats)

        return result

    except Exception as e:

        logger.error(f"计算波动率因子失败：{e}")

        return pd.DataFrame()

def _compute_vol_stats(daily_df: pd.DataFrame, idx_df: pd.DataFrame, index) -> dict:

    """"计算波动率统计"""

    stats = {}

    try:

        for ts_code, group in daily_df.groupby('ts_code'):

            group = group.sort_values('trade_date', ascending=False)

            pct = group['pct_chg'].values

            s = {}

            # 20日波动率

            if len(pct) >= 20:

                s['vol_20d'] = np.std(pct[:20])

            # 60日波动率

            if len(pct) >= 60:

                s['vol_60d'] = np.std(pct[:60])

            # 年化波动率
            if len(pct) >= 20:

                s['vol_annualized'] = np.std(pct[:20]) * np.sqrt(250)

            # 下行波动率
            if len(pct) >= 20:

                neg = pct[:20][pct[:20] < 0]

                s['vol_downside'] = np.std(neg) if len(neg) > 1 else np.nan

            # 上行波动率
            if len(pct) >= 20:

                pos = pct[:20][pct[:20] > 0]

                s['vol_upside'] = np.std(pos) if len(pos) > 1 else np.nan

            # 最大回撤
            if len(group) >= 60:

                close_arr = group.iloc[:60]['close'].values[::-1]

                peak = np.maximum.accumulate(close_arr)

                drawdown = (peak - close_arr) / peak

                s['max_drawdown'] = np.max(drawdown)

            # 波动率的波动
            if len(pct) >= 60:

                rolling_vol = np.array([np.std(pct[i:i+20]) for i in range(0, 40, 5) if i + 20 <= len(pct)])

                if len(rolling_vol) > 1:

                    s['vol_of_vol'] = np.std(rolling_vol)

            # Beta和IVOL(需要指数数

            if not idx_df.empty and len(pct) >= 60:

                idx_pct = idx_df.head(60)['pct_chg'].values

                if len(idx_pct) >= 60:

                    stock_r = pct[:60]

                    idx_r = idx_pct[:60]

                    cov_mat = np.cov(stock_r, idx_r)

                    if cov_mat[1, 1] > 0:

                        beta = cov_mat[0, 1] / cov_mat[1, 1]
                        s['beta'] = beta

                        # IVOL = 残差标准差（带截距项的 CAPM 回归）
                        # stock_r = alpha + beta * idx_r + residual
                        alpha = np.mean(stock_r) - beta * np.mean(idx_r)
                        residual = stock_r - alpha - beta * idx_r

                        s['ivol'] = np.std(residual)

            stats[ts_code] = s

    except Exception as e:

        logger.warning(f"计算波动率统计失败：{e}")

    return stats

def compute_vol_20d(df, vol_stats=None, **kwargs):

    """"VOL_20D = 20日波动率"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'vol_20d' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['vol_20d']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_60d(df, vol_stats=None, **kwargs):

    """"VOL_60D = 60日波动率"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'vol_60d' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['vol_60d']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_annualized(df, vol_stats=None, **kwargs):

    """"VOL_ANNUALIZED = 年化波动率"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'vol_annualized' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['vol_annualized']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_idiosyncratic(df, vol_stats=None, **kwargs):

    """"VOL_IDIOSYNCRATIC = 特质波动率"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'ivol' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['ivol']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_downside(df, vol_stats=None, **kwargs):

    """"VOL_DOWNSIDE = 下行波动率"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'vol_downside' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['vol_downside']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_upside(df, vol_stats=None, **kwargs):

    """"VOL_UPSIDE = 上行波动率"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'vol_upside' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['vol_upside']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_omega(df, vol_stats=None, **kwargs):

    """"VOL_OMEGA = Omega比率(上行/下行波动率比)"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats:

                    s = vol_stats[ts_code]

                    up = s.get('vol_upside', np.nan)

                    down = s.get('vol_downside', np.nan)

                    if np.isfinite(up) and np.isfinite(down) and down > 0:

                        result.loc[ts_code] = up / down

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_beta(df, vol_stats=None, **kwargs):

    """"VOL_BETA = Beta系数"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'beta' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['beta']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_max_drawdown(df, vol_stats=None, **kwargs):

    """"VOL_MAX_DRAWDOWN = 最大回撤"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'max_drawdown' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['max_drawdown']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_vol_vol_of_vol(df, vol_stats=None, **kwargs):

    """"VOL_VOL_OF_VOL = 波动率的波动"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if vol_stats:

            for ts_code in df.index:

                if ts_code in vol_stats and 'vol_of_vol' in vol_stats[ts_code]:

                    result.loc[ts_code] = vol_stats[ts_code]['vol_of_vol']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

