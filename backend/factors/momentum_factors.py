""""动量因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'M_MOM_12_1': {

            'dimension': 'M_',

            'name': '12月动量',

            'direction': 'positive',

            'compute_func': compute_m_mom_12_1

        },

        'M_MOM_6M': {

            'dimension': 'M_',

            'name': '6月动量',

            'direction': 'positive',

            'compute_func': compute_m_mom_6m

        },

        'M_MOM_3M': {

            'dimension': 'M_',

            'name': '3月动量',

            'direction': 'positive',

            'compute_func': compute_m_mom_3m

        },

        'M_RS_3M': {

            'dimension': 'M_',

            'name': '3月相对强度',

            'direction': 'positive',

            'compute_func': compute_m_rs_3m

        },

        'M_RS_6M': {

            'dimension': 'M_',

            'name': '6月相对强度',

            'direction': 'positive',

            'compute_func': compute_m_rs_6m

        },

        'M_NEW_HIGH_20': {

            'dimension': 'M_',

            'name': '20日新高占比',

            'direction': 'positive',

            'compute_func': compute_m_new_high_20

        },

        'M_NEW_HIGH_60': {

            'dimension': 'M_',

            'name': '60日新高占比',

            'direction': 'positive',

            'compute_func': compute_m_new_high_60

        },

        'M_MA_BULL': {

            'dimension': 'M_',

            'name': '均线多头排列',

            'direction': 'positive',

            'compute_func': compute_m_ma_bull

        },

        'M_RSI_OVERSOLD': {

            'dimension': 'M_',

            'name': 'RSI超卖',

            'direction': 'positive',

            'compute_func': compute_m_rsi_oversold

        },

        'M_RSI_50_70': {

            'dimension': 'M_',

            'name': 'RSI50-70区间',

            'direction': 'positive',

            'compute_func': compute_m_rsi_50_70

        },

        'M_VOL_ACCEL': {

            'dimension': 'M_',

            'name': '放量加速',

            'direction': 'positive',

            'compute_func': compute_m_vol_accel

        },

        'M_MACD_GOLDEN': {

            'dimension': 'M_',

            'name': 'MACD金叉',

            'direction': 'positive',

            'compute_func': compute_m_macd_golden

        },

        'M_FACTOR_MOM': {

            'dimension': 'M_',

            'name': '因子动量',

            'direction': 'positive',

            'compute_func': compute_m_factor_mom

        },

        'M_INDU_MOM': {

            'dimension': 'M_',

            'name': '行业动量',

            'direction': 'positive',

            'compute_func': compute_m_indu_mom

        },

        'M_TIME_SERIES_MOM': {

            'dimension': 'M_',

            'name': '时间序列动量',

            'direction': 'positive',

            'compute_func': compute_m_time_series_mom

        },

        'M_CROSS_SECTIONAL_RS': {

            'dimension': 'M_',

            'name': '截面相对强度',

            'direction': 'positive',

            'compute_func': compute_m_cross_sectional_rs

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

        # 从stock_basic取行业信息（用于 M_INDU_MOM）
        try:
            sb_df = conn.execute(f""""
                SELECT ts_code, industry FROM stock_basic
                WHERE ts_code IN ('{pool_str}')
            """).fetchdf()
            industry_dict = dict(zip(sb_df['ts_code'], sb_df['industry']))
        except Exception:
            industry_dict = {}

        # 从daily取行情50个交易日）

        daily_df = conn.execute(f""""

            SELECT ts_code, trade_date, close, vol, amount, pct_chg

            FROM daily

            WHERE trade_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, trade_date DESC

        """).fetchdf()

        # 从stk_factor_pro取技术指        
        try:

            stk_df = conn.execute(f""""

                SELECT ts_code, trade_date, ma_qfq_5, ma_qfq_10, ma_qfq_20, ma_qfq_60,

                       rsi_6, rsi_12, rsi_24, volume_ratio, macd_dif, macd_dea,

                       boll_upper, boll_mid, boll_lower

                FROM stk_factor_pro

                WHERE trade_date = '{trade_date}'

                  AND ts_code IN ('{pool_str}')

            """).fetchdf()

            stk_df = stk_df.set_index('ts_code') if not stk_df.empty else pd.DataFrame()

        except Exception:

            stk_df = pd.DataFrame()

        # 从index_daily取指数数        try:

            idx_df = conn.execute(f""""

                SELECT ts_code, trade_date, close, pct_chg

                FROM index_daily

                WHERE trade_date <= '{trade_date}'

                  AND ts_code = '000300.SH'

                ORDER BY trade_date DESC

            """).fetchdf()

        except Exception:

            idx_df = pd.DataFrame()

        # 构建当日行情快照

        today_daily = daily_df[daily_df['trade_date'] == trade_date].copy()

        today_daily = today_daily.set_index('ts_code') if not today_daily.empty else pd.DataFrame()

        # 构建历史价格字典

        price_dict = {}

        for ts_code, group in daily_df.groupby('ts_code'):

            price_dict[ts_code] = group.sort_values('trade_date', ascending=False)

        # 合并当日数据

        df = today_daily.copy() if not today_daily.empty else pd.DataFrame(index=stock_pool)

        if not stk_df.empty:

            common_idx = df.index.intersection(stk_df.index)

            for col in stk_df.columns:

                if col != 'trade_date':

                    df.loc[common_idx, col] = stk_df.loc[common_idx, col]

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['M_MOM_12_1'] = compute_m_mom_12_1(df, price_dict=price_dict)

        result['M_MOM_6M'] = compute_m_mom_6m(df, price_dict=price_dict)

        result['M_MOM_3M'] = compute_m_mom_3m(df, price_dict=price_dict)

        result['M_RS_3M'] = compute_m_rs_3m(df, price_dict=price_dict, idx_df=idx_df)

        result['M_RS_6M'] = compute_m_rs_6m(df, price_dict=price_dict, idx_df=idx_df)

        result['M_NEW_HIGH_20'] = compute_m_new_high_20(df, price_dict=price_dict)

        result['M_NEW_HIGH_60'] = compute_m_new_high_60(df, price_dict=price_dict)

        result['M_MA_BULL'] = compute_m_ma_bull(df)

        result['M_RSI_OVERSOLD'] = compute_m_rsi_oversold(df)

        result['M_RSI_50_70'] = compute_m_rsi_50_70(df)

        result['M_VOL_ACCEL'] = compute_m_vol_accel(df)

        # 取前一交易日 MACD 数据，用于判断真正金叉（前日DIF<=DEA，当日DIF>DEA）
        prev_macd_dict = {}
        try:
            prev_stk = conn.execute(f""""
                SELECT ts_code, macd_dif, macd_dea
                FROM stk_factor_pro
                WHERE trade_date = (
                    SELECT MAX(trade_date) FROM stk_factor_pro
                    WHERE trade_date < '{trade_date}'
                )
                  AND ts_code IN ('{pool_str}')
            """).fetchdf()
            if not prev_stk.empty:
                for _, row in prev_stk.iterrows():
                    prev_macd_dict[row['ts_code']] = {
                        'dif': row['macd_dif'],
                        'dea': row['macd_dea'],
                    }
        except Exception:
            pass

        result['M_MACD_GOLDEN'] = compute_m_macd_golden(df, prev_macd_dict=prev_macd_dict)

        result['M_FACTOR_MOM'] = compute_m_factor_mom(df, price_dict=price_dict)

        # 计算行业动量：每个行业近4个用交易日的平均涨幅
        indu_mom_dict = {}
        if industry_dict and not daily_df.empty:
            indu_ret = {}  # industry -> list of 60d returns
            for ts_code, grp in price_dict.items():
                indu = industry_dict.get(ts_code)
                if indu is None:
                    continue
                prices = grp['close'].values
                if len(prices) >= 2:
                    ret_60 = (prices[0] - prices[min(59, len(prices)-1)]) / prices[min(59, len(prices)-1)] \
                        if prices[min(59, len(prices)-1)] > 0 else np.nan
                    if np.isfinite(ret_60):
                        indu_ret.setdefault(indu, []).append(ret_60)
            indu_mean = {indu: np.mean(rets) for indu, rets in indu_ret.items() if rets}
            for ts_code in df.index:
                indu = industry_dict.get(ts_code)
                if indu and indu in indu_mean:
                    indu_mom_dict[ts_code] = indu_mean[indu]

        result['M_INDU_MOM'] = compute_m_indu_mom(df, indu_mom_dict=indu_mom_dict)

        result['M_TIME_SERIES_MOM'] = compute_m_time_series_mom(df, price_dict=price_dict)

        result['M_CROSS_SECTIONAL_RS'] = compute_m_cross_sectional_rs(df, price_dict=price_dict)

        return result

    except Exception as e:

        logger.error(f"计算动量因子失败：{e}")

        return pd.DataFrame()

def _get_price_n_days_ago(price_dict, ts_code, n_days):

    """"获取n个交易日前的收盘"""

    try:

        if ts_code not in price_dict:

            return np.nan

        prices = price_dict[ts_code]

        if len(prices) <= n_days:

            return np.nan

        return prices.iloc[n_days]['close']

    except Exception:

        return np.nan

def compute_m_mom_12_1(df, price_dict=None, **kwargs):

    """"M_MOM_12_1 = 12月动量跳1"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict:

            for ts_code in df.index:

                p_1m = _get_price_n_days_ago(price_dict, ts_code, 20)

                p_13m = _get_price_n_days_ago(price_dict, ts_code, 260)

                if np.isfinite(p_1m) and np.isfinite(p_13m) and p_13m > 0:

                    result.loc[ts_code] = p_1m / p_13m - 1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_mom_6m(df, price_dict=None, **kwargs):

    """"M_MOM_6M = 6月动"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict:

            for ts_code in df.index:

                p_0 = _get_price_n_days_ago(price_dict, ts_code, 0)

                p_6m = _get_price_n_days_ago(price_dict, ts_code, 120)

                if np.isfinite(p_0) and np.isfinite(p_6m) and p_6m > 0:

                    result.loc[ts_code] = p_0 / p_6m - 1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_mom_3m(df, price_dict=None, **kwargs):

    """"M_MOM_3M = 3月动"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict:

            for ts_code in df.index:

                p_0 = _get_price_n_days_ago(price_dict, ts_code, 0)

                p_3m = _get_price_n_days_ago(price_dict, ts_code, 60)

                if np.isfinite(p_0) and np.isfinite(p_3m) and p_3m > 0:

                    result.loc[ts_code] = p_0 / p_3m - 1

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_rs_3m(df, price_dict=None, idx_df=None, **kwargs):

    """"M_RS_3M = 3月相对强相对指数)"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict and idx_df is not None and not idx_df.empty:

            idx_ret = 0

            if len(idx_df) > 60:

                idx_ret = idx_df.iloc[0]['close'] / idx_df.iloc[60]['close'] - 1

            for ts_code in df.index:

                p_0 = _get_price_n_days_ago(price_dict, ts_code, 0)

                p_3m = _get_price_n_days_ago(price_dict, ts_code, 60)

                if np.isfinite(p_0) and np.isfinite(p_3m) and p_3m > 0:

                    stock_ret = p_0 / p_3m - 1

                    result.loc[ts_code] = stock_ret - idx_ret

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_rs_6m(df, price_dict=None, idx_df=None, **kwargs):

    """"M_RS_6M = 6月相对强"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict and idx_df is not None and not idx_df.empty:

            idx_ret = 0

            if len(idx_df) > 120:

                idx_ret = idx_df.iloc[0]['close'] / idx_df.iloc[120]['close'] - 1

            for ts_code in df.index:

                p_0 = _get_price_n_days_ago(price_dict, ts_code, 0)

                p_6m = _get_price_n_days_ago(price_dict, ts_code, 120)

                if np.isfinite(p_0) and np.isfinite(p_6m) and p_6m > 0:

                    stock_ret = p_0 / p_6m - 1

                    result.loc[ts_code] = stock_ret - idx_ret

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_new_high_20(df, price_dict=None, **kwargs):

    """"M_NEW_HIGH_20 = 20日新高占"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict:

            for ts_code in df.index:

                prices = price_dict.get(ts_code)

                if prices is not None and len(prices) >= 20:

                    high_20 = prices.iloc[:20]['close'].max()

                    cur_close = prices.iloc[0]['close']

                    result.loc[ts_code] = 1 if cur_close >= high_20 else 0

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_new_high_60(df, price_dict=None, **kwargs):

    """"M_NEW_HIGH_60 = 60日新高占"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if price_dict:

            for ts_code in df.index:

                prices = price_dict.get(ts_code)

                if prices is not None and len(prices) >= 60:

                    high_60 = prices.iloc[:60]['close'].max()

                    cur_close = prices.iloc[0]['close']

                    result.loc[ts_code] = 1 if cur_close >= high_60 else 0

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_ma_bull(df, **kwargs):

    """"M_MA_BULL = 均线多头排列(MA5>MA10>MA20>MA60)"""

    try:

        ma5 = df['ma_qfq_5'] if 'ma_qfq_5' in df.columns else pd.Series(np.nan, index=df.index)

        ma10 = df['ma_qfq_10'] if 'ma_qfq_10' in df.columns else pd.Series(np.nan, index=df.index)

        ma20 = df['ma_qfq_20'] if 'ma_qfq_20' in df.columns else pd.Series(np.nan, index=df.index)

        ma60 = df['ma_qfq_60'] if 'ma_qfq_60' in df.columns else pd.Series(np.nan, index=df.index)

        bull = (ma5 > ma10) & (ma10 > ma20) & (ma20 > ma60)

        return np.where(bull, 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_rsi_oversold(df, **kwargs):

    """"M_RSI_OVERSOLD = RSI超卖信号(RSI<30)"""

    try:

        rsi = df['rsi_6'] if 'rsi_6' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(rsi < 30, 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_rsi_50_70(df, **kwargs):

    """"M_RSI_50_70 = RSI 50-70 区间"""

    try:

        rsi = df['rsi_12'] if 'rsi_12' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where((rsi >= 50) & (rsi <= 70), 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_vol_accel(df, **kwargs):

    """"M_VOL_ACCEL = 放量加速（量比>2）"""

    try:

        vol_ratio = df['volume_ratio'] if 'volume_ratio' in df.columns else pd.Series(np.nan, index=df.index)

        return np.where(vol_ratio > 2, 1, 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_macd_golden(df, prev_macd_dict=None, **kwargs):

    """"M_MACD_GOLDEN = MACD真正金叉: 前日DIF<=DEA且当日DIF>DEA"""

    try:

        dif = df['macd_dif'] if 'macd_dif' in df.columns else pd.Series(np.nan, index=df.index)

        dea = df['macd_dea'] if 'macd_dea' in df.columns else pd.Series(np.nan, index=df.index)

        result = pd.Series(0.0, index=df.index)

        for ts_code in df.index:

            cur_dif = dif.loc[ts_code] if ts_code in dif.index else np.nan

            cur_dea = dea.loc[ts_code] if ts_code in dea.index else np.nan

            if not (np.isfinite(cur_dif) and np.isfinite(cur_dea)):

                result.loc[ts_code] = np.nan

                continue

            if prev_macd_dict and ts_code in prev_macd_dict:

                prev = prev_macd_dict[ts_code]

                prev_dif = prev.get('dif', np.nan)

                prev_dea = prev.get('dea', np.nan)

                if np.isfinite(prev_dif) and np.isfinite(prev_dea):

                    result.loc[ts_code] = 1.0 if (prev_dif <= prev_dea and cur_dif > cur_dea) else 0.0

                else:

                    result.loc[ts_code] = 1.0 if cur_dif > cur_dea else 0.0

            else:

                result.loc[ts_code] = 1.0 if cur_dif > cur_dea else 0.0

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_factor_mom(df, price_dict=None, **kwargs):

    """"M_FACTOR_MOM = 3月动量×6月动量的几何均值（多时间尺度复合）"""

    try:

        mom3 = compute_m_mom_3m(df, price_dict=price_dict)

        mom6 = compute_m_mom_6m(df, price_dict=price_dict)

        m3 = pd.Series(mom3, index=df.index) if isinstance(mom3, np.ndarray) else mom3

        m6 = pd.Series(mom6, index=df.index) if isinstance(mom6, np.ndarray) else mom6

        sign = np.sign(m3) * np.sign(m6)

        geo = np.sqrt(np.abs(m3) * np.abs(m6))

        return (sign * geo).values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_indu_mom(df, indu_mom_dict=None, **kwargs):

    """"M_INDU_MOM = 所属行业近60日平均涨幅"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if indu_mom_dict:

            for ts_code in df.index:

                val = indu_mom_dict.get(ts_code)

                if val is not None:

                    result.loc[ts_code] = val

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_time_series_mom(df, price_dict=None, **kwargs):

    """"M_TIME_SERIES_MOM = 趋势延续得分：1/3/6月动量均为正则得分更高"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if not price_dict:

            return result.values

        mom3 = compute_m_mom_3m(df, price_dict=price_dict)

        mom6 = compute_m_mom_6m(df, price_dict=price_dict)

        mom12 = compute_m_mom_12_1(df, price_dict=price_dict)

        m3 = pd.Series(mom3, index=df.index) if isinstance(mom3, np.ndarray) else mom3

        m6 = pd.Series(mom6, index=df.index) if isinstance(mom6, np.ndarray) else mom6

        m12 = pd.Series(mom12, index=df.index) if isinstance(mom12, np.ndarray) else mom12

        score = ((m3 > 0).astype(float) + (m6 > 0).astype(float) + (m12 > 0).astype(float)) / 3.0

        return score.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_m_cross_sectional_rs(df, price_dict=None, **kwargs):

    """"M_CROSS_SECTIONAL_RS = 截面相对强度"""

    try:

        mom_3m = compute_m_mom_3m(df, price_dict=price_dict)

        if isinstance(mom_3m, np.ndarray):

            mom_3m = pd.Series(mom_3m, index=df.index)

        mean_mom = mom_3m.mean()

        return (mom_3m - mean_mom).values

    except Exception:

        return pd.Series(np.nan, index=df.index)

