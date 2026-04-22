""""情绪因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'SENT_SHORT_RATIO': {

            'dimension': 'SENT_',

            'name': '融券卖出',

            'direction': 'negative',

            'compute_func': compute_sent_short_ratio

        },

        'SENT_MARGIN_RATIO': {

            'dimension': 'SENT_',

            'name': '融资买入',

            'direction': 'positive',

            'compute_func': compute_sent_margin_ratio

        },

        'SENT_TURNOVER_Z': {

            'dimension': 'SENT_',

            'name': '换手率Z分数',

            'direction': 'bidirectional',

            'compute_func': compute_sent_turnover_z

        },

        'SENT_NEW_HIGH_R': {

            'dimension': 'SENT_',

            'name': '新高占比',

            'direction': 'positive',

            'compute_func': compute_sent_new_high_r

        },

        'SENT_COMMENT_SCORE': {

            'dimension': 'SENT_',

            'name': '评论情绪分数',

            'direction': 'positive',

            'compute_func': compute_sent_comment_score

        },

        'SENT_NEWS_SENTIMENT': {

            'dimension': 'SENT_',

            'name': '新闻情绪',

            'direction': 'positive',

            'compute_func': compute_sent_news_sentiment

        },

        'SENT_CROWDING': {

            'dimension': 'SENT_',

            'name': '拥挤',

            'direction': 'negative',

            'compute_func': compute_sent_crowding

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

        # 从daily_basic取换手率数据

        db_df = conn.execute(f""""

            SELECT ts_code, turnover_rate, turnover_rate_f, total_mv, circ_mv

            FROM daily_basic

            WHERE trade_date = '{trade_date}'

              AND ts_code IN ('{pool_str}')

        """).fetchdf()

        db_df = db_df.set_index('ts_code') if not db_df.empty else pd.DataFrame()

        # 从margin_detail取融资融券数据
        margin_df = conn.execute(f""""
            SELECT ts_code, rzmre, rqmcl, rzye, rqye, rzrqye
            FROM margin_detail
            WHERE trade_date = '{trade_date}'
              AND ts_code IN ('{pool_str}')
        """).fetchdf()
        margin_df = margin_df.set_index('ts_code') if not margin_df.empty else pd.DataFrame()
        # 转换数值类型
        for col in ['rzmre', 'rqmcl', 'rzye', 'rqye', 'rzrqye']:
            if col in margin_df.columns:
                margin_df[col] = pd.to_numeric(margin_df[col], errors='coerce')

        # 从daily取行情数据        
        daily_df = conn.execute(f""""

            SELECT ts_code, trade_date, close, high, pct_chg

            FROM daily

            WHERE trade_date <= '{trade_date}'

              AND ts_code IN ('{pool_str}')

            ORDER BY ts_code, trade_date DESC

        """).fetchdf()

        # 合并数据
        df = db_df.copy() if not db_df.empty else pd.DataFrame(index=stock_pool)
        if not margin_df.empty:
            common_idx = df.index.intersection(margin_df.index)
            for col in margin_df.columns:
                df.loc[common_idx, col] = margin_df.loc[common_idx, col]

        # 计算换手率Z分数

        turnover_stats = {}

        for ts_code, group in daily_df.groupby('ts_code'):

            if len(group) >= 20:

                turnovers = []

                for _, row in group.head(20).iterrows():

                    if ts_code in db_df.index:

                        turnovers.append(db_df.loc[ts_code, 'turnover_rate'])

                if len(turnovers) > 1:

                    turnover_stats[ts_code] = {

                        'mean': np.mean(turnovers),

                        'std': np.std(turnovers),

                        'current': turnovers[0] if turnovers else np.nan

                    }

        # 计算新高占比

        new_high_stats = {}

        for ts_code, group in daily_df.groupby('ts_code'):

            if len(group) >= 60:

                high_60 = group.head(60)['high'].max()

                cur_close = group.iloc[0]['close']

                new_high_stats[ts_code] = 1 if cur_close >= high_60 else 0

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['SENT_SHORT_RATIO'] = compute_sent_short_ratio(df)

        result['SENT_MARGIN_RATIO'] = compute_sent_margin_ratio(df)

        result['SENT_TURNOVER_Z'] = compute_sent_turnover_z(df, turnover_stats=turnover_stats)

        result['SENT_NEW_HIGH_R'] = compute_sent_new_high_r(df, new_high_stats=new_high_stats)

        result['SENT_COMMENT_SCORE'] = compute_sent_comment_score(df)

        result['SENT_NEWS_SENTIMENT'] = compute_sent_news_sentiment(df)

        result['SENT_CROWDING'] = compute_sent_crowding(df)

        return result

    except Exception as e:

        logger.error(f"计算情绪因子失败: {e}")

        return pd.DataFrame()

def compute_sent_short_ratio(df, **kwargs):

    """"SENT_SHORT_RATIO = 融券卖出占比 = 融券卖出量 / 流通股本

    使用 margin_detail.rqmcl（融券卖出量，单位：万股）/ circ_mv 推算的流通股数
    """

    try:
        rqmcl = df['rqmcl'] if 'rqmcl' in df.columns else pd.Series(np.nan, index=df.index)
        circ_mv = df['circ_mv'] if 'circ_mv' in df.columns else pd.Series(np.nan, index=df.index)
        # circ_mv 单位是万元，假设股价约等于当日均价，估算流通股数（万股）
        # 更简单的做法：直接用 rqmcl / 100 作为标准化后的值（每100万股为一个单位）
        # 或者使用融券余额占比：rqye / rzrqye
        rqye = df['rqye'] if 'rqye' in df.columns else pd.Series(np.nan, index=df.index)
        rzrqye = df['rzrqye'] if 'rzrqye' in df.columns else pd.Series(np.nan, index=df.index)
        # 融券余额占比（更稳定）
        return np.where(rzrqye > 0, rqye / rzrqye, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_sent_margin_ratio(df, **kwargs):

    """"SENT_MARGIN_RATIO = 融资买入占比 = 融资买入额 / 融资融券余额

    使用 margin_detail.rzmre（融资买入额，单位：万元）/ rzrqye
    """

    try:
        rzmre = df['rzmre'] if 'rzmre' in df.columns else pd.Series(np.nan, index=df.index)
        rzrqye = df['rzrqye'] if 'rzrqye' in df.columns else pd.Series(np.nan, index=df.index)
        return np.where(rzrqye > 0, rzmre / rzrqye, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_sent_turnover_z(df, turnover_stats=None, **kwargs):

    """"SENT_TURNOVER_Z = 换手率Z分数"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if turnover_stats:

            for ts_code in df.index:

                if ts_code in turnover_stats:

                    s = turnover_stats[ts_code]

                    if s['std'] > 0:

                        result.loc[ts_code] = (s['current'] - s['mean']) / s['std']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_sent_new_high_r(df, new_high_stats=None, **kwargs):

    """"SENT_NEW_HIGH_R = 新高占比"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if new_high_stats:

            for ts_code in df.index:

                if ts_code in new_high_stats:

                    result.loc[ts_code] = new_high_stats[ts_code]

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_sent_comment_score(df, **kwargs):

    """"SENT_COMMENT_SCORE = 评论情绪分数(需AKShare)"""

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_sent_news_sentiment(df, **kwargs):

    """"SENT_NEWS_SENTIMENT = 新闻情绪(需AKShare)"""

    try:

        return pd.Series(np.nan, index=df.index)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_sent_crowding(df, **kwargs):

    """"SENT_CROWDING = 拥挤度 (简化用换手率)"""

    try:

        turnover = df['turnover_rate'] if 'turnover_rate' in df.columns else pd.Series(np.nan, index=df.index)

        return turnover.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

