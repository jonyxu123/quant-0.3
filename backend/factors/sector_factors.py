""""行业因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'SECTOR_STRENGTH': {

            'dimension': 'SECTOR_',

            'name': '行业强度',

            'direction': 'positive',

            'compute_func': compute_sector_strength

        },

        'SECTOR_MOM_1M': {

            'dimension': 'SECTOR_',

            'name': '行业1月动',

            'direction': 'positive',

            'compute_func': compute_sector_mom_1m

        },

        'SECTOR_FLOW_IN': {

            'dimension': 'SECTOR_',

            'name': '行业资金流入',

            'direction': 'positive',

            'compute_func': compute_sector_flow_in

        },

        'SECTOR_RELATIVE_R': {

            'dimension': 'SECTOR_',

            'name': '行业相对收益',

            'direction': 'positive',

            'compute_func': compute_sector_relative_r

        },

    }

def compute(trade_date: str, conn, stock_pool: list = None) -> pd.DataFrame:

    """"计算该维度全部因子

    使用申万行业分类 + 股票日线数据计算：
    - shenwan_member: 股票所属行业映射
    - daily: 股票日线收益（用于计算行业动量）
    """

    try:

        if stock_pool is None:

            stock_pool_df = conn.execute(

                "SELECT ts_code FROM stock_basic WHERE list_date IS NOT NULL"

            ).fetchdf()

            stock_pool = stock_pool_df['ts_code'].tolist()

        if not stock_pool:

            return pd.DataFrame()

        pool_str = "','".join(stock_pool)

        df = pd.DataFrame(index=stock_pool)

        # 1. 获取股票所属申万一级行业
        try:
            member_df = conn.execute(f""""
                SELECT DISTINCT ON (ts_code)
                    ts_code,
                    index_code as sw_code
                FROM (
                    SELECT
                        sm.con_code as ts_code,
                        sm.index_code,
                        sm.in_date,
                        ROW_NUMBER() OVER (PARTITION BY sm.con_code ORDER BY sm.in_date DESC) as rn
                    FROM shenwan_member sm
                    WHERE sm.con_code IN ('{pool_str}')
                      AND (sm.out_date IS NULL OR sm.out_date > '{trade_date}')
                )
                WHERE rn = 1
            """).fetchdf()
            member_df = member_df.set_index('ts_code') if not member_df.empty else pd.DataFrame()
        except Exception:
            # DuckDB 可能不支持 DISTINCT ON，用备选方案
            member_df = conn.execute(f""""
                SELECT
                    sm.con_code as ts_code,
                    sm.index_code as sw_code
                FROM shenwan_member sm
                INNER JOIN (
                    SELECT con_code, MAX(in_date) as max_in_date
                    FROM shenwan_member
                    WHERE con_code IN ('{pool_str}')
                      AND (out_date IS NULL OR out_date > '{trade_date}')
                    GROUP BY con_code
                ) latest ON sm.con_code = latest.con_code AND sm.in_date = latest.max_in_date
            """).fetchdf()
            member_df = member_df.set_index('ts_code') if not member_df.empty else pd.DataFrame()

        # 2. 获取申万行业日线数据（用于计算行业动量）
        sw_daily_df = conn.execute(f""""
            SELECT ts_code as sw_code, trade_date, close, pct_change
            FROM shenwan_daily
            WHERE trade_date <= '{trade_date}'
            ORDER BY ts_code, trade_date DESC
        """).fetchdf()

        # 3. 计算每个行业近20日涨跌幅（作为行业动量）
        industry_stats = {}
        if not sw_daily_df.empty:
            # 按行业计算20日收益（1月动量）
            for sw_code, group in sw_daily_df.groupby('sw_code'):
                # 只取一级行业 (801XXX.SI)
                if not sw_code.startswith('801') or not sw_code.endswith('.SI'):
                    continue
                # 获取近20日数据
                recent = group.head(20)
                if len(recent) > 0:
                    avg_return = recent['pct_change'].mean()
                    # 累计收益率
                    total_return = recent['pct_change'].sum()
                    industry_stats[sw_code] = {
                        'avg_return_20d': avg_return,
                        'total_return_20d': total_return,
                        'count': len(recent)
                    }

        # 4. 构建股票->行业的因子映射
        # 注意：member_df中的sw_code不带.SI后缀，需要添加以匹配shenwan_daily
        stock_industry_map = {}
        for ts_code in stock_pool:
            if ts_code in member_df.index:
                sw_code = member_df.loc[ts_code, 'sw_code']
                # 添加.SI后缀以匹配shenwan_daily表的ts_code格式
                if not sw_code.endswith('.SI'):
                    sw_code = f"{sw_code}.SI"
                stock_industry_map[ts_code] = sw_code

        result = pd.DataFrame(index=df.index)

        result['SECTOR_STRENGTH'] = compute_sector_strength(df, industry_stats=industry_stats, stock_industry_map=stock_industry_map)

        result['SECTOR_MOM_1M'] = compute_sector_mom_1m(df, industry_stats=industry_stats, stock_industry_map=stock_industry_map)

        result['SECTOR_FLOW_IN'] = compute_sector_flow_in(df)

        result['SECTOR_RELATIVE_R'] = compute_sector_relative_r(df, industry_stats=industry_stats, stock_industry_map=stock_industry_map)

        return result

    except Exception as e:

        logger.error(f"计算行业因子失败: {e}")

        return pd.DataFrame()

def compute_sector_strength(df, industry_stats=None, stock_industry_map=None, **kwargs):

    """"SECTOR_STRENGTH = 行业强度

    基于行业成分股近20日平均涨跌幅排名标准化后的值
    """

    try:
        result = pd.Series(np.nan, index=df.index)
        if industry_stats and stock_industry_map:
            # 获取所有行业动量值
            values = [v['total_return_20d'] for v in industry_stats.values() if 'total_return_20d' in v]
            if len(values) > 0:
                mean_val = np.mean(values)
                std_val = np.std(values) if len(values) > 1 else 1
                for ts_code in df.index:
                    if ts_code in stock_industry_map:
                        sw_code = stock_industry_map[ts_code]
                        if sw_code in industry_stats:
                            # 标准化到 -3 ~ +3 范围
                            val = industry_stats[sw_code]['total_return_20d']
                            result.loc[ts_code] = (val - mean_val) / std_val if std_val > 0 else 0
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_sector_mom_1m(df, industry_stats=None, stock_industry_map=None, **kwargs):

    """"SECTOR_MOM_1M = 行业1月动量

    使用行业成分股近20日累计收益率（%）
    """

    try:
        result = pd.Series(np.nan, index=df.index)
        if industry_stats and stock_industry_map:
            for ts_code in df.index:
                if ts_code in stock_industry_map:
                    sw_code = stock_industry_map[ts_code]
                    if sw_code in industry_stats:
                        result.loc[ts_code] = industry_stats[sw_code].get('total_return_20d', np.nan)
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_sector_flow_in(df, **kwargs):

    """"SECTOR_FLOW_IN = 行业资金流入（暂用0占位，需moneyflow数据）"""

    try:
        # 简化实现，返回0表示暂不可用
        return pd.Series(0, index=df.index).astype(float)
    except Exception:
        return pd.Series(np.nan, index=df.index)

def compute_sector_relative_r(df, industry_stats=None, stock_industry_map=None, **kwargs):

    """"SECTOR_RELATIVE_R = 行业相对收益

    个股收益 / 行业平均收益 - 1，表示个股相对于行业的超额收益
    """

    try:
        result = pd.Series(np.nan, index=df.index)
        # 需要个股收益数据，这里简化为行业收益的相对排名
        if industry_stats and stock_industry_map:
            # 计算行业中位数收益作为基准
            all_returns = [v.get('total_return_20d', 0) for v in industry_stats.values()]
            if len(all_returns) > 0:
                median_return = np.median(all_returns)
                for ts_code in df.index:
                    if ts_code in stock_industry_map:
                        sw_code = stock_industry_map[ts_code]
                        if sw_code in industry_stats:
                            ind_return = industry_stats[sw_code].get('total_return_20d', 0)
                            # 行业相对市场基准的收益
                            result.loc[ts_code] = (ind_return - median_return) / abs(median_return) if median_return != 0 else 0
        return result.values
    except Exception:
        return pd.Series(np.nan, index=df.index)

