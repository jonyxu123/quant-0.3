""""机构因子模块"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'I_FUND_COVER': {

            'dimension': 'I_',

            'name': '基金覆盖',

            'direction': 'positive',

            'compute_func': compute_i_fund_cover

        },

        'I_FUND_HOLD_R': {
            'dimension': 'I_',
            'name': '基金持仓比例',
            'direction': 'positive',
            'compute_func': compute_i_fund_hold_r
        },
        'I_FUND_HOLD_PCT': {  # 别名，与策略命名保持一致
            'dimension': 'I_',
            'name': '基金持仓比例(策略别名)',
            'direction': 'positive',
            'compute_func': compute_i_fund_hold_r
        },
        'I_NORTH_HOLD_R': {

            'dimension': 'I_',

            'name': '北向持仓比例',

            'direction': 'positive',

            'compute_func': compute_i_north_hold_r

        },

        'I_NORTH_NET_IN_5D': {

            'dimension': 'I_',

            'name': '北向5日净流入',

            'direction': 'positive',

            'compute_func': compute_i_north_net_in_5d

        },

        'I_NORTH_NET_IN_20D': {

            'dimension': 'I_',

            'name': '北向20日净流入',

            'direction': 'positive',

            'compute_func': compute_i_north_net_in_20d

        },

        'I_QFII_HOLD': {

            'dimension': 'I_',

            'name': 'QFII持仓',

            'direction': 'positive',

            'compute_func': compute_i_qfii_hold

        },

        'I_INSIDER_BUY_R': {

            'dimension': 'I_',

            'name': '内部人买入比',

            'direction': 'positive',

            'compute_func': compute_i_insider_buy_r

        },

        'I_INST_HOLD_CHG': {

            'dimension': 'I_',

            'name': '机构持仓变化',

            'direction': 'positive',

            'compute_func': compute_i_inst_hold_chg

        },

        'I_MAIN_FORCE_R': {

            'dimension': 'I_',

            'name': '主力占比',

            'direction': 'positive',

            'compute_func': compute_i_main_force_r

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

        try:

            fp_df = conn.execute(f""""

                SELECT ts_code, end_date, amount AS hold_amount, stk_mkv_ratio AS hold_ratio

                FROM fund_portfolio

                WHERE ann_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ts_code, end_date DESC

            """).fetchdf()

        except Exception:

            fp_df = pd.DataFrame()

        try:

            hsgt_df = conn.execute(f""""

                SELECT ts_code, trade_date, net_amount

                FROM hsgt_top10

                WHERE trade_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ts_code, trade_date DESC

            """).fetchdf()

            if not hsgt_df.empty and 'net_amount' in hsgt_df.columns:

                hsgt_df['net_amount'] = pd.to_numeric(hsgt_df['net_amount'], errors='coerce')

        except Exception:

            hsgt_df = pd.DataFrame()

        try:

            hh_df = conn.execute(f""""

                SELECT ts_code, trade_date, vol AS hold_amount, ratio AS hold_ratio, ratio AS vol_ratio

                FROM hk_hold

                WHERE trade_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ts_code, trade_date DESC

            """).fetchdf()

            if not hh_df.empty:

                for col in ['hold_amount', 'hold_ratio', 'vol_ratio']:

                    if col in hh_df.columns:

                        hh_df[col] = pd.to_numeric(hh_df[col], errors='coerce')

        except Exception:

            hh_df = pd.DataFrame()

        # 构建数据

        df = pd.DataFrame(index=stock_pool)

        # 内部人交易数据
        try:
            ht_df = conn.execute(f""""
                SELECT ts_code, trade_date, change_vol, change_type
                FROM stk_holdertrade
                WHERE trade_date <= '{trade_date}'
                  AND ts_code IN ('{pool_str}')
                ORDER BY ts_code, trade_date DESC
            """).fetchdf()
        except Exception:
            ht_df = pd.DataFrame()

        # 前10大流通股东持股数据（用于机构持仓变化）
        try:
            th_df = conn.execute(f""""
                SELECT ts_code, ann_date, holder_name, hold_ratio, hold_ratio_ann
                FROM top10_floatholders
                WHERE ann_date <= '{trade_date}'
                  AND ts_code IN ('{pool_str}')
                ORDER BY ts_code, ann_date DESC
            """).fetchdf()
        except Exception:
            th_df = pd.DataFrame()

        # 资金流向数据（用于主力占比）
        try:
            mf_df = conn.execute(f""""
                SELECT ts_code, buy_lg_amount, sell_lg_amount, buy_elg_amount, sell_elg_amount
                FROM moneyflow
                WHERE trade_date = '{trade_date}'
                  AND ts_code IN ('{pool_str}')
            """).fetchdf()
            mf_df = mf_df.set_index('ts_code') if not mf_df.empty else pd.DataFrame()
        except Exception:
            mf_df = pd.DataFrame()

        # 计算基金持仓统计

        fund_stats = _compute_fund_stats(fp_df, df.index)

        # 计算北向统计

        north_stats = _compute_north_stats(hsgt_df, hh_df, df.index, trade_date)

        # 内部人交易统计
        insider_stats = _compute_insider_stats(ht_df, df.index)

        # 机构持股变化统计
        inst_chg_stats = _compute_inst_chg_stats(th_df, df.index)

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['I_FUND_COVER'] = compute_i_fund_cover(df, fund_stats=fund_stats)

        result['I_FUND_HOLD_R'] = compute_i_fund_hold_r(df, fund_stats=fund_stats)

        result['I_NORTH_HOLD_R'] = compute_i_north_hold_r(df, north_stats=north_stats)

        result['I_NORTH_NET_IN_5D'] = compute_i_north_net_in_5d(df, north_stats=north_stats)

        result['I_NORTH_NET_IN_20D'] = compute_i_north_net_in_20d(df, north_stats=north_stats)

        result['I_QFII_HOLD'] = compute_i_qfii_hold(df, inst_chg_stats=inst_chg_stats)

        result['I_INSIDER_BUY_R'] = compute_i_insider_buy_r(df, insider_stats=insider_stats)

        result['I_INST_HOLD_CHG'] = compute_i_inst_hold_chg(df, inst_chg_stats=inst_chg_stats)

        result['I_MAIN_FORCE_R'] = compute_i_main_force_r(df, mf_df=mf_df)

        return result

    except Exception as e:

        logger.error(f"计算机构因子失败: {e}")

        return pd.DataFrame()

def _compute_fund_stats(fp_df: pd.DataFrame, index) -> dict:

    """"计算基金持仓统计"""

    stats = {}

    try:

        if fp_df.empty:

            return stats

        for ts_code, group in fp_df.groupby('ts_code'):

            latest = group.head(1)

            stats[ts_code] = {

                'count': len(group['end_date'].unique()),

                'hold_ratio': latest['hold_ratio'].values[0] if 'hold_ratio' in latest.columns and len(latest) > 0 else 0,

                'hold_amount': latest['hold_amount'].values[0] if 'hold_amount' in latest.columns and len(latest) > 0 else 0,

            }

    except Exception as e:

        logger.warning(f"计算基金持仓统计失败: {e}")

    return stats

def _compute_north_stats(hsgt_df: pd.DataFrame, hh_df: pd.DataFrame, index, trade_date: str) -> dict:

    """"计算北向资金统计"""

    stats = {}

    try:

        # 从 hk_hold 取最新持仓比例
        if not hh_df.empty:

            for ts_code, group in hh_df.groupby('ts_code'):

                latest = group.head(1)

                stats[ts_code] = {

                    'hold_ratio': latest['hold_ratio'].values[0] if 'hold_ratio' in latest.columns and len(latest) > 0 else 0,

                    'net_in_5d': 0,

                    'net_in_20d': 0,

                }

        # 从 hsgt_top10 取净流入金额累加
        if not hsgt_df.empty:

            for ts_code, group in hsgt_df.groupby('ts_code'):

                if ts_code not in stats:

                    stats[ts_code] = {'hold_ratio': 0, 'net_in_5d': 0, 'net_in_20d': 0}

                stats[ts_code]['net_in_5d'] = group.head(5)['net_amount'].sum() if len(group) >= 1 else 0

                stats[ts_code]['net_in_20d'] = group.head(20)['net_amount'].sum() if len(group) >= 1 else 0

    except Exception as e:

        logger.warning(f"计算北向资金统计失败: {e}")

    return stats

def compute_i_fund_cover(df, fund_stats=None, **kwargs):

    """"I_FUND_COVER = 基金覆盖"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if fund_stats:

            for ts_code in df.index:

                if ts_code in fund_stats:

                    result.loc[ts_code] = fund_stats[ts_code]['count']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_i_fund_hold_r(df, fund_stats=None, **kwargs):

    """"I_FUND_HOLD_R = 基金持仓比例"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if fund_stats:

            for ts_code in df.index:

                if ts_code in fund_stats:

                    result.loc[ts_code] = fund_stats[ts_code]['hold_ratio']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_i_north_hold_r(df, north_stats=None, **kwargs):

    """"I_NORTH_HOLD_R = 北向持仓比例"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if north_stats:

            for ts_code in df.index:

                if ts_code in north_stats:

                    result.loc[ts_code] = north_stats[ts_code]['hold_ratio']

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_i_north_net_in_5d(df, north_stats=None, **kwargs):

    """"I_NORTH_NET_IN_5D = 北向5日净流入"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if north_stats:

            for ts_code in df.index:

                if ts_code in north_stats:

                    result.loc[ts_code] = north_stats[ts_code].get('net_in_5d', 0)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_i_north_net_in_20d(df, north_stats=None, **kwargs):

    """"I_NORTH_NET_IN_20D = 北向20日净流入"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if north_stats:

            for ts_code in df.index:

                if ts_code in north_stats:

                    result.loc[ts_code] = north_stats[ts_code].get('net_in_20d', 0)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def _compute_insider_stats(ht_df: pd.DataFrame, index) -> dict:
    """"stk_holdertrade: 计算近90日内部人净买入量"""
    stats = {}
    if ht_df.empty:
        return stats
    try:
        for ts_code, grp in ht_df.groupby('ts_code'):
            recent = grp.head(90)
            buy_vol = recent[recent['change_type'] == 'Increase']['change_vol'].sum() if 'change_type' in recent.columns else 0
            sell_vol = recent[recent['change_type'] == 'Decrease']['change_vol'].sum() if 'change_type' in recent.columns else 0
            stats[ts_code] = {'net_buy_vol': float(buy_vol) - float(sell_vol)}
    except Exception as e:
        logger.warning(f"insider stats failed: {e}")
    return stats

def _compute_inst_chg_stats(th_df: pd.DataFrame, index) -> dict:
    """"top10_floatholders: 计算最新两期机构持股比变化"""
    stats = {}
    if th_df.empty:
        return stats
    try:
        for ts_code, grp in th_df.groupby('ts_code'):
            periods = grp['ann_date'].unique()
            periods = sorted(periods, reverse=True)
            # QFII: 持股名称包含外资标志
            latest = grp[grp['ann_date'] == periods[0]]
            qfii_ratio = latest[latest['holder_name'].str.contains(
                r'QFII|外资|威尔|Goldman|Morgan|Citibank|Deutsche', na=False
            )]['hold_ratio'].sum() if 'hold_ratio' in latest.columns else 0.0
            # 机构持仓变化: 最新期合计比例 - 上一期合计比例
            cur_ratio = latest['hold_ratio'].sum() if 'hold_ratio' in latest.columns else 0.0
            prev_ratio = 0.0
            if len(periods) >= 2:
                prev = grp[grp['ann_date'] == periods[1]]
                prev_ratio = prev['hold_ratio'].sum() if 'hold_ratio' in prev.columns else 0.0
            stats[ts_code] = {
                'qfii_ratio': float(qfii_ratio),
                'inst_chg': float(cur_ratio) - float(prev_ratio),
            }
    except Exception as e:
        logger.warning(f"inst chg stats failed: {e}")
    return stats

def compute_i_qfii_hold(df, inst_chg_stats=None, **kwargs):

    """"I_QFII_HOLD = 前10大流通股东中境外机构持仓比例"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if inst_chg_stats:

            for ts_code in df.index:

                if ts_code in inst_chg_stats:

                    result.loc[ts_code] = inst_chg_stats[ts_code].get('qfii_ratio', np.nan)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_i_insider_buy_r(df, insider_stats=None, **kwargs):

    """"I_INSIDER_BUY_R = 近90日内部人净买入股数（万股）"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if insider_stats:

            for ts_code in df.index:

                if ts_code in insider_stats:

                    result.loc[ts_code] = insider_stats[ts_code].get('net_buy_vol', np.nan)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_i_inst_hold_chg(df, inst_chg_stats=None, **kwargs):

    """"I_INST_HOLD_CHG = 前10大流通股东机构合计持股比华比变化（pp）"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if inst_chg_stats:

            for ts_code in df.index:

                if ts_code in inst_chg_stats:

                    result.loc[ts_code] = inst_chg_stats[ts_code].get('inst_chg', np.nan)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_i_main_force_r(df, mf_df=None, **kwargs):

    """"I_MAIN_FORCE_R = 当日大单+超大单净流入占总成交额比例"""

    try:

        result = pd.Series(np.nan, index=df.index)

        if mf_df is not None and not mf_df.empty:

            for ts_code in df.index:

                if ts_code in mf_df.index:

                    row = mf_df.loc[ts_code]

                    buy  = row.get('buy_lg_amount', 0) + row.get('buy_elg_amount', 0)

                    sell = row.get('sell_lg_amount', 0) + row.get('sell_elg_amount', 0)

                    total = buy + sell

                    if total > 0:

                        result.loc[ts_code] = (buy - sell) / total

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

