""""资金流因子模"""

import pandas as pd

import numpy as np

from loguru import logger

def register() -> dict:

    """"注册因子到全局注册"""

    return {

        'MF_MAIN_NET_IN': {

            'dimension': 'MF_',

            'name': '主力净流入',

            'direction': 'positive',

            'compute_func': compute_mf_main_net_in

        },

        'MF_SUPER_ORDER_NET': {

            'dimension': 'MF_',

            'name': '超大单净流入',

            'direction': 'positive',

            'compute_func': compute_mf_super_order_net

        },

        'MF_BIG_ORDER_NET': {

            'dimension': 'MF_',

            'name': '大单净流入',

            'direction': 'positive',

            'compute_func': compute_mf_big_order_net

        },

        'MF_MARGIN_NET_BUY': {

            'dimension': 'MF_',

            'name': '融资净买入',

            'direction': 'positive',

            'compute_func': compute_mf_margin_net_buy

        },

        'MF_MARGIN_ACCEL': {

            'dimension': 'MF_',

            'name': '融资加',

            'direction': 'positive',

            'compute_func': compute_mf_margin_accel

        },

        'MF_NORTH_NET_IN_5D': {

            'dimension': 'MF_',

            'name': '北向5日净流入',

            'direction': 'positive',

            'compute_func': compute_mf_north_net_in_5d

        },

        'MF_NORTH_NET_IN_20D': {

            'dimension': 'MF_',

            'name': '北向20日净流入',

            'direction': 'positive',

            'compute_func': compute_mf_north_net_in_20d

        },

        'MF_SECTOR_FLOW_IN': {

            'dimension': 'MF_',

            'name': '板块资金流入',

            'direction': 'positive',

            'compute_func': compute_mf_sector_flow_in

        },

        'MF_INDU_FLOW_IN': {

            'dimension': 'MF_',

            'name': '行业资金流入',

            'direction': 'positive',

            'compute_func': compute_mf_indu_flow_in

        },

        'MF_HK_HOLD_CHG': {

            'dimension': 'MF_',

            'name': '港股通持仓变',

            'direction': 'positive',

            'compute_func': compute_mf_hk_hold_chg

        },

        'MF_ETF_INFLOW': {

            'dimension': 'MF_',

            'name': 'ETF资金流入',

            'direction': 'positive',

            'compute_func': compute_mf_etf_inflow

        },

        'MF_MAIN_BUY_R': {

            'dimension': 'MF_',

            'name': '主力买入占比',

            'direction': 'positive',

            'compute_func': compute_mf_main_buy_r

        },

        'MF_RETAIL_OUTFLOW': {

            'dimension': 'MF_',

            'name': '散户流出',

            'direction': 'positive',

            'compute_func': compute_mf_retail_outflow

        },

        'MF_CROWDING': {

            'dimension': 'MF_',

            'name': '拥挤',

            'direction': 'negative',

            'compute_func': compute_mf_crowding

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

        # 从moneyflow取资金流数据

        try:

            mf_df = conn.execute(f""""

                SELECT ts_code, buy_sm_amount, sell_sm_amount,

                       buy_md_amount, sell_md_amount,

                       buy_lg_amount, sell_lg_amount,

                       buy_elg_amount, sell_elg_amount,

                       net_mf_amount, net_mf_vol

                FROM moneyflow

                WHERE trade_date = '{trade_date}'

                  AND ts_code IN ('{pool_str}')

            """).fetchdf()

            mf_df = mf_df.set_index('ts_code') if not mf_df.empty else pd.DataFrame()

        except Exception:

            mf_df = pd.DataFrame()

        # 从hsgt_hold取北向数        
        try:

            hh_df = conn.execute(f""""

                SELECT ts_code, trade_date,
                       vol AS hold_amount, ratio AS hold_ratio, ratio AS vol_ratio

                FROM hk_hold

                WHERE trade_date <= '{trade_date}'

                  AND ts_code IN ('{pool_str}')

                ORDER BY ts_code, trade_date DESC

            """).fetchdf()

        except Exception:

            hh_df = pd.DataFrame()

        # 合并数据

        df = mf_df.copy() if not mf_df.empty else pd.DataFrame(index=stock_pool)

        # 将 hk_hold.ratio 转为数値
        if not hh_df.empty and 'hold_ratio' in hh_df.columns:
            hh_df['hold_ratio'] = pd.to_numeric(hh_df['hold_ratio'], errors='coerce')

        # 计算北向持股比例变化量（最新 - 5日前 / 20日前）

        north_stats = {}

        if not hh_df.empty:

            for ts_code, group in hh_df.groupby('ts_code'):

                group = group.sort_values('trade_date', ascending=False)

                ratios = group['hold_ratio'].values if 'hold_ratio' in group.columns else []

                latest = ratios[0] if len(ratios) >= 1 else np.nan

                r5  = ratios[min(4,  len(ratios)-1)] if len(ratios) >= 2 else np.nan

                r20 = ratios[min(19, len(ratios)-1)] if len(ratios) >= 2 else np.nan

                north_stats[ts_code] = {

                    'net_in_5d':  (latest - r5)  if np.isfinite(latest) and np.isfinite(r5)  else 0,

                    'net_in_20d': (latest - r20) if np.isfinite(latest) and np.isfinite(r20) else 0,

                }

        # 计算各因子
        result = pd.DataFrame(index=df.index)

        result['MF_MAIN_NET_IN'] = compute_mf_main_net_in(df)

        result['MF_SUPER_ORDER_NET'] = compute_mf_super_order_net(df)

        result['MF_BIG_ORDER_NET'] = compute_mf_big_order_net(df)

        result['MF_MARGIN_NET_BUY'] = compute_mf_margin_net_buy(df)

        result['MF_MARGIN_ACCEL'] = compute_mf_margin_accel(df)

        result['MF_NORTH_NET_IN_5D'] = compute_mf_north_net_in_5d(df, north_stats=north_stats)

        result['MF_NORTH_NET_IN_20D'] = compute_mf_north_net_in_20d(df, north_stats=north_stats)

        result['MF_SECTOR_FLOW_IN'] = compute_mf_sector_flow_in(df)

        result['MF_INDU_FLOW_IN'] = compute_mf_indu_flow_in(df)

        result['MF_HK_HOLD_CHG'] = compute_mf_hk_hold_chg(df)

        result['MF_ETF_INFLOW'] = compute_mf_etf_inflow(df)

        result['MF_MAIN_BUY_R'] = compute_mf_main_buy_r(df)

        result['MF_RETAIL_OUTFLOW'] = compute_mf_retail_outflow(df)

        result['MF_CROWDING'] = compute_mf_crowding(df)

        return result

    except Exception as e:

        logger.error(f"计算资金流因子失 {e}")

        return pd.DataFrame()

def compute_mf_main_net_in(df, **kwargs):

    """"MF_MAIN_NET_IN = 主力净流入"""

    try:

        return df['net_mf_amount'].values if 'net_mf_amount' in df.columns else pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_super_order_net(df, **kwargs):

    """"MF_SUPER_ORDER_NET = 超大单净流入"""

    try:

        buy = df['buy_elg_amount'] if 'buy_elg_amount' in df.columns else pd.Series(0, index=df.index)

        sell = df['sell_elg_amount'] if 'sell_elg_amount' in df.columns else pd.Series(0, index=df.index)

        return (buy - sell).values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_big_order_net(df, **kwargs):

    """"MF_BIG_ORDER_NET = 大单净流入"""

    try:

        buy = df['buy_lg_amount'] if 'buy_lg_amount' in df.columns else pd.Series(0, index=df.index)

        sell = df['sell_lg_amount'] if 'sell_lg_amount' in df.columns else pd.Series(0, index=df.index)

        return (buy - sell).values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_margin_net_buy(df, **kwargs):

    """"MF_MARGIN_NET_BUY = 融资净买入 [STUB: 需AKShare数据，暂返回0]"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_margin_accel(df, **kwargs):

    """"MF_MARGIN_ACCEL = 融资加速 [STUB: 需AKShare数据，暂返回0]"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_north_net_in_5d(df, north_stats=None, **kwargs):

    """"MF_NORTH_NET_IN_5D = 北向5日净流入"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if north_stats:

            for ts_code in df.index:

                if ts_code in north_stats:

                    result.loc[ts_code] = north_stats[ts_code].get('net_in_5d', 0)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_north_net_in_20d(df, north_stats=None, **kwargs):

    """"MF_NORTH_NET_IN_20D = 北向20日净流入"""

    try:

        result = pd.Series(0, index=df.index).astype(float)

        if north_stats:

            for ts_code in df.index:

                if ts_code in north_stats:

                    result.loc[ts_code] = north_stats[ts_code].get('net_in_20d', 0)

        return result.values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_sector_flow_in(df, **kwargs):

    """"MF_SECTOR_FLOW_IN = 板块资金流入 [STUB: 需AKShare数据，暂返回0]"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_indu_flow_in(df, **kwargs):

    """"MF_INDU_FLOW_IN = 行业资金流入 [STUB: 需AKShare数据，暂返回0]"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_hk_hold_chg(df, **kwargs):

    """"MF_HK_HOLD_CHG = 港股通持仓变动 [STUB: 需AKShare数据，暂返回0]"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_etf_inflow(df, **kwargs):

    """"MF_ETF_INFLOW = ETF资金流入 [STUB: 需AKShare数据，暂返回0]"""

    try:

        return pd.Series(0, index=df.index).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_main_buy_r(df, **kwargs):

    """"MF_MAIN_BUY_R = 主力买入占比"""

    try:

        buy_lg = df['buy_lg_amount'] if 'buy_lg_amount' in df.columns else pd.Series(0, index=df.index)

        buy_elg = df['buy_elg_amount'] if 'buy_elg_amount' in df.columns else pd.Series(0, index=df.index)

        sell_lg = df['sell_lg_amount'] if 'sell_lg_amount' in df.columns else pd.Series(0, index=df.index)

        sell_elg = df['sell_elg_amount'] if 'sell_elg_amount' in df.columns else pd.Series(0, index=df.index)

        total_buy = buy_lg + buy_elg + sell_lg + sell_elg

        main_buy = buy_lg + buy_elg

        return np.where(total_buy > 0, main_buy / total_buy, np.nan)

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_retail_outflow(df, **kwargs):

    """"MF_RETAIL_OUTFLOW = 散户流出"""

    try:

        buy_sm = df['buy_sm_amount'] if 'buy_sm_amount' in df.columns else pd.Series(0, index=df.index)

        sell_sm = df['sell_sm_amount'] if 'sell_sm_amount' in df.columns else pd.Series(0, index=df.index)

        return (sell_sm - buy_sm).values

    except Exception:

        return pd.Series(np.nan, index=df.index)

def compute_mf_crowding(df, **kwargs):

    """"MF_CROWDING = 拥挤简
    """

    try:

        main_net = df['net_mf_amount'] if 'net_mf_amount' in df.columns else pd.Series(0, index=df.index)

        return np.where(main_net != 0, np.abs(main_net) / (np.abs(main_net) + 1), 0).astype(float)

    except Exception:

        return pd.Series(np.nan, index=df.index)

